"""Phase 6 end to end on the running stack, with real models:

Airflow task fails -> on_failure_callback -> POST /incidents -> Celery worker runs the
graph -> tier-2 proposal pauses -> approval appears in the API inbox -> (worker restart
changes nothing) -> human decision via the API -> worker resumes -> verified.

    uv run pytest tests/e2e/test_approval_flow.py -m e2e -v
"""

from __future__ import annotations

import subprocess
import time

import httpx
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

from faults import injector
from faults.scenario import load_scenario
from faults.verify import clean_chain
from scripts.airflow_client import AirflowClient

pytestmark = pytest.mark.e2e
API = "http://localhost:8000"
TERMINAL = {"resolved", "escalated", "failed"}


def wait_for(fn, timeout_s: float, every_s: float = 3, what: str = "condition"):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            value = fn()
        except httpx.HTTPError:  # the API can be slow while Airflow reruns a DAG
            value = None
        if value:
            return value
        time.sleep(every_s)
    raise TimeoutError(f"{what} not reached within {timeout_s:.0f}s")


def incident_for(run_id: str) -> dict | None:
    rows = httpx.get(f"{API}/incidents", params={"limit": 50}, timeout=10).json()
    return next((r for r in rows if r["run_id"] == run_id), None)


def pending_for(incident_id: str) -> dict | None:
    rows = httpx.get(f"{API}/approvals", timeout=10).json()
    return next((a for a in rows if a["incident_id"] == incident_id), None)


def status(incident_id: str) -> str:
    return httpx.get(f"{API}/incidents/{incident_id}", timeout=10).json()["status"]


def fail_pipeline(af: AirflowClient, scenario_id: str) -> tuple:
    sc = load_scenario(scenario_id)
    injector.revert(sc)
    assert clean_chain(af, sc.month)
    injector.apply(sc)
    run_id = af.trigger("ingest_taxi_trips", {"month": sc.month})
    af.wait_for_run("ingest_taxi_trips", run_id)  # fails for ingest faults, succeeds for dbt ones
    incident = wait_for(
        lambda: incident_for(run_id) or _transform_incident(af, run_id),
        240,
        what="incident from the failure callback",
    )
    return sc, incident


def _transform_incident(af: AirflowClient, ingest_run_id: str) -> dict | None:
    """For dbt-stage faults the incident comes from the transform run the ingest triggered."""
    run = af.latest_run("transform_dbt")
    if run and run["state"] == "failed":
        return incident_for(run["dag_run_id"])
    return None


def restart_worker() -> None:
    subprocess.run(["docker", "compose", "restart", "worker"], check=True, capture_output=True)
    wait_for(
        lambda: (
            subprocess.run(
                [
                    "docker",
                    "inspect",
                    "-f",
                    "{{.State.Health.Status}}",
                    "pipeline-incident-agent-worker-1",
                ],
                capture_output=True,
                text=True,
            ).stdout.strip()
            == "healthy"
        ),
        120,
        5,
        "worker healthy",
    )


@pytest.fixture(scope="module")
def af() -> AirflowClient:
    return AirflowClient()


def _approve_and_finish(card: dict, incident_id: str) -> str:
    r = httpx.post(
        f"{API}/approvals/{card['id']}/decision",
        json={"decision": "approve", "decided_by": "e2e"},
        timeout=10,
    )
    assert r.status_code == 202
    while True:  # a multi-step fix may ask for more approvals; approve the expected ones
        s = wait_for(
            lambda: (st := status(incident_id)) in TERMINAL | {"awaiting_approval"} and st,
            900,
            5,
            "incident to finish or ask again",
        )
        if s != "awaiting_approval":
            return s
        nxt = pending_for(incident_id)
        if nxt and nxt["id"] != card["id"]:
            card = nxt
            httpx.post(
                f"{API}/approvals/{card['id']}/decision",
                json={"decision": "approve", "decided_by": "e2e"},
                timeout=10,
            )
        time.sleep(3)


def test_pending_approval_survives_worker_restart(af):
    """F01: the mapping waits for approval; restarting the worker changes nothing."""
    sc, incident = fail_pipeline(af, "F01-v1")
    try:
        card = wait_for(lambda: pending_for(incident["id"]), 300, what="approval request")
        assert card["tool"] == "upsert_column_mapping"

        restart_worker()
        assert status(incident["id"]) == "awaiting_approval"
        assert pending_for(incident["id"])["id"] == card["id"]

        # an edit with an invalid argument is refused with a readable message
        bad = {**card["args"], "transform": "uppercase"}
        r = httpx.post(
            f"{API}/approvals/{card['id']}/decision",
            json={"decision": "edit", "args": bad},
            timeout=10,
        )
        assert r.status_code == 422 and "transform" in r.json()["detail"]

        # approving after the restart completes the incident
        assert _approve_and_finish(card, incident["id"]) == "resolved"
    finally:
        injector.revert(sc)
        clean_chain(af, sc.month)


def test_f04_preview_shows_the_true_row_count(af):
    """F04 pauses at the gate; the dry-run row count matches the landing file."""
    sc, incident = fail_pipeline(af, "F04-v1")
    try:
        card = wait_for(
            lambda: (
                pending_for(incident["id"])
                or (status(incident["id"]) in TERMINAL and {"terminal": status(incident["id"])})
            ),
            300,
            what="approval request",
        )
        assert "terminal" not in card, f"no proposal: incident ended {card}"
        assert card["tool"] == "quarantine_rows"
        assert card["args"]["predicate"]["op"] == "is_null"

        column = card["args"]["predicate"]["column"]
        table = pq.read_table(injector._landing_trips(sc.month))
        source = next(n for n in table.column_names if n.lower() == column)
        expected = pc.sum(pc.is_null(table[source])).as_py()
        assert card["preview"]["data"]["landing_rows_diverted_at_next_ingest"] == expected

        assert _approve_and_finish(card, incident["id"]) == "resolved"
    finally:
        injector.revert(sc)
        clean_chain(af, sc.month)


def test_rejected_proposals_escalate(af):
    sc, incident = fail_pipeline(af, "F06-v1")
    rejections = 0
    try:
        deadline = time.time() + 900
        while time.time() < deadline:
            s = status(incident["id"])
            if s in TERMINAL:
                break
            card = pending_for(incident["id"])
            if card:
                r = httpx.post(
                    f"{API}/approvals/{card['id']}/decision",
                    json={"decision": "reject", "comment": "not this one", "decided_by": "e2e"},
                    timeout=10,
                )
                assert r.status_code == 202
                rejections += 1
            time.sleep(3)
        detail = httpx.get(f"{API}/incidents/{incident['id']}", timeout=10).json()
        assert detail["status"] == "escalated"
        assert 1 <= rejections <= 2
        assert detail["escalation"] is not None
    finally:
        injector.revert(sc)
        clean_chain(af, sc.month)
