"""API + worker against a real Postgres: incident -> pending approval -> decision -> resume.

The LLM is scripted and tools are faked, so this tests the plumbing: storage, dedupe,
the inbox, validation of human decisions, pause/resume through checkpoints, timeouts.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from fastapi.testclient import TestClient
from langgraph.checkpoint.memory import InMemorySaver
from sqlalchemy import update

from agent import llm as llm_module
from agent.agents import common
from agent.graph import compile_graph
from agent.models import Approval
from agent.state import RouteDecision
from agent.tools.registry import ToolResult
from agent.tools.store import InMemoryToolStore
from tests.fakes import MAPPING, POSTMORTEM, RERUN, FakeLLM, diagnosis, done, propose

pytestmark = pytest.mark.usefixtures("platform")

QUARANTINE = {
    "table": "raw.yellow_trips",
    "month": "2026-06",
    "predicate": {"column": "passenger_count", "op": "is_null", "value": None},
}


@pytest.fixture
def system(monkeypatch):
    """API client whose enqueue runs the worker job inline, with a fresh graph and fakes."""
    from api import main as api_main
    from worker import runner

    executed: list[tuple[str, dict, str | None]] = []

    def run_tool(name, args, ctx):
        executed.append((name, args, ctx.approval_id))
        if name == "rerun_dag":
            return ToolResult(ok=True, data={"dag_id": args["dag_id"], "run_id": "r1"})
        if name == "wait_for_dag_run":
            return ToolResult(ok=True, data={"chain_state": "success"})
        return ToolResult(ok=True, data={})

    monkeypatch.setattr(common, "run_tool", run_tool)
    monkeypatch.setattr(
        common,
        "run_dry",
        lambda n, a, c: ToolResult(ok=True, data={"effect": "moves 41,212 rows to quarantine"}),
    )
    monkeypatch.setattr(common, "store_factory", InMemoryToolStore)
    runner.set_graph(compile_graph(InMemorySaver()))

    jobs: list[tuple[str, tuple]] = []

    def enqueue(task, *args):
        jobs.append((task, args))
        getattr(runner, task)(*args)

    monkeypatch.setattr(api_main, "enqueue", enqueue)
    yield {"client": TestClient(api_main.app), "executed": executed, "jobs": jobs}
    runner.set_graph(None)
    llm_module.set_llm(None)


def post_incident(client, run_id: str) -> str:
    r = client.post(
        "/incidents",
        json={
            "dag_id": "ingest_taxi_trips",
            "run_id": run_id,
            "task_id": "validate_contract",
            "try_number": 2,
            "conf": {"month": "2026-06"},
            "exception": "ContractViolation: column 'passenger_count' null rate 0.56 exceeds max",
        },
    )
    assert r.status_code in (200, 202), r.text
    return r.json()["id"]


def test_pause_edit_refused_approve_resume(system):
    llm_module.set_llm(
        FakeLLM(
            {
                "diagnostics": [done()],
                "diagnostics_finalize": [diagnosis("NULL_SPIKE")],
                "remediation": [done(), done()],
                "remediation_finalize": [
                    propose("quarantine_rows", QUARANTINE),
                    propose("rerun_dag", RERUN),
                ],
                "reporter": [POSTMORTEM],
            }
        )
    )
    client = system["client"]
    incident_id = post_incident(client, "run-pause")

    # the graph paused: incident awaiting approval, one pending card with the preview
    assert client.get(f"/incidents/{incident_id}").json()["status"] == "awaiting_approval"
    pending = client.get("/approvals").json()
    assert len(pending) == 1
    card = pending[0]
    assert card["tool"] == "quarantine_rows"
    assert card["preview"]["data"]["effect"] == "moves 41,212 rows to quarantine"
    assert card["diagnosis"]["root_cause"] == "NULL_SPIKE"
    assert not [n for n, _, _ in system["executed"] if n == "quarantine_rows"]

    # an edit with an invalid argument is refused with a readable message
    bad = {**QUARANTINE, "predicate": {"column": "passenger_count", "op": "delete"}}
    r = client.post(f"/approvals/{card['id']}/decision", json={"decision": "edit", "args": bad})
    assert r.status_code == 422
    assert "invalid arguments for quarantine_rows" in r.json()["detail"]
    assert "predicate.op" in r.json()["detail"]
    assert client.get("/approvals").json()[0]["id"] == card["id"]  # still pending

    # approve -> resume -> quarantine with the approval id -> automatic rerun -> resolved
    r = client.post(f"/approvals/{card['id']}/decision", json={"decision": "approve"})
    assert r.status_code == 202
    detail = client.get(f"/incidents/{incident_id}").json()
    assert detail["status"] == "resolved"
    assert detail["root_cause"] == "NULL_SPIKE"
    assert [
        (n, ap) for n, _, ap in system["executed"] if n in ("quarantine_rows", "rerun_dag")
    ] == [("quarantine_rows", card["id"]), ("rerun_dag", None)]
    assert client.get("/approvals").json() == []
    assert (
        client.post(f"/approvals/{card['id']}/decision", json={"decision": "approve"}).status_code
        == 409
    )


def test_edited_args_are_what_runs(system):
    llm_module.set_llm(
        FakeLLM(
            {
                "diagnostics": [done()],
                "diagnostics_finalize": [diagnosis()],
                "remediation": [done(), done()],
                "remediation_finalize": [
                    propose("upsert_column_mapping", {**MAPPING, "source_col": "pickup_dt"}),
                    propose("rerun_dag", RERUN),
                ],
                "reporter": [POSTMORTEM],
            }
        )
    )
    client = system["client"]
    incident_id = post_incident(client, "run-edit")
    card = client.get("/approvals").json()[0]
    r = client.post(
        f"/approvals/{card['id']}/decision",
        json={"decision": "edit", "args": MAPPING, "comment": "source is pickup_datetime"},
    )
    assert r.status_code == 202
    ran = [a for n, a, _ in system["executed"] if n == "upsert_column_mapping"]
    assert ran == [MAPPING]
    assert client.get(f"/incidents/{incident_id}").json()["status"] == "resolved"


def test_two_rejections_escalate(system):
    llm_module.set_llm(
        FakeLLM(
            {
                "diagnostics": [done()],
                "diagnostics_finalize": [diagnosis("NULL_SPIKE")],
                "remediation": [done(), done()],
                "remediation_finalize": [
                    propose("quarantine_rows", QUARANTINE),
                    propose("quarantine_rows", QUARANTINE),
                ],
                "supervisor": [RouteDecision(next="remediation", reason="try once more")],
                "reporter": [POSTMORTEM],
            }
        )
    )
    client = system["client"]
    incident_id = post_incident(client, "run-reject")
    # a rejection needs a reason
    first = client.get("/approvals").json()[0]
    assert (
        client.post(f"/approvals/{first['id']}/decision", json={"decision": "reject"}).status_code
        == 422
    )
    for _ in range(2):
        card = client.get("/approvals").json()[0]
        r = client.post(
            f"/approvals/{card['id']}/decision",
            json={"decision": "reject", "comment": "do not drop these rows"},
        )
        assert r.status_code == 202
    detail = client.get(f"/incidents/{incident_id}").json()
    assert detail["status"] == "escalated"
    assert [a["decision"] for a in detail["approvals"]] == ["reject", "reject"]
    assert "quarantine_rows" not in [n for n, _, _ in system["executed"]]


def test_duplicate_incident_is_deduped(system):
    llm_module.set_llm(
        FakeLLM(
            {
                "diagnostics": [done()],
                "diagnostics_finalize": [diagnosis("MISSING_SOURCE", rec="escalate")],
                "reporter": [POSTMORTEM],
            }
        )
    )
    client = system["client"]
    first = post_incident(client, "run-dup")
    second = post_incident(client, "run-dup")
    assert first == second
    assert [t for t, _ in system["jobs"]].count("run_incident") == 1
    assert client.get(f"/incidents/{first}").json()["status"] == "escalated"


def test_stale_approval_times_out_and_escalates(system):
    from worker import runner

    llm_module.set_llm(
        FakeLLM(
            {
                "diagnostics": [done()],
                "diagnostics_finalize": [diagnosis("NULL_SPIKE")],
                "remediation": [done()],
                "remediation_finalize": [propose("quarantine_rows", QUARANTINE)],
                "reporter": [POSTMORTEM],
            }
        )
    )
    client = system["client"]
    incident_id = post_incident(client, "run-stale")
    card = client.get("/approvals").json()[0]
    from agent.db import agent_engine

    with agent_engine().begin() as conn:  # pretend it has waited 25 hours
        conn.execute(
            update(Approval)
            .where(Approval.incident_id == incident_id)
            .values(requested_at=datetime.now(UTC) - timedelta(hours=25))
        )
    expired = runner.expire_stale_approvals()
    assert (incident_id, card["id"]) in expired
    runner.resume_incident(incident_id, card["id"])
    detail = client.get(f"/incidents/{incident_id}").json()
    assert detail["status"] == "escalated"
    assert detail["approvals"][0]["decision"] == "timeout"


def test_duplicate_delivery_while_running_is_skipped(system):
    from worker import runner

    with runner.incident_lock("inc-locked") as first:
        assert first
        assert runner.run_incident("inc-locked") == "skipped"
