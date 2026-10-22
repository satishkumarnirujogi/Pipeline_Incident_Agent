"""Phase 10 endpoints against a real Postgres: timeline (with real checkpoints), metrics,
and the dev-only demo controls."""

from __future__ import annotations

import json
from datetime import UTC, datetime

import psycopg
import pytest
from fastapi.testclient import TestClient

from agent import llm as llm_module
from agent.agents import common
from agent.graph import compile_graph, postgres_checkpointer
from agent.settings import get_settings
from agent.tools.registry import ToolResult
from agent.tools.store import InMemoryToolStore
from tests.fakes import POSTMORTEM, RERUN, FakeLLM, diagnosis, done, propose
from tests.integration.test_api_worker import QUARANTINE, post_incident

pytestmark = pytest.mark.usefixtures("platform")


@pytest.fixture
def client(monkeypatch):
    """API whose enqueue runs the worker inline, on a graph with Postgres checkpoints."""
    from api import main as api_main
    from worker import runner

    def run_tool(name, args, ctx):
        if name == "rerun_dag":
            return ToolResult(ok=True, data={"dag_id": args["dag_id"], "run_id": "r1"})
        if name == "wait_for_dag_run":
            return ToolResult(ok=True, data={"chain_state": "success"})
        return ToolResult(ok=True, data={})

    monkeypatch.setattr(common, "run_tool", run_tool)
    monkeypatch.setattr(
        common, "run_dry", lambda n, a, c: ToolResult(ok=True, data={"effect": "preview"})
    )
    monkeypatch.setattr(common, "store_factory", InMemoryToolStore)
    monkeypatch.setattr(api_main, "enqueue", lambda task, *args: getattr(runner, task)(*args))
    with postgres_checkpointer() as saver:
        runner.set_graph(compile_graph(saver))
        yield TestClient(api_main.app)
    runner.set_graph(None)
    llm_module.set_llm(None)


# ------------------------------------------------------------------ timeline


def test_timeline_merges_database_events_with_the_checkpoint(client):
    llm_module.set_llm(
        FakeLLM(
            {
                "diagnostics": [done()],
                "diagnostics_finalize": [diagnosis("NULL_SPIKE")],
                "remediation_finalize": [
                    propose("quarantine_rows", QUARANTINE),
                    propose("rerun_dag", RERUN),
                ],
                "reporter": [POSTMORTEM],
            }
        )
    )
    incident_id = post_incident(client, "run-timeline")
    approval = client.get("/approvals").json()[0]
    client.post(f"/approvals/{approval['id']}/decision", json={"decision": "approve"})

    t = client.get(f"/incidents/{incident_id}/timeline").json()
    assert t["status"] == "resolved"
    kinds = [e["kind"] for e in t["events"]]
    assert kinds[0] == "incident"
    assert "incident resolved" in [e["what"] for e in t["events"]]
    assert {"run", "llm", "approval"} <= set(kinds)  # tools are faked: no tool/postmortem rows
    assert [e["at"] for e in t["events"]] == sorted(e["at"] for e in t["events"])
    decided = [e for e in t["events"] if e["what"] == "quarantine_rows: approve"]
    assert decided and decided[0]["ok"] is True
    # from the checkpoint: the Supervisor's decisions and the actions with verification
    assert t["routing"][0]["rule"] == 2 and t["routing"][-1]["next"] == "reporter"
    assert [a["tool"] for a in t["actions"]] == ["quarantine_rows", "rerun_dag"]
    assert t["actions"][-1]["verification"]["chain_state"] == "success"
    assert t["diagnosis"]["root_cause"] == "NULL_SPIKE"

    listed = [i for i in client.get("/incidents").json() if i["id"] == incident_id][0]
    assert listed["cost_eur"] > 0  # the fake model records a cost per call


def test_timeline_of_an_unknown_incident_is_404(client):
    assert client.get("/incidents/00000000-0000-0000-0000-000000000000/timeline").status_code == 404


# ------------------------------------------------------------------- metrics


def _run(conn, run_id, split, config, successes, n=10):
    conn.execute(
        "INSERT INTO eval.runs (id, split, config, finished_at) VALUES (%s,%s,%s,now())",
        (run_id, split, json.dumps(config)),
    )
    for i in range(n):
        ok = i < successes
        conn.execute(
            "INSERT INTO eval.results (eval_run_id, scenario_id, fault, split, month, "
            "expected_resolution, expected_root_cause, status, root_cause, resolution_ok, "
            "verified, verification, root_cause_correct, forbidden_count, forbidden_detail, "
            "success, mttr_s, steps, cost_eur, started_at) VALUES (%s,%s,'null_spike',%s,"
            "'2026-06','fixed','NULL_SPIKE',%s,'NULL_SPIKE',%s,%s,'{}',true,0,'[]',%s,60,10,"
            "0.005,%s)",
            (
                run_id,
                f"K{i:02d}-v1",
                split,
                "resolved" if ok else "escalated",
                ok,
                ok,
                ok,
                datetime.now(UTC),
            ),
        )


@pytest.fixture(scope="module")
def kpi_runs(platform):
    with psycopg.connect(platform["dsn"]("agent_app", "agent"), autocommit=True) as conn:
        _run(conn, "kpi-full", "test", {"ablation": "full", "chaos": False}, successes=9)
        _run(
            conn,
            "kpi-chaos",
            "test",
            {"ablation": "full", "chaos": True, "baseline": "kpi-full"},
            successes=8,
        )
        _run(conn, "kpi-memoryless", "test", {"ablation": "no_memory", "chaos": False}, 10)
        _run(conn, "kpi-recheck", "test", {"ablation": "full", "chaos": False}, 2, n=2)
    yield
    with psycopg.connect(platform["dsn"]("agent_app", "agent"), autocommit=True) as conn:
        conn.execute("DELETE FROM eval.results WHERE eval_run_id LIKE 'kpi-%'")
        conn.execute("DELETE FROM eval.runs WHERE id LIKE 'kpi-%'")


def test_latest_is_the_newest_full_run_without_chaos(client, kpi_runs):
    m = client.get("/metrics/latest").json()
    assert m["eval_run_id"] == "kpi-full"  # not the ablation, the chaos run or a re-check
    assert float(m["headline"]["task_completion_rate"]) == 0.9
    assert m["per_fault"][0]["fault"] == "null_spike"
    chaos = client.get("/metrics/latest", params={"eval_run_id": "kpi-chaos"}).json()
    assert float(chaos["headline"]["recovery_rate"]) == round(8 / 9, 3)
    assert client.get("/metrics/latest", params={"eval_run_id": "nope"}).status_code == 404


def test_trend_and_ablations(client, kpi_runs):
    trend = [r["eval_run_id"] for r in client.get("/metrics/runs").json()]
    assert {"kpi-full", "kpi-chaos", "kpi-memoryless"} <= set(trend)
    assert "kpi-recheck" not in trend
    table = client.get("/metrics/ablations", params={"split": "test"}).json()
    assert table["baseline"] == "kpi-full"
    configs = {r["config"]: r for r in table["rows"]}
    assert set(configs) == {"C. No memory", "D. Full system", "D. Full system, chaos on"}
    assert float(configs["D. Full system, chaos on"]["recovery_rate"]) == round(8 / 9, 3)


# ---------------------------------------------------------------------- demo


@pytest.fixture
def env(monkeypatch):
    def set_(value: str) -> None:
        monkeypatch.setenv("ENV", value)
        get_settings.cache_clear()

    return set_


def test_demo_is_off_outside_dev(client, env):
    env("prod")
    assert client.get("/demo/scenarios").status_code == 404
    assert client.post("/demo/inject/F04-v1").status_code == 404


def test_demo_inject_applies_the_fault_and_triggers_the_dag(client, env, monkeypatch):
    from faults import injector
    from scripts import airflow_client

    env("dev")
    calls: list[tuple] = []
    monkeypatch.setattr(injector, "revert", lambda sc, *a: calls.append(("revert", sc.id)) or {})
    monkeypatch.setattr(injector, "apply", lambda sc, *a: calls.append(("apply", sc.id)) or {})

    class FakeAirflow:
        def trigger(self, dag_id, conf):
            calls.append(("trigger", dag_id, conf["month"]))
            return "manual__demo"

    monkeypatch.setattr(airflow_client, "AirflowClient", FakeAirflow)
    assert any(s["id"] == "F04-v1" for s in client.get("/demo/scenarios").json())
    r = client.post("/demo/inject/F04-v1")
    assert r.status_code == 202 and r.json()["state"] == "running"
    # TestClient runs the background task before returning
    assert calls == [
        ("revert", "F04-v1"),
        ("apply", "F04-v1"),
        ("trigger", "ingest_taxi_trips", "2026-06"),
    ]
    status = client.get("/demo/status").json()
    assert status["state"] == "done" and "manual__demo" in status["message"]
    assert client.post("/demo/inject/F99-v9").status_code == 404
