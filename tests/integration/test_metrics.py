"""Phase 8: every worker job is an agent_runs row whose totals equal its llm_calls rows."""

from __future__ import annotations

import uuid

import psycopg
import pytest
from fastapi.testclient import TestClient
from langgraph.checkpoint.memory import InMemorySaver

from agent import llm as llm_module
from agent.agents import common
from agent.graph import compile_graph
from agent.tools.registry import ToolResult
from agent.tools.store import InMemoryToolStore
from tests.fakes import POSTMORTEM, RERUN, FakeLLM, diagnosis, done, propose

pytestmark = pytest.mark.usefixtures("platform")

QUARANTINE = {
    "table": "raw.yellow_trips",
    "month": "2026-06",
    "predicate": {"column": "fare_amount", "op": "lt", "value": 0},
}


@pytest.fixture
def client(monkeypatch):
    from api import main as api_main
    from worker import runner

    monkeypatch.setattr(
        common,
        "run_tool",
        lambda n, a, c: ToolResult(
            ok=True, data={"dag_id": "transform_dbt", "run_id": "r1", "chain_state": "success"}
        ),
    )
    monkeypatch.setattr(common, "run_dry", lambda n, a, c: ToolResult(ok=True, data={}))
    monkeypatch.setattr(common, "store_factory", InMemoryToolStore)
    runner.set_graph(compile_graph(InMemorySaver()))
    monkeypatch.setattr(api_main, "enqueue", lambda task, *args: getattr(runner, task)(*args))
    yield TestClient(api_main.app)
    runner.set_graph(None)
    llm_module.set_llm(None)


def rows(platform, sql: str, incident_id: str) -> list[tuple]:
    with psycopg.connect(platform["dsn"]("agent_app", "agent")) as conn:
        return conn.execute(sql, (incident_id,)).fetchall()


def test_runs_and_llm_calls_add_up_across_run_and_resume(platform, client):
    llm_module.set_llm(
        FakeLLM(
            {
                "diagnostics": [done()],
                "diagnostics_finalize": [diagnosis("OUT_OF_RANGE_VALUES")],
                "remediation_finalize": [
                    propose("quarantine_rows", QUARANTINE),
                    propose("rerun_dag", RERUN),
                ],
                "reporter": [POSTMORTEM],
            }
        )
    )
    r = client.post(
        "/incidents",
        json={
            "dag_id": "transform_dbt",
            "run_id": f"m-{uuid.uuid4()}",
            "task_id": "dbt_build",
            "try_number": 1,
            "conf": {"month": "2026-06"},
        },
    )
    incident_id = r.json()["id"]
    card = client.get("/approvals").json()[0]
    client.post(f"/approvals/{card['id']}/decision", json={"decision": "approve"})
    assert client.get(f"/incidents/{incident_id}").json()["status"] == "resolved"

    runs = rows(
        platform,
        "SELECT id, kind, outcome, steps, tokens_in, cost_eur FROM agent_runs "
        "WHERE incident_id = %s ORDER BY id",
        incident_id,
    )
    assert [(k, o) for _, k, o, _, _, _ in runs] == [
        ("run", "awaiting_approval"),
        ("resume", "resolved"),
    ]
    assert all(steps > 0 for _, _, _, steps, _, _ in runs)
    for run_id, *_, tokens_in, cost in runs:
        calls = rows(
            platform,
            "SELECT count(*), sum(tokens_in), sum(cost_eur) FROM llm_calls "
            "WHERE incident_id = %s AND agent_run_id = " + str(run_id),
            incident_id,
        )[0]
        assert calls[0] > 0
        assert (tokens_in, cost) == (calls[1], calls[2])  # run totals == sum of its calls
    total = rows(
        platform,
        "SELECT sum(cost_eur), count(*) FROM llm_calls WHERE incident_id = %s",
        incident_id,
    )[0]
    assert sum(r[5] for r in runs) == total[0]
    assert total[1] == 5  # diagnostics, finalize, 2 remediation decisions, reporter
