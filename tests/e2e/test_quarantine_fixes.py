"""End to end on the running stack: the remediation tools actually fix F04 and F06.

    uv run pytest tests/e2e -m e2e -v      (needs make up-pipeline + make bootstrap)

No LLM involved: this drives the same tool calls the Remediation agent will propose,
with a recorded human approval, and checks the pipeline goes green.
"""

from __future__ import annotations

import uuid

import psycopg
import pytest
from sqlalchemy.orm import Session

import agent.tools  # noqa: F401
from agent.db import agent_engine
from agent.models import Approval
from agent.settings import get_settings
from agent.tools.registry import ToolContext, args_hash, call_tool, dry_run, validate_args
from agent.tools.store import PostgresToolStore
from faults import injector
from faults.scenario import load_scenario
from faults.verify import clean_chain, run_dag
from scripts.airflow_client import AirflowClient

pytestmark = pytest.mark.e2e


@pytest.fixture(scope="module")
def af() -> AirflowClient:
    return AirflowClient()


def grant(incident: str, tool: str, args: dict) -> str:
    parsed = validate_args(tool, args)
    a = Approval(
        incident_id=incident,
        tool=tool,
        args=parsed.model_dump(mode="json"),
        args_hash=args_hash(parsed),
        decision="approve",
        decided_by="e2e-test",
    )
    with Session(agent_engine()) as s:
        s.add(a)
        s.commit()
        return str(a.id)


def quarantined(batch_id: str) -> int:
    with psycopg.connect(get_settings().dsn("pipeline", "warehouse")) as conn:
        return conn.execute(
            "SELECT count(*) FROM quarantine.yellow_trips WHERE batch_id = %s", (batch_id,)
        ).fetchone()[0]


def fix(incident: str, args: dict, rerun_dag: str, month: str) -> dict:
    """quarantine (approved) -> rerun (tier 1) -> wait for the whole chain."""
    ctx = ToolContext(incident_id=incident, agent="remediation", store=PostgresToolStore())
    preview = dry_run("quarantine_rows", args, ctx)
    assert preview.ok, preview.message

    ctx.approval_id = grant(incident, "quarantine_rows", args)
    q = call_tool("quarantine_rows", args, ctx)
    assert q.ok, q.message

    ctx.approval_id, ctx.step = None, 1
    rerun = call_tool("rerun_dag", {"dag_id": rerun_dag, "conf": {"month": month}}, ctx)
    assert rerun.ok, rerun.message
    waited = call_tool(
        "wait_for_dag_run",
        {"dag_id": rerun_dag, "run_id": rerun.data["run_id"], "timeout_s": 600},
        ctx,
    )
    assert waited.ok, waited.message
    return {"preview": preview.data, "quarantine": q.data, "wait": waited.data}


def test_f04_null_spike_fixed_by_ingest_quarantine(af):
    sc = load_scenario("F04-v1")
    injector.revert(sc)
    assert clean_chain(af, sc.month)
    injector.apply(sc)
    dag_id, run_id, state = run_dag(af, "ingest_taxi_trips", sc.month)
    assert (dag_id, state) == ("ingest_taxi_trips", "failed")

    out = fix(
        f"e2e-{uuid.uuid4()}",
        {
            "table": "raw.yellow_trips",
            "month": sc.month,
            "predicate": {"column": "passenger_count", "op": "is_null"},
        },
        "ingest_taxi_trips",
        sc.month,
    )
    try:
        assert out["preview"]["landing_rows_diverted_at_next_ingest"] > 300_000
        assert out["wait"]["chain_state"] == "success", out["wait"]
        assert (
            quarantined(out["quarantine"]["batch_id"])
            == out["preview"]["landing_rows_diverted_at_next_ingest"]
        )
    finally:
        injector.revert(sc)
    assert clean_chain(af, sc.month)


def test_f06_negative_fares_fixed_by_raw_quarantine(af):
    sc = load_scenario("F06-v1")
    injector.revert(sc)
    assert clean_chain(af, sc.month)
    injector.apply(sc)
    dag_id, run_id, state = run_dag(af, "ingest_taxi_trips", sc.month)
    assert (dag_id, state) == ("transform_dbt", "failed")

    out = fix(
        f"e2e-{uuid.uuid4()}",
        {
            "table": "raw.yellow_trips",
            "month": sc.month,
            "predicate": {"column": "fare_amount", "op": "lt", "value": 0},
        },
        "transform_dbt",
        sc.month,
    )
    try:
        assert out["quarantine"]["rows_moved"] > 30_000
        assert out["wait"]["chain_state"] == "success", out["wait"]
    finally:
        injector.revert(sc)
    assert clean_chain(af, sc.month)
