"""Every registered tool rejects invalid input at the boundary, before doing anything."""

import pytest

import agent.tools  # noqa: F401
from agent.tools.registry import REGISTRY, ToolContext, call_tool
from agent.tools.store import InMemoryToolStore

INVALID = {
    "get_task_log": {
        "dag_id": "ingest_taxi_trips",
        "run_id": "r",
        "task_id": "x; rm",
        "try_number": 1,
    },
    "get_dag_run_history": {"dag_id": "ingest_taxi_trips", "limit": 500},
    "describe_landing_file": {"month": "2026-13"},
    "compare_schema_to_contract": {"dataset": "users", "month": "2026-06"},
    "get_dbt_failures": {"month": "June"},
    "profile_column": {
        "table": "pg_catalog.pg_authid",
        "column": "rolpassword",
        "month": "2026-06",
    },
    "run_readonly_sql": {"query": ""},
    "check_api_health": {"month": "06-2026"},
    "search_incident_memory": {"text": "x", "k": 50},
    "wait_for_dag_run": {"dag_id": "ingest_weather", "run_id": "r", "timeout_s": 0},
    "rerun_dag": {"dag_id": "ingest_weather", "conf": {"month": "2026-06", "x": 1}},
    "upsert_column_mapping": {
        "dataset": "yellow_trips",
        "source_col": "fare_amount",
        "target_col": "fare_amount",
        "transform": "none",
    },
    "quarantine_rows": {
        "table": "raw.yellow_trips",
        "month": "2026-06",
        "predicate": {"column": "fare_amount", "op": "lt"},
    },
    "restore_quarantined": {"batch_id": "not-a-uuid"},
    "escalate_to_human": {"summary": "short", "reason": "r", "suggested_owner": "x"},
    "write_postmortem": {"markdown": "too short"},
    "save_incident_memory": {
        "summary": "a summary of it",
        "root_cause": "GREMLINS",
        "fix": "rerun",
    },
}


def test_every_tool_has_an_invalid_input_case():
    assert set(INVALID) == set(REGISTRY) - {n for n in REGISTRY if n.startswith("t_")}


@pytest.mark.parametrize("tool", sorted(INVALID))
def test_invalid_input_is_a_validation_error(tool):
    spec = REGISTRY[tool]
    agent = sorted(spec.agents)[0]
    ctx = ToolContext(incident_id="inc", agent=agent, store=InMemoryToolStore())
    r = call_tool(tool, INVALID[tool], ctx)
    assert not r.ok
    assert r.error_type == "validation", (tool, r)
    assert ctx.store.actions == {}  # nothing was started
