"""Warehouse- and agent-DB tools against a real Postgres with the real roles."""

from __future__ import annotations

import uuid

import psycopg
import pytest

import agent.tools  # noqa: F401
from agent.db import agent_engine
from agent.models import Approval
from agent.tools.registry import ToolContext, args_hash, call_tool, dry_run, validate_args
from tests.integration.conftest import MONTH, ROWS

pytestmark = pytest.mark.usefixtures("platform")


def ctx(agent: str = "diagnostics", incident: str = "inc-db", **kw) -> ToolContext:
    from agent.tools.store import PostgresToolStore

    return ToolContext(incident_id=incident, agent=agent, store=PostgresToolStore(), **kw)


def grant(incident: str, tool: str, args: dict, decision: str = "approve") -> str:
    """Record a human decision the way the API will, so tier-2 calls can run."""
    from sqlalchemy.orm import Session

    parsed = validate_args(tool, args)
    approval = Approval(
        incident_id=incident,
        tool=tool,
        args=parsed.model_dump(mode="json"),
        args_hash=args_hash(parsed),
        decision=decision,
        decided_by="test",
    )
    with Session(agent_engine()) as s:
        s.add(approval)
        s.commit()
        return str(approval.id)


def raw_count(platform, where: str = "true") -> int:
    with psycopg.connect(platform["dsn"]("postgres", "warehouse")) as c:
        return c.execute(f"SELECT count(*) FROM raw.yellow_trips WHERE {where}").fetchone()[0]


# ---------------------------------------------------------------- tier 0


def test_describe_landing_file(platform):
    r = call_tool("describe_landing_file", {"month": MONTH}, ctx())
    assert r.ok, r.message
    assert r.data["rows"] == ROWS
    nulls = {c["name"]: c["nulls"] for c in r.data["columns"]}
    assert nulls["passenger_count"] == ROWS // 4
    assert len(r.data["sample_rows"]) == 5


def test_describe_missing_landing_file(platform):
    r = call_tool("describe_landing_file", {"month": "2026-09"}, ctx())
    assert r.error_type == "not_found"


def test_describe_empty_landing_file(platform, data_dir):
    (data_dir / "landing" / "trips" / "yellow_tripdata_2026-10.parquet").write_bytes(b"")
    r = call_tool("describe_landing_file", {"month": "2026-10"}, ctx())
    assert r.ok and r.data["readable"] is False and r.data["size_bytes"] == 0


def test_compare_schema_trips_matches(platform):
    r = call_tool("compare_schema_to_contract", {"dataset": "yellow_trips", "month": MONTH}, ctx())
    assert r.ok, r.message
    assert r.data["matches_contract"] and r.data["missing"] == []


def test_compare_schema_trips_missing_month(platform):
    r = call_tool(
        "compare_schema_to_contract", {"dataset": "yellow_trips", "month": "2026-09"}, ctx()
    )
    assert r.error_type == "not_found"


def test_get_dbt_failures(platform):
    r = call_tool("get_dbt_failures", {"month": MONTH}, ctx())
    assert r.ok, r.message
    assert r.data["status_counts"] == {"pass": 1, "fail": 1, "warn": 1}
    fails = [p for p in r.data["problems"] if p["status"] == "fail"]
    assert fails[0]["failures"] == 50 and "relationships" in fails[0]["node"]


def test_get_dbt_failures_no_run(platform):
    assert call_tool("get_dbt_failures", {"month": "2026-09"}, ctx()).error_type == "not_found"


def test_profile_column_with_baseline(platform):
    r = call_tool(
        "profile_column",
        {"table": "raw.yellow_trips", "column": "fare_amount", "month": MONTH},
        ctx(),
    )
    assert r.ok, r.message
    assert r.data["rows"] == ROWS and r.data["negative_share"] == 0.1
    assert "negative_share" in r.data["baseline"]


def test_profile_column_unknown_column(platform):
    r = call_tool(
        "profile_column", {"table": "raw.yellow_trips", "column": "secret", "month": MONTH}, ctx()
    )
    assert r.error_type == "validation"


def test_profile_column_empty_month(platform):
    r = call_tool(
        "profile_column",
        {"table": "raw.yellow_trips", "column": "fare_amount", "month": "2026-09"},
        ctx(),
    )
    assert r.error_type == "not_found"


def test_run_readonly_sql(platform):
    r = call_tool(
        "run_readonly_sql",
        {
            "query": "SELECT pulocationid, count(*) AS n FROM "
            "raw.yellow_trips WHERE pulocationid = 999 GROUP BY 1"
        },
        ctx(),
    )
    assert r.ok, r.message
    assert r.data["rows"] == [[999, ROWS // 20]]


def test_run_readonly_sql_limits_rows(platform):
    r = call_tool("run_readonly_sql", {"query": "SELECT * FROM raw.yellow_trips"}, ctx())
    assert r.ok and len(r.data["rows"]) == 200 and r.data["truncated"]


def test_run_readonly_sql_bad_table(platform):
    r = call_tool("run_readonly_sql", {"query": "SELECT * FROM raw.nope"}, ctx())
    assert r.error_type == "not_found"


def test_incident_memory_round_trip(platform):
    reporter = ctx("reporter", incident="inc-old")
    saved = call_tool(
        "save_incident_memory",
        {
            "summary": "validate_contract failed: missing column tpep_pickup_datetime after "
            "upstream rename to pickup_datetime",
            "root_cause": "SCHEMA_DRIFT",
            "fix": "column mapping, rerun ingest",
        },
        reporter,
    )
    assert saved.ok, saved.message
    r = call_tool(
        "search_incident_memory",
        {"text": "missing column tpep_pickup_datetime", "k": 3},
        ctx(incident="inc-new"),
    )
    assert r.ok and r.data["matches"][0]["root_cause"] == "SCHEMA_DRIFT"
    own = call_tool(
        "search_incident_memory",
        {"text": "missing column tpep_pickup_datetime"},
        ctx(incident="inc-old"),
    )
    assert own.data["matches"] == []  # an incident never finds itself


def test_write_postmortem_upserts(platform):
    c = ctx("reporter", incident="inc-pm")
    assert call_tool("write_postmortem", {"markdown": "# Postmortem\n\nfirst version"}, c).ok
    assert call_tool("write_postmortem", {"markdown": "# Postmortem\n\nsecond version"}, c).ok
    with psycopg.connect(platform["dsn"]("agent_app", "agent")) as conn:
        rows = conn.execute(
            "SELECT markdown FROM postmortems WHERE incident_id = 'inc-pm'"
        ).fetchall()
    assert rows == [("# Postmortem\n\nsecond version",)]


def test_every_call_is_logged(platform):
    call_tool("get_dbt_failures", {"month": MONTH}, ctx(incident="inc-log"))
    call_tool("drop_table", {"table": "raw.yellow_trips"}, ctx(incident="inc-log"))
    with psycopg.connect(platform["dsn"]("agent_app", "agent")) as conn:
        rows = conn.execute(
            "SELECT tool, ok, safety_violation FROM tool_calls "
            "WHERE incident_id = 'inc-log' ORDER BY id"
        ).fetchall()
    assert rows == [("get_dbt_failures", True, False), ("drop_table", False, True)]


# --------------------------------------------------------- tier 2: mapping


MAPPING = {
    "dataset": "yellow_trips",
    "source_col": "pickup_datetime",
    "target_col": "tpep_pickup_datetime",
    "transform": "none",
}


def test_mapping_needs_approval(platform):
    r = call_tool("upsert_column_mapping", MAPPING, ctx("remediation", incident="inc-map0"))
    assert r.error_type == "approval_required"


def test_mapping_rejects_non_contract_target(platform):
    args = {**MAPPING, "target_col": "made_up"}
    approval = grant("inc-map1", "upsert_column_mapping", args)
    r = call_tool(
        "upsert_column_mapping", args, ctx("remediation", incident="inc-map1", approval_id=approval)
    )
    assert r.error_type == "validation" and "contract" in r.message, r.message


def test_mapping_dry_run_shows_the_fix(platform, data_dir):
    import pyarrow.parquet as pq

    from tests.integration.conftest import _trips_table

    drifted = _trips_table().rename_columns(
        ["VendorID", "pickup_datetime", *_trips_table().column_names[2:]]
    )
    pq.write_table(drifted, data_dir / "landing" / "trips" / "yellow_tripdata_2026-11.parquet")
    preview = dry_run("upsert_column_mapping", MAPPING, ctx("remediation"))
    assert preview.ok, preview.message
    assert preview.data["before"]["missing"] == ["tpep_pickup_datetime"]
    assert preview.data["after"]["matches_contract"]
    (data_dir / "landing" / "trips" / "yellow_tripdata_2026-11.parquet").unlink()


def test_mapping_with_approval_runs_once(platform):
    approval = grant("inc-map2", "upsert_column_mapping", MAPPING)
    c = ctx("remediation", incident="inc-map2", approval_id=approval)
    first = call_tool("upsert_column_mapping", MAPPING, c)
    second = call_tool("upsert_column_mapping", MAPPING, c)
    assert first.ok, first.message
    assert second.replayed
    with psycopg.connect(platform["dsn"]("postgres", "warehouse")) as conn:
        rows = conn.execute(
            "SELECT target_col, created_by FROM ops.column_mapping "
            "WHERE source_col = 'pickup_datetime'"
        ).fetchall()
        conn.execute("DELETE FROM ops.column_mapping")
    assert rows == [("tpep_pickup_datetime", "agent:inc-map2")]


# ------------------------------------------------------ tier 2: quarantine


def test_quarantine_dry_run_counts_raw_and_landing(platform):
    args = {
        "table": "raw.yellow_trips",
        "month": MONTH,
        "predicate": {"column": "pulocationid", "op": "not_in_dim"},
    }
    r = dry_run("quarantine_rows", args, ctx("remediation"))
    assert r.ok, r.message
    assert r.data["rows_moved_from_raw_now"] == ROWS // 20
    assert r.data["landing_rows_diverted_at_next_ingest"] == ROWS // 20


def test_quarantine_and_restore(platform):
    args = {
        "table": "raw.yellow_trips",
        "month": MONTH,
        "predicate": {"column": "fare_amount", "op": "lt", "value": 0},
    }
    before = raw_count(platform)
    approval = grant("inc-q", "quarantine_rows", args)
    r = call_tool(
        "quarantine_rows", args, ctx("remediation", incident="inc-q", approval_id=approval)
    )
    assert r.ok, r.message
    assert r.data["rows_moved"] == ROWS // 10
    assert raw_count(platform) == before - ROWS // 10
    assert raw_count(platform, "fare_amount < 0") == 0

    batch = {"batch_id": r.data["batch_id"]}
    approval2 = grant("inc-q", "restore_quarantined", batch)
    back = call_tool(
        "restore_quarantined",
        batch,
        ctx("remediation", incident="inc-q", approval_id=approval2, step=1),
    )
    assert back.ok, back.message
    assert back.data["rows_restored"] == ROWS // 10
    assert raw_count(platform) == before
    with psycopg.connect(platform["dsn"]("postgres", "warehouse")) as conn:
        active = conn.execute(
            "SELECT active FROM ops.quarantine_rules WHERE batch_id = %s", (r.data["batch_id"],)
        ).fetchone()[0]
    assert active is False


def test_quarantine_unknown_column(platform):
    args = {
        "table": "raw.yellow_trips",
        "month": MONTH,
        "predicate": {"column": "nope", "op": "is_null"},
    }
    approval = grant("inc-q2", "quarantine_rows", args)
    r = call_tool(
        "quarantine_rows", args, ctx("remediation", incident="inc-q2", approval_id=approval)
    )
    assert r.error_type == "validation"


def test_restore_unknown_batch(platform):
    batch = {"batch_id": str(uuid.uuid4())}
    approval = grant("inc-q3", "restore_quarantined", batch)
    r = call_tool(
        "restore_quarantined", batch, ctx("remediation", incident="inc-q3", approval_id=approval)
    )
    assert r.error_type == "not_found"


def test_escalation_is_stored(platform):
    r = call_tool(
        "escalate_to_human",
        {
            "summary": "credentials rejected by the warehouse",
            "reason": "secrets are out of scope",
            "suggested_owner": "security",
        },
        ctx("diagnostics", incident="inc-esc"),
    )
    assert r.ok
    with psycopg.connect(platform["dsn"]("agent_app", "agent")) as conn:
        owner = conn.execute(
            "SELECT suggested_owner FROM escalations WHERE incident_id = 'inc-esc'"
        ).fetchone()[0]
    assert owner == "security"
