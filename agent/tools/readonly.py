"""Tier-0 tools: read-only diagnostics, health probes, waiting, and the reporter's writes."""

from __future__ import annotations

import time
from collections import Counter
from typing import Any

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import sqlglot
from pydantic import Field
from sqlalchemy import text
from sqlglot import exp

from agent import memory
from agent.db import agent_engine, warehouse_engine
from agent.models import Postmortem
from agent.settings import get_settings
from agent.state import RootCause
from agent.tools.common import (
    NUMERIC_TYPES,
    PROFILE_TABLES,
    Args,
    DagId,
    Dataset,
    Identifier,
    Month,
    ProfileTable,
    baseline,
    landing_trips_path,
    load_contract,
    query,
    require_column,
    type_ok,
)
from agent.tools.errors import InvalidInput, NotFound, PermissionDenied
from agent.tools.orchestrator import airflow
from agent.tools.registry import TIER_READONLY, ToolContext, tool_spec

DIAG = {"diagnostics"}

# ------------------------------------------------------------------ Airflow


class TaskLogIn(Args):
    dag_id: DagId
    run_id: str = Field(min_length=1, max_length=250)
    task_id: Identifier
    try_number: int = Field(ge=1, le=10)
    tail_lines: int = Field(default=100, ge=1, le=300)


@tool_spec(tier=TIER_READONLY, input_model=TaskLogIn, agents=DIAG, timeout_s=20)
def get_task_log(a: TaskLogIn, ctx: ToolContext) -> dict:
    """Tail of a task's log for one try, with exceptions and their last frames. Secrets are
    redacted."""
    lines = airflow().task_log(a.dag_id, a.run_id, a.task_id, a.try_number)
    return {"total_lines": len(lines), "lines": lines[-a.tail_lines :]}


class RunHistoryIn(Args):
    dag_id: DagId
    limit: int = Field(default=10, ge=1, le=20)


@tool_spec(tier=TIER_READONLY, input_model=RunHistoryIn, agents=DIAG, timeout_s=20)
def get_dag_run_history(a: RunHistoryIn, ctx: ToolContext) -> dict:
    """Recent runs of a DAG (newest first) with state, duration and month, to tell new breakage
    from a flaky pattern."""
    runs = airflow().dag_runs(a.dag_id, a.limit)
    return {
        "runs": [
            {k: r[k] for k in ("run_id", "state", "start_date", "duration_s")}
            | {"month": r["conf"].get("month")}
            for r in runs
        ],
        "states": dict(Counter(r["state"] for r in runs)),
    }


class WaitIn(Args):
    dag_id: DagId
    run_id: str = Field(min_length=1, max_length=250)
    timeout_s: int = Field(default=300, ge=10, le=600)
    follow_triggered: bool = True


@tool_spec(tier=TIER_READONLY, input_model=WaitIn, agents={"remediation"}, timeout_s=660)
def wait_for_dag_run(a: WaitIn, ctx: ToolContext) -> dict:
    """Wait until a run finishes. For a successful ingest_taxi_trips run, also wait for the
    transform_dbt run it triggered; chain_state is success only if the whole chain is."""
    af = airflow()
    started = time.monotonic()
    run = af.wait(a.dag_id, a.run_id, a.timeout_s)
    out: dict[str, Any] = {"run": run, "followed": None}
    states = [run["state"]]
    if a.follow_triggered and a.dag_id == "ingest_taxi_trips" and run["state"] == "success":
        remaining = max(10.0, a.timeout_s - (time.monotonic() - started))
        child = af.triggered_transform(after=run["run_after"] or "", timeout_s=60)
        if child is None:
            states.append("not_triggered")
        else:
            child = af.wait("transform_dbt", child["run_id"], remaining)
            out["followed"] = child
            states.append(child["state"])
    out["chain_state"] = (
        "success"
        if all(s == "success" for s in states)
        else "running"
        if any(s in ("running", "queued") for s in states)
        else "failed"
    )
    return out


# -------------------------------------------------------------- landing data


class LandingFileIn(Args):
    dataset: Dataset = "yellow_trips"
    month: Month


@tool_spec(tier=TIER_READONLY, input_model=LandingFileIn, agents=DIAG)
def describe_landing_file(a: LandingFileIn, ctx: ToolContext) -> dict:
    """Columns, types, per-column null counts, row count and 5 sample rows of the month's
    landing file, from Parquet metadata."""
    if a.dataset != "yellow_trips":
        raise InvalidInput("weather has no landing file; use compare_schema_to_contract")
    path = landing_trips_path(a.month)
    if not path.exists():
        raise NotFound(f"no landing file at {path.as_posix()}")
    size = path.stat().st_size
    if size == 0:
        return {"path": path.as_posix(), "size_bytes": 0, "readable": False}
    pf = pq.ParquetFile(path)
    md = pf.metadata
    nulls: Counter[str] = Counter()
    for rg in range(md.num_row_groups):
        for c in range(md.num_columns):
            col = md.row_group(rg).column(c)
            if col.statistics is not None and col.statistics.has_null_count:
                nulls[col.path_in_schema] += col.statistics.null_count
    sample = next(pf.iter_batches(batch_size=5)).to_pylist()
    return {
        "path": path.as_posix(),
        "size_bytes": size,
        "readable": True,
        "rows": md.num_rows,
        "columns": [
            {"name": f.name, "type": str(f.type), "nulls": nulls.get(f.name, 0)}
            for f in pf.schema_arrow
        ],
        "sample_rows": sample,
    }


class SchemaIn(Args):
    dataset: Dataset
    month: Month


def _mappings(dataset: str) -> list[dict]:
    return query(
        "agent_ro",
        "SELECT source_col, target_col, transform FROM ops.column_mapping "
        "WHERE dataset = :d ORDER BY id",
        {"d": dataset},
    )


_TRANSFORM_TYPES = {
    "strip_currency": pa.float64(),
    "to_float": pa.float64(),
    "to_timestamp": pa.timestamp("us"),
}


def trips_schema_diff(month: str, extra_mapping: dict | None = None) -> dict:
    """Landing schema with mappings applied vs. the contract, using the pipeline's type rules.
    Shared with the upsert_column_mapping dry run (extra_mapping = the proposed one)."""
    path = landing_trips_path(month)
    if not path.exists() or path.stat().st_size == 0:
        raise NotFound(f"no readable landing file at {path.as_posix()}")
    schema = pq.read_schema(path)
    mappings = _mappings("yellow_trips")
    if extra_mapping:
        mappings = [m for m in mappings if m["source_col"] != extra_mapping["source_col"]]
        mappings.append(extra_mapping)
    by_source = {m["source_col"]: m for m in mappings}

    effective: dict[str, pa.DataType] = {}
    for f in schema:
        m = by_source.get(f.name)
        name = m["target_col"] if m else f.name
        effective[name] = _TRANSFORM_TYPES.get(m["transform"], f.type) if m else f.type

    contract = load_contract("yellow_trips")["columns"]
    missing = [c for c in contract if c not in effective]
    mistyped = [
        {"column": c, "expected": spec["type"], "actual": str(effective[c])}
        for c, spec in contract.items()
        if c in effective and not type_ok(spec["type"], effective[c])
    ]
    return {
        "missing": missing,
        "mistyped": mistyped,
        "extra": sorted(set(effective) - set(contract)),
        "mappings_applied": mappings,
        "matches_contract": not missing and not mistyped,
    }


def _weather_payload(month: str) -> dict:
    r = httpx.get(f"{get_settings().proxy_url}/v1/archive", params={"month": month}, timeout=10)
    r.raise_for_status()
    return r.json()


@tool_spec(tier=TIER_READONLY, input_model=SchemaIn, agents=DIAG, timeout_s=30)
def compare_schema_to_contract(a: SchemaIn, ctx: ToolContext) -> dict:
    """Missing, extra and mistyped columns of the month's source data versus the contract,
    after current column mappings. For weather, also hourly completeness per day."""
    if a.dataset == "yellow_trips":
        return trips_schema_diff(a.month)
    hourly = _weather_payload(a.month).get("hourly") or {}
    contract = load_contract("weather_hourly")
    present = set(hourly)
    per_day = Counter(t[:10] for t in hourly.get("time", []))
    short = {d: n for d, n in sorted(per_day.items()) if n != 24}
    return {
        "missing": sorted(set(contract["columns"]) - present),
        "extra": sorted(present - set(contract["columns"])),
        "hours_total": len(hourly.get("time", [])),
        "days": len(per_day),
        "days_without_24_hours": len(short),
        "examples": dict(list(short.items())[:5]),
    }


# --------------------------------------------------------------- warehouse


class DbtIn(Args):
    month: Month


@tool_spec(tier=TIER_READONLY, input_model=DbtIn, agents=DIAG)
def get_dbt_failures(a: DbtIn, ctx: ToolContext) -> dict:
    """Failed, errored and warning dbt tests and models from the latest dbt run for the month,
    with failing row counts (from ops.dbt_runs)."""
    rows = query(
        "agent_ro",
        "SELECT run_id, loaded_at, run_results FROM ops.dbt_runs WHERE month = :m "
        "ORDER BY id DESC LIMIT 1",
        {"m": a.month},
    )
    if not rows:
        raise NotFound(f"no dbt run recorded for {a.month}")
    results = rows[0]["run_results"].get("results", [])
    status = Counter(r["status"] for r in results)
    problems = [
        {
            "node": r["unique_id"].split(".", 2)[-1],
            "status": r["status"],
            "failures": r.get("failures"),
            "message": (r.get("message") or "")[:300],
        }
        for r in results
        if r["status"] in ("fail", "error", "warn")
    ]
    return {
        "run_id": rows[0]["run_id"],
        "loaded_at": rows[0]["loaded_at"],
        "status_counts": dict(status),
        "problems": problems,
    }


class ProfileIn(Args):
    table: ProfileTable
    column: Identifier
    month: Month


@tool_spec(tier=TIER_READONLY, input_model=ProfileIn, agents=DIAG)
def profile_column(a: ProfileIn, ctx: ToolContext) -> dict:
    """Null rate, min, max, negative share and distinct count of one column for one month,
    with the clean baseline where docs/data_profile.json has one."""
    dtype = require_column(a.table, a.column)
    month_col = PROFILE_TABLES[a.table]
    numeric = dtype in NUMERIC_TYPES
    col = f'"{a.column}"'  # validated identifier
    neg = f"avg(({col} < 0)::int)" if numeric else "NULL"
    stats = query(
        "agent_ro",
        f"SELECT count(*) AS rows, avg(({col} IS NULL)::int) AS null_rate, "
        f"min({col})::text AS min, max({col})::text AS max, {neg} AS negative_share, "
        f"count(DISTINCT {col}) AS distinct_count FROM {a.table} WHERE {month_col} = :m",
        {"m": a.month},
    )[0]
    if stats["rows"] == 0:
        raise NotFound(f"{a.table} has no rows for {a.month}")
    for k in ("null_rate", "negative_share"):
        if stats[k] is not None:
            stats[k] = round(float(stats[k]), 5)

    ref: dict[str, Any] = {}
    for month in baseline().get("trips", []):
        rates = {k.lower(): v for k, v in month["null_rate"].items()}
        if a.column in rates:
            ref.setdefault("null_rate", []).append(round(rates[a.column], 5))
        if a.column == "fare_amount":
            ref.setdefault("negative_share", []).append(round(month["fare_negative_rate"], 5))
    return {
        "table": a.table,
        "column": a.column,
        "month": a.month,
        "type": dtype,
        **stats,
        "baseline": {k: {"min": min(v), "max": max(v)} for k, v in ref.items()},
    }


class SqlIn(Args):
    query: str = Field(min_length=1, max_length=4000)


MAX_SQL_ROWS = 200
_FORBIDDEN_NODES = tuple(
    getattr(exp, n)
    for n in (
        "Insert",
        "Update",
        "Delete",
        "Drop",
        "Create",
        "Alter",
        "AlterTable",
        "Command",
        "Merge",
        "TruncateTable",
        "Into",
        "Copy",
        "Set",
        "Transaction",
        "Commit",
        "Rollback",
        "Grant",
        "Lock",
        "Use",
        "Pragma",
    )
    if hasattr(exp, n)
)
_FORBIDDEN_FUNCS = {
    "pg_sleep",
    "pg_sleep_for",
    "pg_sleep_until",
    "set_config",
    "pg_terminate_backend",
    "pg_cancel_backend",
    "pg_reload_conf",
    "lo_import",
    "lo_export",
    "pg_read_file",
    "pg_read_binary_file",
    "pg_ls_dir",
    "pg_stat_file",
    "dblink",
    "dblink_exec",
    "nextval",
    "setval",
    "pg_advisory_lock",
    "pg_advisory_xact_lock",
}


def check_readonly_sql(sql: str) -> exp.Query:
    """Reject anything that is not exactly one plain SELECT, before it reaches the database."""
    try:
        statements = [s for s in sqlglot.parse(sql, read="postgres") if s is not None]
    except sqlglot.errors.ParseError as exc:
        raise InvalidInput(f"could not parse SQL: {str(exc).splitlines()[0]}") from None
    if len(statements) != 1:
        raise PermissionDenied(f"exactly one statement allowed, got {len(statements)}")
    tree = statements[0]
    if not isinstance(tree, exp.Query):
        raise PermissionDenied(f"only SELECT queries are allowed, got {tree.key.upper()}")
    for node in tree.walk():
        if isinstance(node, _FORBIDDEN_NODES):
            raise PermissionDenied(f"{node.key.upper()} is not allowed in read-only SQL")
        if isinstance(node, exp.Func):
            fname = (node.name if isinstance(node, exp.Anonymous) else node.sql_name()).lower()
            if fname in _FORBIDDEN_FUNCS:
                raise PermissionDenied(f"function {fname} is not allowed")
    return tree


@tool_spec(tier=TIER_READONLY, input_model=SqlIn, agents=DIAG, timeout_s=35)
def run_readonly_sql(a: SqlIn, ctx: ToolContext) -> dict:
    """One SELECT against the warehouse as the read-only role, in a read-only transaction,
    at most 200 rows. Anything else is rejected before it reaches the database."""
    tree = check_readonly_sql(a.query)
    wrapped = f"SELECT * FROM ({tree.sql(dialect='postgres')}) AS q LIMIT {MAX_SQL_ROWS + 1}"
    with warehouse_engine("agent_ro").connect() as conn:
        conn.exec_driver_sql("SET TRANSACTION READ ONLY")
        result = conn.execute(text(wrapped).execution_options(no_parameters=True))
        columns = list(result.keys())
        rows = [list(r) for r in result.fetchall()]
    return {"columns": columns, "rows": rows[:MAX_SQL_ROWS], "truncated": len(rows) > MAX_SQL_ROWS}


# ------------------------------------------------------------------- health


class HealthIn(Args):
    month: Month | None = None
    wait_until_healthy_s: int = Field(default=0, ge=0, le=300)


HEALTH_POLL_S = 15


def _probe(target: str) -> dict[str, Any]:
    started = time.monotonic()
    try:
        r = httpx.get(target, timeout=5)
    except httpx.TimeoutException:
        return {
            "healthy": False,
            "status_code": None,
            "error": "timed out after 5s",
            "latency_ms": int((time.monotonic() - started) * 1000),
        }
    except httpx.TransportError as exc:
        return {
            "healthy": False,
            "status_code": None,
            "error": type(exc).__name__,
            "latency_ms": int((time.monotonic() - started) * 1000),
        }
    out: dict[str, Any] = {
        "healthy": r.status_code == 200,
        "status_code": r.status_code,
        "latency_ms": int((time.monotonic() - started) * 1000),
    }
    if "/v1/archive" in target and r.status_code == 200:
        out["hours_returned"] = len(r.json().get("hourly", {}).get("time", []))
    return out


@tool_spec(
    tier=TIER_READONLY, input_model=HealthIn, agents={"diagnostics", "remediation"}, timeout_s=330
)
def check_api_health(a: HealthIn, ctx: ToolContext) -> dict:
    """Probe the weather API (the data endpoint for a month, or /health). Reports status code
    and latency; an unreachable or slow API is a result here, not an error. With
    wait_until_healthy_s, keeps probing until it is healthy or the wait is over."""
    url = get_settings().proxy_url
    target = f"{url}/v1/archive?month={a.month}" if a.month else f"{url}/health"
    deadline = time.monotonic() + a.wait_until_healthy_s
    probes = 1
    out = _probe(target)
    while not out["healthy"] and time.monotonic() + HEALTH_POLL_S <= deadline:
        time.sleep(HEALTH_POLL_S)
        out = _probe(target)
        probes += 1
    return {**out, "probes": probes}


# ------------------------------------------------------------------- memory


class MemorySearchIn(Args):
    text: str = Field(min_length=3, max_length=1000)
    k: int = Field(default=3, ge=1, le=5)


@tool_spec(tier=TIER_READONLY, input_model=MemorySearchIn, agents=DIAG, timeout_s=60)
def search_incident_memory(a: MemorySearchIn, ctx: ToolContext) -> dict:
    """Past incidents most similar to the text, with their root cause and fix."""
    return {"matches": memory.search(a.text, a.k, exclude_incident=ctx.incident_id)}


# ----------------------------------------------------------------- reporter


class PostmortemIn(Args):
    markdown: str = Field(min_length=20, max_length=20_000)


@tool_spec(tier=TIER_READONLY, input_model=PostmortemIn, agents={"reporter"})
def write_postmortem(a: PostmortemIn, ctx: ToolContext) -> dict:
    """Store the postmortem for this incident (replaces an earlier version)."""
    from sqlalchemy.dialects.postgresql import insert as pg_insert

    stmt = pg_insert(Postmortem).values(incident_id=ctx.incident_id, markdown=a.markdown)
    stmt = stmt.on_conflict_do_update(
        index_elements=[Postmortem.incident_id], set_={"markdown": a.markdown}
    )
    with agent_engine().begin() as conn:
        conn.execute(stmt)
    return {"incident_id": ctx.incident_id, "chars": len(a.markdown)}


class MemorySaveIn(Args):
    summary: str = Field(min_length=10, max_length=2000)
    root_cause: RootCause
    fix: str = Field(min_length=3, max_length=1000)


@tool_spec(tier=TIER_READONLY, input_model=MemorySaveIn, agents={"reporter"}, timeout_s=60)
def save_incident_memory(a: MemorySaveIn, ctx: ToolContext) -> dict:
    """Embed the incident summary and store it in incident memory for future searches."""
    memory_id = memory.save(ctx.incident_id, a.summary, a.root_cause.value, a.fix)
    return {"memory_id": memory_id}
