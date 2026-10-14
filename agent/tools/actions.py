"""Tier-1 and tier-2 tools, and escalation.

Tier 1 runs automatically (at most twice per incident); tier 2 needs a granted
approval and supports a read-only dry run that the approval request shows.
Writes go through agent_rw, which has no DDL, DROP, ALTER or TRUNCATE rights.
"""

from __future__ import annotations

import uuid
from typing import Literal

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from pydantic import Field, model_validator
from sqlalchemy import text

from agent.db import warehouse_engine
from agent.tools.common import (
    Args,
    DagId,
    Identifier,
    Month,
    landing_trips_path,
    load_contract,
    query,
    require_column,
    table_columns,
)
from agent.tools.errors import InvalidInput, NotFound
from agent.tools.orchestrator import airflow
from agent.tools.readonly import trips_schema_diff
from agent.tools.registry import (
    TIER_CHANGES_DATA,
    TIER_ESCALATION,
    TIER_REVERSIBLE,
    ToolContext,
    tool_spec,
)
from agent.tools.store import PostgresToolStore

REMEDIATION = {"remediation"}
BATCH_SIZE = 100_000

# ------------------------------------------------------------------- tier 1


class RerunConf(Args):
    month: Month


class RerunIn(Args):
    dag_id: DagId
    conf: RerunConf


@tool_spec(
    tier=TIER_REVERSIBLE, input_model=RerunIn, agents=REMEDIATION, idempotent=True, timeout_s=20
)
def rerun_dag(a: RerunIn, ctx: ToolContext) -> dict:
    """Trigger a new run of a DAG for the same month. Safe because every load replaces its
    partition. Follow with wait_for_dag_run to verify."""
    run = airflow().trigger(a.dag_id, a.conf.model_dump())
    return {"dag_id": a.dag_id, "run_id": run["run_id"], "state": run["state"]}


# ------------------------------------------------------- tier 2: mapping


class MappingIn(Args):
    dataset: Literal["yellow_trips"]
    source_col: Identifier
    target_col: Identifier
    transform: Literal["none", "strip_currency", "to_float", "to_timestamp"] = "none"

    @model_validator(mode="after")
    def _meaningful(self) -> MappingIn:
        if self.source_col == self.target_col and self.transform == "none":
            raise ValueError("a mapping must rename the column or transform it")
        return self


def _check_target(a: MappingIn) -> None:
    columns = load_contract(a.dataset)["columns"]
    if a.target_col not in columns:
        raise InvalidInput(
            f"target_col {a.target_col!r} is not a contract column; "
            f"contract columns: {sorted(columns)}"
        )


def _mapping_preview(a: MappingIn, ctx: ToolContext) -> dict:
    """Schema diff before and after the proposed mapping, for the newest landing file."""
    _check_target(a)
    months = sorted(
        p.stem.rsplit("_", 1)[-1]
        for p in landing_trips_path("2000-01").parent.glob("yellow_tripdata_*.parquet")
        if p.stat().st_size > 0
    )
    if not months:
        return {"note": "no landing file to preview against"}
    month = months[-1]
    proposed = {"source_col": a.source_col, "target_col": a.target_col, "transform": a.transform}
    return {
        "month": month,
        "before": trips_schema_diff(month),
        "after": trips_schema_diff(month, proposed),
        "rollback": "delete the ops.column_mapping row for this source_col (by a human)",
    }


@tool_spec(
    tier=TIER_CHANGES_DATA,
    input_model=MappingIn,
    agents=REMEDIATION,
    idempotent=True,
    dry_run=_mapping_preview,
)
def upsert_column_mapping(a: MappingIn, ctx: ToolContext) -> dict:
    """Add or update a row in ops.column_mapping: the next ingest renames source_col to
    target_col and applies the transform before validating. Fixes schema drift and type
    mismatches without touching the contract."""
    _check_target(a)
    with warehouse_engine("agent_rw").begin() as conn:
        mapping_id = conn.execute(
            text(
                "INSERT INTO ops.column_mapping (dataset, source_col, target_col, transform, "
                "created_by) VALUES (:d, :s, :t, :x, :by) "
                "ON CONFLICT (dataset, source_col) DO UPDATE SET target_col = EXCLUDED.target_col,"
                " transform = EXCLUDED.transform, created_by = EXCLUDED.created_by RETURNING id"
            ),
            {
                "d": a.dataset,
                "s": a.source_col,
                "t": a.target_col,
                "x": a.transform,
                "by": f"agent:{ctx.incident_id}",
            },
        ).scalar_one()
    return {"mapping_id": mapping_id, **a.model_dump(exclude={"dataset"})}


# ----------------------------------------------------- tier 2: quarantine

ZONE_COLUMNS = {"pulocationid", "dolocationid"}


class Predicate(Args):
    column: Identifier
    op: Literal["is_null", "lt", "gt", "not_in_dim"]
    value: float | None = None

    @model_validator(mode="after")
    def _value_matches_op(self) -> Predicate:
        if self.op in ("lt", "gt") and self.value is None:
            raise ValueError(f"op {self.op} needs a value")
        if self.op in ("is_null", "not_in_dim") and self.value is not None:
            raise ValueError(f"op {self.op} takes no value")
        if self.op == "not_in_dim" and self.column not in ZONE_COLUMNS:
            raise ValueError(f"not_in_dim applies to {sorted(ZONE_COLUMNS)} only")
        return self


class QuarantineIn(Args):
    table: Literal["raw.yellow_trips"]
    month: Month
    predicate: Predicate


def _where(p: Predicate) -> tuple[str, dict]:
    """Structured predicate -> SQL. The column is a validated identifier, the value a
    bound parameter: the model never writes SQL here."""
    col = f'"{p.column}"'
    if p.op == "is_null":
        return f"{col} IS NULL", {}
    if p.op == "lt":
        return f"{col} < :v", {"v": p.value}
    if p.op == "gt":
        return f"{col} > :v", {"v": p.value}
    return f"{col} NOT IN (SELECT location_id FROM marts.dim_zones)", {}


def _validate_column(a: QuarantineIn) -> None:
    require_column(a.table, a.predicate.column)


def _landing_matches(a: QuarantineIn) -> int | None:
    """Rows of the landing file the rule would divert at the next ingest."""
    path = landing_trips_path(a.month)
    if not path.exists() or path.stat().st_size == 0:
        return None
    zone_ids = None
    if a.predicate.op == "not_in_dim":
        zone_ids = pa.array(
            [
                r["location_id"]
                for r in query("agent_ro", "SELECT location_id FROM marts.dim_zones")
            ],
            pa.int64(),
        )
    total = 0
    for batch in pq.ParquetFile(path).iter_batches(batch_size=BATCH_SIZE):
        by_lower = {n.lower(): n for n in batch.schema.names}
        if a.predicate.column not in by_lower:
            return None
        col = batch.column(by_lower[a.predicate.column])
        p = a.predicate
        if p.op == "is_null":
            mask = pc.is_null(col)
        elif p.op == "not_in_dim":
            mask = pc.invert(pc.is_in(pc.cast(col, pa.int64()), value_set=zone_ids))
        else:
            try:
                scalar = pa.scalar(p.value).cast(col.type)
            except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
                return None  # e.g. the column arrives as strings
            mask = pc.less(col, scalar) if p.op == "lt" else pc.greater(col, scalar)
        total += pc.sum(pc.fill_null(mask, False)).as_py() or 0
    return total


def _quarantine_preview(a: QuarantineIn, ctx: ToolContext) -> dict:
    _validate_column(a)
    where, params = _where(a.predicate)
    in_raw = query(
        "agent_ro",
        f"SELECT count(*) AS n FROM {a.table} WHERE pickup_month = :m AND {where}",
        {"m": a.month, **params},
    )[0]["n"]
    total = query(
        "agent_ro", f"SELECT count(*) AS n FROM {a.table} WHERE pickup_month = :m", {"m": a.month}
    )[0]["n"]
    in_landing = _landing_matches(a)
    return {
        "rows_moved_from_raw_now": in_raw,
        "raw_rows_for_month": total,
        "landing_rows_diverted_at_next_ingest": in_landing,
        "effect": f"moves {in_raw:,} rows of {a.table} {a.month} to quarantine.yellow_trips"
        + (f"; diverts {in_landing:,} landing rows on the next ingest" if in_landing else ""),
        "rollback": "restore_quarantined(batch_id) moves the rows back and disables the rule",
    }


@tool_spec(
    tier=TIER_CHANGES_DATA,
    input_model=QuarantineIn,
    agents=REMEDIATION,
    idempotent=True,
    dry_run=_quarantine_preview,
    timeout_s=60,
)
def quarantine_rows(a: QuarantineIn, ctx: ToolContext) -> dict:
    """Move rows matching a structured predicate from raw.yellow_trips to
    quarantine.yellow_trips in one transaction, and keep the rule active so the next ingest
    diverts matching landing rows too. Returns a batch_id for restore_quarantined."""
    _validate_column(a)
    where, params = _where(a.predicate)
    cols = ", ".join(f'"{c}"' for c in table_columns(a.table))
    batch_id = str(uuid.uuid4())
    with warehouse_engine("agent_rw").begin() as conn:
        rule_id = conn.execute(
            text(
                "INSERT INTO ops.quarantine_rules (batch_id, dataset, month, column_name, op, "
                "value, created_by) VALUES (:b, 'yellow_trips', :m, :c, :o, :v, :by) "
                "RETURNING id"
            ),
            {
                "b": batch_id,
                "m": a.month,
                "c": a.predicate.column,
                "o": a.predicate.op,
                "v": a.predicate.value,
                "by": f"agent:{ctx.incident_id}",
            },
        ).scalar_one()
        moved = conn.execute(
            text(
                f"INSERT INTO quarantine.yellow_trips ({cols}, batch_id) "
                f"SELECT {cols}, :b FROM {a.table} WHERE pickup_month = :m AND {where}"
            ),
            {"b": batch_id, "m": a.month, **params},
        ).rowcount
        conn.execute(
            text(f"DELETE FROM {a.table} WHERE pickup_month = :m AND {where}"),
            {"m": a.month, **params},
        )
    return {
        "batch_id": batch_id,
        "rule_id": rule_id,
        "rows_moved": moved,
        "note": "rule stays active: the next ingest diverts matching landing rows",
    }


class RestoreIn(Args):
    batch_id: uuid.UUID


def _rule(batch_id: uuid.UUID) -> dict:
    rows = query(
        "agent_ro",
        "SELECT month, active FROM ops.quarantine_rules WHERE batch_id = :b",
        {"b": str(batch_id)},
    )
    if not rows:
        raise NotFound(f"no quarantine batch {batch_id}")
    return rows[0]


def _restore_preview(a: RestoreIn, ctx: ToolContext) -> dict:
    rule = _rule(a.batch_id)
    n = query(
        "agent_ro",
        "SELECT count(*) AS n FROM quarantine.yellow_trips WHERE batch_id = :b",
        {"b": str(a.batch_id)},
    )[0]["n"]
    return {"rows_restored_to_raw": n, "month": rule["month"], "rule_active": rule["active"]}


@tool_spec(
    tier=TIER_CHANGES_DATA,
    input_model=RestoreIn,
    agents=REMEDIATION,
    idempotent=True,
    dry_run=_restore_preview,
    timeout_s=60,
)
def restore_quarantined(a: RestoreIn, ctx: ToolContext) -> dict:
    """Undo quarantine_rows: move the batch back to raw.yellow_trips and disable its rule."""
    _rule(a.batch_id)
    cols = ", ".join(f'"{c}"' for c in table_columns("raw.yellow_trips"))
    with warehouse_engine("agent_rw").begin() as conn:
        restored = conn.execute(
            text(
                f"INSERT INTO raw.yellow_trips ({cols}) SELECT {cols} "
                "FROM quarantine.yellow_trips WHERE batch_id = :b"
            ),
            {"b": str(a.batch_id)},
        ).rowcount
        conn.execute(
            text("DELETE FROM quarantine.yellow_trips WHERE batch_id = :b"), {"b": str(a.batch_id)}
        )
        conn.execute(
            text(
                "UPDATE ops.quarantine_rules SET active = false, restored_at = now() "
                "WHERE batch_id = :b"
            ),
            {"b": str(a.batch_id)},
        )
    return {"batch_id": str(a.batch_id), "rows_restored": restored}


# --------------------------------------------------------------- escalation


class EscalateIn(Args):
    summary: str = Field(min_length=10, max_length=2000)
    reason: str = Field(min_length=3, max_length=1000)
    suggested_owner: Literal["data-platform", "upstream-provider", "security", "analytics"]


@tool_spec(
    tier=TIER_ESCALATION, input_model=EscalateIn, agents={"diagnostics", "remediation", "reporter"}
)
def escalate_to_human(a: EscalateIn, ctx: ToolContext) -> dict:
    """Hand the incident to a human with a summary of what was found and tried. Always
    allowed; ends the run as escalated."""
    store = ctx.store
    add = getattr(store, "add_escalation", None) or PostgresToolStore().add_escalation
    escalation_id = add({"incident_id": ctx.incident_id, **a.model_dump()})
    return {"escalation_id": escalation_id, "status": "escalated"}
