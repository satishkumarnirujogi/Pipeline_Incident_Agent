"""Validate data against a contract in contracts/*.yaml.

Column mappings from ops.column_mapping are applied first: that table is how
a schema-drift or type-mismatch fix takes effect. Trip files are read in
100k-row batches; rate checks add counts across batches and decide at the end,
so memory stays flat whatever the file size.

Messages are ordinary validation errors. They describe what was observed, never
a diagnosis; the agent has to work out the cause itself.
"""

from __future__ import annotations

import calendar
import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import yaml

log = logging.getLogger(__name__)

BATCH_SIZE = 100_000
MAX_LOGGED_ROW_IDS = 20


class ContractViolation(Exception):
    """Data does not satisfy its contract."""


@dataclass
class ContractResult:
    dataset: str
    rows: int
    kept_rows: int = 0
    quarantined: dict[str, int] = field(default_factory=dict)  # batch_id -> rows
    warnings: list[str] = field(default_factory=list)
    null_counts: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class ColumnMapping:
    source_col: str
    target_col: str
    transform: str = "none"


def load_contract(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def get_mappings(conn: Any, dataset: str) -> list[ColumnMapping]:
    rows = conn.execute(
        "SELECT source_col, target_col, transform FROM ops.column_mapping "
        "WHERE dataset = %s ORDER BY id",
        (dataset,),
    ).fetchall()
    return [ColumnMapping(*r) for r in rows]


# ------------------------------------------------------------------ mappings


def _strip_currency(arr: pa.Array) -> pa.Array:
    if not pa.types.is_string(arr.type) and not pa.types.is_large_string(arr.type):
        return pc.cast(arr, pa.float64())
    cleaned = pc.replace_substring_regex(arr, pattern=r"[$,\s]", replacement="")
    return pc.cast(cleaned, pa.float64())


_TRANSFORMS = {
    "none": lambda a: a,
    "strip_currency": _strip_currency,
    "to_float": lambda a: pc.cast(a, pa.float64()),
    "to_timestamp": lambda a: pc.cast(a, pa.timestamp("us")),
}


def apply_mappings(batch: pa.RecordBatch, mappings: list[ColumnMapping]) -> pa.RecordBatch:
    """Rename source columns to their target names and apply allowed transforms."""
    if not mappings:
        return batch
    by_source = {m.source_col: m for m in mappings}
    names, arrays = [], []
    for name, arr in zip(batch.schema.names, batch.columns, strict=True):
        m = by_source.get(name)
        if m is None:
            names.append(name)
            arrays.append(arr)
        else:
            names.append(m.target_col)
            arrays.append(_TRANSFORMS[m.transform](arr))
    return pa.RecordBatch.from_arrays(arrays, names=names)


# --------------------------------------------------------------------- types


def _type_ok(expected: str, actual: pa.DataType) -> bool:
    if expected == "int":
        return pa.types.is_integer(actual)
    if expected == "float":
        return pa.types.is_floating(actual)
    if expected == "timestamp":
        return pa.types.is_timestamp(actual)
    if expected == "string":
        return pa.types.is_string(actual) or pa.types.is_large_string(actual)
    raise ValueError(f"unknown contract type {expected!r}")


def _check_schema(schema: pa.Schema, contract: dict, warnings: list[str]) -> None:
    columns: dict[str, dict] = contract["columns"]
    present = set(schema.names)
    missing = [c for c in columns if c not in present]
    if missing:
        raise ContractViolation(
            f"{contract['dataset']}: missing required columns {missing}; file has {schema.names}"
        )
    for name, spec in columns.items():
        actual = schema.field(name).type
        if not _type_ok(spec["type"], actual):
            raise ContractViolation(
                f"{contract['dataset']}: column {name!r} has type {actual}, expected {spec['type']}"
            )
    extra = sorted(present - set(columns))
    if extra:
        msg = f"{contract['dataset']}: columns not in contract: {extra}"
        if not contract.get("allow_extra_columns", False):
            raise ContractViolation(msg)
        warnings.append(msg)


def _month_bounds(month: str) -> tuple[datetime, datetime]:
    year, mon = map(int, month.split("-"))
    start = datetime(year, mon, 1)
    end = datetime(year + mon // 12, mon % 12 + 1, 1)
    return start, end


# --------------------------------------------------------------------- trips


@dataclass(frozen=True)
class QuarantineRule:
    """An active row in ops.quarantine_rules: rows matching it never reach raw."""

    batch_id: str
    column: str  # raw (lowercase) column name
    op: str  # is_null | lt | gt | not_in_dim
    value: float | None = None


def get_quarantine_rules(conn: Any, dataset: str, month: str) -> list[QuarantineRule]:
    rows = conn.execute(
        "SELECT batch_id::text, column_name, op, value FROM ops.quarantine_rules "
        "WHERE dataset = %s AND month = %s AND active ORDER BY id",
        (dataset, month),
    ).fetchall()
    return [QuarantineRule(*r) for r in rows]


def get_zone_ids(conn: Any) -> pa.Array:
    rows = conn.execute("SELECT location_id FROM marts.dim_zones").fetchall()
    return pa.array([r[0] for r in rows], pa.int64())


def rule_mask(
    batch: pa.RecordBatch, rule: QuarantineRule, zone_ids: pa.Array | None
) -> pa.BooleanArray:
    """True where the row matches the rule. Columns are matched case-insensitively,
    because rules use raw column names (pulocationid) and files use source names."""
    by_lower = {n.lower(): n for n in batch.schema.names}
    if rule.column not in by_lower:
        raise ContractViolation(f"quarantine rule {rule.batch_id}: no column {rule.column!r}")
    col = batch.column(by_lower[rule.column])
    if rule.op == "is_null":
        return pc.is_null(col)
    if rule.op == "lt":
        mask = pc.less(col, pa.scalar(rule.value).cast(col.type))
    elif rule.op == "gt":
        mask = pc.greater(col, pa.scalar(rule.value).cast(col.type))
    elif rule.op == "not_in_dim":
        if zone_ids is None:
            raise ContractViolation("quarantine rule not_in_dim needs the zone dimension")
        mask = pc.invert(pc.is_in(pc.cast(col, pa.int64()), value_set=zone_ids))
    else:
        raise ContractViolation(f"unknown quarantine op {rule.op!r}")
    return pc.fill_null(mask, False)


def validate_trips(
    path: Path,
    contract: dict,
    mappings: list[ColumnMapping],
    month: str,
    out_path: Path,
    rules: list[QuarantineRule] | None = None,
    zone_ids: pa.Array | None = None,
    quarantine_dir: Path | None = None,
) -> ContractResult:
    """Validate a trips Parquet file and write the mapped, validated copy to out_path.

    Rows matching an active quarantine rule are written to quarantine_dir/<batch_id>.parquet
    instead and excluded from the rate checks. min_rows counts input rows, so quarantining
    a large share does not by itself fail the contract.
    """
    rules = rules or []
    pf = pq.ParquetFile(path)
    result = ContractResult(dataset=contract["dataset"], rows=0)
    columns: dict[str, dict] = contract["columns"]
    checks = contract.get("checks", {})
    start, end = _month_bounds(month)

    nulls = {c: 0 for c in columns}
    null_row_ids: dict[str, list[int]] = {c: [] for c in columns}
    outside_month = 0
    outside_row_ids: list[int] = []
    offset = 0
    kept = 0

    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(".parquet.part")
    writer: pq.ParquetWriter | None = None
    q_writers: dict[str, pq.ParquetWriter] = {}
    q_tmp: dict[str, Path] = {}
    try:
        for raw_batch in pf.iter_batches(batch_size=BATCH_SIZE):
            batch = apply_mappings(raw_batch, mappings)
            if writer is None:
                _check_schema(batch.schema, contract, result.warnings)
                writer = pq.ParquetWriter(tmp, batch.schema)

            # Assign each row to the first rule it matches; others stay in the pipeline.
            quarantined = pa.array([False] * batch.num_rows)
            for rule in rules:
                mask = pc.and_(rule_mask(batch, rule, zone_ids), pc.invert(quarantined))
                if pc.any(mask).as_py():
                    if rule.batch_id not in q_writers:
                        assert quarantine_dir is not None, "quarantine_dir required with rules"
                        quarantine_dir.mkdir(parents=True, exist_ok=True)
                        q_tmp[rule.batch_id] = quarantine_dir / f"{rule.batch_id}.parquet.part"
                        q_writers[rule.batch_id] = pq.ParquetWriter(
                            q_tmp[rule.batch_id], batch.schema
                        )
                    q_writers[rule.batch_id].write_batch(batch.filter(mask))
                    n = pc.sum(mask).as_py()
                    result.quarantined[rule.batch_id] = result.quarantined.get(rule.batch_id, 0) + n
                    quarantined = pc.or_(quarantined, mask)
            keep = pc.invert(quarantined)

            for name in columns:
                col = batch.column(name)
                if col.null_count:
                    null_mask = pc.and_(pc.is_null(col), keep)
                    n_null = pc.sum(null_mask).as_py() or 0
                    nulls[name] += n_null
                    if n_null and len(null_row_ids[name]) < MAX_LOGGED_ROW_IDS:
                        idx = pc.indices_nonzero(null_mask).to_pylist()
                        null_row_ids[name] += [offset + i for i in idx[:MAX_LOGGED_ROW_IDS]]

            if "max_pickup_outside_month_rate" in checks:
                pickup = batch.column("tpep_pickup_datetime")
                mask = pc.and_(
                    pc.or_(
                        pc.less(pickup, pa.scalar(start, pickup.type)),
                        pc.greater_equal(pickup, pa.scalar(end, pickup.type)),
                    ),
                    keep,
                )
                n_out = pc.sum(mask).as_py() or 0
                if n_out:
                    outside_month += n_out
                    if len(outside_row_ids) < MAX_LOGGED_ROW_IDS:
                        idx = pc.indices_nonzero(mask).to_pylist()
                        outside_row_ids += [offset + i for i in idx[:MAX_LOGGED_ROW_IDS]]

            kept_batch = batch.filter(keep) if rules else batch
            writer.write_batch(kept_batch)
            kept += kept_batch.num_rows
            offset += batch.num_rows
    finally:
        if writer is not None:
            writer.close()
        for w in q_writers.values():
            w.close()

    rows = offset
    result.rows = rows
    result.kept_rows = kept
    result.null_counts = nulls
    errors: list[str] = []
    checked = max(kept, 1)  # rate checks cover the rows that stay in the pipeline

    min_rows = checks.get("min_rows")
    if min_rows is not None and rows < min_rows:
        errors.append(f"row count {rows:,} is below minimum {min_rows:,}")

    for name, spec in columns.items():
        n = nulls[name]
        if not n:
            continue
        if not spec.get("nullable", True):
            errors.append(
                f"column {name!r} is not nullable but has {n:,} nulls; "
                f"first row ids {null_row_ids[name][:MAX_LOGGED_ROW_IDS]}"
            )
        elif "max_null_rate" in spec and n / checked > spec["max_null_rate"]:
            errors.append(
                f"column {name!r} null rate {n / checked:.4f} exceeds max "
                f"{spec['max_null_rate']} ({n:,} of {kept:,} rows); "
                f"first row ids {null_row_ids[name][:MAX_LOGGED_ROW_IDS]}"
            )

    max_out = checks.get("max_pickup_outside_month_rate")
    if max_out is not None and outside_month / checked > max_out:
        errors.append(
            f"{outside_month:,} of {kept:,} pickups fall outside {month} "
            f"(rate {outside_month / checked:.4f}, max {max_out}); "
            f"first row ids {outside_row_ids[:MAX_LOGGED_ROW_IDS]}"
        )

    for w in result.warnings:
        log.warning(w)
    if errors:
        tmp.unlink(missing_ok=True)
        for p in q_tmp.values():
            p.unlink(missing_ok=True)
        raise ContractViolation(f"{contract['dataset']} {month}: " + "; ".join(errors))

    tmp.replace(out_path)
    for p in q_tmp.values():
        p.replace(p.with_suffix(""))  # <batch_id>.parquet.part -> <batch_id>.parquet
    for batch_id, n in result.quarantined.items():
        log.info(
            "%s %s: %s rows quarantined under batch %s",
            contract["dataset"],
            month,
            f"{n:,}",
            batch_id,
        )
    log.info("%s %s: %s rows passed the contract", contract["dataset"], month, f"{kept:,}")
    return result


# ------------------------------------------------------------------- weather


def validate_weather(payload: dict, contract: dict, month: str) -> pd.DataFrame:
    """Validate an Open-Meteo style payload; return one row per hour."""
    hourly = payload.get("hourly")
    if not isinstance(hourly, dict) or "time" not in hourly:
        raise ContractViolation(f"weather {month}: payload has no hourly.time array")
    lengths = {k: len(v) for k, v in hourly.items()}
    if len(set(lengths.values())) != 1:
        raise ContractViolation(f"weather {month}: hourly arrays differ in length {lengths}")

    df = pd.DataFrame(hourly)
    df["time"] = pd.to_datetime(df["time"])
    # JSON has no int/float distinction: a whole-number series (e.g. all 0) is still a float.
    for name in df.columns.drop("time"):
        if pd.api.types.is_integer_dtype(df[name]):
            df[name] = df[name].astype("float64")
    _check_schema(pa.Schema.from_pandas(df, preserve_index=False), contract, [])

    errors: list[str] = []
    for name, spec in contract["columns"].items():
        n_null = int(df[name].isna().sum())
        if n_null and not spec.get("nullable", True):
            errors.append(f"column {name!r} has {n_null} nulls")
        if "min" in spec and (df[name] < spec["min"]).any():
            errors.append(f"column {name!r} has values below {spec['min']}")
        if "max" in spec and (df[name] > spec["max"]).any():
            errors.append(f"column {name!r} has values above {spec['max']}")

    checks = contract.get("checks", {})
    start, end = _month_bounds(month)
    days_in_month = calendar.monthrange(start.year, start.month)[1]
    if checks.get("all_hours_in_month") and ((df.time < start) | (df.time >= end)).any():
        errors.append(f"timestamps outside {month}")
    if checks.get("no_duplicate_hours") and df.time.duplicated().any():
        errors.append(f"{int(df.time.duplicated().sum())} duplicate hours")
    if "hours_per_day" in checks:
        per_day = df.groupby(df.time.dt.date).size()
        short = per_day[per_day != checks["hours_per_day"]]
        if len(short):
            sample = {str(d): int(n) for d, n in short.head(5).items()}
            errors.append(
                f"{len(short)} days without {checks['hours_per_day']} hourly rows, e.g. {sample}"
            )
    if checks.get("rows_equal_days_times_24") and len(df) != days_in_month * 24:
        errors.append(f"expected {days_in_month * 24} hourly rows, got {len(df)}")

    if errors:
        raise ContractViolation(f"{contract['dataset']} {month}: " + "; ".join(errors))
    log.info("%s %s: %s hourly rows passed the contract", contract["dataset"], month, len(df))
    return df
