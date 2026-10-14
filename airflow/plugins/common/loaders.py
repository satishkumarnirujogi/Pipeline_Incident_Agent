"""Idempotent loads: replace one partition in one transaction.

DELETE the partition, then COPY every batch, all inside one transaction.
Same input -> same table state, every time. This is what makes "rerun" a
safe fix for the agent.
"""

from __future__ import annotations

import io
import logging
from collections.abc import Iterable
from typing import Any

import pyarrow as pa
import pyarrow.csv as pacsv
from psycopg import sql

log = logging.getLogger(__name__)


def _table_columns(conn: Any, table: str) -> list[str]:
    schema, name = table.split(".")
    rows = conn.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
        (schema, name),
    ).fetchall()
    if not rows:
        raise LookupError(f"table {table} does not exist")
    return [r[0] for r in rows]


def _to_copy_batch(
    batch: pa.RecordBatch, columns: list[str], partition_col: str, value: str, run_id: str | None
) -> pa.RecordBatch:
    """Lowercase names, drop columns the table lacks, add partition and run id, order by table."""
    by_name = {n.lower(): a for n, a in zip(batch.schema.names, batch.columns, strict=True)}
    n = batch.num_rows
    by_name[partition_col] = pa.array([value] * n, pa.string())
    if "_run_id" in columns:
        by_name["_run_id"] = pa.array([run_id] * n, pa.string())
    arrays, names = [], []
    for col in columns:
        if col in by_name:
            arrays.append(by_name[col])
            names.append(col)
    return pa.RecordBatch.from_arrays(arrays, names=names)


def replace_partition(
    conn: Any,
    table: str,
    partition_col: str,
    value: str,
    batches: Iterable[pa.RecordBatch],
    run_id: str | None = None,
) -> int:
    """DELETE the partition and COPY all batches in one transaction. Returns rows loaded."""
    table_cols = _table_columns(conn, table)
    load_cols = [c for c in table_cols if c != "_loaded_at"]
    schema, name = table.split(".")
    ident = sql.Identifier(schema, name)
    rows = 0
    copy_cols: list[str] | None = None
    dropped: set[str] = set()

    with conn.transaction():
        deleted = conn.execute(
            sql.SQL("DELETE FROM {} WHERE {} = %s").format(ident, sql.Identifier(partition_col)),
            (value,),
        ).rowcount
        for batch in batches:
            dropped |= {n.lower() for n in batch.schema.names} - set(table_cols)
            copy_batch = _to_copy_batch(batch, load_cols, partition_col, value, run_id)
            if copy_cols is None:
                copy_cols = copy_batch.schema.names
            buf = io.BytesIO()
            pacsv.write_csv(copy_batch, buf, write_options=pacsv.WriteOptions(include_header=False))
            stmt = sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT csv)").format(
                ident, sql.SQL(", ").join(map(sql.Identifier, copy_cols))
            )
            with conn.cursor().copy(stmt) as copy:
                copy.write(buf.getvalue())
            rows += copy_batch.num_rows

    if dropped:
        log.warning("%s: columns not in table, not loaded: %s", table, sorted(dropped))
    log.info(
        "%s %s=%s: deleted %s rows, loaded %s rows", table, partition_col, value, deleted, rows
    )
    return rows
