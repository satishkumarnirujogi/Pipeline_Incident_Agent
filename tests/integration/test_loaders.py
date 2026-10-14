"""replace_partition against a real Postgres: loading twice leaves the same state."""

import psycopg
import pyarrow as pa
import pytest
from common.loaders import replace_partition
from testcontainers.community.postgres import PostgresContainer


@pytest.fixture(scope="module")
def conn():
    with PostgresContainer("pgvector/pgvector:pg16", driver=None) as pg:
        with psycopg.connect(pg.get_connection_url(), autocommit=True) as c:
            c.execute("CREATE SCHEMA raw")
            c.execute(
                "CREATE TABLE raw.t (month text NOT NULL, id int, v double precision, "
                "label text, _run_id text, _loaded_at timestamptz NOT NULL DEFAULT now())"
            )
            yield c


def _batches(n: int, start: int = 0):
    for lo in range(start, start + n, 100):
        ids = list(range(lo, min(lo + 100, start + n)))
        yield pa.record_batch(
            {
                "ID": ids,
                "v": [i * 1.5 for i in ids],
                "label": [None if i % 2 == 0 else "a,b" for i in ids],
                "extra": ids,
            }
        )


def _count(conn, month: str) -> int:
    return conn.execute("SELECT count(*) FROM raw.t WHERE month = %s", (month,)).fetchone()[0]


def test_reload_is_idempotent(conn):
    assert replace_partition(conn, "raw.t", "month", "2026-06", _batches(250), run_id="r1") == 250
    assert replace_partition(conn, "raw.t", "month", "2026-06", _batches(250), run_id="r2") == 250
    assert _count(conn, "2026-06") == 250
    runs = conn.execute("SELECT DISTINCT _run_id FROM raw.t WHERE month = '2026-06'").fetchall()
    assert runs == [("r2",)]


def test_other_partitions_untouched(conn):
    replace_partition(conn, "raw.t", "month", "2026-07", _batches(10))
    replace_partition(conn, "raw.t", "month", "2026-06", _batches(5))
    assert _count(conn, "2026-07") == 10
    assert _count(conn, "2026-06") == 5


def test_nulls_and_commas_round_trip(conn):
    replace_partition(conn, "raw.t", "month", "2026-08", _batches(2))
    rows = conn.execute("SELECT id, label FROM raw.t WHERE month='2026-08' ORDER BY id").fetchall()
    assert rows == [(0, None), (1, "a,b")]


def test_failed_load_rolls_back(conn):
    replace_partition(conn, "raw.t", "month", "2026-09", _batches(10))

    def broken():
        yield from _batches(5)
        raise RuntimeError("source died mid-load")

    with pytest.raises(RuntimeError):
        replace_partition(conn, "raw.t", "month", "2026-09", broken())
    assert _count(conn, "2026-09") == 10  # delete was rolled back with the partial COPY
