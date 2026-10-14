"""A throwaway Postgres built exactly like the real one: db/init.sql (roles, grants),
warehouse migrations and Alembic, plus a small seeded month and landing file."""

from __future__ import annotations

import json
import time
from pathlib import Path

import psycopg
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from testcontainers.community.postgres import PostgresContainer

ROOT = Path(__file__).parents[2]
PASSWORDS = {
    "AIRFLOW_DB_PASSWORD": "af-pw",
    "PIPELINE_DB_PASSWORD": "pipe-pw",
    "AGENT_RO_PASSWORD": "ro-pw",
    "AGENT_RW_PASSWORD": "rw-pw",
    "AGENT_APP_PASSWORD": "app-pw",
}
MONTH = "2026-06"
ROWS = 1000


def _wait_ready(dsn: str, timeout: float = 60) -> None:
    deadline = time.time() + timeout
    while True:
        try:
            with psycopg.connect(dsn, connect_timeout=3):
                return
        except psycopg.OperationalError:
            if time.time() > deadline:
                raise
            time.sleep(1)


def _trips_table(n: int = ROWS) -> pa.Table:
    """Source-named columns, like a TLC file. Every 10th fare negative, every 20th zone 999,
    every 4th passenger_count null."""
    base = 1_780_300_000_000_000  # 2026-06-01
    return pa.table(
        {
            "VendorID": pa.array([1 + i % 2 for i in range(n)], pa.int32()),
            "tpep_pickup_datetime": pa.array(
                [base + i * 60_000_000 for i in range(n)], pa.timestamp("us")
            ),
            "tpep_dropoff_datetime": pa.array(
                [base + i * 60_000_000 + 600_000_000 for i in range(n)], pa.timestamp("us")
            ),
            "passenger_count": pa.array([None if i % 4 == 0 else 1 for i in range(n)], pa.int64()),
            "trip_distance": pa.array([1.0 + i % 7 for i in range(n)]),
            "PULocationID": pa.array(
                [999 if i % 20 == 0 else 1 + i % 263 for i in range(n)], pa.int32()
            ),
            "DOLocationID": pa.array([1 + (i * 7) % 263 for i in range(n)], pa.int32()),
            "payment_type": pa.array([1] * n, pa.int64()),
            "fare_amount": pa.array([-5.0 if i % 10 == 0 else 10.0 + i % 30 for i in range(n)]),
            "total_amount": pa.array([12.0 + i % 30 for i in range(n)]),
        }
    )


@pytest.fixture(scope="session")
def pg():
    container = PostgresContainer(
        "pgvector/pgvector:pg16",
        username="postgres",
        password="super-pw",
        dbname="postgres",
        driver=None,
    )
    for k, v in PASSWORDS.items():
        container.with_env(k, v)
    container.with_volume_mapping(
        str(ROOT / "db" / "initdb.sh"), "/docker-entrypoint-initdb.d/00_initdb.sh", "ro"
    )
    container.with_volume_mapping(str(ROOT / "db" / "init.sql"), "/opt/db/init.sql", "ro")
    with container:
        port = int(container.get_exposed_port(5432))
        host = container.get_container_host_ip()

        def dsn(role: str, db: str) -> str:
            pw = {
                "pipeline": "pipe-pw",
                "agent_ro": "ro-pw",
                "agent_rw": "rw-pw",
                "agent_app": "app-pw",
                "postgres": "super-pw",
            }[role]
            return f"postgresql://{role}:{pw}@{host}:{port}/{db}"

        _wait_ready(dsn("agent_app", "agent"))
        yield {"host": host, "port": port, "dsn": dsn}


@pytest.fixture(scope="session")
def data_dir(tmp_path_factory) -> Path:
    d = tmp_path_factory.mktemp("data")
    (d / "landing" / "trips").mkdir(parents=True)
    pq.write_table(_trips_table(), d / "landing" / "trips" / f"yellow_tripdata_{MONTH}.parquet")
    return d


@pytest.fixture(scope="session")
def platform(pg, data_dir):
    """Migrated and seeded; settings and engines point at the container."""
    mp = pytest.MonkeyPatch()
    for k, v in PASSWORDS.items():
        mp.setenv(k, v)
    mp.setenv("RUNNING_IN_DOCKER", "false")
    mp.setenv("POSTGRES_HOST_PORT", str(pg["port"]))
    mp.setenv("DATA_DIR", str(data_dir))
    mp.setenv("CONTRACTS_DIR", str(ROOT / "contracts"))

    from agent import db, memory
    from agent.settings import get_settings
    from agent.tools import common
    from scripts import migrate

    get_settings.cache_clear()
    db.reset_engines()
    common.load_contract.cache_clear()
    memory.set_embedder(memory.HashEmbedder())

    migrate.migrate_warehouse(pg["dsn"]("pipeline", "warehouse"))
    migrate.migrate_agent(
        pg["dsn"]("agent_app", "agent").replace("postgresql://", "postgresql+psycopg://", 1)
    )
    _seed(pg["dsn"]("pipeline", "warehouse"))
    yield pg
    memory.set_embedder(None)
    mp.undo()
    get_settings.cache_clear()
    db.reset_engines()


def _seed(dsn: str) -> None:
    table = _trips_table()
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            "CREATE TABLE marts.dim_zones (location_id int, borough text, zone text, "
            "service_zone text)"
        )
        conn.execute(
            "INSERT INTO marts.dim_zones SELECT g, 'B', 'Z' || g, 'Yellow Zone' "
            "FROM generate_series(1, 265) g"
        )
        rows = table.to_pylist()
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO raw.yellow_trips (pickup_month, vendorid, tpep_pickup_datetime, "
                "tpep_dropoff_datetime, passenger_count, trip_distance, pulocationid, "
                "dolocationid, payment_type, fare_amount, total_amount, _run_id) VALUES "
                "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'seed')",
                [
                    (
                        MONTH,
                        r["VendorID"],
                        r["tpep_pickup_datetime"],
                        r["tpep_dropoff_datetime"],
                        r["passenger_count"],
                        r["trip_distance"],
                        r["PULocationID"],
                        r["DOLocationID"],
                        r["payment_type"],
                        r["fare_amount"],
                        r["total_amount"],
                    )
                    for r in rows
                ],
            )
        run_results = {
            "results": [
                {
                    "unique_id": "test.pia.unique_stg_yellow_trips_trip_id.abc",
                    "status": "pass",
                    "failures": 0,
                    "message": None,
                },
                {
                    "unique_id": "test.pia.relationships_stg_yellow_trips_pu_location_id.def",
                    "status": "fail",
                    "failures": 50,
                    "message": "Got 50 results, configured to fail if != 0",
                },
                {
                    "unique_id": "test.pia.accepted_range_trip_distance.ghi",
                    "status": "warn",
                    "failures": 3,
                    "message": "Got 3 results",
                },
            ]
        }
        conn.execute(
            "INSERT INTO ops.dbt_runs (month, run_id, run_results) VALUES (%s, %s, %s)",
            (MONTH, "manual__seed", json.dumps(run_results)),
        )
