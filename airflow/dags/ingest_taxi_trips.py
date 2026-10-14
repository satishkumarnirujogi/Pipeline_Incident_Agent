"""Ingest one month of yellow taxi trips: landing -> contract -> raw -> transform.

Trigger with conf {"month": "YYYY-MM"}.
"""

from __future__ import annotations

import hashlib
from datetime import timedelta
from pathlib import Path

import pyarrow.parquet as pq
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Param, dag, get_current_context, task
from common.callbacks import notify_incident
from common.contract import get_mappings, load_contract, validate_trips
from common.db import CONTRACTS_DIR, DATA_DIR, warehouse_connect
from common.loaders import replace_partition

DATASET = "yellow_trips"
BATCH_SIZE = 100_000


def _landing_path(month: str) -> Path:
    return DATA_DIR / "landing" / "trips" / f"yellow_tripdata_{month}.parquet"


def _validated_path(month: str) -> Path:
    return DATA_DIR / "staging" / "trips" / f"yellow_tripdata_{month}.parquet"


@dag(
    dag_id="ingest_taxi_trips",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={"month": Param("2026-06", type="string", pattern=r"^\d{4}-\d{2}$")},
    default_args={
        "retries": 1,
        "retry_delay": timedelta(seconds=30),
        "on_failure_callback": notify_incident,
    },
    tags=["ingest", "trips"],
)
def ingest_taxi_trips():
    @task
    def check_landing_file() -> str:
        month = get_current_context()["params"]["month"]
        path = _landing_path(month)
        if not path.exists():
            raise FileNotFoundError(f"landing file not found: {path}")
        if path.stat().st_size == 0:
            raise ValueError(f"landing file is empty: {path}")
        return str(path)

    @task
    def validate_contract(landing: str) -> dict:
        month = get_current_context()["params"]["month"]
        contract = load_contract(CONTRACTS_DIR / f"{DATASET}.yaml")
        with warehouse_connect() as conn:
            mappings = get_mappings(conn, DATASET)
        out = _validated_path(month)
        result = validate_trips(Path(landing), contract, mappings, month, out)
        return {"path": str(out), "rows": result.rows, "warnings": result.warnings}

    @task
    def load_raw(validated: dict) -> dict:
        ctx = get_current_context()
        month = ctx["params"]["month"]
        pf = pq.ParquetFile(validated["path"])
        with warehouse_connect() as conn:
            rows = replace_partition(
                conn,
                "raw.yellow_trips",
                "pickup_month",
                month,
                pf.iter_batches(batch_size=BATCH_SIZE),
                run_id=ctx["run_id"],
            )
        return {"path": validated["path"], "rows": rows}

    @task
    def record_partition(loaded: dict) -> None:
        ctx = get_current_context()
        h = hashlib.sha256()
        with open(loaded["path"], "rb") as f:
            for chunk in iter(lambda: f.read(1 << 20), b""):
                h.update(chunk)
        with warehouse_connect() as conn:
            conn.execute(
                "INSERT INTO ops.partition_log (dataset, partition, row_count, checksum, run_id) "
                "VALUES (%s, %s, %s, %s, %s)",
                (DATASET, ctx["params"]["month"], loaded["rows"], h.hexdigest(), ctx["run_id"]),
            )

    trigger_transform = TriggerDagRunOperator(
        task_id="trigger_transform",
        trigger_dag_id="transform_dbt",
        conf={"month": "{{ params.month }}"},
        wait_for_completion=False,
    )

    loaded = load_raw(validate_contract(check_landing_file()))
    record_partition(loaded) >> trigger_transform


ingest_taxi_trips()
