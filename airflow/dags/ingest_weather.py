"""Ingest one month of hourly weather from the weather proxy.

Trigger with conf {"month": "YYYY-MM"}. Two retries let short API blips
self-heal; only persistent failures become incidents.
"""

from __future__ import annotations

import json
import os
from datetime import timedelta

import httpx
import pyarrow as pa
from airflow.sdk import Param, dag, get_current_context, task
from common.callbacks import notify_incident
from common.contract import load_contract, validate_weather
from common.db import CONTRACTS_DIR, DATA_DIR, warehouse_connect
from common.loaders import replace_partition

DATASET = "weather_hourly"
PROXY_URL = os.environ.get("WEATHER_PROXY_URL", "http://weather-proxy:8090")


def _staged_path(month: str):
    return DATA_DIR / "staging" / "weather" / f"{month}.json"


@dag(
    dag_id="ingest_weather",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={"month": Param("2026-06", type="string", pattern=r"^\d{4}-\d{2}$")},
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=20),
        "on_failure_callback": notify_incident,
    },
    tags=["ingest", "weather"],
)
def ingest_weather():
    @task
    def fetch_weather() -> str:
        month = get_current_context()["params"]["month"]
        resp = httpx.get(f"{PROXY_URL}/v1/archive", params={"month": month}, timeout=10)
        resp.raise_for_status()
        path = _staged_path(month)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(resp.text, encoding="utf-8")
        return str(path)

    @task
    def validate_contract(path: str) -> str:
        month = get_current_context()["params"]["month"]
        contract = load_contract(CONTRACTS_DIR / f"{DATASET}.yaml")
        with open(path, encoding="utf-8") as f:
            validate_weather(json.load(f), contract, month)
        return path

    @task
    def load_raw(path: str) -> int:
        ctx = get_current_context()
        month = ctx["params"]["month"]
        contract = load_contract(CONTRACTS_DIR / f"{DATASET}.yaml")
        with open(path, encoding="utf-8") as f:
            df = validate_weather(json.load(f), contract, month)
        batch = pa.RecordBatch.from_pandas(df, preserve_index=False)
        with warehouse_connect() as conn:
            return replace_partition(
                conn, "raw.weather_hourly", {"month": month}, [batch], run_id=ctx["run_id"]
            )

    load_raw(validate_contract(fetch_weather()))


ingest_weather()
