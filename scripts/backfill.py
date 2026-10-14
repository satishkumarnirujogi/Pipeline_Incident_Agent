"""First clean pipeline runs: weather, then trips (which triggers transform_dbt).

Run with: make backfill   (or: uv run python -m scripts.backfill --month 2026-06)
"""

from __future__ import annotations

import argparse
import sys
import time

from scripts import bootstrap_airflow
from scripts.airflow_client import AirflowClient
from scripts.seed import MONTHS


def run_and_wait(af: AirflowClient, dag_id: str, month: str) -> str:
    run_id = af.trigger(dag_id, {"month": month})
    print(f"triggered {dag_id} {run_id}", flush=True)
    state = af.wait_for_run(dag_id, run_id)
    print(f"  -> {state}  {af.task_states(dag_id, run_id)}", flush=True)
    return state


def wait_for_triggered_transform(af: AirflowClient, after: float) -> str:
    """ingest_taxi_trips triggers transform_dbt without waiting; find and wait for that run."""
    for _ in range(60):
        run = af.latest_run("transform_dbt")
        if run and run["run_after"] and _ts(run["run_after"]) >= after:
            state = af.wait_for_run("transform_dbt", run["dag_run_id"])
            tasks = af.task_states("transform_dbt", run["dag_run_id"])
            print(f"transform_dbt {run['dag_run_id']} -> {state}  {tasks}", flush=True)
            return state
        time.sleep(3)
    raise TimeoutError("transform_dbt was not triggered")


def _ts(iso: str) -> float:
    from datetime import datetime

    return datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--month", default=MONTHS[0])
    args = parser.parse_args()

    if bootstrap_airflow.main() != 0:
        return 1
    af = AirflowClient()
    if run_and_wait(af, "ingest_weather", args.month) != "success":
        return 1
    started = time.time() - 5
    if run_and_wait(af, "ingest_taxi_trips", args.month) != "success":
        return 1
    return 0 if wait_for_triggered_transform(af, started) == "success" else 1


if __name__ == "__main__":
    sys.exit(main())
