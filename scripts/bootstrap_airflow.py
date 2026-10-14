"""Create the Airflow `warehouse` connection and unpause the DAGs. Safe to rerun.

Run with: make bootstrap   (also part of make backfill)
"""

from __future__ import annotations

import sys

from agent.settings import get_settings
from scripts.airflow_client import AirflowClient

DAGS = ["ingest_weather", "ingest_taxi_trips", "transform_dbt"]


def main() -> int:
    s = get_settings()
    af = AirflowClient()
    af.upsert_connection(
        {
            "connection_id": "warehouse",
            "conn_type": "postgres",
            "host": "postgres",
            "port": 5432,
            "login": "pipeline",
            "password": s.pipeline_db_password,
            "schema": "warehouse",
            "description": "Warehouse as the pipeline role (used by DAG tasks)",
        }
    )
    print("connection warehouse: ok")

    for dag_id in DAGS:
        try:
            af.wait_for_dag(dag_id)
        except TimeoutError:
            for err in af.import_errors():
                print(f"import error in {err['filename']}:\n{err['stack_trace']}")
            return 1
        af.unpause(dag_id)
        print(f"dag {dag_id}: unpaused")
    return 0


if __name__ == "__main__":
    sys.exit(main())
