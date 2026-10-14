"""Transform raw -> staging -> marts with dbt, and publish dbt's run results.

Triggered by ingest_taxi_trips with conf {"month": "YYYY-MM"}. dbt tests are
the data-quality layer; publish_run_results always runs, so the agent can
read which test failed from ops.dbt_runs instead of parsing logs.
"""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Param, TriggerRule, dag, get_current_context, task
from common.callbacks import notify_incident
from common.db import warehouse_connect

DBT_DIR = Path("/opt/dbt")
DBT = "/opt/dbt-venv/bin/dbt"


@dag(
    dag_id="transform_dbt",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={"month": Param("2026-06", type="string", pattern=r"^\d{4}-\d{2}$")},
    default_args={
        "retries": 0,
        "retry_delay": timedelta(seconds=30),
        "on_failure_callback": notify_incident,
    },
    tags=["transform", "dbt"],
)
def transform_dbt():
    dbt_build = BashOperator(
        task_id="dbt_build",
        cwd=str(DBT_DIR),
        bash_command=(
            f"([ -d dbt_packages/dbt_utils ] || {DBT} deps --profiles-dir .) && "
            f"{DBT} build --profiles-dir . --vars '{{month: \"{{{{ params.month }}}}\"}}'"
        ),
    )

    @task(trigger_rule=TriggerRule.ALL_DONE, on_failure_callback=None)
    def publish_run_results() -> None:
        ctx = get_current_context()
        path = DBT_DIR / "target" / "run_results.json"
        if not path.exists():
            raise FileNotFoundError(f"dbt run results not found: {path}")
        with warehouse_connect() as conn:
            conn.execute(
                "INSERT INTO ops.dbt_runs (month, run_id, run_results) VALUES (%s, %s, %s)",
                (ctx["params"]["month"], ctx["run_id"], path.read_text(encoding="utf-8")),
            )

    # Leaf tasks decide the run state. Without this, a failed dbt_build followed by a
    # successful publish_run_results would leave the run green.
    build_succeeded = EmptyOperator(task_id="build_succeeded")

    dbt_build >> publish_run_results() >> build_succeeded
    dbt_build >> build_succeeded


transform_dbt()
