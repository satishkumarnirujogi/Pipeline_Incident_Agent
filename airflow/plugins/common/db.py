"""Warehouse connection for DAG tasks.

Credentials come from the Airflow connection `warehouse` (created by
scripts/bootstrap_airflow.py), so the F10 fault can break them through the
Airflow REST API exactly as a rotated secret would.
"""

from __future__ import annotations

import os
from pathlib import Path

import psycopg

DATA_DIR = Path(os.environ.get("DATA_DIR", "/opt/data"))
CONTRACTS_DIR = Path(os.environ.get("CONTRACTS_DIR", "/opt/contracts"))
WAREHOUSE_CONN_ID = "warehouse"


def warehouse_connect() -> psycopg.Connection:
    from airflow.sdk import Connection  # imported lazily so tests run without Airflow

    c = Connection.get(WAREHOUSE_CONN_ID)
    return psycopg.connect(
        host=c.host,
        port=c.port or 5432,
        user=c.login,
        password=c.password,
        dbname=c.schema,
        connect_timeout=10,
        application_name="airflow-task",
    )
