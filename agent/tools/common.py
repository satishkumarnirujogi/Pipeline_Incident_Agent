"""Shared input types and helpers for the tools. Only agent/tools/*.py touch the warehouse."""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Annotated, Any, Literal

import pyarrow as pa
import yaml
from pydantic import BaseModel, ConfigDict, StringConstraints
from sqlalchemy import text

from agent.db import warehouse_engine
from agent.settings import get_settings
from agent.tools.errors import InvalidInput, NotFound

DagId = Literal["ingest_taxi_trips", "ingest_weather", "transform_dbt"]
Month = Annotated[str, StringConstraints(pattern=r"^\d{4}-(0[1-9]|1[0-2])$")]
Identifier = Annotated[str, StringConstraints(pattern=r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")]
Dataset = Literal["yellow_trips", "weather_hourly"]

# Tables the agents may profile, with the column that holds the month.
PROFILE_TABLES: dict[str, str] = {
    "raw.yellow_trips": "pickup_month",
    "raw.weather_hourly": "month",
    "quarantine.yellow_trips": "pickup_month",
    "staging.stg_yellow_trips": "pickup_month",
    "marts.fct_trips": "pickup_month",
}
ProfileTable = Literal[
    "raw.yellow_trips",
    "raw.weather_hourly",
    "quarantine.yellow_trips",
    "staging.stg_yellow_trips",
    "marts.fct_trips",
]
NUMERIC_TYPES = {"integer", "bigint", "smallint", "double precision", "real", "numeric"}


class Args(BaseModel):
    """Base for tool inputs: unknown arguments are an error, not silently dropped."""

    model_config = ConfigDict(extra="forbid")


def landing_trips_path(month: str) -> Path:
    return get_settings().data_dir / "landing" / "trips" / f"yellow_tripdata_{month}.parquet"


@lru_cache
def load_contract(dataset: str) -> dict[str, Any]:
    path = get_settings().contracts_dir / f"{dataset}.yaml"
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def type_ok(expected: str, actual: pa.DataType) -> bool:
    """Same rules as the pipeline's contract validator."""
    return {
        "int": pa.types.is_integer,
        "float": pa.types.is_floating,
        "timestamp": pa.types.is_timestamp,
        "string": lambda t: pa.types.is_string(t) or pa.types.is_large_string(t),
    }[expected](actual)


def query(role: str, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    with warehouse_engine(role).connect() as conn:
        result = conn.execute(text(sql), params or {})
        return [dict(r._mapping) for r in result]


def table_columns(table: str) -> dict[str, str]:
    """column -> data_type, read through agent_ro."""
    schema, name = table.split(".")
    rows = query(
        "agent_ro",
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_schema = :s AND table_name = :t ORDER BY ordinal_position",
        {"s": schema, "t": name},
    )
    if not rows:
        raise NotFound(f"table {table} does not exist yet")
    return {r["column_name"]: r["data_type"] for r in rows}


def require_column(table: str, column: str) -> str:
    cols = table_columns(table)
    if column not in cols:
        raise InvalidInput(f"{table} has no column {column!r}; columns: {sorted(cols)}")
    return cols[column]


@lru_cache
def baseline() -> dict[str, Any]:
    path = Path("docs/data_profile.json")
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
