"""Apply and revert faults from faults/scenarios/*.yaml.

    python -m faults.injector apply F01-v3
    python -m faults.injector revert F01-v3
    python -m faults.injector release 2026-07      # clean landing + weather cache for a month
    python -m faults.injector reset                # revert everything (make reset-data)

Runs on the host: Postgres on localhost:POSTGRES_HOST_PORT, the proxy on
localhost:WEATHER_PROXY_HOST_PORT, Airflow on localhost:8080. Every apply and
revert first releases the scenario's month, then writes a row to eval.injections.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import zlib
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import numpy as np
import psycopg
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from agent.settings import get_settings
from faults.scenario import Scenario, load_all, load_scenario
from scripts.airflow_client import AirflowClient
from scripts.seed import BATCH_SIZE, LANDING_TRIPS, MONTHS, PROXY_CACHE, TRIPS_SAMPLED, WEATHER

WAREHOUSE_CONN = {
    "connection_id": "warehouse",
    "conn_type": "postgres",
    "host": "postgres",
    "port": 5432,
    "login": "pipeline",
    "schema": "warehouse",
    "description": "Warehouse as the pipeline role (used by DAG tasks)",
}


class InjectionError(RuntimeError):
    pass


# ------------------------------------------------------------------ helpers


def _clean_trips(month: str) -> Path:
    return TRIPS_SAMPLED / f"yellow_tripdata_{month}.parquet"


def _landing_trips(month: str) -> Path:
    return LANDING_TRIPS / f"yellow_tripdata_{month}.parquet"


def release(month: str) -> None:
    """Put the clean sample and weather for a month where the DAGs read them."""
    LANDING_TRIPS.mkdir(parents=True, exist_ok=True)
    PROXY_CACHE.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(_clean_trips(month), _landing_trips(month))
    shutil.copyfile(WEATHER / f"{month}.json", PROXY_CACHE / f"{month}.json")


def _agent_db() -> psycopg.Connection:
    conn = psycopg.connect(
        get_settings().dsn("agent_app", "agent", from_host=True), autocommit=True
    )
    conn.execute(
        """CREATE TABLE IF NOT EXISTS eval.injections (
               id           bigserial PRIMARY KEY,
               scenario_id  text NOT NULL,
               eval_run_id  text,
               action       text NOT NULL CHECK (action IN ('apply', 'revert')),
               mechanism    text NOT NULL,
               details      jsonb NOT NULL DEFAULT '{}',
               at           timestamptz NOT NULL DEFAULT now()
           )"""
    )
    return conn


def _record(sc: Scenario, action: str, details: dict, eval_run_id: str | None) -> None:
    with _agent_db() as conn:
        conn.execute(
            "INSERT INTO eval.injections (scenario_id, eval_run_id, action, mechanism, details) "
            "VALUES (%s, %s, %s, %s, %s)",
            (sc.id, eval_run_id, action, sc.mechanism, json.dumps(details)),
        )


def _last_apply_details(scenario_id: str) -> dict:
    with _agent_db() as conn:
        row = conn.execute(
            "SELECT details FROM eval.injections WHERE scenario_id = %s AND action = 'apply' "
            "ORDER BY id DESC LIMIT 1",
            (scenario_id,),
        ).fetchone()
    return row[0] if row else {}


def _rng(sc: Scenario) -> np.random.Generator:
    return np.random.default_rng(zlib.crc32(sc.id.encode()))


# ------------------------------------------------------------ file mechanism


def _fmt_currency(arr: pa.Array, thousands: bool) -> pa.Array:
    fmt = "${:,.2f}" if thousands else "${:.2f}"
    return pa.array([None if v is None else fmt.format(v) for v in arr.to_pylist()], pa.string())


def _to_string(arr: pa.Array) -> pa.Array:
    if pa.types.is_timestamp(arr.type):
        seconds = pc.cast(arr, pa.timestamp("s"), safe=False)  # %S on us adds ".000000"
        return pc.strftime(seconds, format="%Y-%m-%d %H:%M:%S")
    return pa.array([None if v is None else f"{v:.2f}" for v in arr.to_pylist()], pa.string())


def _mask(rng: np.random.Generator, n: int, share: float) -> pa.Array:
    return pa.array(rng.random(n) < share)


def transform_batch(
    batch: pa.RecordBatch, op: str, params: dict, rng: np.random.Generator
) -> pa.RecordBatch:
    """One file fault applied to one batch. Pure, so it is unit-testable."""
    if op == "rename":
        mapping = params["rename"]
        return batch.rename_columns([mapping.get(n, n) for n in batch.schema.names])

    col = params["column"]
    i = batch.schema.get_field_index(col)
    arr = batch.column(i)
    if op == "currency_strings":
        new = _fmt_currency(arr, params.get("thousands", False))
    elif op == "plain_strings":
        new = _to_string(arr)
    elif op == "null_share":
        new = pc.if_else(_mask(rng, len(arr), params["share"]), pa.scalar(None, arr.type), arr)
    elif op == "set_value_share":
        value = pa.scalar(params["value"], arr.type)
        new = pc.if_else(_mask(rng, len(arr), params["share"]), value, arr)
    else:
        raise InjectionError(f"unknown file op {op!r}")
    return batch.set_column(i, pa.field(col, new.type), new)


def _apply_file(sc: Scenario) -> dict:
    op = sc.params["op"]
    dest = _landing_trips(sc.month)
    if op == "remove":
        dest.unlink(missing_ok=True)
        return {"op": op, "path": dest.as_posix()}
    if op == "empty":
        dest.write_bytes(b"")
        return {"op": op, "path": dest.as_posix()}

    rng = _rng(sc)
    pf = pq.ParquetFile(_clean_trips(sc.month))
    tmp = dest.with_suffix(".parquet.part")
    writer = None
    rows = 0
    try:
        for batch in pf.iter_batches(batch_size=BATCH_SIZE):
            out = transform_batch(batch, op, sc.params, rng)
            if writer is None:
                writer = pq.ParquetWriter(tmp, out.schema)
            writer.write_batch(out)
            rows += out.num_rows
    finally:
        if writer is not None:
            writer.close()
    tmp.replace(dest)
    return {"op": op, "path": dest.as_posix(), "rows": rows}


# ------------------------------------------------------------- sql mechanism

_TRIP_COLS = (
    "pickup_month, vendorid, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, "
    "trip_distance, ratecodeid, store_and_fwd_flag, pulocationid, dolocationid, payment_type, "
    "fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, "
    "congestion_surcharge, airport_fee, cbd_congestion_fee, request_source"
)


def _warehouse() -> psycopg.Connection:
    return psycopg.connect(
        get_settings().dsn("pipeline", "warehouse", from_host=True), autocommit=True
    )


def _apply_sql(sc: Scenario) -> dict:
    # Looks like a second load of the same rows: a normal Airflow manual run id.
    run_id = f"manual__{datetime.now(UTC).isoformat()}"
    with _warehouse() as conn:
        loaded = conn.execute(
            "SELECT count(*) FROM raw.yellow_trips WHERE pickup_month = %s", (sc.month,)
        ).fetchone()[0]
        if loaded == 0:
            raise InjectionError(f"raw.yellow_trips has no rows for {sc.month}; load it first")
        inserted = conn.execute(
            f"INSERT INTO raw.yellow_trips ({_TRIP_COLS}, _run_id) "
            f"SELECT {_TRIP_COLS}, %s FROM raw.yellow_trips WHERE pickup_month = %s "
            "ORDER BY md5(tpep_pickup_datetime::text || pulocationid::text) LIMIT %s",
            (run_id, sc.month, sc.params["rows"]),
        ).rowcount
    return {"run_id": run_id, "rows": inserted}


def _revert_sql(sc: Scenario) -> dict:
    run_id = _last_apply_details(sc.id).get("run_id")
    if not run_id:
        return {"deleted": 0}
    with _warehouse() as conn:
        deleted = conn.execute(
            "DELETE FROM raw.yellow_trips WHERE pickup_month = %s AND _run_id = %s",
            (sc.month, run_id),
        ).rowcount
    return {"run_id": run_id, "deleted": deleted}


# ----------------------------------------------------------- proxy mechanism


def _proxy(body: dict) -> dict:
    s = get_settings()
    r = httpx.post(
        f"http://localhost:{s.weather_proxy_host_port}/_control",
        json=body,
        headers={"X-Control-Token": s.weather_proxy_control_token},
        timeout=10,
    )
    r.raise_for_status()
    return r.json()


def _apply_proxy(sc: Scenario) -> dict:
    body = {
        k: v
        for k, v in sc.params.items()
        if k in ("mode", "duration_s", "drop_hours_per_day", "drop_every_nth_day")
    }
    return {"control": body, "proxy": _proxy(body)}


# ---------------------------------------------------- airflow_conn mechanism


def _apply_conn(sc: Scenario) -> dict:
    s = get_settings()
    conn = {**WAREHOUSE_CONN, "password": s.pipeline_db_password}
    if sc.params["break"] == "password":
        conn["password"] = "rotated-" + sc.id.lower()
    elif sc.params["break"] == "login":
        conn["login"] = sc.params["login"]
    AirflowClient().upsert_connection(conn)
    return {"break": sc.params["break"]}  # never record the secret itself


def restore_connection() -> None:
    conn = {**WAREHOUSE_CONN, "password": get_settings().pipeline_db_password}
    AirflowClient().upsert_connection(conn)


# ---------------------------------------------------------------- dispatch


def apply(sc: Scenario, eval_run_id: str | None = None) -> dict:
    release(sc.month)
    details = {
        "file": _apply_file,
        "sql": _apply_sql,
        "proxy": _apply_proxy,
        "airflow_conn": _apply_conn,
    }[sc.mechanism](sc)
    _record(sc, "apply", details, eval_run_id)
    return details


def revert(sc: Scenario, eval_run_id: str | None = None) -> dict:
    release(sc.month)  # restores the clean landing file and weather cache
    details: dict[str, Any] = {"released": sc.month}
    if sc.mechanism == "sql":
        details |= _revert_sql(sc)
    elif sc.mechanism == "proxy":
        details["proxy"] = _proxy({"mode": "normal", "duration_s": 1})
    elif sc.mechanism == "airflow_conn":
        restore_connection()
        details["connection"] = "restored"
    _record(sc, "revert", details, eval_run_id)
    return details


def reset_all() -> None:
    """Back to a clean platform: every month released, proxy normal, connection restored,
    injected duplicate rows deleted."""
    for month in MONTHS:
        release(month)
    _proxy({"mode": "normal", "duration_s": 1})
    restore_connection()
    for sc in load_all():
        if sc.mechanism == "sql":
            _revert_sql(sc)


def main() -> int:
    parser = argparse.ArgumentParser(prog="python -m faults.injector")
    sub = parser.add_subparsers(dest="cmd", required=True)
    for cmd in ("apply", "revert"):
        p = sub.add_parser(cmd)
        p.add_argument("scenario_id")
        p.add_argument("--eval-run-id")
    sub.add_parser("release").add_argument("month")
    sub.add_parser("reset")
    args = parser.parse_args()

    try:
        if args.cmd == "apply":
            print(json.dumps(apply(load_scenario(args.scenario_id), args.eval_run_id)))
        elif args.cmd == "revert":
            print(json.dumps(revert(load_scenario(args.scenario_id), args.eval_run_id)))
        elif args.cmd == "release":
            release(args.month)
            print(f"released {args.month}")
        else:
            reset_all()
            print("reset: all months released, proxy normal, connection restored")
    except (InjectionError, FileNotFoundError) as exc:
        print(f"INJECTION FAILED: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
