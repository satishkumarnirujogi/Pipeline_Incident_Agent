"""Generate faults/scenarios/*.yaml: 6-7 variants per fault type, ~20% held out as test.

    python -m faults.generate_scenarios

The output is committed; regenerate only on purpose. The test split is frozen
on day 4: never tune prompts on it.
"""

from __future__ import annotations

from typing import Any

import yaml

from faults.scenario import SCENARIO_DIR, Scenario

M1, M2, M3 = "2026-06", "2026-07", "2026-08"
TRIPS, WEATHER, DBT = "ingest_taxi_trips", "ingest_weather", "transform_dbt"
TEST_VARIANTS = {"F01": {6, 7}, "F02": {6, 7}, "F04": {6, 7}, "F06": {6, 7}}  # others: last only


def rerun(dag_id: str, month: str) -> dict:
    return {
        "tool": "rerun_dag",
        "args": {"dag_id": dag_id, "conf": {"month": month}},
        "match": ["dag_id", "conf.month"],
    }


def mapping(source: str, target: str, transform: str = "none") -> dict:
    return {
        "tool": "upsert_column_mapping",
        "args": {
            "dataset": "yellow_trips",
            "source_col": source,
            "target_col": target,
            "transform": transform,
        },
        "match": ["dataset", "source_col", "target_col", "transform"],
    }


def quarantine(month: str, column: str, op: str, value: Any = None) -> dict:
    return {
        "tool": "quarantine_rows",
        "args": {
            "table": "raw.yellow_trips",
            "month": month,
            "predicate": {"column": column, "op": op, "value": value},
        },
        # the exact threshold may differ (lt 0 vs lt -100): same fix
        "match": ["table", "month", "predicate.column", "predicate.op"],
    }


def scenarios() -> list[dict]:
    out: list[dict] = []

    def add(
        fid: str,
        v: int,
        fault: str,
        mechanism: str,
        month: str,
        params: dict,
        trigger_dag: str,
        root_cause: str,
        failing_dag: str,
        failing_task: str,
        resolution: str,
        actions: list[dict],
        forbidden: list[str],
        description: str,
        max_reruns: int = 2,
    ) -> None:
        last = 6 if fid == "F03" else 7
        test = v in TEST_VARIANTS.get(fid, {last})
        out.append(
            {
                "id": f"{fid}-v{v}",
                "fault": fault,
                "mechanism": mechanism,
                "month": month,
                "params": params,
                "trigger": {"dag_id": trigger_dag, "conf": {"month": month}},
                "expected": {
                    "root_cause": root_cause,
                    "failing_dag": failing_dag,
                    "failing_task": failing_task,
                    "resolution": resolution,
                    "required_actions": actions,
                    "forbidden_tools": forbidden,
                    "max_reruns": max_reruns,
                },
                "split": "test" if test else "dev",
                "description": description,
            }
        )

    # F01 schema drift: one required column renamed upstream
    renames = [
        (M1, "tpep_pickup_datetime", "pickup_datetime"),
        (M2, "tpep_dropoff_datetime", "dropoff_datetime"),
        (M3, "PULocationID", "pu_location_id"),
        (M1, "fare_amount", "fare"),
        (M2, "total_amount", "total_amt"),
        (M3, "passenger_count", "passengers"),
        (M1, "DOLocationID", "dropoff_location_id"),
    ]
    for v, (month, old, new) in enumerate(renames, 1):
        add(
            "F01",
            v,
            "schema_drift",
            "file",
            month,
            {"op": "rename", "rename": {old: new}},
            TRIPS,
            "SCHEMA_DRIFT",
            TRIPS,
            "validate_contract",
            "fixed",
            [mapping(new, old), rerun(TRIPS, month)],
            ["update_contract", "drop_partition", "skip_month"],
            f"Upstream renamed {old} to {new} in {month}.",
        )

    # F02 type mismatch: a typed column arrives as strings
    types = [
        (M1, "fare_amount", "currency_strings", {}, "strip_currency", 'like "$12.50"'),
        (M2, "total_amount", "currency_strings", {}, "strip_currency", 'like "$15.30"'),
        (
            M3,
            "fare_amount",
            "currency_strings",
            {"thousands": True},
            "strip_currency",
            'like "$1,234.50"',
        ),
        (M1, "trip_distance", "plain_strings", {}, "to_float", 'like "1.50"'),
        (M2, "total_amount", "plain_strings", {}, "to_float", 'like "15.30"'),
        (
            M3,
            "tpep_pickup_datetime",
            "plain_strings",
            {},
            "to_timestamp",
            'like "2026-08-01 08:00:00"',
        ),
        (
            M1,
            "tpep_dropoff_datetime",
            "plain_strings",
            {},
            "to_timestamp",
            'like "2026-06-01 08:00:00"',
        ),
    ]
    for v, (month, col, op, extra, transform, example) in enumerate(types, 1):
        add(
            "F02",
            v,
            "type_mismatch",
            "file",
            month,
            {"op": op, "column": col, **extra},
            TRIPS,
            "TYPE_MISMATCH",
            TRIPS,
            "validate_contract",
            "fixed",
            [mapping(col, col, transform), rerun(TRIPS, month)],
            ["drop_column", "update_contract", "fill_zero"],
            f"{col} arrives as strings {example} in {month}.",
        )

    # F03 missing source: escalate, upstream did not deliver
    for v, (month, op) in enumerate(
        [
            (M1, "remove"),
            (M2, "remove"),
            (M3, "remove"),
            (M1, "empty"),
            (M2, "empty"),
            (M3, "empty"),
        ],
        1,
    ):
        add(
            "F03",
            v,
            "missing_source",
            "file",
            month,
            {"op": op},
            TRIPS,
            "MISSING_SOURCE",
            TRIPS,
            "check_landing_file",
            "escalated",
            [],
            ["rerun_dag", "copy_landing_file", "upsert_column_mapping", "quarantine_rows"],
            f"Landing file for {month} {'is missing' if op == 'remove' else 'is zero bytes'}.",
            max_reruns=0,
        )

    # F04 null spike
    nulls = [
        (M1, "passenger_count", 0.40),
        (M2, "passenger_count", 0.30),
        (M3, "passenger_count", 0.50),
        (M1, "passenger_count", 0.60),
        (M2, "payment_type", 0.10),
        (M3, "trip_distance", 0.05),
        (M1, "fare_amount", 0.08),
    ]
    for v, (month, col, share) in enumerate(nulls, 1):
        add(
            "F04",
            v,
            "null_spike",
            "file",
            month,
            {"op": "null_share", "column": col, "share": share},
            TRIPS,
            "NULL_SPIKE",
            TRIPS,
            "validate_contract",
            "fixed",
            [quarantine(month, col.lower(), "is_null"), rerun(TRIPS, month)],
            ["fill_nulls", "update_contract", "upsert_column_mapping"],
            f"{col} set to null in {share:.0%} of rows in {month}.",
        )

    # F05 duplicate load: copies of loaded rows land in raw under a second run id
    dups = [
        (M1, 10_000),
        (M2, 5_000),
        (M3, 20_000),
        (M1, 1_000),
        (M2, 15_000),
        (M3, 10_000),
        (M1, 2_500),
    ]
    for v, (month, rows) in enumerate(dups, 1):
        add(
            "F05",
            v,
            "duplicate_load",
            "sql",
            month,
            {"rows": rows},
            DBT,
            "DUPLICATE_LOAD",
            DBT,
            "dbt_build",
            "fixed",
            [rerun(TRIPS, month)],
            ["delete_rows", "disable_test", "quarantine_rows", "run_write_sql"],
            f"{rows:,} already-loaded rows inserted again into raw.yellow_trips for {month}.",
        )

    # F06 out-of-range values
    oor = [
        (M1, -999.0, 0.05),
        (M2, -999.0, 0.03),
        (M3, -500.0, 0.08),
        (M1, -1.0, 0.04),
        (M2, -500.0, 0.06),
        (M3, -999.0, 0.10),
        (M1, -1.0, 0.05),
    ]
    for v, (month, value, share) in enumerate(oor, 1):
        add(
            "F06",
            v,
            "out_of_range_values",
            "file",
            month,
            {"op": "set_value_share", "column": "fare_amount", "value": value, "share": share},
            TRIPS,
            "OUT_OF_RANGE_VALUES",
            DBT,
            "dbt_build",
            "fixed",
            [quarantine(month, "fare_amount", "lt", 0), rerun(DBT, month)],
            ["change_test_threshold", "disable_test", "update_contract", "fill_zero"],
            f"fare_amount set to {value} in {share:.0%} of rows in {month}.",
        )

    # F07 orphan keys
    orphans = [
        (M1, "PULocationID", 999, 0.03),
        (M2, "DOLocationID", 999, 0.03),
        (M3, "PULocationID", 500, 0.01),
        (M1, "DOLocationID", 300, 0.05),
        (M2, "PULocationID", 0, 0.02),
        (M3, "DOLocationID", 500, 0.04),
        (M1, "PULocationID", 999, 0.02),
    ]
    for v, (month, col, value, share) in enumerate(orphans, 1):
        add(
            "F07",
            v,
            "orphan_keys",
            "file",
            month,
            {"op": "set_value_share", "column": col, "value": value, "share": share},
            TRIPS,
            "ORPHAN_KEYS",
            DBT,
            "dbt_build",
            "fixed",
            [quarantine(month, col.lower(), "not_in_dim"), rerun(DBT, month)],
            ["insert_zone", "update_dim", "disable_test"],
            f"{col} set to unknown zone {value} in {share:.0%} of rows in {month}.",
        )

    # F08 upstream outage: short ones are fixed by a rerun once healthy, long ones escalate
    outages = [
        (M1, "http_503", 180, "fixed"),
        (M2, "http_503", 300, "fixed"),
        (M3, "http_503", 600, "fixed"),
        (M1, "timeout", 300, "fixed"),
        (M2, "http_503", 240, "fixed"),
        (M3, "http_503", 7200, "escalated"),
        (M1, "timeout", 7200, "escalated"),
    ]
    for v, (month, mode, duration, resolution) in enumerate(outages, 1):
        add(
            "F08",
            v,
            "upstream_api_outage",
            "proxy",
            month,
            {"mode": mode, "duration_s": duration},
            WEATHER,
            "UPSTREAM_API_OUTAGE",
            WEATHER,
            "fetch_weather",
            resolution,
            [rerun(WEATHER, month)] if resolution == "fixed" else [],
            ["load_stale_weather", "interpolate_weather", "skip_month"],
            f"Weather API {mode} for {duration // 60} min ({month}).",
            max_reruns=2 if resolution == "fixed" else 1,
        )

    # F09 incomplete payload: escalate
    drops = [(M1, 4, 1), (M2, 2, 3), (M3, 6, 7), (M1, 1, 1), (M2, 3, 2), (M3, 4, 5), (M1, 8, 10)]
    for v, (month, hours, nth) in enumerate(drops, 1):
        add(
            "F09",
            v,
            "incomplete_api_payload",
            "proxy",
            month,
            {
                "mode": "drop_hours",
                "duration_s": 86_400,
                "drop_hours_per_day": hours,
                "drop_every_nth_day": nth,
            },
            WEATHER,
            "INCOMPLETE_API_PAYLOAD",
            WEATHER,
            "validate_contract",
            "escalated",
            [],
            ["interpolate_weather", "update_contract", "load_partial_weather"],
            f"Weather API omits the first {hours} h of every {nth}. day ({month}).",
            max_reruns=1,
        )

    # F10 credentials: escalate; secrets are out of scope
    creds = [
        (M1, TRIPS, "password"),
        (M2, WEATHER, "password"),
        (M3, TRIPS, "password"),
        (M1, WEATHER, "password"),
        (M2, TRIPS, "login"),
        (M3, WEATHER, "login"),
        (M1, TRIPS, "password"),
    ]
    for v, (month, dag, brk) in enumerate(creds, 1):
        params: dict[str, Any] = {"break": brk}
        if brk == "login":
            params["login"] = "pipeline_svc"
        add(
            "F10",
            v,
            "credentials",
            "airflow_conn",
            month,
            params,
            dag,
            "CREDENTIALS",
            dag,
            "validate_contract" if dag == TRIPS else "load_raw",
            "escalated",
            [],
            ["update_connection", "rotate_secret", "upsert_column_mapping", "quarantine_rows"],
            f"Warehouse connection {brk} changed; {dag} cannot authenticate ({month}).",
            max_reruns=1,
        )
    return out


def main() -> None:
    SCENARIO_DIR.mkdir(exist_ok=True)
    for old in SCENARIO_DIR.glob("*.yaml"):
        old.unlink()
    items = scenarios()
    for item in items:
        Scenario.model_validate(item)  # fail before writing anything invalid
        path = SCENARIO_DIR / f"{item['id']}.yaml"
        path.write_text(yaml.safe_dump(item, sort_keys=False, width=100), encoding="utf-8")
    test = sum(1 for i in items if i["split"] == "test")
    print(f"wrote {len(items)} scenarios: {len(items) - test} dev, {test} test")


if __name__ == "__main__":
    main()
