"""Profile the sampled data and write the baselines the contracts are built from.

Run with: make profile   (or: uv run python -m scripts.profile_data)

Writes docs/data_profile.md (for people) and docs/data_profile.json (for code).
Reads Parquet in 100k-row batches, so memory stays flat.
"""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.compute as pc
import pyarrow.parquet as pq

from scripts.seed import BATCH_SIZE, MONTHS, TRIPS_SAMPLED, WEATHER, ZONES

NATURAL_KEY = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "total_amount",
    "payment_type",  # upstream re-sends some trips with payment_type 0; see data_profile.md
]
UNKNOWN_ZONES = {264, 265}  # present in the lookup, but mean "Unknown" / "N/A"
OUT_MD = Path("docs/data_profile.md")
OUT_JSON = Path("docs/data_profile.json")


def _zone_ids() -> set[int]:
    return set(pd.read_csv(ZONES)["LocationID"].astype(int))


def profile_trips(month: str, zones: set[int]) -> dict:
    pf = pq.ParquetFile(TRIPS_SAMPLED / f"yellow_tripdata_{month}.parquet")
    nulls: Counter[str] = Counter()
    c: Counter[str] = Counter()
    mins: dict[str, float] = {}
    maxs: dict[str, float] = {}
    key_hashes: list[np.ndarray] = []
    zone_arr = np.fromiter(zones, dtype=np.int64)
    month_start = pd.Timestamp(f"{month}-01")
    month_end = month_start + pd.offsets.MonthBegin(1)

    for batch in pf.iter_batches(batch_size=BATCH_SIZE):
        n = batch.num_rows
        c["rows"] += n
        for name in batch.schema.names:
            nulls[name] += batch.column(name).null_count

        df = batch.select(
            NATURAL_KEY + ["passenger_count", "trip_distance", "fare_amount"]
        ).to_pandas()
        c["fare_negative"] += int((df.fare_amount < 0).sum())
        c["total_negative"] += int((df.total_amount < 0).sum())
        c["distance_gt_100"] += int((df.trip_distance > 100).sum())
        c["distance_zero"] += int((df.trip_distance == 0).sum())
        c["passenger_null_or_zero"] += int(
            (df.passenger_count.isna() | (df.passenger_count == 0)).sum()
        )
        for col in ("PULocationID", "DOLocationID"):
            ids = df[col].to_numpy(dtype=np.int64)
            c[f"{col}_not_in_lookup"] += int((~np.isin(ids, zone_arr)).sum())
            c[f"{col}_unknown_zone"] += int(np.isin(ids, list(UNKNOWN_ZONES)).sum())
        pickup = df.tpep_pickup_datetime
        c["pickup_outside_month"] += int(((pickup < month_start) | (pickup >= month_end)).sum())
        c["dropoff_before_pickup"] += int((df.tpep_dropoff_datetime < pickup).sum())

        for col in ("tpep_pickup_datetime", "fare_amount", "trip_distance", "total_amount"):
            mm = pc.min_max(batch.column(col))
            lo, hi = mm["min"].as_py(), mm["max"].as_py()
            mins[col] = lo if col not in mins or lo < mins[col] else mins[col]
            maxs[col] = hi if col not in maxs or hi > maxs[col] else maxs[col]

        key_hashes.append(pd.util.hash_pandas_object(df[NATURAL_KEY], index=False).to_numpy())

    hashes = np.concatenate(key_hashes)
    rows = c["rows"]
    duplicates = rows - len(np.unique(hashes))
    rate = lambda k: c[k] / rows  # noqa: E731

    return {
        "month": month,
        "rows": rows,
        "columns": pf.schema_arrow.names,
        "types": {f.name: str(f.type) for f in pf.schema_arrow},
        "null_rate": {k: nulls[k] / rows for k in pf.schema_arrow.names},
        "fare_negative_rate": rate("fare_negative"),
        "total_negative_rate": rate("total_negative"),
        "distance_gt_100_rate": rate("distance_gt_100"),
        "distance_zero_rate": rate("distance_zero"),
        "passenger_null_or_zero_rate": rate("passenger_null_or_zero"),
        "pu_not_in_lookup_rate": rate("PULocationID_not_in_lookup"),
        "do_not_in_lookup_rate": rate("DOLocationID_not_in_lookup"),
        "pu_unknown_zone_rate": rate("PULocationID_unknown_zone"),
        "do_unknown_zone_rate": rate("DOLocationID_unknown_zone"),
        "pickup_outside_month_rate": rate("pickup_outside_month"),
        "dropoff_before_pickup_rate": rate("dropoff_before_pickup"),
        "duplicate_rate": duplicates / rows,
        "pickup_min": str(mins["tpep_pickup_datetime"]),
        "pickup_max": str(maxs["tpep_pickup_datetime"]),
        "fare_min": mins["fare_amount"],
        "fare_max": maxs["fare_amount"],
        "distance_max": maxs["trip_distance"],
        "total_min": mins["total_amount"],
        "total_max": maxs["total_amount"],
    }


def profile_weather(month: str) -> dict:
    payload = json.loads((WEATHER / f"{month}.json").read_text(encoding="utf-8"))
    df = pd.DataFrame(payload["hourly"])
    df["time"] = pd.to_datetime(df["time"])
    per_day = df.groupby(df.time.dt.date).size()
    out: dict = {
        "month": month,
        "rows": len(df),
        "days": int(per_day.size),
        "days_not_24h": int((per_day != 24).sum()),
        "duplicate_hours": int(df.time.duplicated().sum()),
        "nulls": int(df.drop(columns="time").isna().sum().sum()),
        "units": payload.get("hourly_units", {}),
    }
    for col in df.columns.drop("time"):
        out[col] = {"min": float(df[col].min()), "max": float(df[col].max())}
    return out


def _pct(x: float) -> str:
    return f"{x * 100:.3f}%"


def render_md(trips: list[dict], weather: list[dict]) -> str:
    months = [t["month"] for t in trips]
    head = "| Check | " + " | ".join(months) + " |\n|---|" + "---|" * len(months) + "\n"

    def row(label: str, key: str, fmt=_pct) -> str:
        return f"| {label} | " + " | ".join(fmt(t[key]) for t in trips) + " |\n"

    md = [
        "# Data profile\n\n",
        "Generated by `make profile` from the 20% samples in `data/raw_cache/sampled/` "
        "(every fifth row). These baselines set the contract thresholds in `contracts/`. "
        "Rates are shares of rows.\n\n",
        "\n## Yellow taxi trips\n\n",
        head,
        row("Rows (sampled)", "rows", lambda v: f"{v:,}"),
        row("Pickup min", "pickup_min", str),
        row("Pickup max", "pickup_max", str),
        row("Pickup outside the month", "pickup_outside_month_rate"),
        row("Dropoff before pickup", "dropoff_before_pickup_rate"),
        row("`fare_amount` < 0", "fare_negative_rate"),
        row("`total_amount` < 0", "total_negative_rate"),
        row("`fare_amount` min", "fare_min", lambda v: f"{v:,.2f}"),
        row("`fare_amount` max", "fare_max", lambda v: f"{v:,.2f}"),
        row("`trip_distance` > 100", "distance_gt_100_rate"),
        row("`trip_distance` = 0", "distance_zero_rate"),
        row("`trip_distance` max", "distance_max", lambda v: f"{v:,.2f}"),
        row("`passenger_count` null or 0", "passenger_null_or_zero_rate"),
        row("`PULocationID` not in zone lookup", "pu_not_in_lookup_rate"),
        row("`DOLocationID` not in zone lookup", "do_not_in_lookup_rate"),
        row("`PULocationID` unknown zone (264/265)", "pu_unknown_zone_rate"),
        row("`DOLocationID` unknown zone (264/265)", "do_unknown_zone_rate"),
        row("Duplicate rate on natural key", "duplicate_rate"),
        "\nNatural key: `" + ", ".join(NATURAL_KEY) + "`.\n",
        "\n### Null rate per column\n\n",
        head,
    ]
    for col in trips[0]["columns"]:
        md.append(f"| `{col}` | " + " | ".join(_pct(t["null_rate"][col]) for t in trips) + " |\n")

    md += ["\n### Column types\n\n", "| Column | Type |\n|---|---|\n"]
    for col, typ in trips[0]["types"].items():
        md.append(f"| `{col}` | `{typ}` |\n")

    wmonths = [w["month"] for w in weather]
    md += [
        "\n## Hourly weather (Open-Meteo, NYC)\n\n",
        "| Check | " + " | ".join(wmonths) + " |\n|---|" + "---|" * len(wmonths) + "\n",
    ]
    for label, key in [
        ("Rows", "rows"),
        ("Days", "days"),
        ("Days without 24 hours", "days_not_24h"),
        ("Duplicate hours", "duplicate_hours"),
        ("Null values", "nulls"),
    ]:
        md.append(f"| {label} | " + " | ".join(str(w[key]) for w in weather) + " |\n")
    for var in ("temperature_2m", "precipitation", "rain", "snowfall", "wind_speed_10m"):
        unit = weather[0]["units"].get(var, "")
        md.append(
            f"| `{var}` min / max ({unit}) | "
            + " | ".join(f"{w[var]['min']:g} / {w[var]['max']:g}" for w in weather)
            + " |\n"
        )
    md.append(FINDINGS)
    return "".join(md)


FINDINGS = """
## Findings that shape the contracts and dbt tests

| Finding | Decision |
|---|---|
| `passenger_count` (and `RatecodeID`, `store_and_fwd_flag`, `congestion_surcharge`, `Airport_fee`) is null on 26-28% of rows | Contract `max_null_rate: 0.35`; F04 (~40% of rows nulled) lands near 56% |
| Some trips are delivered twice, identical except `payment_type` 1 vs 0 | `payment_type` joins the natural key; duplicate rate is then 0% in all months |
| 0.36-0.42% of fares are negative (refunds/voids), min near -1000 | dbt negative-fare test errors above 2%; F06 (-999 on ~5%) exceeds it |
| `trip_distance` has extreme outliers (max > 200,000) on <0.01% of rows | dbt `accepted_range` 0-200 warns; it is not a fault signal |
| Every zone id is in the lookup; 264/265 ("Unknown"/"N/A") appear on <1% | dbt `relationships` test errors on any orphan; F07 (zone 999) fails it |
| A few pickups fall outside the file's month (as early as 2008) | Contract allows up to 0.1%; staging filters to the month |
| New 2026 columns `cbd_congestion_fee`, `request_source` | `allow_extra_columns: true`; logged, not fatal |
| Weather: 24 hours every day, no gaps, no nulls | Contract requires exactly that; F09 (dropped hours) fails it |

Value rules live in dbt rather than the contract, so each fault fails at the task
`faults/catalog.md` names: F01/F02/F04 in `validate_contract`, F05-F07 in `dbt_build`.
"""


def main() -> None:
    zones = _zone_ids()
    trips = [profile_trips(m, zones) for m in MONTHS]
    weather = [profile_weather(m) for m in MONTHS]
    OUT_MD.parent.mkdir(exist_ok=True)
    OUT_JSON.write_text(json.dumps({"trips": trips, "weather": weather}, indent=2) + "\n")
    OUT_MD.write_text(render_md(trips, weather), encoding="utf-8")
    print(f"wrote {OUT_MD} and {OUT_JSON}")


if __name__ == "__main__":
    main()
