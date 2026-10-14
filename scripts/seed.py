"""Download the sources once, keep a fixed 20% sample, write a manifest, seed landing.

Run with: make seed   (or: uv run python -m scripts.seed)

- Skips anything already on disk, so a second run is a no-op.
- Retries HTTP errors with exponential backoff; fails loudly if a file is
  missing or empty.
- Samples every fifth row of each month in 100k-row batches, so memory stays
  flat (low-memory mode); never loads a whole month at once.
- Copies month 1 into data/landing and the proxy cache. Months 2 and 3 are
  released later by the eval runner.
"""

from __future__ import annotations

import argparse
import calendar
import hashlib
import json
import shutil
import sys
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pyarrow.parquet as pq
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

MONTHS = ["2026-06", "2026-07", "2026-08"]
SAMPLE_EVERY = 5  # keep every fifth row = 20%
BATCH_SIZE = 100_000

DATA = Path("data")
RAW = DATA / "raw_cache"
TRIPS_FULL = RAW / "trips"
TRIPS_SAMPLED = RAW / "sampled"
WEATHER = RAW / "weather"
ZONES = RAW / "taxi_zone_lookup.csv"
MANIFEST = RAW / "manifest.json"
LANDING_TRIPS = DATA / "landing" / "trips"
LANDING_ZONES = DATA / "landing" / "zones"
PROXY_CACHE = DATA / "proxy_cache" / "weather"

TLC = "https://d37ci6vzurychx.cloudfront.net"
OPEN_METEO = "https://archive-api.open-meteo.com/v1/archive"
WEATHER_PARAMS = {
    "latitude": 40.71,
    "longitude": -74.01,
    "timezone": "America/New_York",
    "hourly": "temperature_2m,precipitation,rain,snowfall,wind_speed_10m",
}


class SeedError(RuntimeError):
    pass


_retry = retry(
    retry=retry_if_exception_type((httpx.TransportError, httpx.HTTPStatusError)),
    wait=wait_exponential(multiplier=2, max=60),
    stop=stop_after_attempt(5),
    reraise=True,
)


def _log(msg: str) -> None:
    print(msg, flush=True)


@_retry
def _download(url: str, dest: Path, params: dict | None = None) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    with httpx.stream("GET", url, params=params, timeout=120, follow_redirects=True) as r:
        if r.status_code in (403, 404):
            # CloudFront answers 403 for files that do not exist; retrying will not help.
            raise SeedError(f"{url} not found (HTTP {r.status_code})")
        r.raise_for_status()
        with tmp.open("wb") as f:
            for chunk in r.iter_bytes(1 << 20):
                f.write(chunk)
    if tmp.stat().st_size == 0:
        tmp.unlink()
        raise SeedError(f"{url} returned an empty body")
    tmp.replace(dest)


def _month_range(month: str) -> tuple[str, str, int]:
    year, mon = map(int, month.split("-"))
    days = calendar.monthrange(year, mon)[1]
    return f"{month}-01", f"{month}-{days:02d}", days


def _sample(src: Path, dest: Path) -> None:
    """Keep rows 0, 5, 10, ... of the whole file, batch by batch."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(".parquet.part")
    pf = pq.ParquetFile(src)
    offset = 0
    with pq.ParquetWriter(tmp, pf.schema_arrow) as writer:
        for batch in pf.iter_batches(batch_size=BATCH_SIZE):
            first = (-offset) % SAMPLE_EVERY
            idx = list(range(first, batch.num_rows, SAMPLE_EVERY))
            if idx:
                writer.write_batch(batch.take(idx))
            offset += batch.num_rows
    tmp.replace(dest)


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _rows(path: Path) -> int:
    if path.suffix == ".parquet":
        return pq.ParquetFile(path).metadata.num_rows
    if path.suffix == ".csv":
        with path.open(encoding="utf-8") as f:
            return sum(1 for _ in f) - 1
    if path.suffix == ".json":
        return len(json.loads(path.read_text(encoding="utf-8"))["hourly"]["time"])
    raise ValueError(path)


def _check_weather(path: Path, month: str) -> None:
    _, _, days = _month_range(month)
    hours = len(json.loads(path.read_text(encoding="utf-8"))["hourly"]["time"])
    if hours != days * 24:
        path.unlink()
        raise SeedError(f"weather {month}: expected {days * 24} hourly rows, got {hours}")


def fetch(months: list[str]) -> list[str]:
    """Download and sample whatever is missing. Returns what was done."""
    done: list[str] = []
    if not ZONES.exists():
        _log("download taxi_zone_lookup.csv")
        _download(f"{TLC}/misc/taxi_zone_lookup.csv", ZONES)
        done.append(str(ZONES))

    for month in months:
        full = TRIPS_FULL / f"yellow_tripdata_{month}.parquet"
        sampled = TRIPS_SAMPLED / f"yellow_tripdata_{month}.parquet"
        weather = WEATHER / f"{month}.json"

        if not full.exists() and not sampled.exists():
            _log(f"download yellow_tripdata_{month}.parquet")
            _download(f"{TLC}/trip-data/yellow_tripdata_{month}.parquet", full)
            done.append(str(full))
        if not sampled.exists():
            _log(f"sample {month} (every {SAMPLE_EVERY}th row)")
            _sample(full, sampled)
            done.append(str(sampled))

        if not weather.exists():
            start, end, _ = _month_range(month)
            _log(f"download weather {month}")
            _download(
                OPEN_METEO, weather, params={**WEATHER_PARAMS, "start_date": start, "end_date": end}
            )
            _check_weather(weather, month)
            done.append(str(weather))
    return done


def write_manifest(months: list[str]) -> None:
    old = {}
    if MANIFEST.exists():
        old = {e["path"]: e for e in json.loads(MANIFEST.read_text(encoding="utf-8"))["files"]}

    paths = [ZONES]
    for month in months:
        full = TRIPS_FULL / f"yellow_tripdata_{month}.parquet"
        paths += [p for p in (full,) if p.exists()]
        paths += [TRIPS_SAMPLED / f"yellow_tripdata_{month}.parquet", WEATHER / f"{month}.json"]

    entries = []
    for p in paths:
        if not p.exists() or p.stat().st_size == 0:
            raise SeedError(f"expected file missing or empty: {p}")
        key = p.as_posix()
        prev = old.get(key)
        if prev and prev["size_bytes"] == p.stat().st_size:
            entries.append(prev)  # unchanged; skip re-hashing
            continue
        entries.append(
            {
                "path": key,
                "file": p.name,
                "kind": p.parent.name if p.parent != RAW else "zones",
                "size_bytes": p.stat().st_size,
                "rows": _rows(p),
                "sha256": _sha256(p),
                "recorded_at": datetime.now(UTC).isoformat(timespec="seconds"),
            }
        )

    manifest = {"months": months, "sample_every": SAMPLE_EVERY, "files": entries}
    if not MANIFEST.exists() or json.loads(MANIFEST.read_text(encoding="utf-8")) != manifest:
        MANIFEST.write_text(json.dumps(manifest, indent=2) + "\n")
        _log(f"wrote {MANIFEST}")


def seed_landing(months: list[str]) -> list[str]:
    """Month 1 only: trips into landing, weather into the proxy cache, zones into landing."""
    month = months[0]
    copies = [
        (
            TRIPS_SAMPLED / f"yellow_tripdata_{month}.parquet",
            LANDING_TRIPS / f"yellow_tripdata_{month}.parquet",
        ),
        (WEATHER / f"{month}.json", PROXY_CACHE / f"{month}.json"),
        (ZONES, LANDING_ZONES / ZONES.name),
    ]
    done = []
    for src, dest in copies:
        if not dest.exists():
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dest)
            done.append(str(dest))
    return done


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--months", nargs="+", default=MONTHS)
    args = parser.parse_args()
    try:
        done = fetch(args.months)
        write_manifest(args.months)
        done += seed_landing(args.months)
    except SeedError as exc:
        print(f"SEED FAILED: {exc}", file=sys.stderr)
        return 1
    _log("nothing to do; everything already seeded" if not done else f"done: {len(done)} steps")
    return 0


if __name__ == "__main__":
    sys.exit(main())
