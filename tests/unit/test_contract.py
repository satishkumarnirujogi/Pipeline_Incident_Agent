from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from common.contract import (
    ColumnMapping,
    ContractViolation,
    apply_mappings,
    load_contract,
    validate_trips,
    validate_weather,
)

CONTRACTS = Path(__file__).parents[2] / "contracts"
MONTH = "2026-06"


@pytest.fixture
def trips_contract() -> dict:
    c = load_contract(CONTRACTS / "yellow_trips.yaml")
    c["checks"]["min_rows"] = 10
    return c


def _trips(n: int = 20, **overrides) -> pa.Table:
    ts = pa.array([1_780_000_000_000_000 + i for i in range(n)], pa.timestamp("us"))  # 2026-05-28
    june = pa.compute.add(ts, pa.scalar(400_000_000_000, pa.duration("us")))  # -> 2026-06-02
    cols = {
        "VendorID": pa.array([1] * n, pa.int32()),
        "tpep_pickup_datetime": june,
        "tpep_dropoff_datetime": june,
        "passenger_count": pa.array([1] * n, pa.int64()),
        "trip_distance": pa.array([1.5] * n),
        "PULocationID": pa.array([1] * n, pa.int32()),
        "DOLocationID": pa.array([2] * n, pa.int32()),
        "payment_type": pa.array([1] * n, pa.int64()),
        "fare_amount": pa.array([10.0] * n),
        "total_amount": pa.array([12.0] * n),
        "request_source": pa.array(["app"] * n),
    }
    cols.update(overrides)
    return pa.table(cols)


def _write(tmp_path: Path, table: pa.Table) -> Path:
    path = tmp_path / "in.parquet"
    pq.write_table(table, path)
    return path


def test_clean_file_passes_and_writes_output(tmp_path, trips_contract):
    out = tmp_path / "out.parquet"
    result = validate_trips(_write(tmp_path, _trips()), trips_contract, [], MONTH, out)
    assert result.rows == 20
    assert out.exists()
    assert any("request_source" in w for w in result.warnings)  # extra column: warning only


def test_missing_column_fails_and_names_it(tmp_path, trips_contract):
    table = _trips().rename_columns(["VendorID", "pickup_datetime", *_trips().column_names[2:]])
    with pytest.raises(ContractViolation, match="tpep_pickup_datetime"):
        validate_trips(_write(tmp_path, table), trips_contract, [], MONTH, tmp_path / "o.parquet")
    assert not (tmp_path / "o.parquet").exists()


def test_mapping_fixes_schema_drift(tmp_path, trips_contract):
    table = _trips().rename_columns(["VendorID", "pickup_datetime", *_trips().column_names[2:]])
    mapping = [ColumnMapping("pickup_datetime", "tpep_pickup_datetime")]
    result = validate_trips(
        _write(tmp_path, table), trips_contract, mapping, MONTH, tmp_path / "o.parquet"
    )
    assert result.rows == 20


def test_string_fares_fail_until_strip_currency_mapping(tmp_path, trips_contract):
    path = _write(tmp_path, _trips(fare_amount=pa.array(["$12.50"] * 20)))
    with pytest.raises(ContractViolation, match="fare_amount"):
        validate_trips(path, trips_contract, [], MONTH, tmp_path / "o.parquet")
    mapping = [ColumnMapping("fare_amount", "fare_amount", "strip_currency")]
    validate_trips(path, trips_contract, mapping, MONTH, tmp_path / "o.parquet")
    assert pq.read_table(tmp_path / "o.parquet")["fare_amount"][0].as_py() == 12.5


def test_null_spike_fails_with_row_ids(tmp_path, trips_contract):
    counts = pa.array([None if i % 2 else 1 for i in range(20)], pa.int64())
    path = _write(tmp_path, _trips(passenger_count=counts))
    with pytest.raises(ContractViolation, match=r"passenger_count.*null rate 0\.5.*row ids \[1, 3"):
        validate_trips(path, trips_contract, [], MONTH, tmp_path / "o.parquet")


def test_too_few_rows_fails(tmp_path, trips_contract):
    with pytest.raises(ContractViolation, match="below minimum"):
        validate_trips(
            _write(tmp_path, _trips(n=5)), trips_contract, [], MONTH, tmp_path / "o.parquet"
        )


def test_apply_mappings_leaves_unmapped_columns():
    batch = pa.record_batch({"a": [1], "b": [2]})
    out = apply_mappings(batch, [ColumnMapping("a", "x")])
    assert out.schema.names == ["x", "b"]


# ------------------------------------------------------------------- weather


@pytest.fixture
def weather_contract() -> dict:
    return load_contract(CONTRACTS / "weather_hourly.yaml")


def _weather(hours: list[int] | None = None) -> dict:
    hours = hours if hours is not None else list(range(30 * 24))
    return {
        "hourly": {
            "time": [f"2026-06-{h // 24 + 1:02d}T{h % 24:02d}:00" for h in hours],
            "temperature_2m": [20.0] * len(hours),
            "precipitation": [0] * len(hours),  # whole numbers in JSON are still floats
            "rain": [0.0] * len(hours),
            "snowfall": [0.0] * len(hours),
            "wind_speed_10m": [5.0] * len(hours),
        }
    }


def test_weather_clean_month_passes(weather_contract):
    assert len(validate_weather(_weather(), weather_contract, MONTH)) == 720


def test_weather_dropped_hours_fail(weather_contract):
    hours = [h for h in range(720) if not (h < 48 and h % 24 < 4)]  # 4 hours missing on 2 days
    with pytest.raises(ContractViolation, match="2 days without 24 hourly rows"):
        validate_weather(_weather(hours), weather_contract, MONTH)


def test_weather_out_of_range_fails(weather_contract):
    payload = _weather()
    payload["hourly"]["temperature_2m"][0] = 80.0
    with pytest.raises(ContractViolation, match="temperature_2m"):
        validate_weather(payload, weather_contract, MONTH)
