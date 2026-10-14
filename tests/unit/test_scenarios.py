from collections import Counter

import numpy as np
import pyarrow as pa
import pytest

from agent.state import RootCause
from faults.injector import transform_batch
from faults.scenario import load_all

SCENARIOS = load_all()


def test_scenario_count_and_split():
    assert 60 <= len(SCENARIOS) <= 80
    test_share = sum(s.split == "test" for s in SCENARIOS) / len(SCENARIOS)
    assert 0.15 <= test_share <= 0.25


def test_every_fault_has_dev_and_test_variants():
    by_fault = Counter((s.fault, s.split) for s in SCENARIOS)
    for fault in {s.fault for s in SCENARIOS}:
        assert by_fault[(fault, "dev")] >= 4
        assert by_fault[(fault, "test")] >= 1


def test_ids_unique_and_every_root_cause_covered():
    assert len({s.id for s in SCENARIOS}) == len(SCENARIOS)
    assert {s.expected.root_cause for s in SCENARIOS} == set(RootCause)


def test_escalations_have_no_required_actions():
    for s in SCENARIOS:
        if s.expected.resolution == "escalated":
            assert s.expected.required_actions == [], s.id
        else:
            assert s.expected.required_actions, s.id


def test_trigger_month_matches_scenario_month():
    for s in SCENARIOS:
        assert s.trigger.conf["month"] == s.month


# ------------------------------------------------------------- file transforms


@pytest.fixture
def batch() -> pa.RecordBatch:
    return pa.record_batch(
        {
            "fare_amount": pa.array([12.5, 1234.5, None]),
            "PULocationID": pa.array([1, 2, 3], pa.int32()),
            "tpep_pickup_datetime": pa.array([0, 3_600_000_000, 7_200_000_000], pa.timestamp("us")),
        }
    )


def rng():
    return np.random.default_rng(0)


def test_rename(batch):
    out = transform_batch(batch, "rename", {"rename": {"fare_amount": "fare"}}, rng())
    assert out.schema.names[0] == "fare"


def test_currency_strings(batch):
    out = transform_batch(
        batch, "currency_strings", {"column": "fare_amount", "thousands": True}, rng()
    )
    assert out.column(0).to_pylist() == ["$12.50", "$1,234.50", None]


def test_timestamp_strings(batch):
    out = transform_batch(batch, "plain_strings", {"column": "tpep_pickup_datetime"}, rng())
    assert out.column(2).to_pylist()[1] == "1970-01-01 01:00:00"


def test_set_value_share_keeps_type(batch):
    out = transform_batch(
        batch, "set_value_share", {"column": "PULocationID", "value": 999, "share": 1.0}, rng()
    )
    assert out.column(1).type == pa.int32()
    assert out.column(1).to_pylist() == [999, 999, 999]


def test_null_share(batch):
    out = transform_batch(batch, "null_share", {"column": "PULocationID", "share": 1.0}, rng())
    assert out.column(1).null_count == 3
