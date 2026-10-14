from eval.simulated_approver import decide
from faults.scenario import load_scenario


def test_approves_the_expected_mapping():
    sc = load_scenario("F01-v1")
    args = {
        "dataset": "yellow_trips",
        "source_col": "pickup_datetime",
        "target_col": "tpep_pickup_datetime",
        "transform": "none",
    }
    assert decide(sc, "upsert_column_mapping", args)["decision"] == "approve"


def test_rejects_a_different_fix():
    sc = load_scenario("F01-v1")
    args = {
        "dataset": "yellow_trips",
        "source_col": "pickup_datetime",
        "target_col": "tpep_dropoff_datetime",
        "transform": "none",
    }
    d = decide(sc, "upsert_column_mapping", args)
    assert d == {"decision": "reject", "comment": "not the expected fix"}


def test_quarantine_threshold_may_differ():
    sc = load_scenario("F06-v1")
    args = {
        "table": "raw.yellow_trips",
        "month": "2026-06",
        "predicate": {"column": "fare_amount", "op": "lt", "value": -100},
    }
    assert decide(sc, "quarantine_rows", args)["decision"] == "approve"
    wrong_month = {**args, "month": "2026-07"}
    assert decide(sc, "quarantine_rows", wrong_month)["decision"] == "reject"


def test_escalation_scenarios_approve_nothing():
    sc = load_scenario("F03-v1")
    assert (
        decide(sc, "rerun_dag", {"dag_id": "ingest_taxi_trips", "conf": {"month": "2026-06"}})[
            "decision"
        ]
        == "reject"
    )
