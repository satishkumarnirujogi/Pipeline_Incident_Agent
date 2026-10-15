"""eval/queries.sql against real Postgres: hand-made results with known metrics."""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime

import psycopg
import pytest

from eval import report

pytestmark = pytest.mark.usefixtures("platform")
RUN, CHAOS_RUN = "test-run", "test-run-chaos"


def result(
    conn,
    run,
    scenario,
    fault,
    expected,
    status,
    success,
    rc_ok,
    forbidden=0,
    cost=0.01,
    mttr=120.0,
    requested=1,
    granted=1,
    incident=None,
):
    conn.execute(
        "INSERT INTO eval.results (eval_run_id, scenario_id, fault, split, month, incident_id, "
        "expected_resolution, expected_root_cause, status, root_cause, resolution_ok, verified, "
        "verification, root_cause_correct, forbidden_count, forbidden_detail, success, "
        "approvals_requested, approvals_granted, mttr_s, steps, cost_eur, started_at) VALUES "
        "(%s,%s,%s,'dev','2026-06',%s,%s,'X',%s,'X',%s,%s,'{}',%s,%s,'[]',%s,%s,%s,%s,10,%s,%s)",
        (
            run,
            scenario,
            fault,
            incident,
            expected,
            status,
            success,
            success,
            rc_ok,
            forbidden,
            success,
            requested,
            granted,
            mttr,
            cost,
            datetime.now(UTC),
        ),
    )


@pytest.fixture(scope="module")
def data(platform):
    inc = [str(uuid.uuid4()) for _ in range(4)]
    with psycopg.connect(platform["dsn"]("agent_app", "agent"), autocommit=True) as conn:
        conn.execute(
            "INSERT INTO eval.runs (id, split, config) VALUES (%s,'dev',%s), (%s,'dev',%s)",
            (RUN, json.dumps({"chaos": False}), CHAOS_RUN, json.dumps({"chaos": True})),
        )
        result(
            conn, RUN, "F01-v1", "schema_drift", "fixed", "resolved", True, True, incident=inc[0]
        )
        result(
            conn,
            RUN,
            "F01-v2",
            "schema_drift",
            "fixed",
            "escalated",
            False,
            True,
            requested=2,
            granted=0,
            incident=inc[1],
        )
        result(
            conn,
            RUN,
            "F03-v1",
            "missing_source",
            "escalated",
            "escalated",
            True,
            True,
            requested=0,
            granted=0,
            incident=inc[2],
        )
        result(
            conn,
            RUN,
            "F05-v1",
            "duplicate_load",
            "fixed",
            "resolved",
            False,
            False,
            forbidden=1,
            incident=inc[3],
        )
        result(conn, CHAOS_RUN, "F01-v1", "schema_drift", "fixed", "resolved", True, True)
        result(conn, CHAOS_RUN, "F03-v1", "missing_source", "escalated", "failed", False, True)
        for i, ok in enumerate([True, True, True, False]):  # 3 of 4 tool calls ok
            conn.execute(
                "INSERT INTO tool_calls (incident_id, agent, tool, tier, args_hash, ok, "
                "error_type, retryable, attempt, latency_ms, replayed, "
                "safety_violation, chaos_injected) VALUES (%s,'diagnostics',"
                "'get_task_log',0,'h',%s,%s,false,1,5,false,false,false)",
                (inc[i], ok, None if ok else "upstream"),
            )
    return inc


def test_headline(data):
    h = report.run("headline", {"eval_run_id": RUN})[0]
    assert (h["scenarios"], h["successes"]) == (4, 2)
    assert float(h["task_completion_rate"]) == 0.5
    assert float(h["root_cause_accuracy"]) == 0.75
    assert h["safety_violations"] == 1
    assert float(h["cost_per_completed_eur"]) == 0.02  # all 4 runs' cost / 2 successes
    assert float(h["approval_precision"]) == 0.5  # 2 granted of 4 requested
    assert float(h["mttr_median_min"]) == 2.0


def test_tool_call_success_and_errors(data):
    t = report.run("tool_call_success", {"eval_run_id": RUN})[0]
    assert (t["tool_calls"], t["ok"]) == (4, 3) and float(t["tool_call_success_rate"]) == 0.75
    errors = report.run("tool_call_errors", {"eval_run_id": RUN})
    assert errors == [{"tool": "get_task_log", "error_type": "upstream", "calls": 1}]


def test_escalation_precision_and_recall(data):
    e = report.run("escalation", {"eval_run_id": RUN})[0]
    assert float(e["escalation_precision"]) == 0.5  # 1 of 2 escalations was expected
    assert float(e["escalation_recall"]) == 1.0


def test_recovery_rate(data):
    r = report.run("recovery_rate", {"eval_run_id": CHAOS_RUN, "baseline_run_id": RUN})[0]
    assert (r["solved_without_chaos"], r["still_solved_with_chaos"]) == (2, 1)
    assert float(r["recovery_rate"]) == 0.5


def test_report_files(data, tmp_path, monkeypatch):
    monkeypatch.setattr(report, "REPORTS", tmp_path)
    path = report.write(CHAOS_RUN, baseline_run_id=RUN)
    text = path.read_text(encoding="utf-8")
    assert "| Recovery rate | 0.500 |" in text
    assert "F03-v1" in text and (path.parent / "results.csv").exists()
