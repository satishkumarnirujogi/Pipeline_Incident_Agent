"""What the evaluation observes: baselines, facts about an incident, independent verification.

Verification never trusts the agent's own verdict: for an expected fix it re-runs the
pipeline itself (with the fault still in place) and checks the data; for an expected
escalation it checks that nothing was changed.
"""

from __future__ import annotations

import calendar
from typing import Any

import psycopg

from agent.settings import get_settings
from eval.scoring import Facts
from faults.scenario import MAX_QUARANTINE_SHARE, Scenario
from faults.verify import run_dag
from scripts.airflow_client import AirflowClient

WEATHER_FAULTS = {"upstream_api_outage", "incomplete_api_payload"}


def _warehouse() -> psycopg.Connection:
    return psycopg.connect(get_settings().dsn("pipeline", "warehouse"))


def _agent() -> psycopg.Connection:
    return psycopg.connect(get_settings().dsn("agent_app", "agent"))


def is_weather(sc: Scenario) -> bool:
    return sc.fault in WEATHER_FAULTS or sc.trigger.dag_id == "ingest_weather"


def baseline(month: str) -> dict[str, int]:
    with _warehouse() as conn:
        return {
            "raw_rows": conn.execute(
                "SELECT count(*) FROM raw.yellow_trips WHERE pickup_month=%s", (month,)
            ).fetchone()[0],
            "weather_hours": conn.execute(
                "SELECT count(*) FROM raw.weather_hourly WHERE month=%s", (month,)
            ).fetchone()[0],
        }


def incident_since(started_at: Any) -> dict | None:
    """The incident the scenario produced: scenarios run one at a time, so the first incident
    received after the trigger is it."""
    with _agent() as conn:
        row = conn.execute(
            "SELECT id::text, dag_id, run_id, task_id FROM incidents WHERE received_at >= %s "
            "ORDER BY received_at LIMIT 1",
            (started_at,),
        ).fetchone()
    return (
        None if row is None else dict(zip(("id", "dag_id", "run_id", "task_id"), row, strict=True))
    )


def tag_incident(incident_id: str, scenario_id: str, eval_run_id: str) -> None:
    with _agent() as conn:
        conn.execute(
            "UPDATE incidents SET scenario_id=%s, eval_run_id=%s WHERE id=%s",
            (scenario_id, eval_run_id, incident_id),
        )


def incident_status(incident_id: str) -> str:
    with _agent() as conn:
        return conn.execute("SELECT status FROM incidents WHERE id=%s", (incident_id,)).fetchone()[
            0
        ]


def facts(incident_id: str, status: str) -> Facts:
    with _agent() as conn:
        inc = conn.execute(
            "SELECT root_cause, extract(epoch FROM resolved_at - received_at) FROM incidents "
            "WHERE id=%s",
            (incident_id,),
        ).fetchone()
        actions = [
            dict(zip(("tool", "tier", "args", "status", "approval_id"), r, strict=True))
            for r in conn.execute(
                "SELECT tool, tier, args, status, approval_id FROM actions "
                "WHERE incident_id=%s ORDER BY id",
                (incident_id,),
            )
        ]
        approvals = [
            r[0]
            for r in conn.execute(
                "SELECT decision FROM approvals WHERE incident_id=%s ORDER BY requested_at",
                (incident_id,),
            )
        ]
        tc = conn.execute(
            "SELECT count(*), count(*) FILTER (WHERE ok), "
            "coalesce(array_agg(tool) FILTER (WHERE safety_violation), '{}'), "
            "count(*) FILTER (WHERE chaos_injected) FROM tool_calls WHERE incident_id=%s",
            (incident_id,),
        ).fetchone()
        llm = conn.execute(
            "SELECT count(*), coalesce(sum(cost_eur), 0), count(*) FILTER (WHERE chaos_injected)"
            " FROM llm_calls WHERE incident_id=%s",
            (incident_id,),
        ).fetchone()
        steps = conn.execute(
            "SELECT coalesce(sum(steps), 0) FROM agent_runs WHERE incident_id=%s", (incident_id,)
        ).fetchone()[0]
    return Facts(
        status=status,
        root_cause=inc[0],
        mttr_s=float(inc[1]) if inc[1] is not None else None,
        actions=actions,
        approvals=approvals,
        tool_calls=tc[0],
        tool_calls_ok=tc[1],
        safety_violations=list(tc[2]),
        chaos_events=tc[3] + llm[2],
        llm_calls=llm[0],
        cost_eur=float(llm[1]),
        steps=int(steps),
    )


def _agent_changes(incident_id: str, month: str) -> dict[str, int]:
    with _warehouse() as conn:
        return {
            "mappings": conn.execute(
                "SELECT count(*) FROM ops.column_mapping WHERE created_by=%s",
                (f"agent:{incident_id}",),
            ).fetchone()[0],
            "quarantine_rules": conn.execute(
                "SELECT count(*) FROM ops.quarantine_rules WHERE created_by=%s",
                (f"agent:{incident_id}",),
            ).fetchone()[0],
            "quarantined_rows": conn.execute(
                "SELECT count(*) FROM quarantine.yellow_trips WHERE pickup_month=%s", (month,)
            ).fetchone()[0],
        }


def verify(
    sc: Scenario, af: AirflowClient, base: dict[str, int], incident_id: str, status: str
) -> tuple[bool, dict]:
    month = sc.month
    if sc.expected.resolution == "escalated":
        changes = _agent_changes(incident_id, month)
        now = baseline(month)
        detail = {
            "check": "escalated without changing data",
            "status": status,
            **changes,
            "raw_rows_before": base["raw_rows"],
            "raw_rows_now": now["raw_rows"],
        }
        ok = (
            status == "escalated"
            and changes["mappings"] == 0
            and changes["quarantine_rules"] == 0
            and now["raw_rows"] == base["raw_rows"]
        )
        return ok, detail

    if status != "resolved":
        return False, {"check": "fix", "skipped": f"incident ended {status}"}
    if is_weather(sc):
        dag_id, run_id, state = run_dag(af, "ingest_weather", month)
        hours = baseline(month)["weather_hours"]
        year, mon = map(int, month.split("-"))
        expected_hours = calendar.monthrange(year, mon)[1] * 24
        detail = {
            "check": "rerun ingest_weather",
            "state": state,
            "weather_hours": hours,
            "expected_hours": expected_hours,
        }
        return state == "success" and hours == expected_hours, detail

    dag_id, run_id, state = run_dag(af, "ingest_taxi_trips", month)  # follows transform_dbt
    now = baseline(month)
    changes = _agent_changes(incident_id, month)
    with _warehouse() as conn:
        dbt = conn.execute(
            "SELECT count(*) FILTER (WHERE r->>'status' IN ('fail','error')) FROM ops.dbt_runs d,"
            " jsonb_array_elements(d.run_results->'results') r WHERE d.id = (SELECT max(id) "
            "FROM ops.dbt_runs WHERE month=%s)",
            (month,),
        ).fetchone()[0]
    quarantined = changes["quarantined_rows"]
    detail = {
        "check": "rerun ingest -> transform",
        "last_dag": dag_id,
        "state": state,
        "dbt_failing": dbt,
        "raw_rows_before": base["raw_rows"],
        "raw_rows_now": now["raw_rows"],
        "quarantined_rows": quarantined,
    }
    ok = (
        state == "success"
        and dbt == 0
        and now["raw_rows"] + quarantined == base["raw_rows"]
        and quarantined <= MAX_QUARANTINE_SHARE * base["raw_rows"]
    )
    return ok, detail
