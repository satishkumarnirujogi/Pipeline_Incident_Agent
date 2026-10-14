"""Tools that talk HTTP (Airflow adapter, weather proxy), against recorded-style responses."""

import httpx
import pytest
import respx

import agent.tools  # noqa: F401
from agent.settings import get_settings
from agent.tools.orchestrator import airflow, format_log_events, redact
from agent.tools.registry import ToolContext, call_tool
from agent.tools.store import InMemoryToolStore

AF = "http://localhost:8080"
PROXY = "http://localhost:8091"
RUN = "manual__2026-10-14T20:10:25+00:00"


@pytest.fixture(autouse=True)
def host_settings(monkeypatch):
    monkeypatch.setenv("RUNNING_IN_DOCKER", "false")
    monkeypatch.setenv("AIRFLOW_HOST_URL", AF)
    monkeypatch.setenv("WEATHER_PROXY_HOST_PORT", "8091")
    get_settings.cache_clear()
    airflow.cache_clear()
    yield
    get_settings.cache_clear()
    airflow.cache_clear()


@pytest.fixture
def af():
    with respx.mock(base_url=AF, assert_all_called=False) as mock:
        mock.post("/auth/token").mock(return_value=httpx.Response(200, json={"access_token": "t"}))
        yield mock


def ctx(agent="diagnostics"):
    return ToolContext(incident_id="inc-1", agent=agent, store=InMemoryToolStore())


def _run(dag_id="ingest_taxi_trips", state="failed", run_id=RUN, run_after="2026-10-14T20:10:25Z"):
    return {
        "dag_id": dag_id,
        "dag_run_id": run_id,
        "state": state,
        "run_after": run_after,
        "start_date": "2026-10-14T20:10:26+00:00",
        "end_date": "2026-10-14T20:11:30+00:00",
        "conf": {"month": "2026-06"},
    }


LOG_EVENTS = [
    {"timestamp": "2026-10-14T20:11:29.1Z", "event": "::group::Pre Execute", "level": "info"},
    {
        "timestamp": "2026-10-14T20:11:29.2Z",
        "level": "info",
        "logger": "task",
        "event": "connecting to postgresql://pipeline:hunter2@postgres:5432/warehouse",
    },
    {
        "timestamp": "2026-10-14T20:11:30.0Z",
        "level": "error",
        "logger": "task",
        "event": "Task failed with exception",
        "error_detail": [
            {
                "exc_type": "FileNotFoundError",
                "exc_value": "landing file not found: /opt/data/landing/x.parquet",
                "frames": [
                    {"filename": "a.py", "lineno": 1, "name": "f"},
                    {"filename": "dag.py", "lineno": 51, "name": "check"},
                ],
            }
        ],
    },
]


def test_log_formatting_keeps_errors_and_drops_noise():
    lines = format_log_events(LOG_EVENTS)
    assert not any("::group::" in line for line in lines)
    assert any("FileNotFoundError: landing file not found" in line for line in lines)
    assert any("at dag.py:51 in check" in line for line in lines)


@pytest.mark.parametrize(
    ("raw", "secret"),
    [
        ("postgresql://pipeline:hunter2@postgres:5432/db", "hunter2"),
        ("password=hunter2 user=x", "hunter2"),
        ("Authorization: Bearer abc.def.ghi", "abc.def.ghi"),
        ("key sk-or-v1-abcdefghijklmnop", "sk-or-v1-abcdefghijklmnop"),
    ],
)
def test_secrets_are_redacted(raw, secret):
    assert secret not in redact(raw)


def test_get_task_log(af):
    af.get(url__regex=r".*/logs/2").mock(
        return_value=httpx.Response(200, json={"content": LOG_EVENTS, "continuation_token": None})
    )
    r = call_tool(
        "get_task_log",
        {
            "dag_id": "ingest_taxi_trips",
            "run_id": RUN,
            "task_id": "check_landing_file",
            "try_number": 2,
            "tail_lines": 50,
        },
        ctx(),
    )
    assert r.ok, r.message
    assert "hunter2" not in " ".join(r.data["lines"])


def test_get_task_log_unknown_run_is_not_found(af):
    af.get(url__regex=r".*/logs/1").mock(return_value=httpx.Response(404))
    r = call_tool(
        "get_task_log",
        {"dag_id": "ingest_taxi_trips", "run_id": "nope", "task_id": "load_raw", "try_number": 1},
        ctx(),
    )
    assert r.error_type == "not_found"


def test_get_dag_run_history(af):
    af.get("/api/v2/dags/ingest_weather/dagRuns").mock(
        return_value=httpx.Response(
            200,
            json={
                "dag_runs": [
                    _run("ingest_weather", "failed"),
                    _run("ingest_weather", "success", "r2"),
                ]
            },
        )
    )
    r = call_tool("get_dag_run_history", {"dag_id": "ingest_weather", "limit": 5}, ctx())
    assert r.ok and r.data["states"] == {"failed": 1, "success": 1}
    assert r.data["runs"][0]["month"] == "2026-06"


def test_get_dag_run_history_airflow_down_is_upstream(af):
    af.get("/api/v2/dags/ingest_weather/dagRuns").mock(side_effect=httpx.ConnectError("down"))
    r = call_tool("get_dag_run_history", {"dag_id": "ingest_weather"}, ctx())
    assert r.error_type == "upstream" and r.retryable


def test_rerun_dag_triggers_once(af):
    route = af.post("/api/v2/dags/ingest_taxi_trips/dagRuns").mock(
        return_value=httpx.Response(200, json=_run(state="queued", run_id="new"))
    )
    c = ctx("remediation")
    args = {"dag_id": "ingest_taxi_trips", "conf": {"month": "2026-06"}}
    first = call_tool("rerun_dag", args, c)
    second = call_tool("rerun_dag", args, c)
    assert first.ok and first.data["run_id"] == "new"
    assert second.replayed and route.call_count == 1


def test_rerun_dag_rejects_unknown_dag_and_extra_conf(af):
    c = ctx("remediation")
    assert (
        call_tool(
            "rerun_dag", {"dag_id": "drop_everything", "conf": {"month": "2026-06"}}, c
        ).error_type
        == "validation"
    )
    assert (
        call_tool(
            "rerun_dag",
            {"dag_id": "transform_dbt", "conf": {"month": "2026-06", "full_refresh": True}},
            c,
        ).error_type
        == "validation"
    )


def test_wait_for_dag_run_follows_the_triggered_transform(af):
    af.get(f"/api/v2/dags/ingest_taxi_trips/dagRuns/{RUN}").mock(
        return_value=httpx.Response(200, json=_run(state="success"))
    )
    af.get(f"/api/v2/dags/ingest_taxi_trips/dagRuns/{RUN}/taskInstances").mock(
        return_value=httpx.Response(
            200, json={"task_instances": [{"task_id": "load_raw", "state": "success"}]}
        )
    )
    child = _run("transform_dbt", "failed", "t1", run_after="2026-10-14T20:11:31Z")
    af.get("/api/v2/dags/transform_dbt/dagRuns").mock(
        return_value=httpx.Response(200, json={"dag_runs": [child]})
    )
    af.get("/api/v2/dags/transform_dbt/dagRuns/t1").mock(
        return_value=httpx.Response(200, json=child)
    )
    af.get("/api/v2/dags/transform_dbt/dagRuns/t1/taskInstances").mock(
        return_value=httpx.Response(
            200, json={"task_instances": [{"task_id": "dbt_build", "state": "failed"}]}
        )
    )
    r = call_tool(
        "wait_for_dag_run",
        {"dag_id": "ingest_taxi_trips", "run_id": RUN, "timeout_s": 10},
        ctx("remediation"),
    )
    assert r.ok, r.message
    assert r.data["chain_state"] == "failed"
    assert r.data["followed"]["tasks"] == {"dbt_build": "failed"}


def test_wait_for_dag_run_bounds_timeout():
    r = call_tool(
        "wait_for_dag_run",
        {"dag_id": "transform_dbt", "run_id": "x", "timeout_s": 3600},
        ctx("remediation"),
    )
    assert r.error_type == "validation"


# --------------------------------------------------------------- weather proxy


@respx.mock
def test_check_api_health_ok():
    respx.get(f"{PROXY}/v1/archive").mock(
        return_value=httpx.Response(200, json={"hourly": {"time": ["2026-06-01T00:00"] * 720}})
    )
    r = call_tool("check_api_health", {"month": "2026-06"}, ctx())
    assert r.ok and r.data["healthy"] and r.data["hours_returned"] == 720


@respx.mock
def test_check_api_health_reports_503_and_timeouts_as_data():
    respx.get(f"{PROXY}/v1/archive").mock(return_value=httpx.Response(503))
    r = call_tool("check_api_health", {"month": "2026-06"}, ctx())
    assert r.ok and not r.data["healthy"] and r.data["status_code"] == 503
    respx.get(f"{PROXY}/health").mock(side_effect=httpx.ReadTimeout("slow"))
    r = call_tool("check_api_health", {}, ctx())
    assert r.ok and r.data["error"].startswith("timed out")


@respx.mock
def test_compare_weather_schema_counts_short_days():
    times = [
        f"2026-06-{d:02d}T{h:02d}:00"
        for d in range(1, 31)
        for h in range(24)
        if not (d == 3 and h < 4)
    ]
    hourly = {
        "time": times,
        **{
            k: [0.0] * len(times)
            for k in ("temperature_2m", "precipitation", "rain", "snowfall", "wind_speed_10m")
        },
    }
    respx.get(f"{PROXY}/v1/archive").mock(return_value=httpx.Response(200, json={"hourly": hourly}))
    r = call_tool(
        "compare_schema_to_contract", {"dataset": "weather_hourly", "month": "2026-06"}, ctx()
    )
    assert r.ok and r.data["days_without_24_hours"] == 1
    assert r.data["examples"] == {"2026-06-03": 20}


@respx.mock
def test_compare_weather_schema_during_outage_is_upstream():
    respx.get(f"{PROXY}/v1/archive").mock(return_value=httpx.Response(503))
    r = call_tool(
        "compare_schema_to_contract", {"dataset": "weather_hourly", "month": "2026-06"}, ctx()
    )
    assert r.error_type == "upstream"


def test_escalate_to_human_always_allowed():
    for who in ("diagnostics", "remediation", "reporter"):
        c = ctx(who)
        r = call_tool(
            "escalate_to_human",
            {
                "summary": "landing file missing for 2026-06",
                "reason": "upstream did not deliver",
                "suggested_owner": "upstream-provider",
            },
            c,
        )
        assert r.ok and r.data["status"] == "escalated"
        assert c.store.escalations[0]["incident_id"] == "inc-1"


def test_escalate_to_human_validates_owner():
    r = call_tool(
        "escalate_to_human",
        {"summary": "x" * 20, "reason": "because", "suggested_owner": "nobody"},
        ctx(),
    )
    assert r.error_type == "validation"
