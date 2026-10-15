"""Phase 7 on the running stack, real models.

    $env:CHAOS_MODE='on'; $env:CHAOS_CONFIG='eval/chaos_demo.yaml'; docker compose up -d worker
    uv run pytest tests/e2e/test_resilience_live.py -m e2e -v

1. With chaos on, a scenario still completes, and its records show tool retries and an
   LLM fallback.
2. Killing the worker mid-diagnosis and starting it again completes the same incident
   without repeating finished tool calls.
"""

from __future__ import annotations

import subprocess
import time

import psycopg
import pytest

from agent.graph import compile_graph, postgres_checkpointer, run_config
from agent.settings import get_settings
from faults import injector
from faults.verify import clean_chain
from scripts.airflow_client import AirflowClient
from tests.e2e.test_approval_flow import TERMINAL, fail_pipeline, status, wait_for

pytestmark = pytest.mark.e2e
WORKER = "pipeline-incident-agent-worker-1"


def tool_rows(incident_id: str) -> list[tuple]:
    with psycopg.connect(get_settings().dsn("agent_app", "agent")) as conn:
        return conn.execute(
            "SELECT id, tool, args_hash, attempt, ok, chaos_injected FROM tool_calls "
            "WHERE incident_id = %s ORDER BY id",
            (incident_id,),
        ).fetchall()


def run_attempts(incident_id: str) -> int:
    with psycopg.connect(get_settings().dsn("agent_app", "agent")) as conn:
        return conn.execute(
            "SELECT run_attempts FROM incidents WHERE id = %s", (incident_id,)
        ).fetchone()[0]


def graph_state(incident_id: str) -> dict:
    with postgres_checkpointer() as saver:
        return compile_graph(saver).get_state(run_config(incident_id)).values


def worker_env(name: str) -> str:
    return subprocess.run(
        ["docker", "exec", WORKER, "printenv", name], capture_output=True, text=True
    ).stdout.strip()


@pytest.fixture(scope="module")
def af() -> AirflowClient:
    return AirflowClient()


def test_scenario_completes_under_chaos(af):
    if worker_env("CHAOS_MODE") != "on":
        pytest.skip("start the worker with CHAOS_MODE=on")
    sc, incident = fail_pipeline(af, "F05-v1")
    try:
        final = wait_for(
            lambda: (s := status(incident["id"])) in TERMINAL and s, 900, 5, "incident to finish"
        )
        rows = tool_rows(incident["id"])
        llm_calls = graph_state(incident["id"]).get("llm_calls", [])
        retried = [r for r in rows if r[3] > 1]
        chaos_rows = [r for r in rows if r[5]]
        fallbacks = [c for c in llm_calls if c["fallback_used"] and c["ok"]]
        print(
            f"\nstatus {final}; {len(rows)} tool calls, {len(chaos_rows)} chaos-injected, "
            f"{len(retried)} retry attempts; {len(llm_calls)} LLM calls, "
            f"{sum(1 for c in llm_calls if c['chaos_injected'])} chaos-injected, "
            f"{len(fallbacks)} answered by the fallback"
        )
        assert final == "resolved"
        assert chaos_rows and retried, "expected injected tool failures and retries"
        assert fallbacks, "expected at least one LLM call answered by the fallback"
    finally:
        injector.revert(sc)
        clean_chain(af, sc.month)


def test_worker_killed_mid_diagnosis_recovers(af):
    sc, incident = fail_pipeline(af, "F03-v1")
    iid = incident["id"]
    try:
        # kill only once at least one tool node has finished and been checkpointed
        wait_for(lambda: graph_state(iid).get("tool_log"), 180, 1, "a checkpointed tool node")
        subprocess.run(["docker", "kill", WORKER], check=True, capture_output=True)
        killed_at = time.time()
        completed = {(t["tool"], t["args_hash"]) for t in graph_state(iid)["tool_log"]}
        before = tool_rows(iid)
        assert status(iid) not in TERMINAL, "incident finished before the kill; rerun the test"

        subprocess.run(["docker", "start", WORKER], check=True, capture_output=True)
        final = wait_for(
            lambda: (s := status(iid)) in TERMINAL and s, 600, 5, "recovered incident to finish"
        )
        after = [r for r in tool_rows(iid) if r[0] > before[-1][0]]
        repeated = [r for r in after if (r[1], r[2]) in completed]
        print(
            f"\n{len(completed)} tool call(s) checkpointed before the kill, "
            f"{len(before)} rows before, {len(after)} after; repeated: {repeated}; "
            f"recovered in {time.time() - killed_at:.0f}s; final {final}"
        )

        assert final == "escalated"
        assert run_attempts(iid) == 2
        assert not repeated, "a tool call from a finished node ran again after recovery"
        logs = subprocess.run(
            ["docker", "logs", WORKER, "--since", "15m"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        assert f'"event": "recovering", "incident_id": "{iid}"' in logs.stdout + logs.stderr
    finally:
        injector.revert(sc)
        clean_chain(af, sc.month)
