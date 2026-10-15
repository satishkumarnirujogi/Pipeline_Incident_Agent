"""Crash recovery with real Postgres checkpoints.

Run a graph to the middle of diagnosis, let the "process" die (an exception no retry
layer catches), then start a fresh graph on the same thread: finished nodes are not
re-run, only the node that was in flight.
"""

from __future__ import annotations

import uuid
from collections import Counter

import pytest
from langgraph.checkpoint.postgres import PostgresSaver
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool
from sqlalchemy.orm import Session

from agent import llm as llm_module
from agent.agents import common
from agent.db import agent_engine
from agent.graph import compile_graph
from agent.models import Incident
from agent.tools.registry import ToolResult
from agent.tools.store import InMemoryToolStore
from tests.fakes import POSTMORTEM, FakeLLM, call, diagnosis, done

pytestmark = pytest.mark.usefixtures("platform")


class ProcessDied(BaseException):  # noqa: N818 - stands in for SIGKILL
    """Not an Exception: the envelope, node retry policy and Celery all let it through."""


def fresh_graph(platform):
    """What a newly started worker process builds: a new pool and a new compiled graph."""
    pool = ConnectionPool(
        platform["dsn"]("agent_app", "agent"),
        min_size=1,
        max_size=2,
        open=True,
        kwargs={"autocommit": True, "prepare_threshold": 0, "row_factory": dict_row},
    )
    saver = PostgresSaver(pool)
    saver.setup()
    return compile_graph(saver), pool


def test_worker_dies_mid_diagnosis_and_recovers(platform, monkeypatch):
    from worker import runner

    incident_id = str(uuid.uuid4())
    with Session(agent_engine()) as s:
        s.add(
            Incident(
                id=uuid.UUID(incident_id),
                dag_id="ingest_taxi_trips",
                run_id="r-crash",
                task_id="check_landing_file",
                try_number=2,
                conf={"month": "2026-06"},
                exception="FileNotFoundError: landing file not found",
                status="received",
            )
        )
        s.commit()

    executed: list[str] = []
    die_on = {"describe_landing_file": True}

    def run_tool(name, args, ctx):
        executed.append(name)
        if die_on.pop(name, False):
            raise ProcessDied()
        return ToolResult(ok=True, data={"tool": name})

    monkeypatch.setattr(common, "run_tool", run_tool)
    monkeypatch.setattr(common, "store_factory", InMemoryToolStore)
    fake = FakeLLM(
        {
            "diagnostics": [
                call("get_task_log", {"dag_id": "ingest_taxi_trips"}, "c1"),
                call("describe_landing_file", {"month": "2026-06"}, "c2"),
                done(),
            ],
            "diagnostics_finalize": [diagnosis("MISSING_SOURCE", rec="escalate")],
            "reporter": [POSTMORTEM],
        }
    )
    llm_module.set_llm(fake)

    # process 1: dies while running the second tool call
    graph, pool = fresh_graph(platform)
    runner.set_graph(graph)
    with pytest.raises(ProcessDied):
        runner.run_incident(incident_id)
    pool.close()

    # process 2: a fresh worker picks the incident up from the checkpoint
    graph, pool = fresh_graph(platform)
    runner.set_graph(graph)
    try:
        assert runner.run_incident(incident_id) == "escalated"
    finally:
        pool.close()
        runner.set_graph(None)
        llm_module.set_llm(None)

    calls = Counter(executed)
    assert calls["get_task_log"] == 1  # finished before the crash: not repeated
    assert calls["describe_landing_file"] == 2  # in flight at the crash: run again
    assert len(fake.asked["diagnostics"]) == 3  # no LLM turn repeated
    with Session(agent_engine()) as s:
        incident = s.get(Incident, uuid.UUID(incident_id))
        assert incident.status == "escalated" and incident.run_attempts == 2


def test_runs_that_keep_crashing_give_up(platform, monkeypatch):
    from worker import runner

    incident_id = str(uuid.uuid4())
    with Session(agent_engine()) as s:
        s.add(
            Incident(
                id=uuid.UUID(incident_id),
                dag_id="ingest_weather",
                run_id="r-boom",
                task_id="fetch_weather",
                try_number=3,
                conf={"month": "2026-06"},
                status="received",
                run_attempts=runner.MAX_RUN_ATTEMPTS,
            )
        )
        s.commit()
    graph, pool = fresh_graph(platform)
    runner.set_graph(graph)
    try:
        # a checkpoint mid-run exists, so this would be another recovery attempt
        graph.update_state(
            {"configurable": {"thread_id": incident_id}},
            {"incident_id": incident_id},
            as_node="intake",
        )
        assert runner.run_incident(incident_id) == "failed"
    finally:
        pool.close()
        runner.set_graph(None)
    with Session(agent_engine()) as s:
        assert s.get(Incident, uuid.UUID(incident_id)).status == "failed"


def test_orphans_are_found_for_requeue(platform):
    from worker import runner

    incident_id = uuid.uuid4()
    with Session(agent_engine()) as s:
        s.add(
            Incident(
                id=incident_id,
                dag_id="d",
                run_id="r-orphan",
                task_id="t",
                try_number=1,
                conf={},
                status="running",
            )
        )
        s.commit()
    assert str(incident_id) in runner.recover_orphans()


def test_graph_failure_marks_incident_failed_and_escalates(platform, monkeypatch):
    from langgraph.checkpoint.memory import InMemorySaver
    from sqlalchemy import select

    from agent.models import Escalation
    from worker import runner

    incident_id = str(uuid.uuid4())
    with Session(agent_engine()) as s:
        s.add(
            Incident(
                id=uuid.UUID(incident_id),
                dag_id="d",
                run_id="r-fail",
                task_id="t",
                try_number=1,
                conf={"month": "2026-06"},
                status="received",
            )
        )
        s.commit()

    class Broken(FakeLLM):
        def with_tools(self, node, messages, tools):
            raise ValueError("unexpected bug in a node")  # not retried by the node policy

    llm_module.set_llm(Broken({}))
    monkeypatch.setattr(common, "store_factory", InMemoryToolStore)
    runner.set_graph(compile_graph(InMemorySaver()))
    try:
        assert runner.run_incident(incident_id) == "failed"
    finally:
        runner.set_graph(None)
        llm_module.set_llm(None)
    with Session(agent_engine()) as s:
        assert s.get(Incident, uuid.UUID(incident_id)).status == "failed"
        esc = s.execute(
            select(Escalation).where(Escalation.incident_id == incident_id)
        ).scalar_one()
        assert "unexpected bug in a node" in esc.reason
