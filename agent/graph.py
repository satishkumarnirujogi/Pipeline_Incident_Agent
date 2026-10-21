"""Builds and compiles the incident graph.

    intake -> supervisor -> diagnostics <-> diagnostics_tools -> diagnostics_finalize -> supervisor
                         -> remediation <-> remediation_tools -> remediation_finalize
                                -> approval_gate -> execute_action -> supervisor
                                -> escalate -> reporter -> END
                         -> reporter -> END

The Supervisor is the only node that decides where to go next. Every
specialist returns to it, except Remediation, whose proposal always passes the
approval gate first. Both exits end in the Reporter, so escalations get a
postmortem too. thread_id = incident id.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any

from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import RetryPolicy

from agent import ablation
from agent.agents.diagnostics import (
    after_diagnostics_llm,
    after_diagnostics_tools,
    diagnostics_finalize,
    diagnostics_llm,
    diagnostics_tools,
)
from agent.agents.remediation import (
    after_remediation,
    after_remediation_llm,
    after_remediation_tools,
    remediation_finalize,
    remediation_llm,
    remediation_tools,
)
from agent.agents.reporter import reporter_agent
from agent.approval import approval_gate, escalate, execute_action
from agent.state import IncidentState
from agent.supervisor import supervisor

RETRY = RetryPolicy(max_attempts=3, initial_interval=1.0, backoff_factor=2.0)


def intake(state: IncidentState) -> dict:
    """Load the incident (unless the caller passed it) and initialise the counters."""
    incident = state.get("incident")
    if incident is None:
        from sqlalchemy import select

        from agent.db import agent_engine
        from agent.models import Incident

        with agent_engine().connect() as conn:
            row = (
                conn.execute(select(Incident).where(Incident.id == state["incident_id"]))
                .mappings()
                .one()
            )
        incident = {
            k: row[k] for k in ("dag_id", "run_id", "task_id", "try_number", "conf", "exception")
        }
    incident = {**incident, "month": (incident.get("conf") or {}).get("month")}
    return {
        "incident": incident,
        "status": "running",
        "steps": 1,
        "cost_eur": 0.0,
        "diagnostics_attempts": 0,
        "proposals": 0,
        "verified": False,
    }


def build_graph() -> StateGraph:
    # The no_recovery ablation runs without node retry policies.
    retry = RETRY if ablation.recovery_on() else None
    if ablation.single_agent():
        from agent.single import build_single_agent_graph

        return build_single_agent_graph(intake, retry)
    b = StateGraph(IncidentState)
    b.add_node("intake", intake)
    b.add_node(
        "supervisor",
        supervisor,
        retry_policy=retry,
        destinations=("diagnostics", "remediation", "escalate", "reporter"),
    )
    b.add_node("diagnostics", diagnostics_llm, retry_policy=retry)
    b.add_node("diagnostics_tools", diagnostics_tools)
    b.add_node("diagnostics_finalize", diagnostics_finalize, retry_policy=retry)
    b.add_node("remediation", remediation_llm, retry_policy=retry)
    b.add_node("remediation_tools", remediation_tools)
    b.add_node("remediation_finalize", remediation_finalize, retry_policy=retry)
    b.add_node(
        "approval_gate", approval_gate, destinations=("execute_action", "supervisor", "escalate")
    )
    b.add_node("execute_action", execute_action)
    b.add_node("escalate", escalate)
    b.add_node("reporter", reporter_agent, retry_policy=retry)

    b.add_edge(START, "intake")
    b.add_edge("intake", "supervisor")
    b.add_conditional_edges("diagnostics", after_diagnostics_llm)
    b.add_conditional_edges("diagnostics_tools", after_diagnostics_tools)
    b.add_edge("diagnostics_finalize", "supervisor")
    b.add_conditional_edges("remediation", after_remediation_llm)
    b.add_conditional_edges("remediation_tools", after_remediation_tools)
    b.add_conditional_edges("remediation_finalize", after_remediation)
    b.add_edge("execute_action", "supervisor")
    b.add_edge("escalate", "reporter")
    b.add_edge("reporter", END)
    return b


def compile_graph(checkpointer: BaseCheckpointSaver) -> Any:
    return build_graph().compile(checkpointer=checkpointer)


@contextmanager
def postgres_checkpointer():
    """One psycopg pool per process; creates the checkpoint tables on first use."""
    from langgraph.checkpoint.postgres import PostgresSaver
    from psycopg.rows import dict_row
    from psycopg_pool import ConnectionPool

    from agent.settings import get_settings

    with ConnectionPool(
        get_settings().agent_dsn,
        max_size=4,
        kwargs={"autocommit": True, "prepare_threshold": 0, "row_factory": dict_row},
    ) as pool:
        saver = PostgresSaver(pool)
        saver.setup()
        yield saver


def run_config(incident_id: str, callbacks: list | None = None) -> dict:
    return {
        "configurable": {"thread_id": incident_id},
        "callbacks": callbacks or [],
        "recursion_limit": 100,
    }
