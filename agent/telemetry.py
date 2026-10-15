"""Metrics the system writes about itself: agent_runs and llm_calls (tool_calls is written by
the tool envelope). Postgres is the source of truth for every KPI; Langfuse is for looking
at a single run.

The worker binds the incident and the current agent run before invoking the graph; LLM calls
made while bound are written with both ids. Unbound calls (unit tests) are not written.
"""

from __future__ import annotations

import logging
import threading
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import func, insert, select, update

from agent.db import agent_engine
from agent.models import AgentRun, Incident, LLMCall

log = logging.getLogger(__name__)

_bound = threading.local()
_global: dict[str, Any] = {"incident_id": None, "run_id": None}


def current_incident() -> str | None:
    """The incident being worked on. Thread-local first, then the process-wide binding, so
    executor threads (tool and LLM timeouts) still see it."""
    return getattr(_bound, "incident_id", None) or _global["incident_id"]


def current_run() -> int | None:
    return getattr(_bound, "run_id", None) or _global["run_id"]


@contextmanager
def bind(incident_id: str, run_id: int | None = None) -> Iterator[None]:
    prev = (_global["incident_id"], _global["run_id"])
    _global["incident_id"], _global["run_id"] = incident_id, run_id
    _bound.incident_id, _bound.run_id = incident_id, run_id
    try:
        yield
    finally:
        _global["incident_id"], _global["run_id"] = prev
        _bound.incident_id, _bound.run_id = prev


def start_run(incident_id: str, kind: str) -> int:
    with agent_engine().begin() as conn:
        attempt = (
            conn.execute(
                select(Incident.run_attempts).where(Incident.id == uuid.UUID(incident_id))
            ).scalar_one_or_none()
            or 0
        )
        return conn.execute(
            insert(AgentRun)
            .values(incident_id=incident_id, kind=kind, attempt=attempt)
            .returning(AgentRun.id)
        ).scalar_one()


def end_run(run_id: int, outcome: str, steps: int) -> None:
    """Totals come from the run's own llm_calls rows, so they always add up."""
    with agent_engine().begin() as conn:
        totals = conn.execute(
            select(
                func.coalesce(func.sum(LLMCall.tokens_in), 0),
                func.coalesce(func.sum(LLMCall.tokens_out), 0),
                func.coalesce(func.sum(LLMCall.cost_eur), 0),
            ).where(LLMCall.agent_run_id == run_id)
        ).one()
        conn.execute(
            update(AgentRun)
            .where(AgentRun.id == run_id)
            .values(
                ended_at=datetime.now(UTC),
                outcome=outcome,
                steps=steps,
                tokens_in=totals[0],
                tokens_out=totals[1],
                cost_eur=totals[2],
            )
        )


def record_llm_call(record: Any, latency_ms: int) -> None:
    """Write one llm_calls row for a CallRecord. Never raises: metrics must not break a run."""
    incident_id = current_incident()
    if incident_id is None:
        return
    try:
        with agent_engine().begin() as conn:
            conn.execute(
                insert(LLMCall).values(
                    incident_id=incident_id,
                    agent_run_id=current_run(),
                    node=record.node,
                    provider=record.model.split("/")[0],
                    model=record.model,
                    tokens_in=record.tokens_in,
                    tokens_out=record.tokens_out,
                    cost_eur=record.cost_eur,
                    latency_ms=latency_ms,
                    ok=record.ok,
                    error_type=record.error_type,
                    fallback_used=record.fallback_used,
                    chaos_injected=record.chaos_injected,
                )
            )
    except Exception:  # noqa: BLE001
        log.exception("could not write llm_calls row")
