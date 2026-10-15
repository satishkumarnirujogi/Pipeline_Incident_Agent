"""Runs the incident graph inside the Celery worker.

- One compiled graph and one checkpoint pool per worker process.
- A Postgres advisory lock per incident: a duplicate delivery (Celery redelivers
  unacknowledged jobs) exits immediately instead of running the incident twice.
- Crash recovery: if a checkpoint exists, continue from it instead of restarting.
- When the graph pauses at the approval gate, the request is stored as a pending
  approvals row and the job ends: no worker sits blocked while a human thinks.
"""

from __future__ import annotations

import hashlib
import json
import logging
import uuid
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from typing import Any

from langgraph.types import Command
from pydantic import ValidationError
from sqlalchemy import select, text, update
from sqlalchemy.orm import Session

from agent.db import agent_engine
from agent.graph import compile_graph, run_config
from agent.models import Approval, Incident
from agent.tools.registry import args_hash, validate_args

log = logging.getLogger("worker")
APPROVAL_TIMEOUT = timedelta(hours=24)

_graph: Any = None
_pool: Any = None


def graph() -> Any:
    global _graph, _pool
    if _graph is None:
        from langgraph.checkpoint.postgres import PostgresSaver
        from psycopg.rows import dict_row
        from psycopg_pool import ConnectionPool

        from agent.settings import get_settings

        _pool = ConnectionPool(
            get_settings().agent_dsn,
            max_size=4,
            open=True,
            kwargs={"autocommit": True, "prepare_threshold": 0, "row_factory": dict_row},
        )
        saver = PostgresSaver(_pool)
        saver.setup()
        _graph = compile_graph(saver)
    return _graph


def set_graph(compiled: Any) -> None:
    """Tests install a graph with an in-memory checkpointer."""
    global _graph
    _graph = compiled


def _lock_key(incident_id: str) -> int:
    return int.from_bytes(hashlib.sha256(incident_id.encode()).digest()[:8], "big", signed=True)


@contextmanager
def incident_lock(incident_id: str):
    """Yields True if this process holds the incident's advisory lock."""
    with agent_engine().connect() as conn:
        got = conn.execute(
            text("SELECT pg_try_advisory_lock(:k)"), {"k": _lock_key(incident_id)}
        ).scalar_one()
        try:
            yield bool(got)
        finally:
            if got:
                conn.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": _lock_key(incident_id)})
                conn.commit()


def _set_incident(incident_id: str, **values: Any) -> None:
    with agent_engine().begin() as conn:
        conn.execute(update(Incident).where(Incident.id == uuid.UUID(incident_id)).values(**values))


def _pending_approval(incident_id: str) -> Approval | None:
    with Session(agent_engine()) as s:
        return (
            s.execute(
                select(Approval).where(
                    Approval.incident_id == incident_id, Approval.decision.is_(None)
                )
            )
            .scalars()
            .first()
        )


def _after_invoke(incident_id: str, state: dict) -> str:
    """Record what the graph run ended in: a pending approval, or a final status."""
    if "__interrupt__" in state:
        payload = state["__interrupt__"][0].value
        if _pending_approval(incident_id) is None:
            args = payload["args"]
            try:  # store what the envelope will compare: the validated, normalised args
                args = validate_args(payload["tool"], args).model_dump(mode="json")
            except ValidationError:
                pass  # approving cannot help then; the envelope will refuse the call
            with Session(agent_engine()) as s:
                s.add(
                    Approval(
                        incident_id=incident_id,
                        tool=payload["tool"],
                        args=args,
                        args_hash=args_hash(args),
                        payload=json.loads(json.dumps(payload, default=str)),
                    )
                )
                s.commit()
        _set_incident(incident_id, status="awaiting_approval")
        log.info(
            json.dumps(
                {"event": "awaiting_approval", "incident_id": incident_id, "tool": payload["tool"]}
            )
        )
        return "awaiting_approval"
    status = state.get("status", "failed")
    _set_incident(
        incident_id,
        status=status,
        root_cause=(state.get("diagnosis") or {}).get("root_cause"),
        resolved_at=datetime.now(UTC),
    )
    return status


def run_incident(incident_id: str) -> str:
    with incident_lock(incident_id) as locked:
        if not locked:
            log.info(json.dumps({"event": "duplicate_delivery", "incident_id": incident_id}))
            return "skipped"
        g, config = graph(), run_config(incident_id)
        snapshot = g.get_state(config)
        if snapshot.interrupts:
            return "awaiting_approval"  # nothing to do until a human decides
        if snapshot.next:
            # A previous worker died mid-run: continue from the last checkpoint.
            log.info(
                json.dumps(
                    {"event": "recovering", "incident_id": incident_id, "next": list(snapshot.next)}
                )
            )
            state = g.invoke(None, config)
        elif snapshot.values:
            return snapshot.values.get("status", "unknown")  # already finished
        else:
            _set_incident(incident_id, status="running")
            state = g.invoke({"incident_id": incident_id}, config)
        return _after_invoke(incident_id, state)


def resume_incident(incident_id: str, approval_id: str) -> str:
    with incident_lock(incident_id) as locked:
        if not locked:
            return "skipped"
        with Session(agent_engine()) as s:
            approval = s.get(Approval, uuid.UUID(approval_id))
            if approval is None or approval.decision is None:
                raise ValueError(f"approval {approval_id} has no decision")
            decision = {
                "decision": approval.decision,
                "approval_id": approval_id,
                "args": approval.args,
                "comment": approval.comment,
            }
        g, config = graph(), run_config(incident_id)
        snapshot = g.get_state(config)
        _set_incident(incident_id, status="running")
        if snapshot.interrupts:
            state = g.invoke(Command(resume=decision), config)
        elif snapshot.next:
            state = g.invoke(None, config)  # resumed before, then crashed: continue
        else:
            return snapshot.values.get("status", "unknown")
        return _after_invoke(incident_id, state)


def expire_stale_approvals(now: datetime | None = None) -> list[tuple[str, str]]:
    """Pending approvals older than 24 h become 'timeout' decisions; the gate escalates them.
    Returns (incident_id, approval_id) pairs to resume."""
    cutoff = (now or datetime.now(UTC)) - APPROVAL_TIMEOUT
    expired = []
    with Session(agent_engine()) as s:
        for a in s.execute(
            select(Approval).where(Approval.decision.is_(None), Approval.requested_at < cutoff)
        ).scalars():
            a.decision, a.decided_by = "timeout", "system"
            a.decided_at = datetime.now(UTC)
            a.comment = f"no decision within {APPROVAL_TIMEOUT.total_seconds() / 3600:.0f} h"
            expired.append((a.incident_id, str(a.id)))
        s.commit()
    return expired
