"""SQLAlchemy models for the `agent` database. Alembic migrations in db/migrations.

Phase 5 creates what the tools need; agent_runs and llm_calls follow in Phase 8.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from pgvector.sqlalchemy import Vector
from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

EMBEDDING_DIMS = 384


class Base(DeclarativeBase):
    type_annotation_map = {dict[str, Any]: JSONB, list[Any]: JSONB}


def _now() -> Mapped[datetime]:
    return mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class Incident(Base):
    __tablename__ = "incidents"
    __table_args__ = (UniqueConstraint("dag_id", "run_id", "task_id"),)

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    dag_id: Mapped[str] = mapped_column(String(250))
    run_id: Mapped[str] = mapped_column(String(250))
    task_id: Mapped[str] = mapped_column(String(250))
    try_number: Mapped[int] = mapped_column(Integer)
    logical_date: Mapped[str | None] = mapped_column(String(64))
    conf: Mapped[dict[str, Any]] = mapped_column(default=dict)
    exception: Mapped[str] = mapped_column(Text, default="")
    received_at: Mapped[datetime] = _now()
    # received | running | awaiting_approval | resolved | escalated | failed
    status: Mapped[str] = mapped_column(String(32), default="received")
    root_cause: Mapped[str | None] = mapped_column(String(64))
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    scenario_id: Mapped[str | None] = mapped_column(String(32))
    eval_run_id: Mapped[str | None] = mapped_column(String(64))


class ToolCall(Base):
    """One row per tool invocation, written by the tool envelope."""

    __tablename__ = "tool_calls"
    __table_args__ = (Index("ix_tool_calls_incident", "incident_id"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    incident_id: Mapped[str] = mapped_column(String(64))
    agent: Mapped[str] = mapped_column(String(32))
    tool: Mapped[str] = mapped_column(String(64))
    tier: Mapped[int | None] = mapped_column(Integer)
    args_hash: Mapped[str] = mapped_column(String(64))
    ok: Mapped[bool] = mapped_column(Boolean)
    error_type: Mapped[str | None] = mapped_column(String(32))
    retryable: Mapped[bool] = mapped_column(Boolean, default=False)
    attempt: Mapped[int] = mapped_column(Integer, default=1)
    latency_ms: Mapped[int] = mapped_column(Integer)
    replayed: Mapped[bool] = mapped_column(Boolean, default=False)
    safety_violation: Mapped[bool] = mapped_column(Boolean, default=False)
    chaos_injected: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = _now()


class Action(Base):
    """Tier-1/2 side effects. The unique idempotency_key makes them run at most once."""

    __tablename__ = "actions"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    incident_id: Mapped[str] = mapped_column(String(64), index=True)
    tool: Mapped[str] = mapped_column(String(64))
    tier: Mapped[int] = mapped_column(Integer)
    args: Mapped[dict[str, Any]] = mapped_column()
    idempotency_key: Mapped[str] = mapped_column(String(64), unique=True)
    status: Mapped[str] = mapped_column(String(16))  # running | succeeded | failed
    approval_id: Mapped[str | None] = mapped_column(String(64))
    executed_at: Mapped[datetime] = _now()
    result: Mapped[dict[str, Any] | None] = mapped_column()


class Approval(Base):
    __tablename__ = "approvals"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    incident_id: Mapped[str] = mapped_column(String(64), index=True)
    tool: Mapped[str] = mapped_column(String(64))
    # The arguments a human approved (after any edit); the envelope checks the hash.
    args: Mapped[dict[str, Any]] = mapped_column()
    args_hash: Mapped[str] = mapped_column(String(64))
    payload: Mapped[dict[str, Any]] = mapped_column(default=dict)
    requested_at: Mapped[datetime] = _now()
    decided_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    decision: Mapped[str | None] = mapped_column(String(16))  # approve | edit | reject
    decided_by: Mapped[str | None] = mapped_column(String(64))
    comment: Mapped[str | None] = mapped_column(Text)


class Escalation(Base):
    __tablename__ = "escalations"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    incident_id: Mapped[str] = mapped_column(String(64), index=True)
    summary: Mapped[str] = mapped_column(Text)
    reason: Mapped[str] = mapped_column(Text)
    suggested_owner: Mapped[str] = mapped_column(String(64))
    created_at: Mapped[datetime] = _now()


class Postmortem(Base):
    __tablename__ = "postmortems"

    incident_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    markdown: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = _now()


class IncidentMemory(Base):
    __tablename__ = "incident_memory"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    incident_id: Mapped[str] = mapped_column(String(64), unique=True)
    summary: Mapped[str] = mapped_column(Text)
    root_cause: Mapped[str] = mapped_column(String(64))
    fix: Mapped[str] = mapped_column(Text)
    embedding: Mapped[Any] = mapped_column(Vector(EMBEDDING_DIMS))
    created_at: Mapped[datetime] = _now()
