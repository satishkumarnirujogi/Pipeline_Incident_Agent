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
    Numeric,
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
    # Worker runs of this incident (first run + crash recoveries); capped by the runner.
    run_attempts: Mapped[int] = mapped_column(Integer, default=0, server_default="0")


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
    __table_args__ = (
        Index(
            "ix_incident_memory_embedding",
            "embedding",
            postgresql_using="hnsw",
            postgresql_ops={"embedding": "vector_cosine_ops"},
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    incident_id: Mapped[str] = mapped_column(String(64), unique=True)
    summary: Mapped[str] = mapped_column(Text)
    root_cause: Mapped[str] = mapped_column(String(64))
    fix: Mapped[str] = mapped_column(Text)
    embedding: Mapped[Any] = mapped_column(Vector(EMBEDDING_DIMS))
    created_at: Mapped[datetime] = _now()


class AgentRun(Base):
    """One worker job on an incident: the first run, a resume after approval, or a recovery."""

    __tablename__ = "agent_runs"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    incident_id: Mapped[str] = mapped_column(String(64), index=True)
    kind: Mapped[str] = mapped_column(String(16))  # run | resume | recover
    attempt: Mapped[int] = mapped_column(Integer)
    started_at: Mapped[datetime] = _now()
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    # awaiting_approval | resolved | escalated | failed
    outcome: Mapped[str | None] = mapped_column(String(32))
    steps: Mapped[int] = mapped_column(Integer, default=0)
    tokens_in: Mapped[int] = mapped_column(Integer, default=0)
    tokens_out: Mapped[int] = mapped_column(Integer, default=0)
    cost_eur: Mapped[float] = mapped_column(Numeric(12, 8), default=0)


class LLMCall(Base):
    """Every model call, including failed, retried and chaos-injected ones."""

    __tablename__ = "llm_calls"
    __table_args__ = (Index("ix_llm_calls_incident", "incident_id"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    incident_id: Mapped[str] = mapped_column(String(64))
    agent_run_id: Mapped[int | None] = mapped_column(BigInteger, index=True)
    node: Mapped[str] = mapped_column(String(64))
    provider: Mapped[str] = mapped_column(String(32))
    model: Mapped[str] = mapped_column(String(128))
    tokens_in: Mapped[int] = mapped_column(Integer)
    tokens_out: Mapped[int] = mapped_column(Integer)
    cost_eur: Mapped[float] = mapped_column(Numeric(12, 8))
    latency_ms: Mapped[int] = mapped_column(Integer)
    ok: Mapped[bool] = mapped_column(Boolean)
    error_type: Mapped[str | None] = mapped_column(String(32))
    fallback_used: Mapped[bool] = mapped_column(Boolean)
    chaos_injected: Mapped[bool] = mapped_column(Boolean)
    created_at: Mapped[datetime] = _now()
