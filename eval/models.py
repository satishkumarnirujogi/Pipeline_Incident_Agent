"""Evaluation tables in the agent database (schema eval). Migrations in db/migrations."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import BigInteger, Boolean, DateTime, Float, Integer, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from agent.models import Base


class EvalInjection(Base):
    __tablename__ = "injections"
    __table_args__ = {"schema": "eval"}

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    scenario_id: Mapped[str] = mapped_column(Text)
    eval_run_id: Mapped[str | None] = mapped_column(Text)
    action: Mapped[str] = mapped_column(Text)  # apply | revert
    mechanism: Mapped[str] = mapped_column(Text)
    details: Mapped[dict[str, Any]] = mapped_column(default=dict)
    at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class EvalRun(Base):
    __tablename__ = "runs"
    __table_args__ = {"schema": "eval"}

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    split: Mapped[str] = mapped_column(String(16))
    # models, chaos, git sha, scenario list
    config: Mapped[dict[str, Any]] = mapped_column(default=dict, server_default="{}")
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class EvalResult(Base):
    """One row per scenario per eval run. Every metric in the report is a query over this
    table (plus tool_calls / llm_calls by incident_id): see eval/queries.sql."""

    __tablename__ = "results"
    __table_args__ = {"schema": "eval"}

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    eval_run_id: Mapped[str] = mapped_column(String(64), index=True)
    scenario_id: Mapped[str] = mapped_column(String(32))
    fault: Mapped[str] = mapped_column(String(32))
    split: Mapped[str] = mapped_column(String(8))
    month: Mapped[str] = mapped_column(String(7))
    incident_id: Mapped[str | None] = mapped_column(String(64))
    chaos_mode: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")
    worker_killed: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")

    expected_resolution: Mapped[str] = mapped_column(String(16))
    expected_root_cause: Mapped[str] = mapped_column(String(32))
    status: Mapped[str] = mapped_column(String(32))  # incident status, or timeout / error
    root_cause: Mapped[str | None] = mapped_column(String(32))

    resolution_ok: Mapped[bool] = mapped_column(Boolean)
    verified: Mapped[bool] = mapped_column(Boolean)
    verification: Mapped[dict[str, Any]] = mapped_column(default=dict, server_default="{}")
    root_cause_correct: Mapped[bool] = mapped_column(Boolean)
    forbidden_count: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    forbidden_detail: Mapped[list[Any]] = mapped_column(default=list, server_default="[]")
    success: Mapped[bool] = mapped_column(Boolean)

    reruns: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    approvals_requested: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    approvals_granted: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    mttr_s: Mapped[float | None] = mapped_column(Float)
    steps: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    cost_eur: Mapped[float] = mapped_column(Float, default=0.0, server_default="0")
    llm_calls: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    tool_calls: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    tool_calls_ok: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    chaos_events: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    error: Mapped[str | None] = mapped_column(Text)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
