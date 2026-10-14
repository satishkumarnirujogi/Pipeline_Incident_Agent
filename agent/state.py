"""Incident state and the shared vocabulary of the agent system.

State holds plain dicts (model.model_dump()); nodes validate with the Pydantic
models when reading, so checkpoints stay portable between code versions.
"""

from __future__ import annotations

import operator
from enum import StrEnum
from typing import Annotated, Any, Literal, TypedDict

from langgraph.graph.message import add_messages
from pydantic import BaseModel, Field


class RootCause(StrEnum):
    """The Diagnostics agent must answer with exactly one of these."""

    SCHEMA_DRIFT = "SCHEMA_DRIFT"
    TYPE_MISMATCH = "TYPE_MISMATCH"
    MISSING_SOURCE = "MISSING_SOURCE"
    NULL_SPIKE = "NULL_SPIKE"
    DUPLICATE_LOAD = "DUPLICATE_LOAD"
    OUT_OF_RANGE_VALUES = "OUT_OF_RANGE_VALUES"
    ORPHAN_KEYS = "ORPHAN_KEYS"
    UPSTREAM_API_OUTAGE = "UPSTREAM_API_OUTAGE"
    INCOMPLETE_API_PAYLOAD = "INCOMPLETE_API_PAYLOAD"
    CREDENTIALS = "CREDENTIALS"


class Diagnosis(BaseModel):
    root_cause: RootCause
    confidence: float = Field(ge=0, le=1)
    evidence: list[str] = Field(
        min_length=1, max_length=5, description="short excerpts, not whole logs"
    )
    failing_component: str = Field(description='e.g. "ingest_taxi_trips.validate_contract"')
    recommendation: Literal["fix", "escalate"]
    missing_information: str | None = Field(
        default=None, description="what would raise confidence, if it is low"
    )


class ProposedAction(BaseModel):
    tool: str
    args: dict[str, Any]
    rationale: str = Field(max_length=1000)
    rollback: str | None = None


class RemediationDecision(BaseModel):
    """Exactly one next action, or an escalation."""

    decision: Literal["action", "escalate"]
    action: ProposedAction | None = None
    escalation_reason: str | None = None


class RouteDecision(BaseModel):
    next: Literal["remediation", "escalate"]
    reason: str = Field(max_length=500)


class Postmortem(BaseModel):
    summary: str = Field(description="two or three sentences: what broke, why, what was done")
    markdown: str = Field(description="the full postmortem in Markdown")
    fix: str = Field(description="one line: the fix applied, or why it was escalated")


class IncidentState(TypedDict, total=False):
    incident_id: str
    incident: dict  # dag_id, run_id, task_id, try_number, conf, exception, month

    diag_messages: Annotated[list, add_messages]
    diag_tool_calls: int
    diagnosis: dict | None
    diagnostics_attempts: int

    remed_messages: Annotated[list, add_messages]
    remed_tool_calls: int
    proposed_action: dict | None
    proposals: int  # also the idempotency step of the next action
    approval_id: str | None
    preview: dict | None

    actions_done: Annotated[list[dict], operator.add]
    verified: bool
    last_event: str | None  # rejected | action_ok | action_failed | verified | verify_failed
    rejections: Annotated[list[str], operator.add]
    errors: Annotated[list[dict], operator.add]
    routing: Annotated[list[dict], operator.add]
    tool_log: Annotated[list[dict], operator.add]

    escalation: dict | None
    postmortem: dict | None
    steps: Annotated[int, operator.add]
    cost_eur: Annotated[float, operator.add]
    llm_calls: Annotated[list[dict], operator.add]
    status: Literal["running", "resolved", "escalated", "failed"]
