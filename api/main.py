"""FastAPI: incidents, approvals, health. The single gate for human decisions.

POST /incidents      Airflow's failure callback; dedupes on (dag_id, run_id, task_id),
                     stores the incident and enqueues run_incident.
GET  /incidents      list, filter by status
GET  /incidents/{id} detail with approvals, escalation and postmortem
GET  /approvals      the approval inbox (status=pending by default)
POST /approvals/{id}/decision
                     approve | edit | reject; edited args are validated against the tool's
                     input model first, then resume_incident is enqueued.
"""

from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime
from typing import Any, Literal

import psycopg
import redis
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, ValidationError
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

import agent.tools  # noqa: F401 - registers tool input models for validating edits
from agent.db import agent_engine
from agent.models import Approval, Escalation, Incident, Postmortem
from agent.settings import get_settings
from agent.tools.registry import args_hash, validate_args

log = logging.getLogger("uvicorn.error")

app = FastAPI(title="Pipeline Incident Agent API")


def enqueue(task: str, *args: str) -> None:
    """Hand a job to the Celery worker. Tests replace this."""
    from worker.celery_app import celery_app

    celery_app.send_task(task, args=list(args))


# ---------------------------------------------------------------- incidents


class IncidentIn(BaseModel):
    dag_id: str = Field(max_length=250)
    run_id: str = Field(max_length=250)
    task_id: str = Field(max_length=250)
    try_number: int
    logical_date: str | None = None
    conf: dict[str, Any] = Field(default_factory=dict)
    exception: str = Field(default="", max_length=2000)


def _incident_dict(i: Incident) -> dict[str, Any]:
    return {
        "id": str(i.id),
        "dag_id": i.dag_id,
        "run_id": i.run_id,
        "task_id": i.task_id,
        "try_number": i.try_number,
        "month": (i.conf or {}).get("month"),
        "exception": i.exception,
        "status": i.status,
        "root_cause": i.root_cause,
        "received_at": i.received_at,
        "resolved_at": i.resolved_at,
        "mttr_s": (i.resolved_at - i.received_at).total_seconds() if i.resolved_at else None,
        "scenario_id": i.scenario_id,
    }


@app.post("/incidents", status_code=202)
def create_incident(body: IncidentIn) -> JSONResponse:
    # faults/verify.py counts this line; keep its format.
    log.info(
        "incident received dag_id=%s run_id=%s task_id=%s try=%s exception=%r",
        body.dag_id,
        body.run_id,
        body.task_id,
        body.try_number,
        body.exception[:200],
    )
    incident = Incident(
        id=uuid.uuid4(),
        dag_id=body.dag_id,
        run_id=body.run_id,
        task_id=body.task_id,
        try_number=body.try_number,
        logical_date=body.logical_date,
        conf=body.conf,
        exception=body.exception,
        status="received",
    )
    with Session(agent_engine()) as s:
        s.add(incident)
        try:
            s.commit()
        except IntegrityError:
            s.rollback()
            existing = s.execute(
                select(Incident).where(
                    Incident.dag_id == body.dag_id,
                    Incident.run_id == body.run_id,
                    Incident.task_id == body.task_id,
                )
            ).scalar_one()
            return JSONResponse({"id": str(existing.id), "duplicate": True}, status_code=200)
        incident_id = str(incident.id)
    enqueue("run_incident", incident_id)
    return JSONResponse({"id": incident_id, "duplicate": False}, status_code=202)


@app.get("/incidents")
def list_incidents(status: str | None = None, limit: int = Query(50, le=500)) -> list[dict]:
    q = select(Incident).order_by(Incident.received_at.desc()).limit(limit)
    if status:
        q = q.where(Incident.status == status)
    with Session(agent_engine()) as s:
        return [_incident_dict(i) for i in s.execute(q).scalars()]


@app.get("/incidents/{incident_id}")
def get_incident(incident_id: uuid.UUID) -> dict:
    with Session(agent_engine()) as s:
        incident = s.get(Incident, incident_id)
        if incident is None:
            raise HTTPException(404, "incident not found")
        iid = str(incident_id)
        approvals = (
            s.execute(
                select(Approval).where(Approval.incident_id == iid).order_by(Approval.requested_at)
            )
            .scalars()
            .all()
        )
        escalation = (
            s.execute(select(Escalation).where(Escalation.incident_id == iid)).scalars().first()
        )
        postmortem = s.get(Postmortem, iid)
        return {
            **_incident_dict(incident),
            "approvals": [_approval_dict(a) for a in approvals],
            "escalation": None
            if escalation is None
            else {
                "summary": escalation.summary,
                "reason": escalation.reason,
                "suggested_owner": escalation.suggested_owner,
            },
            "postmortem": postmortem.markdown if postmortem else None,
        }


# ---------------------------------------------------------------- approvals


def _approval_dict(a: Approval) -> dict[str, Any]:
    p = a.payload or {}
    return {
        "id": str(a.id),
        "incident_id": a.incident_id,
        "tool": a.tool,
        "args": a.args,
        "rationale": p.get("rationale"),
        "rollback": p.get("rollback"),
        "diagnosis": p.get("diagnosis"),
        "preview": p.get("preview"),
        "requested_at": a.requested_at,
        "decided_at": a.decided_at,
        "decision": a.decision,
        "decided_by": a.decided_by,
        "comment": a.comment,
    }


@app.get("/approvals")
def list_approvals(status: Literal["pending", "decided", "all"] = "pending") -> list[dict]:
    q = select(Approval).order_by(Approval.requested_at)
    if status == "pending":
        q = q.where(Approval.decision.is_(None))
    elif status == "decided":
        q = q.where(Approval.decision.is_not(None))
    with Session(agent_engine()) as s:
        return [_approval_dict(a) for a in s.execute(q).scalars()]


class DecisionIn(BaseModel):
    decision: Literal["approve", "edit", "reject"]
    args: dict[str, Any] | None = None
    comment: str | None = Field(default=None, max_length=1000)
    decided_by: str = Field(default="ui", max_length=64)


def _readable(exc: ValidationError) -> str:
    return "; ".join(
        f"{'.'.join(map(str, e['loc'])) or 'args'}: {e['msg']}" for e in exc.errors()[:5]
    )


@app.post("/approvals/{approval_id}/decision", status_code=202)
def decide(approval_id: uuid.UUID, body: DecisionIn) -> dict:
    if body.decision == "reject" and not (body.comment or "").strip():
        raise HTTPException(422, "a rejection needs a comment saying what is wrong")
    if body.decision == "edit" and body.args is None:
        raise HTTPException(422, "an edit needs the edited args")
    with Session(agent_engine()) as s:
        a = s.get(Approval, approval_id)
        if a is None:
            raise HTTPException(404, "approval not found")
        if a.decision is not None:
            raise HTTPException(409, f"already decided: {a.decision}")
        if body.decision == "edit":
            try:
                parsed = validate_args(a.tool, body.args or {})
            except ValidationError as exc:
                raise HTTPException(
                    422, f"invalid arguments for {a.tool}: {_readable(exc)}"
                ) from None
            a.args = parsed.model_dump(mode="json")
            a.args_hash = args_hash(parsed)
        a.decision, a.comment, a.decided_by = body.decision, body.comment, body.decided_by
        a.decided_at = datetime.now(UTC)
        s.commit()
        incident_id = a.incident_id
    enqueue("resume_incident", incident_id, str(approval_id))
    return {"id": str(approval_id), "decision": body.decision, "incident_id": incident_id}


# ------------------------------------------------------------------- health


@app.get("/health")
def health() -> JSONResponse:
    s = get_settings()
    checks: dict[str, str] = {}
    try:
        with psycopg.connect(s.agent_dsn, connect_timeout=3) as conn:
            conn.execute("SELECT 1")
        checks["postgres"] = "ok"
    except Exception as exc:  # noqa: BLE001 - report any failure as unhealthy
        checks["postgres"] = f"error: {type(exc).__name__}"
    try:
        redis.Redis.from_url(s.redis_url, socket_timeout=3).ping()
        checks["redis"] = "ok"
    except Exception as exc:  # noqa: BLE001
        checks["redis"] = f"error: {type(exc).__name__}"

    healthy = all(v == "ok" for v in checks.values())
    return JSONResponse(
        {"status": "ok" if healthy else "degraded", **checks}, status_code=200 if healthy else 503
    )
