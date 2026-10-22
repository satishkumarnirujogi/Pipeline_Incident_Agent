"""FastAPI: incidents, approvals, health. The single gate for human decisions.

POST /incidents      Airflow's failure callback; dedupes on (dag_id, run_id, task_id),
                     stores the incident and enqueues run_incident.
GET  /incidents      list, filter by status
GET  /incidents/{id} detail with approvals, escalation, postmortem and the trace link
GET  /incidents/{id}/timeline
                     runs, model calls, tool calls, actions, approvals and the outcome in
                     time order, plus routing decisions and actions from the checkpoint
GET  /approvals      the approval inbox (status=pending by default)
POST /approvals/{id}/decision
                     approve | edit | reject; edited args are validated against the tool's
                     input model first, then resume_incident is enqueued.
GET  /metrics/latest the newest full-system eval run without chaos (or ?eval_run_id=)
GET  /metrics/runs   headline numbers of every eval run, for the trend
GET  /metrics/ablations?split=test
                     the ablation table (build guide 9.4)
GET  /demo/scenarios, POST /demo/inject/{id}, POST /demo/revert/{id}, GET /demo/status
                     live demos: apply a fault and trigger its DAG; only when ENV=dev
"""

from __future__ import annotations

import logging
import threading
import uuid
from datetime import UTC, datetime
from typing import Any, Literal

import psycopg
import redis
from fastapi import BackgroundTasks, FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, ValidationError
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

import agent.tools  # noqa: F401 - registers tool input models for validating edits
from agent import logs, tracing
from agent.db import agent_engine
from agent.models import (
    Action,
    AgentRun,
    Approval,
    Escalation,
    Incident,
    LLMCall,
    Postmortem,
    ToolCall,
)
from agent.settings import get_settings
from agent.tools.registry import args_hash, validate_args

logs.configure()
log = logging.getLogger("api")

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
            _log_received(body, str(existing.id), duplicate=True)
            return JSONResponse({"id": str(existing.id), "duplicate": True}, status_code=200)
        incident_id = str(incident.id)
    _log_received(body, incident_id, duplicate=False)
    enqueue("run_incident", incident_id)
    return JSONResponse({"id": incident_id, "duplicate": False}, status_code=202)


def _log_received(body: IncidentIn, incident_id: str, duplicate: bool) -> None:
    # faults/verify.py counts these lines per Airflow run; keep the message format.
    log.info(
        "incident received dag_id=%s run_id=%s task_id=%s try=%s exception=%r",
        body.dag_id,
        body.run_id,
        body.task_id,
        body.try_number,
        body.exception[:200],
        extra={"incident_id": incident_id, "duplicate": duplicate},
    )


@app.get("/incidents")
def list_incidents(status: str | None = None, limit: int = Query(50, le=500)) -> list[dict]:
    q = select(Incident).order_by(Incident.received_at.desc()).limit(limit)
    if status:
        q = q.where(Incident.status == status)
    with Session(agent_engine()) as s:
        rows = [_incident_dict(i) for i in s.execute(q).scalars()]
        costs = dict(
            s.execute(  # type: ignore[arg-type]
                select(LLMCall.incident_id, func.sum(LLMCall.cost_eur))
                .where(LLMCall.incident_id.in_([r["id"] for r in rows]))
                .group_by(LLMCall.incident_id)
            ).all()
        )
    for r in rows:
        r["cost_eur"] = round(float(costs.get(r["id"]) or 0), 5)
    return rows


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
            "trace_url": tracing.trace_url(iid),
        }


def _checkpoint(incident_id: str) -> dict[str, Any]:
    """What only the graph state holds: the Supervisor's routing decisions, the diagnosis
    and the actions with their verification. Read-only; {} before the first checkpoint."""
    from langgraph.checkpoint.postgres import PostgresSaver
    from psycopg.rows import dict_row

    try:
        with psycopg.connect(
            get_settings().agent_dsn, autocommit=True, prepare_threshold=0, row_factory=dict_row
        ) as conn:
            found = PostgresSaver(conn).get_tuple({"configurable": {"thread_id": incident_id}})
    except Exception:  # noqa: BLE001 - the timeline still has the database events
        log.warning("checkpoint unreadable", extra={"incident_id": incident_id}, exc_info=True)
        return {}
    if found is None:
        return {}
    values = found.checkpoint.get("channel_values", {})
    keep = ("tool", "args", "ok", "message", "approved", "verification", "step")
    return {
        "routing": values.get("routing", []),
        "diagnosis": values.get("diagnosis"),
        "actions": [{k: a.get(k) for k in keep} for a in values.get("actions_done", [])],
        "steps": values.get("steps"),
        "verified": values.get("verified"),
    }


def _event(
    at: Any, kind: str, who: str, what: str, ok: bool | None = None, **detail: Any
) -> dict[str, Any]:
    return {"at": at, "kind": kind, "who": who, "what": what, "ok": ok, "detail": detail}


@app.get("/incidents/{incident_id}/timeline")
def incident_timeline(incident_id: uuid.UUID) -> dict:
    iid = str(incident_id)
    with Session(agent_engine()) as s:
        incident = s.get(Incident, incident_id)
        if incident is None:
            raise HTTPException(404, "incident not found")
        events = [_event(incident.received_at, "incident", "airflow", "incident received")]
        for r in s.execute(select(AgentRun).where(AgentRun.incident_id == iid)).scalars():
            events.append(_event(r.started_at, "run", "worker", f"{r.kind} started"))
            if r.ended_at:
                events.append(
                    _event(
                        r.ended_at,
                        "run",
                        "worker",
                        f"{r.kind} ended: {r.outcome}",
                        r.outcome != "failed",
                        steps=r.steps,
                        cost_eur=round(float(r.cost_eur or 0), 5),
                    )
                )
        for c in s.execute(select(LLMCall).where(LLMCall.incident_id == iid)).scalars():
            events.append(
                _event(
                    c.created_at,
                    "llm",
                    c.node,
                    c.model,
                    c.ok,
                    error_type=c.error_type,
                    fallback=c.fallback_used,
                    chaos=c.chaos_injected,
                    tokens=c.tokens_in + c.tokens_out,
                    cost_eur=round(float(c.cost_eur), 5),
                    latency_ms=c.latency_ms,
                )
            )
        for t in s.execute(select(ToolCall).where(ToolCall.incident_id == iid)).scalars():
            events.append(
                _event(
                    t.created_at,
                    "tool",
                    t.agent,
                    t.tool,
                    t.ok,
                    tier=t.tier,
                    error_type=t.error_type,
                    attempt=t.attempt,
                    chaos=t.chaos_injected,
                    replayed=t.replayed,
                    safety_violation=t.safety_violation,
                    latency_ms=t.latency_ms,
                )
            )
        for a in s.execute(select(Action).where(Action.incident_id == iid)).scalars():
            events.append(
                _event(
                    a.executed_at,
                    "action",
                    "remediation",
                    a.tool,
                    None if a.status == "running" else a.status == "succeeded",
                    tier=a.tier,
                    args=a.args,
                    approval_id=a.approval_id,
                )
            )
        for ap in s.execute(select(Approval).where(Approval.incident_id == iid)).scalars():
            events.append(
                _event(ap.requested_at, "approval", "approval_gate", f"{ap.tool} needs approval")
            )
            if ap.decided_at:
                events.append(
                    _event(
                        ap.decided_at,
                        "approval",
                        ap.decided_by or "human",
                        f"{ap.tool}: {ap.decision}",
                        ap.decision in ("approve", "edit"),
                        comment=ap.comment,
                    )
                )
        for e in s.execute(select(Escalation).where(Escalation.incident_id == iid)).scalars():
            events.append(
                _event(e.created_at, "escalation", "agent", e.reason, owner=e.suggested_owner)
            )
        pm = s.get(Postmortem, iid)
        if pm:
            events.append(_event(pm.created_at, "postmortem", "reporter", "postmortem written"))
        if incident.resolved_at:
            events.append(
                _event(incident.resolved_at, "incident", "agent", f"incident {incident.status}")
            )
        status = incident.status
    events.sort(key=lambda e: e["at"])
    return {
        "incident_id": iid,
        "status": status,
        "events": events,
        "trace_url": tracing.trace_url(iid),
        **_checkpoint(iid),
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
    log.info(
        "approval decided",
        extra={
            "incident_id": incident_id,
            "approval_id": str(approval_id),
            "decision": body.decision,
            "decided_by": body.decided_by,
        },
    )
    enqueue("resume_incident", incident_id, str(approval_id))
    return {"id": str(approval_id), "decision": body.decision, "incident_id": incident_id}


# ------------------------------------------------------------------ metrics


@app.get("/metrics/latest")
def metrics_latest(eval_run_id: str | None = None) -> dict:
    from eval import kpis

    try:
        out = kpis.latest(eval_run_id)
    except LookupError:
        raise HTTPException(404, f"eval run {eval_run_id} not found or not finished") from None
    if out is None:
        raise HTTPException(404, "no finished evaluation run yet (make eval)")
    return out


@app.get("/metrics/runs")
def metrics_runs() -> list[dict]:
    from eval import kpis

    return kpis.trend()


@app.get("/metrics/ablations")
def metrics_ablations(split: Literal["dev", "test"] = "test") -> dict:
    from eval import kpis

    return kpis.ablation_table(split)


# --------------------------------------------------------------------- demo
# A live demo changes the shared warehouse, so it is off unless ENV=dev and one runs at a
# time. Never start one while an evaluation is running.

_demo: dict[str, Any] = {"state": "idle"}
_demo_lock = threading.Lock()


def _dev_only() -> None:
    if get_settings().env != "dev":
        raise HTTPException(404, "demo endpoints are disabled unless ENV=dev")


def _scenario(scenario_id: str) -> Any:
    from faults.scenario import load_scenario

    try:
        return load_scenario(scenario_id)
    except FileNotFoundError:
        raise HTTPException(404, f"unknown scenario {scenario_id}") from None


def _demo_job(action: str, scenario_id: str) -> None:
    from faults import injector
    from scripts.airflow_client import AirflowClient

    try:
        sc = _scenario(scenario_id)
        if action == "inject":
            injector.revert(sc)  # start from the clean month
            details = injector.apply(sc)
            run_id = AirflowClient().trigger(sc.trigger.dag_id, {"month": sc.month})
            message = f"applied {sc.fault}; triggered {sc.trigger.dag_id} {run_id}"
        else:
            details = injector.revert(sc)
            weather = sc.trigger.dag_id == "ingest_weather"
            dag = "ingest_weather" if weather else "ingest_taxi_trips"
            run_id = AirflowClient().trigger(dag, {"month": sc.month})
            message = f"reverted; triggered a clean {dag} {run_id}"
        _demo.update(state="done", message=message, details=details, at=datetime.now(UTC))
        log.info("demo %s %s: %s", action, scenario_id, message)
    except Exception as exc:  # noqa: BLE001 - shown on the Demo page
        log.exception("demo %s %s failed", action, scenario_id)
        _demo.update(state="failed", message=f"{type(exc).__name__}: {exc}", at=datetime.now(UTC))
    finally:
        _demo_lock.release()


def _start_demo(action: str, scenario_id: str, tasks: BackgroundTasks) -> dict:
    _dev_only()
    sc = _scenario(scenario_id)
    if not _demo_lock.acquire(blocking=False):
        raise HTTPException(409, f"a demo {_demo.get('action')} is still running")
    _demo.clear()
    _demo.update(
        state="running",
        action=action,
        scenario_id=sc.id,
        month=sc.month,
        message=f"{action} {sc.id} started",
        at=datetime.now(UTC),
    )
    tasks.add_task(_demo_job, action, sc.id)
    return dict(_demo)


@app.get("/demo/scenarios")
def demo_scenarios() -> list[dict]:
    _dev_only()
    from faults.scenario import load_all

    return [
        {
            "id": sc.id,
            "fault": sc.fault,
            "month": sc.month,
            "dag_id": sc.trigger.dag_id,
            "expected": sc.expected.resolution,
            "description": sc.description,
        }
        for sc in load_all("dev")
    ]


@app.post("/demo/inject/{scenario_id}", status_code=202)
def demo_inject(scenario_id: str, tasks: BackgroundTasks) -> dict:
    """Apply the scenario's fault and trigger its DAG; the failure callback opens an incident
    as usual. Follow it on the Incidents page."""
    return _start_demo("inject", scenario_id, tasks)


@app.post("/demo/revert/{scenario_id}", status_code=202)
def demo_revert(scenario_id: str, tasks: BackgroundTasks) -> dict:
    """Undo the fault and any agent changes for its month, then rerun the month clean."""
    return _start_demo("revert", scenario_id, tasks)


@app.get("/demo/status")
def demo_status() -> dict:
    _dev_only()
    return dict(_demo)


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
