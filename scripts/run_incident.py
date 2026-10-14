"""Run one scenario end to end with real models, on the host.

    uv run python -m scripts.run_incident F01-v1 [--keep-fault] [--no-reset]

Clean baseline -> inject the fault -> trigger the DAG -> create the incident ->
run the agent graph (PostgresSaver checkpoints, thread_id = incident id),
answering approvals with the simulated approver -> compare with the scenario's
ground truth -> revert. Phase 6 moves the graph into the Celery worker and the
approvals behind the API and Streamlit.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
import uuid
from datetime import UTC, datetime

from langgraph.types import Command
from sqlalchemy import update
from sqlalchemy.orm import Session

from agent.db import agent_engine
from agent.graph import compile_graph, postgres_checkpointer, run_config
from agent.models import Approval, Incident
from agent.tools.orchestrator import airflow
from agent.tools.registry import args_hash, validate_args
from eval.simulated_approver import decide
from faults import injector
from faults.scenario import Scenario, load_scenario
from faults.verify import clean_chain, run_dag
from scripts.airflow_client import AirflowClient


def _failed_task(af: AirflowClient, dag_id: str, run_id: str) -> tuple[str, int]:
    tis = af._req("GET", f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances")["task_instances"]
    failed = [t for t in tis if t["state"] == "failed"]
    if not failed:
        raise RuntimeError(f"{dag_id} {run_id} has no failed task")
    return failed[0]["task_id"], failed[0]["try_number"]


def _exception(dag_id: str, run_id: str, task_id: str, try_number: int) -> str:
    """What the failure callback sends: the exception text (here read from the task log)."""
    lines = airflow().task_log(dag_id, run_id, task_id, try_number)
    errors = [
        ln.strip()
        for ln in lines
        if ln.startswith("    ") and ":" in ln and not ln.strip().startswith("at ")
    ]
    return (errors[-1] if errors else "task failed")[:2000]


def create_incident(sc: Scenario, dag_id: str, run_id: str, task_id: str, try_number: int) -> str:
    incident = Incident(
        id=uuid.uuid4(),
        dag_id=dag_id,
        run_id=run_id,
        task_id=task_id,
        try_number=try_number,
        conf={"month": sc.month},
        exception=_exception(dag_id, run_id, task_id, try_number),
        status="running",
        scenario_id=sc.id,
    )
    with Session(agent_engine()) as s:
        s.add(incident)
        s.commit()
        return str(incident.id)


def record_approval(incident_id: str, payload: dict, decision: dict) -> str:
    tool, args = payload["tool"], payload["args"]
    parsed = validate_args(tool, args)
    a = Approval(
        incident_id=incident_id,
        tool=tool,
        args=parsed.model_dump(mode="json"),
        args_hash=args_hash(parsed),
        payload=json.loads(json.dumps(payload, default=str)),
        decided_at=datetime.now(UTC),
        decision=decision["decision"],
        decided_by="simulated_approver",
        comment=decision.get("comment"),
    )
    with Session(agent_engine()) as s:
        s.add(a)
        s.commit()
        return str(a.id)


def run_graph(sc: Scenario, incident_id: str) -> dict:
    with postgres_checkpointer() as saver:
        graph = compile_graph(saver)
        config = run_config(incident_id)
        state = graph.invoke({"incident_id": incident_id}, config)
        while "__interrupt__" in state:
            payload = state["__interrupt__"][0].value
            decision = decide(sc, payload["tool"], payload["args"])
            decision["approval_id"] = record_approval(incident_id, payload, decision)
            print(
                f"  approval: {payload['tool']} {json.dumps(payload['args'])} -> "
                f"{decision['decision']}  preview: {payload.get('preview', {}).get('data')}",
                flush=True,
            )
            state = graph.invoke(Command(resume=decision), config)
    with agent_engine().begin() as conn:
        conn.execute(
            update(Incident)
            .where(Incident.id == uuid.UUID(incident_id))
            .values(
                status=state["status"],
                root_cause=(state.get("diagnosis") or {}).get("root_cause"),
                resolved_at=datetime.now(UTC),
            )
        )
    return state


def report(sc: Scenario, state: dict, took: float) -> bool:
    exp = sc.expected
    diag = state.get("diagnosis") or {}
    used = [a["tool"] for a in state.get("actions_done", [])]
    forbidden = [t for t in used if t in exp.forbidden_tools]
    reruns = used.count("rerun_dag")
    outcome_ok = (state["status"] == "resolved") == (exp.resolution == "fixed")
    checks = {
        "resolution": (f"{state['status']} (expected {exp.resolution})", outcome_ok),
        "root_cause": (
            f"{diag.get('root_cause')} (expected {exp.root_cause}, confidence "
            f"{diag.get('confidence')})",
            diag.get("root_cause") == exp.root_cause,
        ),
        "forbidden tools": (forbidden or "none", not forbidden),
        "reruns": (f"{reruns} (max {exp.max_reruns})", reruns <= exp.max_reruns),
    }
    print(f"\n=== {sc.id}: {sc.description}")
    for name, (value, ok) in checks.items():
        print(f"  {'PASS' if ok else 'FAIL'}  {name}: {value}")
    actions = [
        (a["tool"], a["ok"], (a.get("verification") or {}).get("chain_state"))
        for a in state.get("actions_done", [])
    ]
    print(f"  actions: {actions}")
    print(f"  routing: {[(r['rule'], r['next'], r['decided_by']) for r in state['routing']]}")
    calls = state.get("llm_calls", [])
    print(
        f"  cost: EUR {state.get('cost_eur', 0):.4f} over {len(calls)} LLM calls "
        f"({sum(c['tokens_in'] for c in calls):,} in / {sum(c['tokens_out'] for c in calls):,}"
        f" out tokens), {state.get('steps')} steps, {took:.0f}s"
    )
    print(f"  evidence: {diag.get('evidence')}")
    print("  postmortem: " + (state.get("postmortem") or {}).get("summary", "")[:400])
    return all(ok for _, ok in checks.values())


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("scenario_id")
    parser.add_argument("--no-reset", action="store_true", help="skip the clean baseline run")
    parser.add_argument("--keep-fault", action="store_true", help="do not revert at the end")
    args = parser.parse_args()
    logging.basicConfig(level=logging.WARNING, format="%(message)s")
    logging.getLogger("agent").setLevel(logging.INFO)

    sc = load_scenario(args.scenario_id)
    af = AirflowClient()
    if not args.no_reset:
        injector.revert(sc)
        if not clean_chain(af, sc.month):
            print("baseline not green; aborting")
            return 1
    injector.apply(sc)
    dag_id, run_id, state = run_dag(af, sc.trigger.dag_id, sc.month)
    if state != "failed":
        print(f"fault did not fail the pipeline ({dag_id} {state})")
        return 1
    task_id, try_number = _failed_task(af, dag_id, run_id)
    incident_id = create_incident(sc, dag_id, run_id, task_id, try_number)
    print(f"incident {incident_id}: {dag_id}.{task_id} try {try_number} failed", flush=True)

    started = time.time()
    try:
        final = run_graph(sc, incident_id)
        ok = report(sc, final, time.time() - started)
    finally:
        if not args.keep_fault:
            injector.revert(sc)
            clean_chain(af, sc.month)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
