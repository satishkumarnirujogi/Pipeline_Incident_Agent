"""Approval gate, action execution with verification, and escalation.

Tier-2 actions pause at interrupt() until a human approves, edits or rejects.
Everything above interrupt() runs again when the graph resumes, so it is
side-effect free: the dry run is read-only and safe to repeat. Actions execute
only in execute_action, a later node.
"""

from __future__ import annotations

from typing import Literal

from langgraph.types import Command, interrupt
from pydantic import ValidationError

from agent import ablation
from agent.agents import common
from agent.state import IncidentState, ProposedAction, RootCause
from agent.tools.registry import REGISTRY, TIER_CHANGES_DATA, validate_args

VERIFY_TIMEOUT_S = 600


def _back() -> Literal["supervisor", "single_agent"]:
    """After a rejection: the Supervisor decides, or the single agent in ablation A."""
    return "single_agent" if ablation.single_agent() else "supervisor"


def approval_gate(
    state: IncidentState,
) -> Command[Literal["execute_action", "supervisor", "single_agent", "escalate"]]:
    action = ProposedAction.model_validate(state["proposed_action"])
    spec = REGISTRY.get(action.tool)
    if spec is None or spec.tier < TIER_CHANGES_DATA:
        # tier 0/1 run automatically; unknown tools are refused (and logged) by the envelope
        return Command(goto="execute_action")

    preview = common.run_dry(action.tool, action.args, common.ctx(state, "remediation"))
    decision = interrupt(
        {
            "incident_id": state["incident_id"],
            "tool": action.tool,
            "args": action.args,
            "tier": spec.tier,
            "rationale": action.rationale,
            "rollback": action.rollback,
            "diagnosis": state.get("diagnosis"),
            "preview": preview.model_dump(exclude_none=True),
        }
    )

    # Runs once, after resume. The decision itself is stored by whoever resumed the graph
    # (the API in production, the simulated approver in evaluation).
    common.log_event(
        state,
        "approval",
        tool=action.tool,
        decision=decision.get("decision"),
        approval_id=decision.get("approval_id"),
    )
    choice = decision.get("decision")
    if choice == "timeout":  # nobody decided within the approval window
        return Command(
            goto="escalate",
            update={
                "proposed_action": None,
                "steps": 1,
                "escalation": {
                    "reason": decision.get("comment") or "approval timed out",
                    "by": "approval_timeout",
                },
            },
        )
    if choice in ("approve", "edit"):
        update: dict = {"approval_id": decision["approval_id"], "steps": 1}
        if choice == "edit":
            try:
                validate_args(action.tool, decision["args"])
            except ValidationError as exc:
                comment = f"edited arguments invalid: {exc.errors()[0]['msg']}"
                return Command(
                    goto=_back(),
                    update={
                        "rejections": [comment],
                        "last_event": "rejected",
                        "approval_id": None,
                        "proposed_action": None,
                        "steps": 1,
                    },
                )
            update["proposed_action"] = action.model_copy(
                update={"args": decision["args"]}
            ).model_dump(mode="json")
        return Command(goto="execute_action", update=update)
    return Command(
        goto=_back(),
        update={
            "rejections": [decision.get("comment") or "rejected"],
            "last_event": "rejected",
            "proposed_action": None,
            "steps": 1,
        },
    )


def execute_action(state: IncidentState) -> dict:
    """Run the approved (or automatic) action; after a rerun, verify the whole DAG chain."""
    action = ProposedAction.model_validate(state["proposed_action"])
    step = state.get("proposals", 0)
    tool_ctx = common.ctx(state, "remediation", approval_id=state.get("approval_id"), step=step)
    result = common.run_tool(action.tool, action.args, tool_ctx)
    done = {
        "tool": action.tool,
        "args": action.args,
        "ok": result.ok,
        "error_type": result.error_type,
        "message": result.message,
        "data": result.data,
        "approval_id": state.get("approval_id"),
        # Stated plainly so the postmortem cannot get it wrong.
        "approved": (
            f"approved by a human (approval {state['approval_id']})"
            if state.get("approval_id")
            else "automatic (tier 0/1, no approval needed)"
        ),
        "step": step,
    }
    update: dict = {
        "proposed_action": None,
        "approval_id": None,
        "steps": 1,
        "tool_log": [
            {
                "agent": "remediation",
                "tool": action.tool,
                "args_hash": common.args_hash(action.args),
                "ok": result.ok,
                "error_type": result.error_type,
            }
        ],
    }

    if not result.ok:
        update |= {
            "last_event": "action_failed",
            "verified": False,
            "errors": [
                {"tool": action.tool, "error_type": result.error_type, "message": result.message}
            ],
        }
    elif action.tool == "rerun_dag" and isinstance(result.data, dict):
        wait = common.run_tool(
            "wait_for_dag_run",
            {
                "dag_id": result.data["dag_id"],
                "run_id": result.data["run_id"],
                "timeout_s": VERIFY_TIMEOUT_S,
            },
            common.ctx(state, "remediation"),
        )
        waited: dict = wait.data if wait.ok and isinstance(wait.data, dict) else {}
        chain = waited.get("chain_state")
        verified = chain == "success"
        done["verification"] = {
            "chain_state": chain or "unknown",
            "run": waited.get("run"),
            "followed": waited.get("followed"),
            "message": wait.message,
        }
        update |= {"verified": verified, "last_event": "verified" if verified else "verify_failed"}
        if not verified:
            update["errors"] = [
                {
                    "tool": "wait_for_dag_run",
                    "error_type": "verification",
                    "message": f"chain ended {chain or wait.message}",
                }
            ]
    else:
        update |= {"last_event": "action_ok"}
    common.log_event(
        state, "action", tool=action.tool, ok=result.ok, last_event=update["last_event"]
    )
    update["actions_done"] = [done]
    return update


OWNERS = {
    RootCause.MISSING_SOURCE: "upstream-provider",
    RootCause.INCOMPLETE_API_PAYLOAD: "upstream-provider",
    RootCause.UPSTREAM_API_OUTAGE: "upstream-provider",
    RootCause.CREDENTIALS: "security",
}


def escalate(state: IncidentState) -> dict:
    diagnosis = state.get("diagnosis") or {}
    reason = (state.get("escalation") or {}).get("reason") or "escalated"
    root = diagnosis.get("root_cause")
    summary = (
        f"{common.incident_brief(state)['dag_id']}.{common.incident_brief(state)['task_id']} "
        f"failed for {state['incident'].get('month')}. "
        + (
            f"Diagnosis: {root} (confidence {diagnosis.get('confidence')}). "
            f"Evidence: {'; '.join(diagnosis.get('evidence', [])[:2])}. "
            if root
            else ""
        )
        + f"Actions tried: {len(state.get('actions_done', []))}."
    )[:2000]
    owner = OWNERS.get(RootCause(root), "data-platform") if root else "data-platform"
    result = common.run_tool(
        "escalate_to_human",
        {
            "summary": summary if len(summary) >= 10 else f"incident {state['incident_id']}",
            "reason": reason[:1000],
            "suggested_owner": owner,
        },
        common.ctx(state, "remediation"),
    )
    common.log_event(state, "escalated", reason=reason, owner=owner, ok=result.ok)
    return {
        "escalation": {
            **(state.get("escalation") or {}),
            "reason": reason,
            "owner": owner,
            "summary": summary,
        },
        "status": "escalated",
        "steps": 1,
    }
