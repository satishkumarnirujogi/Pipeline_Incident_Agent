"""Remediation agent: proposes exactly one action (or an escalation) per call.

Everything is a proposal that passes the approval gate and is executed and verified by
execute_action. An upstream API outage follows a fixed policy in code instead of the model:
wait for the API (up to OUTAGE_CHECKS waits of OUTAGE_WAIT_S), then rerun, or escalate if
it is still down. Each wait is its own checkpointed tool step.
Multi-step fixes happen through the loop: after an action executes, the
Supervisor sends it back here and it proposes the next step.
"""

from __future__ import annotations

import json
from typing import Literal

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage

from agent.agents import common
from agent.llm import get_llm
from agent.state import IncidentState, ProposedAction, RemediationDecision

TOOLS = ["check_api_health"]
OUTAGE = "UPSTREAM_API_OUTAGE"
OUTAGE_CHECKS = 3  # waits per remediation visit: about 15 min for a transient outage to clear
OUTAGE_WAIT_S = 300  # the most check_api_health waits in one call
MAX_TOOL_CALLS = OUTAGE_CHECKS


def _history(state: IncidentState) -> dict:
    return {
        "incident": common.incident_brief(state),
        # missing_information is for a second diagnosis attempt, not for remediation
        "diagnosis": {
            k: v for k, v in (state.get("diagnosis") or {}).items() if k != "missing_information"
        },
        "actions_done": [
            {k: a.get(k) for k in ("tool", "args", "ok", "message", "verification")}
            for a in state.get("actions_done", [])
        ],
        "rejections": state.get("rejections", []),
        "errors": [e.get("message") for e in state.get("errors", [])],
    }


def _health_checks(state: IncidentState) -> list[dict]:
    """Results of this visit's check_api_health calls, oldest first ({} if one failed)."""
    out = []
    for m in state.get("remed_messages", []):
        if isinstance(m, ToolMessage) and m.name == "check_api_health":
            result = common.unwrap(str(m.content))
            out.append((result.get("data") or {}) if result.get("ok") else {})
    return out


def _is_outage(state: IncidentState) -> bool:
    return (state.get("diagnosis") or {}).get("root_cause") == OUTAGE


def _outage_step(state: IncidentState, seed: list) -> dict:
    """Wait for the API again, or stop waiting (healthy, or out of waits)."""
    checks = _health_checks(state)
    if (checks and checks[-1].get("healthy")) or len(checks) >= OUTAGE_CHECKS:
        return {"remed_messages": [*seed, AIMessage("(outage policy: done waiting)")], "steps": 1}
    month = common.incident_brief(state).get("month")
    args = {"month": month, "wait_until_healthy_s": OUTAGE_WAIT_S}
    cid = f"health-{len(checks) + 1}"
    message = AIMessage("", tool_calls=[{"name": "check_api_health", "args": args, "id": cid}])
    return {"remed_messages": [*seed, message], "steps": 1}


def _outage_decision(state: IncidentState) -> RemediationDecision:
    checks = _health_checks(state)
    month = common.incident_brief(state).get("month")
    last = checks[-1] if checks else {}
    if last.get("healthy"):
        return RemediationDecision(
            decision="action",
            action=ProposedAction(
                tool="rerun_dag",
                args={"dag_id": "ingest_weather", "conf": {"month": month}},
                rationale=(
                    f"The weather API is healthy again (check {len(checks)} of "
                    f"{OUTAGE_CHECKS}); a rerun replaces the month's weather partition."
                ),
                rollback="none needed: the rerun replaces the partition",
            ),
        )
    waited = len(checks) * OUTAGE_WAIT_S // 60
    status = last.get("status_code") or last.get("error") or "no answer"
    return RemediationDecision(
        decision="escalate",
        escalation_reason=(
            f"{OUTAGE} for ingest_weather {month} is not resolved: the API is still down "
            f"({status}) after {len(checks)} health checks over about {waited} min. A rerun "
            "now would fail again. Please check the weather provider; once it recovers, "
            f'rerun_dag ingest_weather with {{"month": "{month}"}} replaces the partition '
            "for that month and is safe. No data was changed."
        ),
    )


def remediation_llm(state: IncidentState) -> dict:
    seed = []
    if not state.get("remed_messages"):
        seed = [
            SystemMessage(common.prompt("remediation")),
            HumanMessage(
                "Propose the next action for this incident.\n\n"
                + json.dumps(_history(state), indent=2, default=str)
            ),
        ]
    if _is_outage(state):
        return _outage_step(state, seed)
    # Every other playbook step follows from the diagnosis: no tool turn, straight to the
    # decision (one LLM call fewer).
    return {"remed_messages": [*seed, AIMessage("(no investigation needed)")], "steps": 1}


def remediation_tools(state: IncidentState) -> dict:
    last: AIMessage = state["remed_messages"][-1]
    remaining = MAX_TOOL_CALLS - state.get("remed_tool_calls", 0)
    tool_messages, rows = common.execute_tool_calls(state, "remediation", last, remaining, TOOLS)
    return {
        "remed_messages": tool_messages,
        "tool_log": rows,
        "steps": 1,
        "remed_tool_calls": state.get("remed_tool_calls", 0) + min(len(rows), remaining),
    }


def after_remediation_llm(
    state: IncidentState,
) -> Literal["remediation_tools", "remediation_finalize"]:
    last = state["remed_messages"][-1]
    if getattr(last, "tool_calls", None) and state.get("remed_tool_calls", 0) < MAX_TOOL_CALLS:
        return "remediation_tools"
    return "remediation_finalize"


def after_remediation_tools(state: IncidentState) -> Literal["remediation", "remediation_finalize"]:
    if state.get("remed_tool_calls", 0) >= MAX_TOOL_CALLS:
        return "remediation_finalize"
    return "remediation"


def remediation_finalize(state: IncidentState) -> dict:
    update: dict = {"steps": 1}
    if _is_outage(state):
        decision = _outage_decision(state)
    else:
        messages = list(state["remed_messages"])
        if getattr(messages[-1], "tool_calls", None):
            tool_messages, _ = common.execute_tool_calls(
                state, "remediation", messages[-1], 0, TOOLS
            )
            messages += tool_messages
        messages.append(HumanMessage("Give your RemediationDecision now: one action, or escalate."))
        result = get_llm().structured("remediation_finalize", messages, RemediationDecision)
        decision = result.parsed  # type: ignore[assignment]
        update |= common.llm_update(result)
    if decision.decision == "action" and decision.action is not None:
        update["proposed_action"] = decision.action.model_dump(mode="json")
        update["proposals"] = state.get("proposals", 0) + 1
        update["escalation"] = None
        common.log_event(state, "proposal", tool=decision.action.tool, args=decision.action.args)
    else:
        update["proposed_action"] = None
        update["escalation"] = {
            "reason": decision.escalation_reason or "remediation found no safe action",
            "by": "remediation",
        }
        common.log_event(state, "proposal", tool=None, escalate=update["escalation"]["reason"])
    return update


def after_remediation(state: IncidentState) -> Literal["approval_gate", "escalate"]:
    return "approval_gate" if state.get("proposed_action") else "escalate"
