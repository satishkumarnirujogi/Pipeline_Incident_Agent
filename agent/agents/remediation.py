"""Remediation agent: proposes exactly one action (or an escalation) per call.

It may check the weather API's health first; everything else is a proposal that
passes the approval gate and is executed and verified by execute_action.
Multi-step fixes happen through the loop: after an action executes, the
Supervisor sends it back here and it proposes the next step.
"""

from __future__ import annotations

import json
from typing import Literal

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage

from agent.agents import common
from agent.llm import get_llm
from agent.state import IncidentState, RemediationDecision

MAX_TOOL_CALLS = 3
TOOLS = ["check_api_health"]


def _history(state: IncidentState) -> dict:
    return {
        "incident": common.incident_brief(state),
        "diagnosis": state.get("diagnosis"),
        "actions_done": [
            {k: a.get(k) for k in ("tool", "args", "ok", "message", "verification")}
            for a in state.get("actions_done", [])
        ],
        "rejections": state.get("rejections", []),
        "errors": [e.get("message") for e in state.get("errors", [])],
    }


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
    messages = [*state.get("remed_messages", []), *seed]
    result = get_llm().with_tools("remediation", messages, common.tool_schemas(TOOLS))
    return {"remed_messages": [*seed, result.message], "steps": 1, **common.llm_update(result)}


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
    messages = list(state["remed_messages"])
    if getattr(messages[-1], "tool_calls", None):
        tool_messages, _ = common.execute_tool_calls(state, "remediation", messages[-1], 0, TOOLS)
        messages += tool_messages
    messages.append(HumanMessage("Give your RemediationDecision now: one action, or escalate."))
    result = get_llm().structured("remediation_finalize", messages, RemediationDecision)
    decision: RemediationDecision = result.parsed  # type: ignore[assignment]
    update: dict = {"steps": 1, **common.llm_update(result)}
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
