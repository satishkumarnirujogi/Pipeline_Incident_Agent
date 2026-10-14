"""Supervisor: the only node that decides where to go next.

Clear cases are decided in code; the LLM is called only when something went
wrong after a proposal (a rejection, a failed action, a failed verification).
Every decision is logged with decided_by, so the report can show how often the
LLM was needed.
"""

from __future__ import annotations

import json
from typing import Literal

from langchain_core.messages import HumanMessage, RemoveMessage, SystemMessage
from langgraph.graph.message import REMOVE_ALL_MESSAGES
from langgraph.types import Command

from agent import budget
from agent.agents import common
from agent.llm import get_llm
from agent.state import IncidentState, RouteDecision

MIN_CONFIDENCE = 0.6
MAX_DIAGNOSTIC_ATTEMPTS = 2
MAX_REJECTIONS = 2
MAX_ACTIONS = 4
Next = Literal["diagnostics", "remediation", "escalate", "reporter"]


def _route(
    state: IncidentState,
    goto: Next,
    rule: int,
    reason: str,
    decided_by: str,
    extra: dict | None = None,
) -> Command[Next]:
    decision = {
        "step": state.get("steps", 0),
        "next": goto,
        "rule": rule,
        "reason": reason,
        "decided_by": decided_by,
    }
    common.log_event(state, "route", **decision)
    update: dict = {"routing": [decision], "steps": 1, "last_event": None, **(extra or {})}
    if goto == "diagnostics":  # fresh context for each diagnostic attempt
        update |= {"diag_messages": [RemoveMessage(id=REMOVE_ALL_MESSAGES)], "diag_tool_calls": 0}
    if goto == "remediation":  # fresh context for each proposal; history comes from state
        update |= {"remed_messages": [RemoveMessage(id=REMOVE_ALL_MESSAGES)], "remed_tool_calls": 0}
    if goto == "escalate" and not state.get("escalation"):
        update["escalation"] = {"reason": reason, "by": "supervisor"}
    return Command(goto=goto, update=update)


def _ask_llm(state: IncidentState) -> tuple[Next, str, dict]:
    record = {
        "diagnosis": state.get("diagnosis"),
        "last_event": state.get("last_event"),
        "actions_done": state.get("actions_done", []),
        "rejections": state.get("rejections", []),
        "errors": state.get("errors", []),
    }
    messages = [
        SystemMessage(common.prompt("supervisor")),
        HumanMessage(json.dumps(record, indent=2, default=str)),
    ]
    result = get_llm().structured("supervisor", messages, RouteDecision)
    decision: RouteDecision = result.parsed  # type: ignore[assignment]
    return decision.next, decision.reason, common.llm_update(result)


def supervisor(state: IncidentState) -> Command[Next]:
    # 1. budgets and loops
    stop = budget.over_budget(state) or budget.loop_detected(state)
    if stop:
        return _route(state, "escalate", 1, stop, "code")

    diagnosis = state.get("diagnosis")
    attempts = state.get("diagnostics_attempts", 0)
    # 2. no diagnosis yet
    if not diagnosis:
        return _route(state, "diagnostics", 2, "no diagnosis yet", "code")
    # 3. not confident enough, one more look
    if diagnosis["confidence"] < MIN_CONFIDENCE and attempts < MAX_DIAGNOSTIC_ATTEMPTS:
        return _route(
            state,
            "diagnostics",
            3,
            f"confidence {diagnosis['confidence']} < {MIN_CONFIDENCE}",
            "code",
        )
    # 4. escalation recommended, or rejected twice
    if diagnosis["recommendation"] == "escalate":
        return _route(
            state,
            "escalate",
            4,
            f"diagnosis recommends escalation ({diagnosis['root_cause']})",
            "code",
        )
    if len(state.get("rejections", [])) >= MAX_REJECTIONS:
        return _route(state, "escalate", 4, f"{MAX_REJECTIONS} proposals rejected", "code")
    if diagnosis["confidence"] < MIN_CONFIDENCE:
        return _route(
            state,
            "escalate",
            4,
            f"still not confident after {attempts} diagnostic attempts",
            "code",
        )

    last = state.get("last_event")
    # 5. the last action was a rerun and the whole chain is green
    if last == "verified":
        return _route(state, "reporter", 5, "rerun verified green", "code")
    # 6. something went wrong after a proposal: the only case the LLM decides
    if last in ("rejected", "action_failed", "verify_failed"):
        goto, reason, cost = _ask_llm(state)
        return _route(state, goto, 6, f"{last}: {reason}", "llm", extra=cost)
    # 7. keep going with the playbook
    if len(state.get("actions_done", [])) < MAX_ACTIONS:
        return _route(state, "remediation", 7, "next remediation step", "code")
    # 8. anything else
    return _route(state, "escalate", 8, f"{MAX_ACTIONS} actions without a verified fix", "code")
