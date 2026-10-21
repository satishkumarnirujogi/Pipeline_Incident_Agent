"""Diagnostics agent: read-only investigation, then one structured Diagnosis.

Three graph nodes, so every step is checkpointed and a crash never repeats a
finished tool call: diagnostics (LLM turn) -> diagnostics_tools -> diagnostics
... until no more tool calls or 8 calls used -> diagnostics_finalize.
"""

from __future__ import annotations

import json
from typing import Literal

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage

from agent import ablation
from agent.agents import common
from agent.llm import get_llm
from agent.state import Diagnosis, IncidentState, recommendation_for
from agent.tools.registry import tools_for

MAX_TOOL_CALLS = 8
ALL_TOOLS = [s.name for s in tools_for("diagnostics") if s.name != "escalate_to_human"]
MEMORY_TOOL = "search_incident_memory"


def tools() -> list[str]:
    """The no_memory ablation takes incident memory away (and its step from the prompt)."""
    return ALL_TOOLS if ablation.memory_on() else [t for t in ALL_TOOLS if t != MEMORY_TOOL]


def _prompt() -> str:
    text = common.prompt("diagnostics")
    if ablation.memory_on():
        return text
    return "\n".join(line for line in text.splitlines() if MEMORY_TOOL not in line)


def _seed(state: IncidentState) -> list:
    brief = common.incident_brief(state)
    text = "A pipeline task failed. Investigate and find the root cause.\n\n" + json.dumps(
        brief, indent=2
    )
    previous = state.get("diagnosis")
    if previous:
        text += (
            "\n\nA previous diagnosis was not confident enough "
            f"({previous['root_cause']}, confidence {previous['confidence']}). "
            f"What was missing: {previous.get('missing_information') or 'not stated'}. "
            "Gather that evidence before concluding."
        )
    return [SystemMessage(_prompt()), HumanMessage(text)]


def diagnostics_llm(state: IncidentState) -> dict:
    seed = [] if state.get("diag_messages") else _seed(state)
    messages = [*state.get("diag_messages", []), *seed]
    result = get_llm().with_tools("diagnostics", messages, common.tool_schemas(tools()))
    return {"diag_messages": [*seed, result.message], "steps": 1, **common.llm_update(result)}


def diagnostics_tools(state: IncidentState) -> dict:
    last: AIMessage = state["diag_messages"][-1]
    remaining = MAX_TOOL_CALLS - state.get("diag_tool_calls", 0)
    tool_messages, rows = common.execute_tool_calls(state, "diagnostics", last, remaining, tools())
    return {
        "diag_messages": tool_messages,
        "tool_log": rows,
        "steps": 1,
        "diag_tool_calls": state.get("diag_tool_calls", 0) + min(len(rows), remaining),
    }


def after_diagnostics_llm(
    state: IncidentState,
) -> Literal["diagnostics_tools", "diagnostics_finalize"]:
    last = state["diag_messages"][-1]
    if getattr(last, "tool_calls", None) and state.get("diag_tool_calls", 0) < MAX_TOOL_CALLS:
        return "diagnostics_tools"
    return "diagnostics_finalize"


def after_diagnostics_tools(state: IncidentState) -> Literal["diagnostics", "diagnostics_finalize"]:
    """Budget used up: conclude now instead of asking the model for another turn."""
    if state.get("diag_tool_calls", 0) >= MAX_TOOL_CALLS:
        return "diagnostics_finalize"
    return "diagnostics"


def diagnostics_finalize(state: IncidentState) -> dict:
    messages = list(state["diag_messages"])
    if getattr(messages[-1], "tool_calls", None):
        # Budget ran out with calls pending: answer them so the conversation stays valid.
        tool_messages, _ = common.execute_tool_calls(state, "diagnostics", messages[-1], 0, tools())
        messages += tool_messages
    messages.append(HumanMessage("Give your Diagnosis now, based only on the evidence above."))
    result = get_llm().structured("diagnostics_finalize", messages, Diagnosis)
    diagnosis = result.parsed.model_dump(mode="json")  # type: ignore[union-attr]
    policy = recommendation_for(diagnosis["root_cause"])
    if diagnosis["recommendation"] != policy:
        common.log_event(
            state,
            "recommendation_overridden",
            root_cause=diagnosis["root_cause"],
            model=diagnosis["recommendation"],
            policy=policy,
        )
        diagnosis["recommendation"] = policy
    common.log_event(state, "diagnosis", **diagnosis)
    return {
        "diagnosis": diagnosis,
        "steps": 1,
        "diagnostics_attempts": state.get("diagnostics_attempts", 0) + 1,
        **common.llm_update(result),
    }
