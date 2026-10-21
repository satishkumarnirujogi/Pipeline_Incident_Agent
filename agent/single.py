"""Ablation A (build guide 9.4): one tool-calling agent with all tools, no supervisor.

    intake -> single_agent <-> single_tools -> approval_gate -> execute_action -> single_agent
                   -> single_finalize -> reporter | escalate -> reporter -> END

The safety layer is the same as the full system's: an action call becomes a proposal that
passes the same approval gate and execute_action (tier 2 waits for a human, every rerun is
verified). What is missing is everything the Supervisor and the specialists add: routing
rules in code, the escalation and outage policies, fresh context per step, and separate
tool sets per role. The answer to an action call reaches the model on its next turn.
"""

from __future__ import annotations

import json
from typing import Any, Literal

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolCall, ToolMessage
from langgraph.graph import END, START, StateGraph
from langgraph.types import Command, RetryPolicy

from agent import budget
from agent.agents import common, diagnostics
from agent.agents.reporter import reporter_agent
from agent.approval import approval_gate, escalate, execute_action
from agent.llm import get_llm
from agent.state import Diagnosis, IncidentState, ProposedAction
from agent.tools.registry import REGISTRY, TIER_ESCALATION, TIER_READONLY

AGENT = "single"
# The registry grants tools by role: reads run as Diagnostics, actions (execute_action) as
# Remediation, exactly the permissions the two specialists have together.
READ_ROLE = "diagnostics"
MAX_TOOL_CALLS = 16  # the specialists' budgets together (8 diagnostics + up to 8 actions)
ACTIONS = ["rerun_dag", "upsert_column_mapping", "quarantine_rows", "restore_quarantined"]
ESCALATE = "escalate_to_human"


def tools() -> list[str]:
    return [*diagnostics.tools(), *ACTIONS, ESCALATE]


def system_prompt() -> str:
    remediation = "\n".join(
        line
        for line in common.prompt("remediation").splitlines()
        if not line.startswith("| UPSTREAM_API_OUTAGE")  # the full system's code policy
    )
    return common.prompt("single_agent").format(
        diagnostics=diagnostics._prompt(), remediation=remediation
    )


def _pending_calls(messages: list) -> list[ToolCall]:
    """Tool calls of the last model turn that have no answer yet (an action in flight)."""
    last_ai = next((m for m in reversed(messages) if isinstance(m, AIMessage)), None)
    if last_ai is None:
        return []
    answered = {m.tool_call_id for m in messages if isinstance(m, ToolMessage)}
    return [c for c in last_ai.tool_calls if c["id"] not in answered]


def _action_outcome(state: IncidentState) -> dict[str, Any]:
    """What happened to the last action: rejected by a human, or executed (and verified)."""
    if state.get("last_event") == "rejected":
        return {"ok": False, "message": f"rejected by a human: {state['rejections'][-1]}"}
    done = (state.get("actions_done") or [{}])[-1]
    return {
        "ok": done.get("ok"),
        "message": done.get("message"),
        "approved": done.get("approved"),
        "data": done.get("data"),
        "verification": done.get("verification"),
        "verified": state.get("verified", False),
    }


def single_agent(state: IncidentState) -> dict:
    messages = list(state.get("single_messages", []))
    seed: list = []
    if not messages:
        brief = common.incident_brief(state)
        seed = [
            SystemMessage(system_prompt()),
            HumanMessage(
                "A pipeline task failed. Investigate, then fix it or escalate.\n\n"
                + json.dumps(brief, indent=2)
            ),
        ]
    answers = [
        ToolMessage(
            content=f'<tool_output tool="{c["name"]}">\n'
            f"{json.dumps(_action_outcome(state), default=str)}\n</tool_output>",
            tool_call_id=c["id"],
            name=c["name"],
        )
        for c in _pending_calls(messages)
    ]
    update: dict = {"steps": 1, "last_event": None}
    stop = budget.over_budget(state) or budget.loop_detected(state)
    if stop:
        common.log_event(state, "single_stop", reason=stop)
        return {
            **update,
            "single_messages": [*seed, *answers],
            "escalation": {"reason": stop, "by": "budget"},
        }
    convo = [*messages, *seed, *answers]
    result = get_llm().with_tools(AGENT, convo, common.tool_schemas(tools()))
    return {
        **update,
        "single_messages": [*seed, *answers, result.message],
        **common.llm_update(result),
    }


def after_single_agent(state: IncidentState) -> Literal["single_tools", "single_finalize"]:
    last = state["single_messages"][-1]
    if (
        isinstance(last, AIMessage)
        and last.tool_calls
        and not (state.get("escalation") or {}).get("by") == "budget"
        and state.get("single_tool_calls", 0) < MAX_TOOL_CALLS
    ):
        return "single_tools"
    return "single_finalize"


def single_tools(
    state: IncidentState,
) -> Command[Literal["single_agent", "approval_gate", "single_finalize"]]:
    """Read-only calls run now; the first action becomes a proposal for the approval gate;
    escalate_to_human records the reason and ends the run."""
    last: AIMessage = state["single_messages"][-1]
    allowed = tools()
    reads: list[ToolCall] = []
    answers: list[ToolMessage] = []
    rows: list[dict] = []
    proposal, escalation = None, None
    for call in last.tool_calls:
        name, args = call["name"], call.get("args") or {}
        spec = REGISTRY.get(name)
        if name == ESCALATE and escalation is None and proposal is None:
            reason = str(args.get("reason") or args.get("summary") or "agent escalated")
            escalation = {"reason": reason[:1000], "by": AGENT}
            answers.append(_answer(call, {"ok": True, "message": "escalation recorded; stop"}))
        elif (
            spec is not None
            and name in allowed
            and spec.tier not in (TIER_READONLY, TIER_ESCALATION)
            and proposal is None
            and escalation is None
        ):
            proposal = ProposedAction(
                tool=name,
                args=args,
                rationale=str(last.content or "single agent action")[:1000],
            ).model_dump(mode="json")
            # answered on the next single_agent turn, with the gate's and the action's outcome
        elif spec is not None and spec.tier not in (TIER_READONLY, TIER_ESCALATION):
            answers.append(_answer(call, {"ok": False, "message": "one action at a time"}))
        else:
            reads.append(call)  # read-only, or not offered / unknown: the executor refuses
    budget_left = MAX_TOOL_CALLS - state.get("single_tool_calls", 0)
    if reads:
        msg = AIMessage("", tool_calls=reads)
        read_answers, rows = common.execute_tool_calls(state, READ_ROLE, msg, budget_left, allowed)
        answers += read_answers
    update: dict = {
        "single_messages": answers,
        "tool_log": rows,
        "steps": 1,
        "single_tool_calls": state.get("single_tool_calls", 0) + len(last.tool_calls),
    }
    if escalation:
        return Command(goto="single_finalize", update={**update, "escalation": escalation})
    if proposal:
        common.log_event(state, "proposal", tool=proposal["tool"], args=proposal["args"])
        update |= {
            "proposed_action": proposal,
            "proposals": state.get("proposals", 0) + 1,
        }
        return Command(goto="approval_gate", update=update)
    return Command(goto="single_agent", update=update)


def _answer(call: ToolCall, body: dict) -> ToolMessage:
    return ToolMessage(
        content=f'<tool_output tool="{call["name"]}">\n{json.dumps(body)}\n</tool_output>',
        tool_call_id=call["id"],
        name=call["name"],
    )


def single_finalize(state: IncidentState) -> Command[Literal["reporter", "escalate"]]:
    """Ask for the root cause (scored like the full system's Diagnosis), then end."""
    messages = list(state["single_messages"])
    answers = [
        _answer(c, {"ok": False, "message": "not run: the incident is ending"})
        for c in _pending_calls(messages)
    ]
    messages += [
        *answers,
        HumanMessage("The incident is ending. Give your Diagnosis of the root cause now."),
    ]
    result = get_llm().structured("single_finalize", messages, Diagnosis)
    diagnosis = result.parsed.model_dump(mode="json")  # type: ignore[union-attr]
    common.log_event(state, "diagnosis", **diagnosis)
    update: dict = {
        "diagnosis": diagnosis,
        "single_messages": answers,
        "steps": 1,
        **common.llm_update(result),
    }
    if state.get("verified"):
        return Command(goto="reporter", update=update)
    if not state.get("escalation"):
        update["escalation"] = {"reason": "agent stopped without a verified fix", "by": AGENT}
    return Command(goto="escalate", update=update)


def build_single_agent_graph(intake: Any, retry: RetryPolicy | None) -> StateGraph:
    b = StateGraph(IncidentState)
    b.add_node("intake", intake)
    b.add_node("single_agent", single_agent, retry_policy=retry)
    b.add_node(
        "single_tools",
        single_tools,
        destinations=("single_agent", "approval_gate", "single_finalize"),
    )
    b.add_node(
        "approval_gate", approval_gate, destinations=("execute_action", "single_agent", "escalate")
    )
    b.add_node("execute_action", execute_action)
    b.add_node(
        "single_finalize",
        single_finalize,
        retry_policy=retry,
        destinations=("reporter", "escalate"),
    )
    b.add_node("escalate", escalate)
    b.add_node("reporter", reporter_agent, retry_policy=retry)
    b.add_edge(START, "intake")
    b.add_edge("intake", "single_agent")
    b.add_conditional_edges("single_agent", after_single_agent)
    b.add_edge("execute_action", "single_agent")
    b.add_edge("escalate", "reporter")
    b.add_edge("reporter", END)
    return b
