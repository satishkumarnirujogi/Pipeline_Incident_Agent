"""Reporter agent: postmortem, incident memory, final status. Runs for every outcome."""

from __future__ import annotations

import json

from langchain_core.messages import HumanMessage, SystemMessage

from agent.agents import common
from agent.llm import get_llm
from agent.state import IncidentState, Postmortem


def final_status(state: IncidentState) -> str:
    if state.get("verified"):
        return "resolved"
    if state.get("escalation"):
        return "escalated"
    return "failed"


def _record(state: IncidentState, status: str) -> dict:
    return {
        "status": status,
        "incident": common.incident_brief(state),
        "diagnosis": state.get("diagnosis"),
        "routing": state.get("routing", []),
        "actions": state.get("actions_done", []),
        "rejections": state.get("rejections", []),
        "escalation": state.get("escalation"),
        "cost_eur": round(state.get("cost_eur", 0.0), 5),
        "steps": state.get("steps", 0),
    }


def reporter_agent(state: IncidentState) -> dict:
    status = final_status(state)
    messages = [
        SystemMessage(common.prompt("reporter")),
        HumanMessage(
            "Incident record:\n\n" + json.dumps(_record(state, status), indent=2, default=str)
        ),
    ]
    result = get_llm().structured("reporter", messages, Postmortem)
    pm: Postmortem = result.parsed  # type: ignore[assignment]

    reporter = common.ctx(state, "reporter")
    common.run_tool("write_postmortem", {"markdown": pm.markdown}, reporter)
    diagnosis = state.get("diagnosis")
    if diagnosis:
        common.run_tool(
            "save_incident_memory",
            {
                "summary": pm.summary,
                "root_cause": diagnosis["root_cause"],
                "fix": pm.fix,
            },
            reporter,
        )
    common.log_event(state, "final", status=status, cost_eur=state.get("cost_eur", 0.0))
    return {
        "postmortem": pm.model_dump(),
        "status": status,
        "steps": 1,
        **common.llm_update(result),
    }
