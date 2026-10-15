"""Reporter agent: postmortem, incident memory, final status. Runs for every outcome."""

from __future__ import annotations

import json

from langchain_core.messages import HumanMessage, SystemMessage

from agent.agents import common
from agent.llm import LLMUnavailable, get_llm
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
    llm_cost: dict = {}
    try:
        result = get_llm().structured("reporter", messages, Postmortem)
        pm: Postmortem = result.parsed  # type: ignore[assignment]
        llm_cost = common.llm_update(result)
    except LLMUnavailable:
        # The outcome must not depend on prose: write the facts without a model.
        pm = template_postmortem(state, status)

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
    return {"postmortem": pm.model_dump(), "status": status, "steps": 1, **llm_cost}


def template_postmortem(state: IncidentState, status: str) -> Postmortem:
    brief = common.incident_brief(state)
    diagnosis = state.get("diagnosis") or {}
    actions = state.get("actions_done", [])
    escalation = state.get("escalation") or {}
    where = f"{brief['dag_id']}.{brief['task_id']} ({brief['month']})"
    fix = (
        "; ".join(f"{a['tool']} {json.dumps(a['args'])}" for a in actions if a.get("ok"))
        or f"escalated: {escalation.get('reason', 'no fix applied')}"
    )
    summary = (
        f"{where} failed: {brief.get('exception') or 'see log'}. Root cause "
        f"{diagnosis.get('root_cause', 'unknown')}. Outcome: {status}; {fix}."
    )[:2000]
    lines = [
        f"# Postmortem: {where}",
        f"Status: **{status}** (written without an LLM: both models were unavailable)",
        "",
        "## Summary",
        summary,
        "",
        "## Root cause",
        f"{diagnosis.get('root_cause')} (confidence {diagnosis.get('confidence')})",
    ]
    lines += [f"- {e}" for e in diagnosis.get("evidence", [])]
    lines += (
        ["", "## Actions"]
        + [
            f"- {a['tool']} {json.dumps(a['args'])}: {'ok' if a.get('ok') else 'failed'}, "
            f"{a.get('approved', '')}"
            for a in actions
        ]
        + ["", "## Cost", f"EUR {state.get('cost_eur', 0):.4f}, {state.get('steps', 0)} steps"]
    )
    return Postmortem(summary=summary, markdown="\n".join(lines), fix=fix[:1000])
