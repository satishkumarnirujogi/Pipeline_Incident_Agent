"""Plumbing shared by the agent nodes: prompts, tool schemas, running tool calls.

Agents never import database or HTTP clients: every side effect goes through
run_tool -> agent.tools.registry.call_tool (typed, logged, risk-tiered).
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable, Mapping
from functools import lru_cache
from pathlib import Path
from typing import Any

from langchain_core.messages import AIMessage, ToolMessage

import agent.tools  # noqa: F401 - registers the tools
from agent.tools.registry import REGISTRY, ToolContext, ToolResult, args_hash, call_tool, dry_run
from agent.tools.store import PostgresToolStore, ToolStore

log = logging.getLogger("agent")
PROMPTS = Path(__file__).parents[1] / "prompts"
MAX_TOOL_OUTPUT_CHARS = 6000

# Tests replace these to run the graph without databases or Airflow.
store_factory: Callable[[], ToolStore] = PostgresToolStore
run_tool: Callable[[str, dict, ToolContext], ToolResult] = call_tool
run_dry: Callable[[str, dict, ToolContext], ToolResult] = dry_run


@lru_cache
def prompt(name: str) -> str:
    return (PROMPTS / f"{name}.md").read_text(encoding="utf-8")


def ctx(
    state: Mapping[str, Any], agent: str, approval_id: str | None = None, step: int = 0
) -> ToolContext:
    return ToolContext(
        incident_id=state["incident_id"],
        month=(state.get("incident") or {}).get("month"),
        agent=agent,
        approval_id=approval_id,
        step=step,
        store=store_factory(),
    )


def tool_schemas(names: list[str]) -> list[dict]:
    """OpenAI-style function definitions from the registry's Pydantic input models."""
    out = []
    for name in names:
        spec = REGISTRY[name]
        out.append(
            {
                "type": "function",
                "function": {
                    "name": name,
                    "description": spec.description,
                    "parameters": spec.input_model.model_json_schema(),
                },
            }
        )
    return out


def wrap(name: str, result: ToolResult) -> str:
    """Tool output for the model: clearly delimited, truncated, data only."""
    body = json.dumps(result.model_dump(exclude_none=True, exclude={"replayed"}), default=str)
    if len(body) > MAX_TOOL_OUTPUT_CHARS:
        body = body[:MAX_TOOL_OUTPUT_CHARS] + ' ..."(truncated)"'
    return f'<tool_output tool="{name}">\n{body}\n</tool_output>'


def unwrap(content: str) -> dict:
    """The ToolResult dict inside wrap()'s output ({} if it was truncated or is not JSON)."""
    body = content.split("\n", 1)[-1].rsplit("\n", 1)[0]
    try:
        result = json.loads(body)
    except ValueError:
        return {}
    return result if isinstance(result, dict) else {}


def execute_tool_calls(
    state: Mapping[str, Any], agent: str, message: AIMessage, remaining: int, allowed: list[str]
) -> tuple[list[ToolMessage], list[dict]]:
    """Run the model's tool calls (up to `remaining`) through the envelope. Every call gets a
    ToolMessage back, including the ones over budget, so the conversation stays valid."""
    messages, log_rows = [], []
    for i, call in enumerate(message.tool_calls):
        name, args = call["name"], call.get("args") or {}
        if i >= remaining:
            result = ToolResult(
                ok=False,
                error_type="permission",
                message="tool-call budget for this step is used up",
            )
        elif name in REGISTRY and name not in allowed:
            # A real tool this step was not given (e.g. rerun_dag during remediation):
            # actions are proposed and pass the approval gate, never called directly.
            result = ToolResult(
                ok=False,
                error_type="permission",
                message=(f"{name} is not available here; propose it as your action instead"),
            )
        else:
            # Unknown tools (e.g. drop_table) go through the envelope, which refuses them and
            # logs a safety-violation attempt.
            result = run_tool(name, args, ctx(state, agent))
        messages.append(ToolMessage(content=wrap(name, result), tool_call_id=call["id"], name=name))
        log_rows.append(
            {
                "agent": agent,
                "tool": name,
                "args_hash": args_hash(args),
                "ok": result.ok,
                "error_type": result.error_type,
            }
        )
        log_event(
            state, "tool_call", agent=agent, tool=name, ok=result.ok, error_type=result.error_type
        )
    return messages, log_rows


def log_event(state: Mapping[str, Any], event: str, **fields: Any) -> None:
    """Structured log line; every line carries the incident id."""
    log.info(
        json.dumps({"event": event, "incident_id": state.get("incident_id"), **fields}, default=str)
    )


def incident_brief(state: Mapping[str, Any]) -> dict:
    inc = state["incident"]
    return {
        k: inc.get(k) for k in ("dag_id", "run_id", "task_id", "try_number", "month", "exception")
    }


def llm_update(result: Any) -> dict:
    """State update for cost accounting from an LLMResult."""
    return {"cost_eur": result.cost_eur, "llm_calls": [c.as_dict() for c in result.calls]}
