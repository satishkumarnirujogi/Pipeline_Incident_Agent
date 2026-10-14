"""Smoke test for the OpenRouter setup: one plain call and one tool call per model.

Run with: uv run python -m scripts.check_llm
"""

import sys

from langchain_core.tools import tool

from agent.llm import _chat_model
from agent.settings import get_settings


@tool
def get_task_log(dag_id: str, task_id: str) -> str:
    """Fetch the log tail of a failed Airflow task."""
    return "KeyError: 'tpep_pickup_datetime'"


def main() -> int:
    s = get_settings()
    if not s.openrouter_api_key:
        print("OPENROUTER_API_KEY is empty in .env")
        return 1
    failed = False
    for model in (s.primary_model, s.fallback_model):
        llm = _chat_model(model, temperature=0)
        try:
            reply = llm.invoke("Reply with the single word: ready")
            usage = reply.usage_metadata or {}
            print(f"[ok]   {model}: {reply.content!r} tokens={usage.get('total_tokens')}")
            msg = llm.bind_tools([get_task_log]).invoke(
                "Task validate_contract in DAG ingest_taxi_trips failed. Fetch its log."
            )
            calls = [c["name"] for c in msg.tool_calls]
            status = "ok" if calls == ["get_task_log"] else "FAIL"
            failed |= status == "FAIL"
            print(f"[{status}] {model}: tool calls {calls}")
        except Exception as exc:  # noqa: BLE001
            failed = True
            print(f"[FAIL] {model}: {type(exc).__name__}: {exc}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
