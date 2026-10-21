"""Model factory, fallbacks and cost accounting. Agents never build LLM clients themselves.

Both models are reached through OpenRouter's OpenAI-compatible API, so one
client class covers every provider. The fallback is a different vendor than
the primary, so a provider outage behind OpenRouter still has a way out.
Every call returns its token usage and cost; Langfuse callbacks and the
llm_calls table arrive in Phase 8.
"""

from __future__ import annotations

import concurrent.futures
import logging
import time
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage
from langchain_core.runnables import RunnableConfig
from langchain_openai import ChatOpenAI
from langgraph.config import get_config
from pydantic import BaseModel, SecretStr, ValidationError

from agent import ablation, chaos, telemetry
from agent.settings import get_settings

log = logging.getLogger(__name__)
# Hard wall-clock limit per model call. The client's timeout is per read, and OpenRouter
# keeps slow requests alive, so without this one runaway generation can hang a run.
CALL_TIMEOUT_S = 90
MAX_OUTPUT_TOKENS = 4000
_executor = concurrent.futures.ThreadPoolExecutor(max_workers=2, thread_name_prefix="llm")


class LLMTimeout(Exception):  # noqa: N818
    """A model call exceeded CALL_TIMEOUT_S."""


class LLMUnavailable(Exception):  # noqa: N818
    """Primary and fallback both failed. A plain Exception on purpose: LangGraph's node
    retry policy retries it (it does not retry RuntimeError)."""


def _parent_config() -> RunnableConfig | None:
    """The calling graph node's config, so model calls (run in a worker thread for the
    timeout) stay nested under the node in traces. None outside a graph."""
    try:
        return get_config()
    except RuntimeError:
        return None


def _bounded(fn: Any, *args: Any) -> Any:
    future = _executor.submit(fn, *args)
    try:
        return future.result(timeout=CALL_TIMEOUT_S)
    except concurrent.futures.TimeoutError:
        future.cancel()
        raise LLMTimeout(f"no answer within {CALL_TIMEOUT_S}s") from None


PRICING = Path(__file__).parents[1] / "config" / "pricing.yaml"


@dataclass
class CallRecord:
    node: str
    model: str
    tokens_in: int
    tokens_out: int
    cost_eur: float
    ok: bool
    fallback_used: bool
    error_type: str | None = None
    chaos_injected: bool = False

    def as_dict(self) -> dict:
        return self.__dict__.copy()


@dataclass
class LLMResult:
    message: AIMessage | None = None
    parsed: BaseModel | None = None
    calls: list[CallRecord] = field(default_factory=list)

    @property
    def cost_eur(self) -> float:
        return sum(c.cost_eur for c in self.calls)


@lru_cache
def _pricing() -> dict:
    return yaml.safe_load(PRICING.read_text(encoding="utf-8"))


def cost_eur(model: str, tokens_in: int, tokens_out: int) -> float:
    p = _pricing()
    prices = p["models"].get(model)
    if prices is None:  # OpenRouter may append a version suffix to the model name
        prices = next((v for k, v in p["models"].items() if model.startswith(k)), None)
    if prices is None:
        log.warning("no price for model %s; counting it at the primary's price", model)
        prices = p["models"][get_settings().primary_model]
    usd = (tokens_in * prices["input_usd_per_m"] + tokens_out * prices["output_usd_per_m"]) / 1e6
    return round(usd * p["eur_per_usd"], 8)


def _chat_model(model: str, temperature: float = 0.0) -> BaseChatModel:
    s = get_settings()
    return ChatOpenAI(
        model=model,
        api_key=SecretStr(s.openrouter_api_key),
        base_url=s.openrouter_base_url,
        temperature=temperature,
        timeout=30,
        max_retries=2 if ablation.recovery_on() else 0,
        max_completion_tokens=MAX_OUTPUT_TOKENS,
        default_headers={"X-Title": "pipeline-incident-agent"},
    )


class LLM:
    """Primary model with a fallback. Tests replace the two chat models with fakes."""

    def __init__(
        self, primary: BaseChatModel | None = None, fallback: BaseChatModel | None = None
    ) -> None:
        s = get_settings()
        self.primary_name, self.fallback_name = s.primary_model, s.fallback_model
        self.primary = primary or _chat_model(s.primary_model)
        self.fallback = fallback or _chat_model(s.fallback_model)

    def _record(
        self,
        node: str,
        model: str,
        msg: AIMessage | None,
        fallback: bool,
        error: str | None = None,
        chaos_hit: bool = False,
        started: float | None = None,
    ) -> CallRecord:
        usage: dict[str, Any] = dict((msg.usage_metadata if msg is not None else None) or {})
        tin, tout = usage.get("input_tokens", 0), usage.get("output_tokens", 0)
        rec = CallRecord(
            node=node,
            model=model,
            tokens_in=tin,
            tokens_out=tout,
            cost_eur=cost_eur(model, tin, tout),
            ok=error is None,
            fallback_used=fallback,
            error_type=error,
            chaos_injected=chaos_hit,
        )
        latency_ms = int((time.monotonic() - started) * 1000) if started else 0
        telemetry.record_llm_call(rec, latency_ms)
        return rec

    def _failed(
        self, node: str, name: str, fallback: bool, exc: Exception, started: float
    ) -> CallRecord:
        log.warning("%s: %s failed: %s", node, name, exc)
        error = (
            "rate_limit"
            if isinstance(exc, chaos.ChaosRateLimit)
            else "timeout"
            if isinstance(exc, LLMTimeout)
            else type(exc).__name__
        )
        return self._record(node, name, None, fallback, error, "(chaos)" in str(exc), started)

    def _models(self) -> list[tuple[BaseChatModel, str, bool]]:
        """Primary, then the fallback; without recovery (ablation) the primary only."""
        models = [(self.primary, self.primary_name, False)]
        if ablation.recovery_on():
            models.append((self.fallback, self.fallback_name, True))
        return models

    def with_tools(self, node: str, messages: list[BaseMessage], tools: list[dict]) -> LLMResult:
        """One tool-calling turn. Falls back to the other provider on any error."""
        result = LLMResult()
        for model, name, fallback in self._models():
            started = time.monotonic()
            try:
                if not fallback:
                    chaos.before_primary_llm(node)
                msg = _bounded(model.bind_tools(tools).invoke, messages, _parent_config())
                result.calls.append(self._record(node, name, msg, fallback, started=started))
                result.message = msg
                return result
            except Exception as exc:  # noqa: BLE001 - provider error -> try the fallback
                result.calls.append(self._failed(node, name, fallback, exc, started))
        raise LLMUnavailable(f"{node}: primary and fallback models both failed")

    def structured(
        self, node: str, messages: list[BaseMessage], schema: type[BaseModel]
    ) -> LLMResult:
        """Structured output. Invalid JSON is re-asked once with the validation error, then
        the fallback model gets the same two chances."""
        result = LLMResult()
        for model, name, fallback in self._models():
            attempt_msgs = list(messages)
            for _repair in range(2 if ablation.recovery_on() else 1):
                started = time.monotonic()
                try:
                    if not fallback:
                        chaos.before_primary_llm(node)
                    runnable = model.with_structured_output(
                        schema, method="function_calling", include_raw=True
                    )
                    out: Any = _bounded(runnable.invoke, attempt_msgs, _parent_config())
                except Exception as exc:  # noqa: BLE001
                    result.calls.append(self._failed(node, name, fallback, exc, started))
                    break  # provider error: go to the fallback model
                raw: AIMessage = out["raw"]
                if out.get("parsed") is not None:
                    result.calls.append(self._record(node, name, raw, fallback, started=started))
                    result.parsed = out["parsed"]
                    result.message = raw
                    return result
                err = out.get("parsing_error")
                result.calls.append(
                    self._record(node, name, raw, fallback, "validation", started=started)
                )
                detail = err.errors() if isinstance(err, ValidationError) else str(err)
                # Re-ask without the broken tool call (it would need a tool result).
                attempt_msgs = [
                    *messages,
                    HumanMessage(
                        f"Your previous {schema.__name__} did not validate: {detail}. "
                        f"Return the {schema.__name__} again, corrected."
                    ),
                ]
        raise LLMUnavailable(f"{node}: no valid {schema.__name__} from either model")


_llm: LLM | None = None


def get_llm() -> LLM:
    global _llm
    if _llm is None:
        _llm = LLM()
    return _llm


def set_llm(llm: LLM | None) -> None:
    """Tests install an LLM built from fake chat models."""
    global _llm
    _llm = llm
