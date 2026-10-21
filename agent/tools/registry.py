"""Tool registry and the envelope every tool call goes through.

Every side effect the system can cause is a typed, logged, risk-tiered tool.
call_tool() is the only way to run one. In order, it:

1. rejects unknown tools (logged as a safety-violation attempt) and tools the
   calling agent does not own;
2. validates the arguments with the tool's Pydantic input model;
3. enforces the tier policy: tier 2 needs a granted approval for exactly these
   arguments, tier 1 runs at most MAX_TIER1_PER_INCIDENT times;
4. for tiers 1-2, returns the stored result if this action already succeeded
   (idempotency key = sha256(incident, tool, canonical args, proposal step));
5. applies the chaos hook (eval only) and the timeout;
6. converts any exception into a ToolResult instead of raising;
7. writes a tool_calls row.

Tier 3 (destructive or out of scope) does not exist as a tool at all.
"""

from __future__ import annotations

import concurrent.futures
import hashlib
import json
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from pydantic import BaseModel
from pydantic_core import to_jsonable_python
from tenacity import (
    RetryCallState,
    Retrying,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential_jitter,
)

from agent import ablation, chaos, tracing
from agent.tools.errors import (
    ApprovalRequired,
    ErrorType,
    PermissionDenied,
    ToolTimeout,
    classify,
)
from agent.tools.store import PostgresToolStore, ToolStore

TIER_READONLY = 0
TIER_REVERSIBLE = 1
TIER_CHANGES_DATA = 2
TIER_ESCALATION = -1  # always allowed; ends the run as escalated
MAX_TIER1_PER_INCIDENT = 2
TOOL_ATTEMPTS = 3
RETRY_WAIT = wait_exponential_jitter(initial=1, max=20, jitter=1)  # tests set wait_none()

AGENTS = frozenset({"diagnostics", "remediation", "reporter"})


class ToolResult(BaseModel):
    ok: bool
    data: dict | list | None = None
    error_type: ErrorType | None = None
    retryable: bool = False
    message: str = ""  # short, for the LLM; never a full stack trace
    replayed: bool = False  # tier 1/2: result of an earlier identical action


@dataclass(frozen=True)
class ToolSpec:
    name: str
    tier: int
    fn: Callable[[Any, ToolContext], dict | list]
    input_model: type[BaseModel]
    agents: frozenset[str]
    timeout_s: float
    idempotent: bool
    description: str
    dry_run: Callable[[Any, ToolContext], dict] | None = None


@dataclass
class ToolContext:
    incident_id: str
    agent: str
    approval_id: str | None = None
    # Which remediation proposal this call executes. Part of the idempotency key, so a
    # crash-and-redo of the same step is a no-op but a deliberate second rerun is not.
    step: int = 0
    month: str | None = None  # the incident's month, for previews
    store: ToolStore = field(default_factory=PostgresToolStore)


REGISTRY: dict[str, ToolSpec] = {}
_executor = concurrent.futures.ThreadPoolExecutor(max_workers=4, thread_name_prefix="tool")


def tool_spec(
    *,
    tier: int,
    input_model: type[BaseModel],
    agents: set[str],
    timeout_s: float = 30,
    idempotent: bool = False,
    dry_run: Callable[[Any, ToolContext], dict] | None = None,
) -> Callable[[Callable], Callable]:
    """Register a tool. fn(args: input_model, ctx) returns JSON-able data."""
    unknown = set(agents) - AGENTS
    if unknown:
        raise ValueError(f"unknown agents {unknown}")
    if tier == TIER_CHANGES_DATA and dry_run is None:
        raise ValueError("tier-2 tools must support dry_run")

    def register(fn: Callable) -> Callable:
        name = fn.__name__
        if name in REGISTRY:
            raise ValueError(f"tool {name} registered twice")
        REGISTRY[name] = ToolSpec(
            name=name,
            tier=tier,
            fn=fn,
            input_model=input_model,
            agents=frozenset(agents),
            timeout_s=timeout_s,
            idempotent=idempotent,
            description=(fn.__doc__ or "").strip(),
            dry_run=dry_run,
        )
        return fn

    return register


def tools_for(agent: str) -> list[ToolSpec]:
    return [s for s in REGISTRY.values() if agent in s.agents]


def canonical_json(data: Any) -> str:
    return json.dumps(to_jsonable_python(data), sort_keys=True, separators=(",", ":"))


def args_hash(args: BaseModel | dict) -> str:
    payload = args.model_dump(mode="json") if isinstance(args, BaseModel) else args
    return hashlib.sha256(canonical_json(payload).encode()).hexdigest()


def idempotency_key(incident_id: str, tool: str, args: BaseModel, step: int) -> str:
    raw = f"{incident_id}|{tool}|{canonical_json(args.model_dump(mode='json'))}|{step}"
    return hashlib.sha256(raw.encode()).hexdigest()


def validate_args(name: str, args: dict[str, Any]) -> BaseModel:
    """Used by the approval gate and execute_action to re-check (possibly edited) args."""
    return REGISTRY[name].input_model.model_validate(args)


def _check_approval(spec: ToolSpec, parsed: BaseModel, ctx: ToolContext) -> None:
    if not ctx.approval_id:
        raise ApprovalRequired(f"{spec.name} changes data and needs an approved approval_id")
    approval = ctx.store.get_approval(ctx.approval_id)
    if approval is None:
        raise ApprovalRequired(f"approval {ctx.approval_id} does not exist")
    if approval["incident_id"] != ctx.incident_id or approval["tool"] != spec.name:
        raise ApprovalRequired(f"approval {ctx.approval_id} is for a different incident or tool")
    if approval["decision"] not in ("approve", "edit"):
        raise ApprovalRequired(
            f"approval {ctx.approval_id} is not granted (decision: {approval['decision']})"
        )
    if approval["args_hash"] != args_hash(parsed):
        raise ApprovalRequired("arguments differ from the approved arguments")


def _run_with_timeout(fn: Callable[[], Any], timeout_s: float, name: str) -> Any:
    future = _executor.submit(fn)
    try:
        return future.result(timeout=timeout_s)
    except concurrent.futures.TimeoutError:
        future.cancel()
        raise ToolTimeout(f"{name} did not finish within {timeout_s:g}s") from None


def call_tool(name: str, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
    started = time.monotonic()
    spec = REGISTRY.get(name)
    tier = spec.tier if spec else None
    hashed = args_hash(args) if isinstance(args, dict) else ""
    violation = False
    chaos_hit = False
    replayed = False
    attempts = 1
    key: str | None = None

    try:
        if spec is None:
            violation = True  # e.g. drop_table: tier 3 does not exist
            raise PermissionDenied(f"unknown tool {name!r}; this request was logged")
        if ctx.agent not in spec.agents:
            raise PermissionDenied(f"agent {ctx.agent} may not use {name}")
        parsed = spec.input_model.model_validate(args)
        hashed = args_hash(parsed)

        if spec.tier == TIER_CHANGES_DATA:
            _check_approval(spec, parsed, ctx)

        prior = None
        if spec.tier in (TIER_REVERSIBLE, TIER_CHANGES_DATA):
            key = idempotency_key(ctx.incident_id, name, parsed, ctx.step)
            prior = ctx.store.find_action(key)
        if prior and prior["status"] == "succeeded":
            replayed, key = True, None
            result = ToolResult(
                ok=True,
                data=prior["result"],
                replayed=True,
                message="already done; returning the stored result",
            )
        else:

            def failed_attempt(n: int, res: ToolResult, chaos_flag: bool) -> None:
                _record(
                    ctx,
                    name,
                    tier,
                    hashed,
                    res,
                    int((time.monotonic() - started) * 1000),
                    False,
                    chaos_flag,
                    False,
                    attempt=n,
                )

            result, attempts, chaos_hit = _execute(spec, parsed, ctx, key, failed_attempt)
    except Exception as exc:  # noqa: BLE001 - envelope boundary
        error_type, retryable, message = classify(exc)
        result = ToolResult(ok=False, error_type=error_type, retryable=retryable, message=message)

    latency_ms = int((time.monotonic() - started) * 1000)
    tracing.tool_span(name, args if isinstance(args, dict) else {}, result, latency_ms)
    _record(
        ctx,
        name,
        tier,
        hashed,
        result,
        latency_ms,
        violation,
        chaos_hit,
        replayed,
        attempt=attempts,
    )
    return result


def _execute(
    spec: ToolSpec,
    parsed: BaseModel,
    ctx: ToolContext,
    key: str | None,
    on_failed_attempt: Callable[[int, ToolResult, bool], None],
) -> tuple[ToolResult, int, bool]:
    """Rate limit, action record, then the tool with retries, chaos hook and timeout.

    Retry layer 2: a retryable error (timeout, 429, 5xx, lost DB connection) is retried
    with exponential backoff and jitter, at most TOOL_ATTEMPTS times, but only for tools
    that are safe to repeat (tier 0 reads, idempotent actions). Each failed attempt is
    reported through on_failed_attempt so it gets its own tool_calls row.
    Returns (result, attempts, chaos_hit_on_last_attempt).
    """
    started = False
    attempts, chaos_hit = 0, False
    try:
        if key:
            if spec.tier == TIER_REVERSIBLE and (
                ctx.store.count_succeeded(ctx.incident_id, TIER_REVERSIBLE)
                >= MAX_TIER1_PER_INCIDENT
            ):
                raise PermissionDenied(
                    f"limit of {MAX_TIER1_PER_INCIDENT} automatic tier-1 actions per incident "
                    "reached; escalate instead"
                )
            ctx.store.start_action(
                {
                    "idempotency_key": key,
                    "incident_id": ctx.incident_id,
                    "tool": spec.name,
                    "tier": spec.tier,
                    "args": parsed.model_dump(mode="json"),
                    "approval_id": ctx.approval_id,
                }
            )
            started = True

        retry_safe = (spec.tier == TIER_READONLY or spec.idempotent) and ablation.recovery_on()

        def before_sleep(state: RetryCallState) -> None:
            exc = state.outcome.exception() if state.outcome else None
            error_type, retryable, message = classify(exc) if exc else ("unknown", False, "")
            on_failed_attempt(
                state.attempt_number,
                ToolResult(ok=False, error_type=error_type, retryable=retryable, message=message),
                message.endswith("(chaos)"),
            )

        retrying = Retrying(
            stop=stop_after_attempt(TOOL_ATTEMPTS if retry_safe else 1),
            wait=RETRY_WAIT,
            retry=retry_if_exception(lambda e: classify(e)[1]),
            before_sleep=before_sleep,
            reraise=True,
        )
        for attempt in retrying:
            with attempt:
                attempts = attempt.retry_state.attempt_number
                chaos.before_tool(spec.name, ctx.incident_id)
                data = to_jsonable_python(
                    _run_with_timeout(lambda: spec.fn(parsed, ctx), spec.timeout_s, spec.name)
                )
        if started and key:
            ctx.store.finish_action(key, "succeeded", data)
        return ToolResult(ok=True, data=data), attempts, False
    except Exception as exc:  # noqa: BLE001 - envelope boundary
        error_type, retryable, message = classify(exc)
        chaos_hit = message.endswith("(chaos)")
        if started and key:
            ctx.store.finish_action(key, "failed", {"error_type": error_type, "message": message})
        result = ToolResult(ok=False, error_type=error_type, retryable=retryable, message=message)
        return result, max(attempts, 1), chaos_hit


def _record(
    ctx: ToolContext,
    name: str,
    tier: int | None,
    hashed: str,
    result: ToolResult,
    latency_ms: int,
    violation: bool,
    chaos_hit: bool,
    replayed: bool,
    attempt: int = 1,
) -> None:
    try:
        ctx.store.record_call(
            {
                "incident_id": ctx.incident_id,
                "agent": ctx.agent,
                "tool": name,
                "tier": tier,
                "args_hash": hashed,
                "ok": result.ok,
                "error_type": result.error_type,
                "retryable": result.retryable,
                "attempt": attempt,
                "latency_ms": latency_ms,
                "replayed": replayed,
                "safety_violation": violation,
                "chaos_injected": chaos_hit,
            }
        )
    except Exception:  # noqa: BLE001 - logging must not change the tool's outcome
        import logging

        logging.getLogger(__name__).exception("failed to record tool call %s", name)


def dry_run(name: str, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
    """Preview a tier-2 action for the approval request. Read-only, safe to repeat."""
    spec = REGISTRY.get(name)
    try:
        if spec is None or spec.dry_run is None:
            raise PermissionDenied(f"{name} has no dry run")
        preview = spec.dry_run
        parsed = spec.input_model.model_validate(args)
        data = _run_with_timeout(lambda: preview(parsed, ctx), spec.timeout_s, f"{name} dry run")
        return ToolResult(ok=True, data=to_jsonable_python(data))
    except Exception as exc:  # noqa: BLE001
        error_type, retryable, message = classify(exc)
        return ToolResult(ok=False, error_type=error_type, retryable=retryable, message=message)
