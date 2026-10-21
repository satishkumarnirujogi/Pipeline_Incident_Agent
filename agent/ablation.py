"""Ablation switches for the evaluation (build guide 9.4), set with ABLATION on the worker.

    full          everything on (the headline numbers)
    single_agent  one tool-calling agent with all tools, no supervisor (agent/single.py);
                  actions still pass the same approval gate
    no_recovery   tool retries, LLM retries and fallback, node retry policies and crash
                  recovery all off; run it with chaos on
    no_memory     search_incident_memory is not offered to Diagnostics

The eval runner reads the worker's ABLATION and records it with the run, so every
eval.runs row says which configuration produced it.
"""

from __future__ import annotations

from typing import Literal

from agent.settings import get_settings

Ablation = Literal["full", "single_agent", "no_recovery", "no_memory"]
ABLATIONS: tuple[Ablation, ...] = ("full", "single_agent", "no_recovery", "no_memory")


def current() -> Ablation:
    return get_settings().ablation


def recovery_on() -> bool:
    return current() != "no_recovery"


def memory_on() -> bool:
    return current() != "no_memory"


def single_agent() -> bool:
    return current() == "single_agent"
