"""Model factory. Agents never build LLM clients themselves; they call get_llm().

Both models are reached through OpenRouter's OpenAI-compatible API, so one
client class covers every provider. The fallback is a different vendor than
the primary, so a provider outage behind OpenRouter still has a way out.
Cost accounting and Langfuse callbacks are added in Phase 8.
"""

from langchain_core.language_models import BaseChatModel
from langchain_core.runnables import Runnable
from langchain_openai import ChatOpenAI
from pydantic import SecretStr

from agent.settings import get_settings


def _chat_model(model: str, temperature: float) -> BaseChatModel:
    s = get_settings()
    return ChatOpenAI(
        model=model,
        api_key=SecretStr(s.openrouter_api_key),
        base_url=s.openrouter_base_url,
        temperature=temperature,
        timeout=30,
        max_retries=2,
        default_headers={"X-Title": "pipeline-incident-agent"},
    )


def get_llm(role: str = "default", temperature: float = 0.0) -> Runnable:
    """Primary model with the fallback model attached."""
    s = get_settings()
    primary = _chat_model(s.primary_model, temperature)
    fallback = _chat_model(s.fallback_model, temperature)
    return primary.with_fallbacks([fallback])
