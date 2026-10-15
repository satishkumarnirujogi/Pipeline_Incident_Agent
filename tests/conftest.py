"""Test-wide settings: retries do not sleep, chaos is off unless a test turns it on."""

import pytest
from tenacity import wait_none


@pytest.fixture(autouse=True)
def _fast_retries(monkeypatch):
    from agent.tools import registry

    monkeypatch.setattr(registry, "RETRY_WAIT", wait_none())


@pytest.fixture(autouse=True)
def _chaos_off(monkeypatch):
    monkeypatch.setenv("CHAOS_MODE", "off")
    from agent.settings import get_settings

    get_settings.cache_clear()
    yield
    get_settings.cache_clear()
