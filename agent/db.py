"""Database engines. One pool per process; agents never import this, only tools do."""

from __future__ import annotations

from functools import lru_cache

from sqlalchemy import Engine, create_engine

from agent.settings import get_settings


def _sqlalchemy_url(dsn: str) -> str:
    return dsn.replace("postgresql://", "postgresql+psycopg://", 1)


@lru_cache
def agent_engine() -> Engine:
    return create_engine(
        _sqlalchemy_url(get_settings().agent_dsn), pool_size=3, max_overflow=2, pool_pre_ping=True
    )


@lru_cache
def warehouse_engine(role: str) -> Engine:
    """role is agent_ro (tier-0 tools) or agent_rw (tier-1/2 tools)."""
    if role not in ("agent_ro", "agent_rw"):
        raise ValueError(f"agents may not connect as {role}")
    return create_engine(
        _sqlalchemy_url(get_settings().dsn(role, "warehouse")),
        pool_size=2,
        max_overflow=2,
        pool_pre_ping=True,
    )


def reset_engines() -> None:
    """For tests that change settings."""
    for factory in (agent_engine, warehouse_engine):
        factory.cache_clear()
