"""Alembic environment for the `agent` database (public schema only)."""

from __future__ import annotations

import os

from alembic import context
from sqlalchemy import create_engine, pool

from agent.db import _sqlalchemy_url
from agent.models import Base
from agent.settings import get_settings

target_metadata = Base.metadata


def _url() -> str:
    # Tests point migrations at a throwaway database.
    url = context.config.attributes.get("url") or os.environ.get("AGENT_DB_URL")
    return url or _sqlalchemy_url(get_settings().agent_dsn)


def include_object(obj, name, type_, reflected, compare_to):  # noqa: ANN001
    # eval.* and LangGraph's checkpoint tables are managed elsewhere.
    return not (type_ == "table" and reflected and compare_to is None)


def run_migrations_offline() -> None:
    context.configure(url=_url(), target_metadata=target_metadata, literal_binds=True)
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    engine = create_engine(_url(), poolclass=pool.NullPool)
    with engine.connect() as connection:
        context.configure(
            connection=connection, target_metadata=target_metadata, include_object=include_object
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
