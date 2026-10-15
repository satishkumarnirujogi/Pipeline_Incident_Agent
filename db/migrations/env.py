"""Alembic environment for the `agent` database: schemas public and eval."""

from __future__ import annotations

import os

from alembic import context
from sqlalchemy import create_engine, pool

import eval.models  # noqa: F401 - registers the eval.* tables
from agent.db import _sqlalchemy_url
from agent.models import Base
from agent.settings import get_settings

target_metadata = Base.metadata


def _url() -> str:
    # Tests point migrations at a throwaway database.
    url = context.config.attributes.get("url") or os.environ.get("AGENT_DB_URL")
    return url or _sqlalchemy_url(get_settings().agent_dsn)


MANAGED_SCHEMAS = {None, "public", "eval"}


def include_name(name, type_, parent_names):  # noqa: ANN001
    return name in MANAGED_SCHEMAS if type_ == "schema" else True


def include_object(obj, name, type_, reflected, compare_to):  # noqa: ANN001
    # LangGraph's checkpoint tables live in public too but are created by PostgresSaver.
    return not (type_ == "table" and reflected and compare_to is None)


def run_migrations_offline() -> None:
    context.configure(url=_url(), target_metadata=target_metadata, literal_binds=True)
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    engine = create_engine(_url(), poolclass=pool.NullPool)
    with engine.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            include_object=include_object,
            include_schemas=True,
            include_name=include_name,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
