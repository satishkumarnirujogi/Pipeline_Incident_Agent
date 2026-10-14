"""Apply database migrations. Safe to rerun.

- warehouse: db/warehouse/NNN_*.sql as the pipeline role, tracked in ops.schema_migrations
- agent:     Alembic (db/migrations) as agent_app

Run with: make migrate   (part of make bootstrap)
"""

from __future__ import annotations

import sys
from pathlib import Path

import psycopg
from alembic import command
from alembic.config import Config

from agent.settings import get_settings

WAREHOUSE_DIR = Path(__file__).parents[1] / "db" / "warehouse"


def migrate_warehouse(dsn: str) -> list[str]:
    applied = []
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS ops.schema_migrations ("
            "name text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())"
        )
        done = {r[0] for r in conn.execute("SELECT name FROM ops.schema_migrations")}
        for path in sorted(WAREHOUSE_DIR.glob("*.sql")):
            if path.name in done:
                continue
            with conn.transaction():
                conn.execute(path.read_text(encoding="utf-8"))
                conn.execute("INSERT INTO ops.schema_migrations (name) VALUES (%s)", (path.name,))
            applied.append(path.name)
    return applied


def migrate_agent(url: str | None = None) -> None:
    cfg = Config(str(Path(__file__).parents[1] / "alembic.ini"))
    if url:
        cfg.attributes["url"] = url
    command.upgrade(cfg, "head")


def main() -> int:
    s = get_settings()
    applied = migrate_warehouse(s.dsn("pipeline", "warehouse"))
    print(f"warehouse: {', '.join(applied) if applied else 'up to date'}")
    migrate_agent()
    print("agent: at head")
    return 0


if __name__ == "__main__":
    sys.exit(main())
