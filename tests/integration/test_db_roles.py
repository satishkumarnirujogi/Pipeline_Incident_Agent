"""Safety layer 1: even if a tool were tricked, the database refuses destructive SQL."""

import psycopg
import pytest

pytestmark = pytest.mark.usefixtures("platform")


def _try(platform, role: str, sql: str) -> None:
    with psycopg.connect(platform["dsn"](role, "warehouse"), autocommit=True) as conn:
        conn.execute(sql)


@pytest.mark.parametrize(
    "sql",
    [
        "CREATE TABLE raw.x (i int)",
        "INSERT INTO raw.yellow_trips (pickup_month) VALUES ('2026-06')",
        "DELETE FROM raw.yellow_trips",
        "UPDATE ops.column_mapping SET target_col = 'x'",
        "INSERT INTO ops.quarantine_rules (batch_id, dataset, month, column_name, op) "
        "VALUES (gen_random_uuid(), 'd', 'm', 'c', 'is_null')",
    ],
)
def test_agent_ro_cannot_write(platform, sql):
    with pytest.raises(psycopg.errors.InsufficientPrivilege):
        _try(platform, "agent_ro", sql)


@pytest.mark.parametrize(
    "sql",
    [
        "DROP TABLE raw.yellow_trips",
        "TRUNCATE raw.yellow_trips",
        "ALTER TABLE raw.yellow_trips DROP COLUMN fare_amount",
        "CREATE TABLE raw.x (i int)",
        "UPDATE raw.yellow_trips SET fare_amount = 0",
        "DELETE FROM ops.column_mapping",
        "UPDATE ops.quarantine_rules SET month = 'x'",
        "DROP TABLE marts.dim_zones",
    ],
)
def test_agent_rw_cannot_do_ddl_or_out_of_scope_writes(platform, sql):
    with pytest.raises(psycopg.errors.InsufficientPrivilege):
        _try(platform, "agent_rw", sql)


def test_agent_roles_cannot_reach_the_agent_database(platform):
    for role in ("agent_ro", "agent_rw"):
        with pytest.raises(psycopg.OperationalError, match="permission denied"):
            psycopg.connect(platform["dsn"](role, "agent")).close()


def test_statement_timeout_is_set(platform):
    with psycopg.connect(platform["dsn"]("agent_ro", "warehouse")) as conn:
        assert conn.execute("SHOW statement_timeout").fetchone()[0] == "30s"
