"""run_readonly_sql must reject anything but one plain SELECT before it reaches the database."""

import pytest

import agent.tools  # noqa: F401
from agent import db
from agent.tools.readonly import check_readonly_sql
from agent.tools.registry import ToolContext, call_tool
from agent.tools.store import InMemoryToolStore


@pytest.fixture
def no_database(monkeypatch):
    def refuse(role):
        raise AssertionError("the query reached the database")

    monkeypatch.setattr(db, "warehouse_engine", refuse)
    monkeypatch.setattr("agent.tools.readonly.warehouse_engine", refuse)


@pytest.mark.parametrize(
    "sql",
    [
        "DELETE FROM raw.yellow_trips",
        "delete from raw.yellow_trips where pickup_month = '2026-06'",
        "DROP TABLE raw.yellow_trips",
        "TRUNCATE raw.yellow_trips",
        "UPDATE ops.column_mapping SET target_col = 'x'",
        "INSERT INTO ops.column_mapping VALUES (1)",
        "SELECT 1; DELETE FROM raw.yellow_trips",
        "WITH d AS (DELETE FROM raw.yellow_trips RETURNING *) SELECT * FROM d",
        "SELECT * INTO raw.copy FROM raw.yellow_trips",
        "SELECT pg_sleep(60)",
        "SELECT pg_terminate_backend(1)",
        "SELECT set_config('statement_timeout', '0', false)",
        "SELECT * FROM raw.yellow_trips FOR UPDATE",
        "CREATE TABLE raw.x (i int)",
        "ALTER TABLE raw.yellow_trips DROP COLUMN fare_amount",
        "GRANT ALL ON raw.yellow_trips TO agent_ro",
        "COPY raw.yellow_trips TO '/tmp/x'",
        "SET statement_timeout = 0",
    ],
)
def test_rejected_before_reaching_the_database(sql, no_database):
    ctx = ToolContext(incident_id="i", agent="diagnostics", store=InMemoryToolStore())
    r = call_tool("run_readonly_sql", {"query": sql}, ctx)
    assert not r.ok
    assert r.error_type in ("permission", "validation")


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT count(*) FROM raw.yellow_trips WHERE pickup_month = '2026-06'",
        "WITH t AS (SELECT fare_amount FROM raw.yellow_trips) SELECT min(fare_amount) FROM t",
        "SELECT a FROM x UNION SELECT b FROM y",
        "select pulocationid, count(*) from raw.yellow_trips group by 1 order by 2 desc",
    ],
)
def test_plain_selects_pass_the_guard(sql):
    check_readonly_sql(sql)
