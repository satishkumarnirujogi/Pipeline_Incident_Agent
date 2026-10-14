-- Quarantine: rows taken out of the pipeline by the quarantine_rows tool.
--
-- A rule (ops.quarantine_rules) is a structured predicate for one dataset and month.
--   * rows already in raw that match are moved to quarantine.yellow_trips at once
--     (F06, F07: the bad rows reached raw and failed dbt);
--   * while the rule is active, ingest diverts matching landing rows there too
--     (F04: the contract stopped the rows before raw).
-- restore_quarantined(batch_id) moves the rows back and deactivates the rule.
-- Runs as the pipeline role (owner); applied by scripts/migrate.py.

CREATE TABLE IF NOT EXISTS ops.quarantine_rules (
    id           bigserial PRIMARY KEY,
    batch_id     uuid NOT NULL UNIQUE,
    dataset      text NOT NULL,
    month        text NOT NULL,
    column_name  text NOT NULL,
    op           text NOT NULL CHECK (op IN ('is_null', 'lt', 'gt', 'not_in_dim')),
    value        double precision,
    active       boolean NOT NULL DEFAULT true,
    created_by   text NOT NULL DEFAULT current_user,
    created_at   timestamptz NOT NULL DEFAULT now(),
    restored_at  timestamptz
);
CREATE INDEX IF NOT EXISTS quarantine_rules_lookup
    ON ops.quarantine_rules (dataset, month) WHERE active;

CREATE TABLE IF NOT EXISTS quarantine.yellow_trips (
    LIKE raw.yellow_trips INCLUDING DEFAULTS,
    batch_id        uuid NOT NULL,
    quarantined_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS quarantine_yellow_trips_batch
    ON quarantine.yellow_trips (batch_id, pickup_month);

-- Default privileges from init.sql already give agent_ro/agent_rw SELECT and agent_rw
-- INSERT/DELETE on quarantine.*. The rules table needs explicit grants:
GRANT INSERT ON ops.quarantine_rules TO agent_rw;
GRANT UPDATE (active, restored_at) ON ops.quarantine_rules TO agent_rw;
GRANT USAGE ON SEQUENCE ops.quarantine_rules_id_seq TO agent_rw;
