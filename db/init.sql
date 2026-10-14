-- Databases, schemas, extensions and least-privilege roles.
-- The roles are the first safety layer: even if an agent is tricked into
-- running destructive SQL, the database refuses it.

-- ---------------------------------------------------------------- roles
CREATE ROLE airflow   LOGIN PASSWORD :'airflow_pw';
CREATE ROLE pipeline  LOGIN PASSWORD :'pipeline_pw';   -- Airflow tasks and dbt
CREATE ROLE agent_ro  LOGIN PASSWORD :'agent_ro_pw';   -- tier-0 tools
CREATE ROLE agent_rw  LOGIN PASSWORD :'agent_rw_pw';   -- tier-1/2 tools
CREATE ROLE agent_app LOGIN PASSWORD :'agent_app_pw';  -- API, worker, checkpoints

ALTER ROLE agent_ro SET statement_timeout = '30s';
ALTER ROLE agent_rw SET statement_timeout = '30s';

-- ------------------------------------------------------------ databases
CREATE DATABASE airflow   OWNER airflow;
CREATE DATABASE warehouse OWNER pipeline;
CREATE DATABASE agent     OWNER agent_app;

REVOKE ALL ON DATABASE airflow   FROM PUBLIC;
REVOKE ALL ON DATABASE warehouse FROM PUBLIC;
REVOKE ALL ON DATABASE agent     FROM PUBLIC;
GRANT CONNECT ON DATABASE airflow   TO airflow;
GRANT CONNECT ON DATABASE warehouse TO pipeline, agent_ro, agent_rw;
GRANT CONNECT ON DATABASE agent     TO agent_app;

-- ------------------------------------------------------------ warehouse
\connect warehouse

REVOKE CREATE ON SCHEMA public FROM PUBLIC;

SET ROLE pipeline;
CREATE SCHEMA raw;
CREATE SCHEMA staging;
CREATE SCHEMA marts;
CREATE SCHEMA quarantine;
CREATE SCHEMA ops;

-- How a schema-drift / type-mismatch fix is applied (Phase 2, step 2.1).
CREATE TABLE ops.column_mapping (
    id          bigserial PRIMARY KEY,
    dataset     text NOT NULL,
    source_col  text NOT NULL,
    target_col  text NOT NULL,
    transform   text NOT NULL DEFAULT 'none'
                CHECK (transform IN ('none', 'strip_currency', 'to_float', 'to_timestamp')),
    created_at  timestamptz NOT NULL DEFAULT now(),
    created_by  text NOT NULL DEFAULT current_user,
    UNIQUE (dataset, source_col)
);

CREATE TABLE ops.partition_log (
    id          bigserial PRIMARY KEY,
    dataset     text NOT NULL,
    partition   text NOT NULL,
    row_count   bigint NOT NULL,
    checksum    text,
    run_id      text,
    loaded_at   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE ops.dbt_runs (
    id           bigserial PRIMARY KEY,
    month        text,
    run_id       text,
    run_results  jsonb NOT NULL,
    loaded_at    timestamptz NOT NULL DEFAULT now()
);
RESET ROLE;

-- agent_ro: read everything, write nothing.
GRANT USAGE ON SCHEMA raw, staging, marts, quarantine, ops TO agent_ro, agent_rw;
GRANT SELECT ON ALL TABLES IN SCHEMA raw, staging, marts, quarantine, ops TO agent_ro, agent_rw;
ALTER DEFAULT PRIVILEGES FOR ROLE pipeline IN SCHEMA raw, staging, marts, quarantine, ops
    GRANT SELECT ON TABLES TO agent_ro, agent_rw;

-- agent_rw: INSERT/DELETE on raw.* and quarantine.*; INSERT/UPDATE on ops.column_mapping.
-- No CREATE on any schema and no ownership, so no DROP, ALTER, TRUNCATE or other DDL.
ALTER DEFAULT PRIVILEGES FOR ROLE pipeline IN SCHEMA raw, quarantine
    GRANT INSERT, DELETE ON TABLES TO agent_rw;
ALTER DEFAULT PRIVILEGES FOR ROLE pipeline IN SCHEMA raw, quarantine
    GRANT USAGE ON SEQUENCES TO agent_rw;
GRANT INSERT, UPDATE ON ops.column_mapping TO agent_rw;
GRANT USAGE ON SEQUENCE ops.column_mapping_id_seq TO agent_rw;

-- ---------------------------------------------------------------- agent
\connect agent

CREATE EXTENSION IF NOT EXISTS vector;
CREATE SCHEMA eval AUTHORIZATION agent_app;
-- public is owned by pg_database_owner (agent_app), so app tables,
-- LangGraph checkpoints and metrics tables are created there by migrations.
