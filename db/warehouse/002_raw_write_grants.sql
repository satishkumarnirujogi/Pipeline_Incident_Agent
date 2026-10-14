-- Fix for databases initialised before this grant was added to init.sql:
-- the raw tables were created before the default privileges, so agent_rw lacked
-- the INSERT/DELETE on raw.* that quarantine_rows and restore_quarantined need.
GRANT INSERT, DELETE ON ALL TABLES IN SCHEMA raw, quarantine TO agent_rw;
