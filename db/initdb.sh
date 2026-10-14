#!/bin/bash
# Runs once, on the first start of an empty Postgres volume.
# Passes role passwords from the environment into init.sql as psql variables.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
  -v airflow_pw="$AIRFLOW_DB_PASSWORD" \
  -v pipeline_pw="$PIPELINE_DB_PASSWORD" \
  -v agent_ro_pw="$AGENT_RO_PASSWORD" \
  -v agent_rw_pw="$AGENT_RW_PASSWORD" \
  -v agent_app_pw="$AGENT_APP_PASSWORD" \
  -f /opt/db/init.sql
