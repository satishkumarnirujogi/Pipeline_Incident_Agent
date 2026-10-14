FROM apache/airflow:3.3.2-python3.12

# dbt pins dependencies that clash with Airflow's, so it lives in its own
# virtualenv and is always called by full path: /opt/dbt-venv/bin/dbt
USER root
RUN python -m venv /opt/dbt-venv \
 && /opt/dbt-venv/bin/pip install --no-cache-dir "dbt-core~=1.12.0" "dbt-postgres~=1.11.0" \
 && chown -R airflow: /opt/dbt-venv

USER airflow
COPY requirements.txt /requirements.txt
# Re-pin apache-airflow so pip can never up/downgrade it while installing extras.
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
