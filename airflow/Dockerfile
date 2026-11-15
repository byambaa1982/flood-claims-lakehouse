FROM apache/airflow:3.4.0-python3.12

# dbt gets its own virtual environment so its dependencies cannot clash with Airflow's.
RUN python -m venv /opt/airflow/dbt-venv \
    && /opt/airflow/dbt-venv/bin/pip install --no-cache-dir dbt-core==1.12.7 dbt-athena==1.11.1
