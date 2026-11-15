"""Run the pipeline end to end: ingest, clean, then build and test the marts.

    ingest     download the chosen years of claims from OpenFEMA into S3 raw
    clean      run the Glue Spark job and wait for it
    dbt_build  build the dbt models on Athena and run their data tests

If any step fails, the ones after it do not run, so a bad load never reaches
the marts.
"""

from __future__ import annotations

import os
import time
from datetime import datetime

import boto3
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Param, dag, task

PROJECT = "/opt/airflow/project"
GLUE_JOB = os.environ.get("GLUE_JOB", "flood-claims-lakehouse-clean-claims")
DBT = "/opt/airflow/dbt-venv/bin/dbt"


@dag(
    dag_id="flood_claims",
    schedule="0 6 1 * *",  # 06:00 UTC on the first of each month
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    params={
        "years": Param(
            "2025-2026",
            type="string",
            description="Years of loss to reload, e.g. 2026, 2024-2026 or 2009-2026",
        )
    },
    tags=["flood-claims"],
    doc_md=__doc__,
)
def flood_claims():
    ingest = BashOperator(
        task_id="ingest",
        bash_command=(
            f"python {PROJECT}/ingest/download_claims.py "
            "--out s3://$RAW_BUCKET/raw --years '{{ params.years }}'"
        ),
    )

    @task
    def clean() -> dict:
        """Start the Glue job and wait. Raise if it does not succeed."""
        glue = boto3.client("glue")
        run_id = glue.start_job_run(JobName=GLUE_JOB)["JobRunId"]
        while True:
            run = glue.get_job_run(JobName=GLUE_JOB, RunId=run_id)["JobRun"]
            state = run["JobRunState"]
            if state not in ("STARTING", "RUNNING", "WAITING", "STOPPING"):
                break
            time.sleep(15)
        if state != "SUCCEEDED":
            raise RuntimeError(f"Glue run {run_id} ended {state}: {run.get('ErrorMessage')}")
        return {"run_id": run_id, "seconds": run["ExecutionTime"]}

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            f"{DBT} build --project-dir {PROJECT}/dbt --profiles-dir {PROJECT}/dbt "
            "--target-path /tmp/dbt-target --log-path /tmp/dbt-logs --no-use-colors"
        ),
    )

    ingest >> clean() >> dbt_build


flood_claims()
