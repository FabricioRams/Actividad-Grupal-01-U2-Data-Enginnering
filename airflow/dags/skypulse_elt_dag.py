"""Apache Airflow DAG for SkyPulse Weather & Air Quality ELT Pipeline (Zero AWS)."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

DEFAULT_ARGS = {
    "owner": "data-engineering-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="skypulse_weather_air_quality_elt",
    default_args=DEFAULT_ARGS,
    description="Extract Open-Meteo telemetry, load into PostgreSQL, and run dbt Medallion models & tests.",
    schedule="0 */6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["skypulse", "dbt", "postgres", "elt", "no-aws"],
) as dag:
    extract_and_load_raw = BashOperator(
        task_id="extract_and_load_open_meteo",
        bash_command="cd /opt/skypulse && python -m src.ingestion.run_pipeline --skip-dbt",
    )

    with TaskGroup(group_id="dbt_medallion_pipeline") as dbt_group:
        dbt_run_staging = BashOperator(
            task_id="dbt_run_staging",
            bash_command=(
                "cd /opt/skypulse/dbt_project && "
                "dbt run --select staging --profiles-dir . --target postgres"
            ),
        )

        dbt_run_marts = BashOperator(
            task_id="dbt_run_intermediate_and_marts",
            bash_command=(
                "cd /opt/skypulse/dbt_project && "
                "dbt run --select intermediate marts --profiles-dir . --target postgres"
            ),
        )

        dbt_test_quality = BashOperator(
            task_id="dbt_test_data_quality",
            bash_command=(
                "cd /opt/skypulse/dbt_project && "
                "dbt test --profiles-dir . --target postgres"
            ),
        )

        dbt_run_staging >> dbt_run_marts >> dbt_test_quality

    extract_and_load_raw >> dbt_group
