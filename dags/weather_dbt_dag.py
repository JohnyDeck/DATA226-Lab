from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_PROJECT_DIR = "/opt/airflow/dbt/weather_analytics"

default_args = {
    "owner": "DATA226",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="weather_dbt_dag",
    description="Run dbt transformations, tests, and snapshots for weather analytics",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["weather", "dbt", "analytics"],
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            "dbt run --profiles-dir ."
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            "dbt test --profiles-dir ."
        ),
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            "dbt snapshot --profiles-dir ."
        ),
    )

    dbt_run >> dbt_test >> dbt_snapshot