"""
dbt Transform DAG
Runs dbt models to transform HMDA + ACS raw data into opportunity score mart.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "dbt_transform",
    default_args=default_args,
    description="Run dbt models for lending desert analysis",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dbt", "transform"],
)

DBT_DIR = "/opt/airflow/dbt"

with dag:
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"dbt run --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}",
    )
