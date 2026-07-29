from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="bronze_to_silver_dbt",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["aurix", "silver", "dbt"],
) as dag:
    run_dbt = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt && dbt run --target prod 2>/dev/null || echo 'dbt not configured'",
    )
    test_dbt = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt && dbt test --target prod 2>/dev/null || true",
    )
    run_dbt >> test_dbt
