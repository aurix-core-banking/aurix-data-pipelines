"""DAG de transformação dbt Silver -> Gold (pós-ingestão)."""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

from alertas_aurix import notificar_falha, notificar_sucesso

default_args = {
    "owner": "aurix",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notificar_falha,
}

with DAG(
    dag_id="silver_to_gold_dbt",
    default_args=default_args,
    schedule="0 2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["aurix", "gold", "dbt"],
) as dag:
    run_dbt = BashOperator(
        task_id="dbt_run_gold",
        bash_command="cd /opt/airflow/dbt && dbt run --target prod --select gold",
    )
    test_dbt = BashOperator(
        task_id="dbt_test_gold",
        bash_command="cd /opt/airflow/dbt && dbt test --target prod --select gold",
    )
    run_dbt >> test_dbt
