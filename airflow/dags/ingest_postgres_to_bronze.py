"""
DAG de ingestão PostgreSQL -> Bronze (diária, incremental).

Executa o script ingestion/pg_to_bronze.py, que realiza a leitura incremental
das tabelas do core banking para o data lake (bronze) via watermark.
"""

from datetime import datetime, timedelta
import os

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

COMANDO_INGESTAO = (
    "cd /opt/airflow/ingestion && python pg_to_bronze.py --incremental"
)

with DAG(
    dag_id="ingest_postgres_to_bronze",
    default_args=default_args,
    schedule="0 0 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["aurix", "bronze", "ingestion"],
) as dag:
    ingestao = BashOperator(
        task_id="ingestao_postgres_bronze",
        bash_command=COMANDO_INGESTAO,
        on_success_callback=notificar_sucesso,
        dag=dag,
    )
