"""
DAG de ingestão Kafka (CDC Debezium) -> Bronze (a cada 15 minutos, modo batch).

Executa o script ingestion/kafka_to_bronze.py, que consome os tópicos
``cdc.aurix.*`` publicados pelo Kafka Connect (Debezium) e grava Parquet
no data lake (bronze), fazendo commit dos offsets consumidos.
"""

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

COMANDO_INGESTAO = "cd /opt/airflow/ingestion && python kafka_to_bronze.py --once"

with DAG(
    dag_id="ingest_kafka_to_bronze",
    default_args=default_args,
    schedule="*/15 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["aurix", "bronze", "ingestion", "cdc"],
) as dag:
    ingestao = BashOperator(
        task_id="ingestao_cdc_kafka_bronze",
        bash_command=COMANDO_INGESTAO,
        on_success_callback=notificar_sucesso,
        dag=dag,
    )
