"""
DAG de reconciliação contábil: core banking (PostgreSQL) vs data lake.

- `reconciliar_saldos`: total por conta (agregação no PostgreSQL) é comparado
  com o ClickHouse (bronze/silver) e com o gold.
- `reconciliar_transacoes`: total por dia.
- `reconciliar_pix`: total via BACEN SPI (integração mockada).
- Em caso de divergência acima do limiar, a tarefa falha e o alerta dispara.

A lógica de comparação vive em ``reconciliation.engine`` (testável sem Airflow).
"""

from datetime import datetime, timedelta
import logging
import os
from pathlib import Path
from typing import Any, Dict

from airflow import DAG
from airflow.operators.python import PythonOperator

from alertas_aurix import notificar_falha, notificar_sucesso

from reconciliation.engine import verificar_divergencias

logger = logging.getLogger(__name__)


def _dir_relatorios() -> str:
    return os.environ.get(
        "RECONCILIACAO_REPORTS_DIR",
        str(Path(__file__).resolve().parents[2]),
    )


def _config() -> Dict[str, Any]:
    return {
        "postgres": {
            "host": os.environ.get("PG_HOST", "postgres"),
            "port": int(os.environ.get("PG_PORT", "5432")),
            "database": os.environ.get("PG_DATABASE", "aurix"),
            "user": os.environ.get("PG_USER", "aurix_user"),
            "password": os.environ.get("PG_PASSWORD", "aurix_secure_password"),
        },
        "clickhouse": {
            "host": os.environ.get("CLICKHOUSE_HOST", "clickhouse"),
            "port": int(os.environ.get("CLICKHOUSE_PORT", "8123")),
            "database": os.environ.get("CLICKHOUSE_DATABASE", "aurix_analytics"),
            "user": os.environ.get("CLICKHOUSE_USER", "aurix"),
            "password": os.environ.get("CLICKHOUSE_PASSWORD", "aurix123"),
        },
    }


def reconciliar_saldos() -> Dict[str, Any]:
    import psycopg2
    import clickhouse_connect

    cfg = _config()
    conn = psycopg2.connect(**cfg["postgres"])
    client = clickhouse_connect.get_client(**cfg["clickhouse"])
    try:
        from reconciliation.engine import reconciliar_saldos as _reconciliar_saldos

        resultado = _reconciliar_saldos(conn, client, _dir_relatorios())
    finally:
        conn.close()
    return verificar_divergencias(resultado, "saldos")


def reconciliar_transacoes() -> Dict[str, Any]:
    import psycopg2
    import clickhouse_connect

    cfg = _config()
    conn = psycopg2.connect(**cfg["postgres"])
    client = clickhouse_connect.get_client(**cfg["clickhouse"])
    try:
        from reconciliation.engine import reconciliar_transacoes as _reconciliar_transacoes

        resultado = _reconciliar_transacoes(conn, client, _dir_relatorios())
    finally:
        conn.close()
    return verificar_divergencias(resultado, "transações")


def reconciliar_pix() -> Dict[str, Any]:
    from reconciliation.engine import reconciliar_pix as _reconciliar_pix

    bacen_mock = os.environ.get("BACEN_MOCK_URL", "http://bacen-mock:8095")
    resultado = _reconciliar_pix(bacen_mock, _dir_relatorios())
    return verificar_divergencias(resultado, "PIX")


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
    dag_id="reconciliacao_contabil",
    default_args=default_args,
    schedule="0 5 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["aurix", "reconciliacao", "compliance"],
) as dag:
    saldos = PythonOperator(
        task_id="reconciliar_saldos",
        python_callable=reconciliar_saldos,
        on_success_callback=notificar_sucesso,
        dag=dag,
    )
    transacoes = PythonOperator(
        task_id="reconciliar_transacoes",
        python_callable=reconciliar_transacoes,
        on_success_callback=notificar_sucesso,
        dag=dag,
    )
    pix = PythonOperator(
        task_id="reconciliar_pix",
        python_callable=reconciliar_pix,
        on_success_callback=notificar_sucesso,
        dag=dag,
    )
    [saldos, transacoes, pix]
