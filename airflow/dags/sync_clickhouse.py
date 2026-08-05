"""
DAG de sincronização pós-Gold: PostgreSQL -> ClickHouse.

Roda após a camada Gold do dbt e executa a sincronização incremental das
tabelas do core banking para o ClickHouse (analytics).
"""

from datetime import datetime, timedelta
import os
from typing import Any, Dict, List

from airflow import DAG
from airflow.operators.python import PythonOperator

from alertas_aurix import notificar_falha, notificar_sucesso

TABELAS = ["transacoes", "contas", "clientes", "solicitacoes_credito", "investimentos"]


def _config() -> Dict[str, Dict[str, Any]]:
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


def sync_clickhouse_apos_gold() -> List[Dict[str, Any]]:
    from sync.postgres_to_clickhouse import PostgresToClickHouseSync

    config = _config()
    sync = PostgresToClickHouseSync(config)
    sync.connect_postgres()
    sync.connect_clickhouse()

    desde = datetime.now() - timedelta(hours=24)
    resultados = []
    for tabela in TABELAS:
        try:
            sync.sync_table(tabela, last_sync_time=desde)
            resultados.append({"tabela": tabela, "status": "OK"})
        except Exception as e:  # noqa: BLE001
            resultados.append({"tabela": tabela, "status": f"ERRO: {e}"})

    falhas = [r for r in resultados if r["status"] != "OK"]
    if falhas:
        raise RuntimeError(f"Falhas na sincronização: {falhas}")
    return resultados


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
    dag_id="sync_clickhouse",
    default_args=default_args,
    schedule="0 3 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["aurix", "clickhouse", "sync", "gold"],
) as dag:
    sincronizar = PythonOperator(
        task_id="sincronizar_clickhouse",
        python_callable=sync_clickhouse_apos_gold,
        on_success_callback=notificar_sucesso,
        dag=dag,
    )
