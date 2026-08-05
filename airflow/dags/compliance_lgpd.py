"""
DAG de compliance LGPD: gera relatório de compliance e purga dados expirados.

- `gerar_relatorio`: executa o módulo compliance/data_compliance.py para gerar
  o inventário e relatório de compliance (LGPD).
- `purgar_dados_expirados`: remove dados com período de retenção expirado
  (configurável por tabela via variáveis do Airflow).
"""

from datetime import datetime, timedelta
import os
from typing import Any, Dict

from airflow import DAG
from airflow.operators.python import PythonOperator

from alertas_aurix import notificar_falha, notificar_sucesso

# Período de retenção por tabela (dias), conforme política LGPD.
RETENCAO_DIAS = {
    "cliente_pf": int(os.environ.get("LGPD_RETENCAO_CLIENTE_PF", "2555")),  # 7 anos
    "cliente_pj": int(os.environ.get("LGPD_RETENCAO_CLIENTE_PJ", "2555")),
    "auditoria": int(os.environ.get("LGPD_RETENCAO_AUDITORIA", "1825")),  # 5 anos
    "transacao": int(os.environ.get("LGPD_RETENCAO_TRANSACAO", "1825")),
}


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


def gerar_relatorio() -> Dict[str, Any]:
    from compliance.data_compliance import LGPDCompliance

    lgpd = LGPDCompliance(_config())
    lgpd.connect_databases()
    return lgpd.generate_compliance_report()


def purgar_dados_expirados() -> Dict[str, int]:
    """Executa DELETE nos dados com retenção expirada por tabela."""
    import psycopg2

    conn = psycopg2.connect(**_config()["postgres"])
    cur = conn.cursor()
    removidos = {}
    try:
        for tabela, dias in RETENCAO_DIAS.items():
            try:
                cur.execute(
                    f"DELETE FROM aurix.{tabela} WHERE data_atualizacao < NOW() - "
                    f"INTERVAL '{dias} days'"
                )
                removidos[tabela] = cur.rowcount
            except psycopg2.errors.UndefinedTable:
                # Tabela pode não existir no ambiente local
                removidos[tabela] = 0
                conn.rollback()
        conn.commit()
    finally:
        cur.close()
        conn.close()
    return removidos


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
    dag_id="compliance_lgpd",
    default_args=default_args,
    schedule="0 4 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["aurix", "compliance", "lgpd"],
) as dag:
    relatorio = PythonOperator(
        task_id="gerar_relatorio_compliance",
        python_callable=gerar_relatorio,
        on_success_callback=notificar_sucesso,
        dag=dag,
    )
    purga = PythonOperator(
        task_id="purgar_dados_expirados",
        python_callable=purgar_dados_expirados,
        on_success_callback=notificar_sucesso,
        dag=dag,
    )
    relatorio >> purga
