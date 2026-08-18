# ============================================================
# Airflow DAG — Data Quality Check (Great Expectations)
# Roda antes de silver_to_gold para garantir qualidade
# ============================================================

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="data_quality_check",
    default_args=default_args,
    description="Data Quality checks com Great Expectations antes de silver/gold",
    schedule_interval="30 0 * * *",  # 00:30, depois do bronze
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["quality", "data", "great-expectations"],
) as dag:

    # ═══ Bronze Quality ═══
    check_bronze = BashOperator(
        task_id="check_bronze_quality",
        bash_command="""
        cd /opt/airflow/data-quality && \
        python great_expectations_runner.py --tables contas,clientes,transacoes
        """,
        dag=dag,
    )

    # ═══ Silver Quality (pós-dbt) ═══
    check_silver = BashOperator(
        task_id="check_silver_quality",
        bash_command="""
        cd /opt/airflow/data-quality && \
        python great_expectations_runner.py --tables silver_contas,silver_clientes,silver_transacoes
        """,
        dag=dag,
    )

    # ═══ Gold Quality (pós-dbt) ═══
    check_gold = BashOperator(
        task_id="check_gold_quality",
        bash_command="""
        cd /opt/airflow/data-quality && \
        python great_expectations_runner.py --tables gold_transacoes_diarias,gold_contas_resumo,gold_clientes_risco
        """,
        dag=dag,
    )

    # ═══ Lineage Collection ═══
    collect_lineage = PythonOperator(
        task_id="collect_lineage",
        python_callable=lambda: __import__("lineage.openlineage_collector", fromlist=["OpenLineageCollector"]).OpenLineageCollector().collect_all(),
        dag=dag,
    )

    # ═══ DataHub Catalog Sync ═══
    sync_datahub = BashOperator(
        task_id="sync_datahub_catalog",
        bash_command="""
        cd /opt/airflow/governance && \
        python datahub_ingestor.py
        """,
        dag=dag,
    )

    # ═══ Ordem ═══
    check_bronze >> check_silver >> check_gold
    check_gold >> collect_lineage >> sync_datahub
