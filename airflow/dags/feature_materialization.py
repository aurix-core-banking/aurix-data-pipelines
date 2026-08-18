# ============================================================
# Airflow DAG — Feature Materialization (Feast)
# Materializa features do feature store para serving ML
# ============================================================

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "ml-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="feature_materialization",
    default_args=default_args,
    description="Materializa features do Feast Feature Store para serving",
    schedule_interval="0 3 * * *",  # 03:00, depois do gold
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ml", "features", "feast"],
) as dag:

    # ═══ Credit Features ═══
    materialize_credit = BashOperator(
        task_id="materialize_credit_features",
        bash_command="""
        cd /opt/airflow/feature-store && \
        python feast_definitions.py apply && \
        feast materialize-incremental $(date -u +%Y-%m-%dT%H:%M:%SZ) --views credit_features
        """,
        dag=dag,
    )

    # ═══ Fraud Features ═══
    materialize_fraud = BashOperator(
        task_id="materialize_fraud_features",
        bash_command="""
        cd /opt/airflow/feature-store && \
        feast materialize-incremental $(date -u +%Y-%m-%dT%H:%M:%SZ) --views fraud_features
        """,
        dag=dag,
    )

    # ═══ Customer Features ═══
    materialize_customer = BashOperator(
        task_id="materialize_customer_features",
        bash_command="""
        cd /opt/airflow/feature-store && \
        feast materialize-incremental $(date -u +%Y-%m-%dT%H:%M:%SZ) --views customer_features
        """,
        dag=dag,
    )

    # ═══ Churn Features ═══
    materialize_churn = BashOperator(
        task_id="materialize_churn_features",
        bash_command="""
        cd /opt/airflow/feature-store && \
        feast materialize-incremental $(date -u +%Y-%m-%dT%H:%M:%SZ) --views churn_features
        """,
        dag=dag,
    )

    # ═══ Ordem (paralelo — independentes) ═══
    [materialize_credit, materialize_fraud, materialize_customer, materialize_churn]
