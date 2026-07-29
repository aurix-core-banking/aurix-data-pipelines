from datetime import datetime
import os
from airflow import DAG
from airflow.operators.python import PythonOperator

def run_pg_to_bronze():
    import pandas as pd
    import boto3
    from sqlalchemy import create_engine
    from io import BytesIO
    pg_url = os.environ.get(
        "PG_BRONZE_URL",
        "postgresql+psycopg2://aurix_user:aurix_secure_password@postgres:5432/aurix"
    )
    engine = create_engine(pg_url)
    endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.environ.get("MINIO_ACCESS_KEY", "aurix_admin")
    secret_key = os.environ.get("MINIO_SECRET_KEY", "aurix_secure_password")
    bucket = os.environ.get("BRONZE_BUCKET", "aurix-bronze")
    tables = ["contas", "clientes", "transacoes"]
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
    )
    prefix = datetime.now().strftime("%Y/%m/%d")
    for table in tables:
        try:
            df = pd.read_sql_table(table, engine, schema=os.environ.get("PG_SCHEMA", "aurix"))
            buf = BytesIO()
            df.to_parquet(buf, index=False)
            buf.seek(0)
            key = f"postgres/{table}/{prefix}/{table}.parquet"
            s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
            print(f"Wrote {bucket}/{key} ({len(df)} rows)")
        except Exception as e:
            print(f"Skip {table}: {e}")

dag = DAG(
    dag_id="ingest_postgres_to_bronze",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["aurix", "bronze", "ingestion"],
)
PythonOperator(
    task_id="pg_to_bronze",
    python_callable=run_pg_to_bronze,
    dag=dag,
)
