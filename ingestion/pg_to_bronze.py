import os
import sys
from datetime import datetime
from io import BytesIO

def main():
    import pandas as pd
    try:
        from sqlalchemy import create_engine
    except ImportError:
        print("pip install sqlalchemy psycopg2-binary pandas pyarrow boto3", file=sys.stderr)
        sys.exit(1)
    try:
        import boto3
    except ImportError:
        print("pip install boto3", file=sys.stderr)
        sys.exit(1)

    pg_host = os.environ.get("PG_HOST", "localhost")
    pg_port = os.environ.get("PG_PORT", "5432")
    pg_user = os.environ.get("PG_USER", "aurix_user")
    pg_pass = os.environ.get("PG_PASSWORD", "aurix_secure_password")
    pg_db = os.environ.get("PG_DATABASE", "aurix")
    pg_schema = os.environ.get("PG_SCHEMA", "aurix")
    endpoint = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
    access_key = os.environ.get("MINIO_ACCESS_KEY", "aurix_admin")
    secret_key = os.environ.get("MINIO_SECRET_KEY", "aurix_secure_password")
    bucket = os.environ.get("BRONZE_BUCKET", "aurix-bronze")

    engine = create_engine(
        f"postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"
    )
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
    )
    prefix = datetime.now().strftime("%Y/%m/%d")
    tables = ["contas", "clientes", "transacoes"]
    for table in tables:
        try:
            df = pd.read_sql_table(table, engine, schema=pg_schema)
            buf = BytesIO()
            df.to_parquet(buf, index=False)
            buf.seek(0)
            key = f"postgres/{table}/{prefix}/{table}.parquet"
            s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
            print(f"Wrote s3://{bucket}/{key} ({len(df)} rows)")
        except Exception as e:
            print(f"Skip {table}: {e}", file=sys.stderr)

if __name__ == "__main__":
    main()
