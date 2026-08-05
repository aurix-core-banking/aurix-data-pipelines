import argparse
import os
import sys
from datetime import datetime
from io import BytesIO


def parse_args():
    parser = argparse.ArgumentParser(description="Ingestão PostgreSQL -> Bronze")
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Executa ingestão incremental baseada em watermark",
    )
    parser.add_argument(
        "--watermark",
        default=None,
        help="Data/hora de referência para ingestão incremental (ISO 8601)",
    )
    return parser.parse_args()


def read_watermark(s3, bucket, key):
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        return obj["Body"].read().decode("utf-8").strip()
    except Exception:
        return None


def write_watermark(s3, bucket, key, value):
    s3.put_object(Bucket=bucket, Key=key, Body=value.encode("utf-8"))


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

    args = None
    try:
        args = parse_args()
    except SystemExit:
        args = argparse.Namespace(
            incremental=(os.environ.get("INCREMENTAL") or "false").lower() == "true",
            watermark=None,
        )
    incremental = args.incremental

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
            watermark_key = f"postgres/{table}/_watermark"
            watermark = read_watermark(s3, bucket, watermark_key)
            if incremental:
                wm = args.watermark or watermark
                if wm:
                    query = (
                        f"SELECT * FROM {pg_schema}.{table} "
                        f"WHERE data_atualizacao > '{wm}'"
                    )
                    df = pd.read_sql_query(query, engine)
                else:
                    df = pd.read_sql_table(table, engine, schema=pg_schema)
            else:
                df = pd.read_sql_table(table, engine, schema=pg_schema)
            buf = BytesIO()
            df.to_parquet(buf, index=False)
            buf.seek(0)
            key = f"postgres/{table}/{prefix}/{table}.parquet"
            s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
            print(f"Wrote s3://{bucket}/{key} ({len(df)} rows)")
            if incremental and not df.empty and "data_atualizacao" in df.columns:
                novo_watermark = df["data_atualizacao"].max().isoformat()
                write_watermark(s3, bucket, watermark_key, novo_watermark)
                print(f"Watermark atualizado para {table}: {novo_watermark}")
        except Exception as e:
            print(f"Skip {table}: {e}", file=sys.stderr)


if __name__ == "__main__":
    main()
