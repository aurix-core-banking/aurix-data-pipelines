import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone


TOPICOS_CDC_PADRAO = [
    "cdc.aurix.contas",
    "cdc.aurix.clientes",
    "cdc.aurix.transacoes",
    "cdc.aurix.pix_pagamentos",
]

GRUPO_CONSUMIDOR = "aurix_cdc_bronze"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Consumo de eventos CDC (Debezium) -> Bronze"
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Consome as mensagens disponíveis e encerra (modo batch/Airflow)",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=None,
        help="Limite de mensagens por execução (default: env CDC_MAX_MESSAGES ou 1000)",
    )
    return parser.parse_args()


def criar_consumidor():
    try:
        from confluent_kafka import Consumer
    except ImportError:
        print("pip install confluent-kafka", file=sys.stderr)
        sys.exit(1)
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    config = {
        "bootstrap.servers": bootstrap,
        "group.id": os.environ.get("CDC_CONSUMER_GROUP", GRUPO_CONSUMIDOR),
        "auto.offset.reset": os.environ.get("CDC_AUTO_OFFSET_RESET", "earliest"),
        "enable.auto.commit": "false",
    }
    return Consumer(config)


def criar_s3():
    try:
        import boto3
    except ImportError:
        print("pip install boto3", file=sys.stderr)
        sys.exit(1)
    endpoint = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
    access_key = os.environ.get("MINIO_ACCESS_KEY", "aurix_admin")
    secret_key = os.environ.get("MINIO_SECRET_KEY", "aurix_secure_password")
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
    )


def normalizar_topicos():
    raw = os.environ.get("CDC_TOPICS", ",".join(TOPICOS_CDC_PADRAO))
    return [t.strip() for t in raw.split(",") if t.strip()]


def parse_mensagem(valor_bytes):
    """Converte o payload Debezium (JSON) em registro de bronze.

    Retorna dict com os dados da linha enriquecidos com metadados CDC
    (``_cdc_op``, ``_cdc_ts``, ``_cdc_deleted``) ou ``None`` quando não há dados.
    """
    if not valor_bytes:
        return None
    try:
        payload = json.loads(valor_bytes.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return None
    op = payload.get("op")
    ts_ms = payload.get("ts_ms") or (payload.get("source") or {}).get("ts_ms")
    dados = payload.get("after")
    if dados is None and op == "d":
        dados = payload.get("before")
    if dados is None:
        return None
    registro = dict(dados)
    registro["_cdc_op"] = op or "u"
    registro["_cdc_ts"] = datetime.fromtimestamp(
        ts_ms / 1000, tz=timezone.utc
    ).isoformat() if ts_ms else None
    registro["_cdc_deleted"] = op == "d"
    return registro


def agrupar_por_topico(registros):
    """Agrupa registros [(topico, registro)] por tópico, preservando ordem."""
    agrupado = {}
    for topico, registro in registros:
        agrupado.setdefault(topico, []).append(registro)
    return agrupado


def escrever_bronze(s3, bucket, topico, registros, prefixo=None):
    """Grava um arquivo Parquet por tópico no bucket bronze."""
    try:
        import pyarrow as pa
    except ImportError:
        print("pip install pyarrow", file=sys.stderr)
        sys.exit(1)
    if prefixo is None:
        prefixo = datetime.now().strftime("%Y/%m/%d")
    tabela = pa.Table.from_pylist(registros)
    buf = pa.BufferOutputStream()
    import pyarrow.parquet as pq

    pq.write_table(tabela, buf)
    nome_tabela = topico.split(".")[-1]
    chave = f"cdc/{nome_tabela}/{prefixo}/{nome_tabela}.parquet"
    s3.put_object(Bucket=bucket, Key=chave, Body=buf.getvalue())
    print(f"Wrote s3://{bucket}/{chave} ({len(registros)} rows)")
    return chave


def consumir_uma_vez(consumer, s3, bucket, max_messages, timeout=60.0):
    """Consome mensagens disponíveis, grava no bronze e faz commit.

    Retorna o total de registros gravados.
    """
    topicos = normalizar_topicos()
    consumer.subscribe(topicos)
    registros = []
    deadline = time.time() + timeout
    vazio_consecutivo = 0
    while (
        len(registros) < max_messages
        and time.time() < deadline
        and vazio_consecutivo < 3
    ):
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            vazio_consecutivo += 1
            continue
        if msg.error():
            if msg.error().code() in (KAFKA_EOF, KAFKA_TIMED_OUT):
                vazio_consecutivo += 1
                continue
            raise RuntimeError(f"Erro no consumidor: {msg.error()}")
        vazio_consecutivo = 0
        registro = parse_mensagem(msg.value())
        if registro is not None:
            registros.append((msg.topic(), registro))
        consumer.commit(asynchronous=False)
    if registros:
        for topico, recs in agrupar_por_topico(registros).items():
            escrever_bronze(s3, bucket, topico, recs)
    consumer.unsubscribe()
    return len(registros)


try:
    from confluent_kafka import KafkaError

    KAFKA_EOF = KafkaError._PARTITION_EOF
    KAFKA_TIMED_OUT = KafkaError._TIMED_OUT
except ImportError:
    KAFKA_EOF = "PARTITION_EOF"
    KAFKA_TIMED_OUT = "TIMED_OUT"


def main():
    args = None
    try:
        args = parse_args()
    except SystemExit:
        args = argparse.Namespace(
            once=(os.environ.get("CDC_ONCE") or "false").lower() == "true",
            max_messages=None,
        )
    max_messages = args.max_messages or int(
        os.environ.get("CDC_MAX_MESSAGES", "1000")
    )
    bucket = os.environ.get("BRONZE_BUCKET", "aurix-bronze")
    consumer = criar_consumidor()
    s3 = criar_s3()
    try:
        total = consumir_uma_vez(consumer, s3, bucket, max_messages)
    finally:
        try:
            consumer.close()
        except Exception:
            pass
    print(f"Total de registros CDC gravados: {total}")


if __name__ == "__main__":
    main()
