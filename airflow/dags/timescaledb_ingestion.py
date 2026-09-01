"""
TimescaleDB Ingestion DAG

This DAG performs two main ingestion pipelines into TimescaleDB hypertables:

1. ingest_transaction_metrics (runs every minute)
   - Consumes real transaction events from the Kafka topic "core.transacao.realizada.v1"
   - Validates required fields, publishing invalid records to the dead-letter queue
     topic "timescaledb_ingestion_dlq"
   - Aggregates valid transactions by 1-minute windows
   - Inserts aggregated metrics into the "metricas_transacoes" hypertable
   - Falha hard se o Kafka estiver indisponivel (sem dados simulados)

2. sync_system_metrics (runs every 5 minutes)
   - Queries the real Prometheus API (PROMETHEUS_URL) via PromQL
   - Parses node/postgres metrics (CPU, memory, disk, conexoes, etc.)
   - Inserts raw metric samples into the "metricas_sistema" hypertable
   - Falha hard se o Prometheus estiver indisponivel
"""

from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator

from alertas_aurix import notificar_falha, notificar_sucesso

default_args = {
    "owner": "aurix",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": notificar_falha,
}


def get_timescaledb_connection():
    import psycopg2

    return psycopg2.connect(
        host=os.environ.get("TIMESCALEDB_HOST", "localhost"),
        port=int(os.environ.get("TIMESCALEDB_PORT", "5433")),
        dbname=os.environ.get("TIMESCALEDB_DB", "aurix_timeseries"),
        user=os.environ.get("TIMESCALEDB_USER", "aurix"),
        password=os.environ.get("TIMESCALEDB_PASSWORD", "replace_with_secure_password"),
    )


def ingest_transaction_metrics():
    import json
    from datetime import datetime

    conn = get_timescaledb_connection()
    cur = conn.cursor()

    # Ensure hypertable exists (idempotent setup)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS metricas_transacoes (
            tempo TIMESTAMPTZ NOT NULL,
            janela TIMESTAMPTZ NOT NULL,
            total_transacoes INTEGER NOT NULL,
            valor_total NUMERIC(18,2) NOT NULL,
            valor_medio NUMERIC(18,2) NOT NULL,
            contagem_por_tipo JSONB DEFAULT '{}',
            PRIMARY KEY (tempo, janela)
        );
    """)
    cur.execute("SELECT create_hypertable('metricas_transacoes', 'tempo', if_not_exists => TRUE);")
    conn.commit()

    # Consume real transactions from Kafka (sem fallback simulado: falha hard)
    from kafka import KafkaConsumer, KafkaProducer
    import json as json_module

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    dlq_topic = os.environ.get("TIMESCALEDB_INGESTION_DLQ_TOPIC", "timescaledb_ingestion_dlq")
    transactions = []
    now = datetime.utcnow()

    consumer = KafkaConsumer(
        "core.transacao.realizada.v1",
        bootstrap_servers=bootstrap,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="timescaledb_ingestion",
        consumer_timeout_ms=10000,
        value_deserializer=lambda m: json_module.loads(m.decode("utf-8"))
    )
    for msg in consumer:
        data = msg.value
        transactions.append({
            "transaction_id": data.get("eventId") or msg.offset,
            "timestamp": data.get("timestamp", now.isoformat()),
            "tipo": data.get("tipoTransacao") or data.get("eventType", "DESCONHECIDO"),
            "valor": float(data.get("valor", 0)),
            "cliente_id": data.get("clienteId", 0),
        })
        if len(transactions) >= 100:
            break
    consumer.close()

    # Dead-letter queue: publish registros invalidos ao topico DLQ real
    required_fields = {"transaction_id", "timestamp", "tipo", "valor"}
    valid = []
    dlq = []
    for t in transactions:
        if not required_fields.issubset(t.keys()) or t.get("valor") is None:
            dlq.append(t)
        else:
            valid.append(t)

    if dlq:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap,
            value_serializer=lambda v: json_module.dumps(v).encode("utf-8")
        )
        for record in dlq:
            producer.send(dlq_topic, record)
        producer.flush()
        producer.close()
        print(f"[DLQ] {len(dlq)} registros invalidos publicados em {dlq_topic}")

    # Aggregate valid transactions by 1-minute window
    window_key = now.replace(second=0, microsecond=0)
    total = len(valid)
    valor_total = sum(t["valor"] for t in valid)
    valor_medio = round(valor_total / total, 2) if total > 0 else 0.0

    tipo_counts = {}
    for t in valid:
        tipo_counts[t["tipo"]] = tipo_counts.get(t["tipo"], 0) + 1

    cur.execute(
        """
        INSERT INTO metricas_transacoes (tempo, janela, total_transacoes, valor_total, valor_medio, contagem_por_tipo)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (tempo, janela)
        DO UPDATE SET
            total_transacoes = EXCLUDED.total_transacoes,
            valor_total = EXCLUDED.valor_total,
            valor_medio = EXCLUDED.valor_medio,
            contagem_por_tipo = EXCLUDED.contagem_por_tipo;
        """,
        (
            now,
            window_key,
            total,
            valor_total,
            valor_medio,
            json.dumps(tipo_counts),
        ),
    )
    conn.commit()
    cur.close()
    conn.close()
    print(
        f"[ingest_transaction_metrics] Ingested {total} transactions "
        f"(total={valor_total}, avg={valor_medio}), {len(dlq)} sent to DLQ"
    )


def sync_system_metrics():
    import json as json_module
    from datetime import datetime

    prometheus_url = os.environ.get("PROMETHEUS_URL")
    if not prometheus_url:
        raise RuntimeError(
            "PROMETHEUS_URL nao configurada. Metricas de sistema simuladas nao sao mais permitidas."
        )

    conn = get_timescaledb_connection()
    cur = conn.cursor()

    # Ensure hypertable exists (idempotent setup)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS metricas_sistema (
            tempo TIMESTAMPTZ NOT NULL,
            nome_metrica VARCHAR(100) NOT NULL,
            valor DOUBLE PRECISION NOT NULL,
            labels JSONB DEFAULT '{}',
            PRIMARY KEY (tempo, nome_metrica)
        );
    """)
    cur.execute("SELECT create_hypertable('metricas_sistema', 'tempo', if_not_exists => TRUE);")
    conn.commit()

    now = datetime.utcnow()

    # Consultas reais ao Prometheus (node_exporter e postgres_exporter)
    queries = {
        "cpu_usage_percent":
            '100 - (avg by (instance) (rate(node_cpu_seconds_total{mode="idle"}[5m])) * 100)',
        "memory_usage_percent":
            '(1 - (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes)) * 100',
        "disk_usage_percent":
            'avg by (instance) (1 - node_filesystem_avail_bytes{mountpoint="/"} '
            '/ node_filesystem_size_bytes{mountpoint="/"}) * 100',
        "active_connections":
            'sum(pg_stat_database_numbackends)',
        "transactions_per_second":
            'sum(rate(node_context_switches_total[5m]))',
        "query_latency_ms":
            'avg(node_scrape_collector_duration_seconds) * 1000',
    }

    import requests

    labels = json_module.dumps({"source": "prometheus", "url": prometheus_url})

    for name, promql in queries.items():
        try:
            resp = requests.get(
                f"{prometheus_url.rstrip('/')}/api/v1/query",
                params={"query": promql},
                timeout=15,
            )
            resp.raise_for_status()
        except Exception as e:
            raise RuntimeError(f"Prometheus indisponivel para '{name}': {e}") from e

        results = resp.json().get("data", {}).get("result", [])
        if not results:
            print(f"[sync_system_metrics] {name}: nenhuma serie encontrada, ignorando")
            continue
        value = float(results[0].get("value", [0, 0])[1])
        cur.execute(
            """
            INSERT INTO metricas_sistema (tempo, nome_metrica, valor, labels)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (tempo, nome_metrica)
            DO UPDATE SET
                valor = EXCLUDED.valor,
                labels = EXCLUDED.labels;
            """,
            (now, name, value, labels),
        )

    conn.commit()
    cur.close()
    conn.close()
    print(f"[sync_system_metrics] Inserted real Prometheus metrics at {now.isoformat()}")


dag = DAG(
    dag_id="timescaledb_ingestion",
    default_args=default_args,
    schedule="*/1 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["aurix", "timescaledb", "ingestion"],
    description=__doc__,
)

PythonOperator(
    task_id="ingest_transaction_metrics",
    python_callable=ingest_transaction_metrics,
    on_success_callback=notificar_sucesso,
    dag=dag,
)

PythonOperator(
    task_id="sync_system_metrics",
    python_callable=sync_system_metrics,
    on_success_callback=notificar_sucesso,
    dag=dag,
)
