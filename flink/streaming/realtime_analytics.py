"""
Aurix Data Pipelines — Analytics em Tempo Real (Flink Streaming).

Métricas em tempo real por segundo, alimentando Elasticsearch para dashboards.
Entrada: Kafka (transacoes, eventos_risco)
Saída: Elasticsearch (aurix-transactions-realtime, aurix-metrics-realtime)

Métricas por segundo:
- Transações por tipo, canal, status
- Volume financeiro acumulado
- Taxa de aprovação em janela deslizante
- Score de risco médio

Uso:
    python flink/streaming/realtime_analytics.py
"""

import os
import logging

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("aurix.flink.streaming.realtime_analytics")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPICO_TRANSACOES = os.getenv("TOPICO_TRANSACOES", "transacoes")
TOPICO_EVENTOS = os.getenv("TOPICO_EVENTOS_RISCO", "eventos_risco")
ES_HOST = os.getenv("ES_HOST", "elasticsearch")
ES_PORT = os.getenv("ES_PORT", "9200")


def criar_env() -> StreamExecutionEnvironment:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(4)
    env.enable_checkpointing(30000)
    return env


def criar_tabelas(tenv: StreamTableEnvironment):
    """Cria tabelas Kafka source e Elasticsearch sink."""

    # Fonte: transações
    tenv.execute_sql("""
        CREATE TABLE IF NOT EXISTS transacoes_source (
            id BIGINT,
            conta_id BIGINT,
            tipo_transacao STRING,
            valor DECIMAL(19,4),
            data_transacao TIMESTAMP(3),
            status STRING,
            canal STRING,
            score_risco FLOAT,
            aprovada BOOLEAN,
            tempo_processamento_ms INT,
            WATERMARK FOR data_transacao AS data_transacao - INTERVAL '1' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = '""" + TOPICO_TRANSACOES + """',
            'properties.bootstrap.servers' = '""" + KAFKA_BOOTSTRAP + """',
            'properties.group.id' = 'aurix-flink-analytics',
            'properties.auto.offset.reset' = 'latest',
            'format' = 'json'
        )
    """)

    # Sink: Elasticsearch — transações em tempo real
    tenv.execute_sql("""
        CREATE TABLE IF NOT EXISTS es_transacoes_sink (
            id BIGINT,
            conta_id BIGINT,
            tipo_transacao STRING,
            valor DECIMAL(19,4),
            data_transacao TIMESTAMP(3),
            status STRING,
            canal STRING,
            score_risco FLOAT,
            processado_em TIMESTAMP(3)
        ) WITH (
            'connector' = 'elasticsearch-7',
            'hosts' = 'http://""" + ES_HOST + """:""" + ES_PORT + """',
            'index' = 'aurix-transactions-realtime'
        )
    """)

    # Sink: Elasticsearch — métricas agregadas por segundo
    tenv.execute_sql("""
        CREATE TABLE IF NOT EXISTS es_metricas_sink (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            tipo_transacao STRING,
            canal STRING,
            status STRING,
            total_transacoes BIGINT,
            valor_total DECIMAL(19,4),
            valor_medio DECIMAL(19,4),
            score_risco_medio FLOAT,
            taxa_aprovacao FLOAT,
            tempo_resposta_medio FLOAT
        ) WITH (
            'connector' = 'elasticsearch-7',
            'hosts' = 'http://""" + ES_HOST + """:""" + ES_PORT + """',
            'index' = 'aurix-metrics-realtime'
        )
    """)


def criar_analytics_por_segundo(tenv: StreamTableEnvironment):
    """Métricas agregadas por segundo com janela tumbling de 1s."""
    logger.info("Configurando analytics por segundo (Tumbling 1s)")

    # Métricas por segundo
    tenv.execute_sql("""
        INSERT INTO es_metricas_sink
        SELECT
            TUMBLE_START(data_transacao, INTERVAL '1' SECOND) as window_start,
            TUMBLE_END(data_transacao, INTERVAL '1' SECOND) as window_end,
            tipo_transacao,
            canal,
            status,
            COUNT(*) as total_transacoes,
            SUM(valor) as valor_total,
            AVG(valor) as valor_medio,
            AVG(score_risco) as score_risco_medio,
            CAST(SUM(CASE WHEN aprovada THEN 1 ELSE 0 END) AS DOUBLE)
                / CAST(COUNT(*) AS DOUBLE) as taxa_aprovacao,
            AVG(CAST(tempo_processamento_ms AS DOUBLE)) as tempo_resposta_medio
        FROM transacoes_source
        GROUP BY
            TUMBLE(data_transacao, INTERVAL '1' SECOND),
            tipo_transacao, canal, status
    """)


def criar_analytics_janela_deslizante(tenv: StreamTableEnvironment):
    """Métricas com janela deslizante de 5 minutos para dashboards."""
    logger.info("Configurando analytics sliding 5 min para Kafka")

    # Sink: Kafka métricas
    tenv.execute_sql("""
        CREATE TABLE IF NOT EXISTS kafka_metricas_sink (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            metrica_tipo STRING,
            valor DOUBLE,
            dimensao STRING,
            dimensao_valor STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'metricas_tempo_real',
            'properties.bootstrap.servers' = '""" + KAFKA_BOOTSTRAP + """',
            'format' = 'json'
        )
    """)

    # Volume por tipo de transação (sliding 5 min)
    tenv.execute_sql("""
        INSERT INTO kafka_metricas_sink
        SELECT
            HOP_START(data_transacao, INTERVAL '10' SECOND, INTERVAL '5' MINUTE) as window_start,
            HOP_END(data_transacao, INTERVAL '10' SECOND, INTERVAL '5' MINUTE) as window_end,
            'VOLUME_TRANSACOES' as metrica_tipo,
            CAST(SUM(valor) AS DOUBLE) as valor,
            'tipo_transacao' as dimensao,
            tipo_transacao as dimensao_valor
        FROM transacoes_source
        GROUP BY
            HOP(data_transacao, INTERVAL '10' SECOND, INTERVAL '5' MINUTE),
            tipo_transacao
    """)

    # Contagem por canal (sliding 5 min)
    tenv.execute_sql("""
        INSERT INTO kafka_metricas_sink
        SELECT
            HOP_START(data_transacao, INTERVAL '10' SECOND, INTERVAL '5' MINUTE) as window_start,
            HOP_END(data_transacao, INTERVAL '10' SECOND, INTERVAL '5' MINUTE) as window_end,
            'TRANSACOES_POR_CANAL' as metrica_tipo,
            CAST(COUNT(*) AS DOUBLE) as valor,
            'canal' as dimensao,
            canal as dimensao_valor
        FROM transacoes_source
        GROUP BY
            HOP(data_transacao, INTERVAL '10' SECOND, INTERVAL '5' MINUTE),
            canal
    """)

    # Taxa de aprovação global (sliding 5 min)
    tenv.execute_sql("""
        INSERT INTO kafka_metricas_sink
        SELECT
            HOP_START(data_transacao, INTERVAL '10' SECOND, INTERVAL '5' MINUTE) as window_start,
            HOP_END(data_transacao, INTERVAL '10' SECOND, INTERVAL '5' MINUTE) as window_end,
            'TAXA_APROVACAO' as metrica_tipo,
            CAST(SUM(CASE WHEN aprovada THEN 1 ELSE 0 END) AS DOUBLE)
                / CAST(COUNT(*) AS DOUBLE) as valor,
            'global' as dimensao,
            'GLOBAL' as dimensao_valor
        FROM transacoes_source
        GROUP BY
            HOP(data_transacao, INTERVAL '10' SECOND, INTERVAL '5' MINUTE)
    """)


def criar_transacoes_raw_es(tenv: StreamTableEnvironment):
    """Salva transações raw no Elasticsearch para busca e analytics."""
    logger.info("Configurando transações raw → Elasticsearch")

    tenv.execute_sql("""
        INSERT INTO es_transacoes_sink
        SELECT
            id, conta_id, tipo_transacao, valor,
            data_transacao, status, canal, score_risco,
            CURRENT_TIMESTAMP as processado_em
        FROM transacoes_source
    """)


def executar():
    """Executa o pipeline completo de analytics em tempo real."""
    logger.info("═══ Analytics em Tempo Real — Flink ═══")

    env = criar_env()
    tenv = StreamTableEnvironment.create(env)

    criar_tabelas(tenv)
    criar_transacoes_raw_es(tenv)
    criar_analytics_por_segundo(tenv)
    criar_analytics_janela_deslizante(tenv)

    logger.info("Pipeline de analytics iniciado.")
    logger.info("  → ES: aurix-transactions-realtime")
    logger.info("  → ES: aurix-metrics-realtime (1s)")
    logger.info("  → Kafka: metricas_tempo_real (sliding 5min)")

    env.execute("Aurix-Realtime-Analytics")


if __name__ == "__main__":
    executar()
