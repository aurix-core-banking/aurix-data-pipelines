"""
Aurix Data Pipelines — Monitoramento PIX em Tempo Real (Flink Streaming).

Monitora transações PIX com CEP (Complex Event Processing):
- PIX alto valor (> R$ 50.000)
- Múltiplos PIX rapidamente (varredura)
- Horário anormal com valor alto
- Múltiplas cidades (clonagem)

Entrada: Kafka (transacoes)
Saída: ClickHouse (eventos_pix) + Kafka (alertas_pix)

Uso:
    python flink/streaming/realtime_pix.py
"""

import json
import os
import logging

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("aurix.flink.streaming.realtime_pix")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPICO_ENTRADA = os.getenv("TOPICO_TRANSACOES", "transacoes")
CH_URL = os.getenv("CLICKHOUSE_URL", "clickhouse://clickhouse:8123")
CH_DB = os.getenv("CLICKHOUSE_DB", "aurix_analytics")


def criar_env() -> StreamExecutionEnvironment:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(4)
    env.enable_checkpointing(30000)
    return env


def criar_tabelas(tenv: StreamTableEnvironment):
    """Cria tabelas source e sink."""

    # Fonte: Kafka transações
    tenv.execute_sql("""
        CREATE TABLE IF NOT EXISTS pix_source (
            id BIGINT,
            conta_id BIGINT,
            tipo_transacao STRING,
            valor DECIMAL(19,4),
            data_transacao TIMESTAMP(3),
            status STRING,
            canal STRING,
            dispositivo STRING,
            ip_address STRING,
            cidade STRING,
            estado STRING,
            score_risco FLOAT,
            WATERMARK FOR data_transacao AS data_transacao - INTERVAL '3' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = '""" + TOPICO_ENTRADA + """',
            'properties.bootstrap.servers' = '""" + KAFKA_BOOTSTRAP + """',
            'properties.group.id' = 'aurix-flink-pix',
            'properties.auto.offset.reset' = 'latest',
            'format' = 'json'
        )
    """)

    # Sink: ClickHouse
    tenv.execute_sql("""
        CREATE TABLE IF NOT EXISTS pix_analytics_sink (
            id BIGINT,
            conta_id BIGINT,
            valor DECIMAL(19,4),
            data_transacao TIMESTAMP(3),
            canal STRING,
            cidade STRING,
            estado STRING,
            score_risco FLOAT,
            hora_dia INT,
            is_fim_de_semana BOOLEAN,
            is_horario_comercial BOOLEAN,
            data_processamento TIMESTAMP(3)
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:""" + CH_URL + """',
            'table-name' = '""" + CH_DB + """.eventos_pix',
            'driver' = 'ru.yandex.clickhouse.ClickHouseDriver'
        )
    """)

    # Sink: Kafka alertas
    tenv.execute_sql("""
        CREATE TABLE IF NOT EXISTS alertas_pix_sink (
            alerta_id STRING,
            conta_id BIGINT,
            transacao_id BIGINT,
            tipo_alerta STRING,
            valor_transacao DECIMAL(19,4),
            descricao STRING,
            score_risco FLOAT,
            timestamp_alerta TIMESTAMP(3),
            recomendacao STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'alertas_pix',
            'properties.bootstrap.servers' = '""" + KAFKA_BOOTSTRAP + """',
            'format' = 'json'
        )
    """)


def criar_cep_patterns(tenv: StreamTableEnvironment):
    """Configura padrões CEP para detecção de fraude PIX."""

    # ── Padrão 1: PIX Alto Valor ──
    logger.info("CEP 1: PIX Alto Valor (> R$ 50.000)")
    sink_ddl = """
        CREATE TABLE IF NOT EXISTS alertas_pix_cep1_sink (
            alerta_id STRING, conta_id BIGINT, transacao_id BIGINT,
            tipo_alerta STRING, valor_transacao DECIMAL(19,4),
            descricao STRING, score_risco FLOAT,
            timestamp_alerta TIMESTAMP(3), recomendacao STRING
        ) WITH (
            'connector' = 'kafka', 'topic' = 'alertas_pix_cep1',
            'properties.bootstrap.servers' = '""" + KAFKA_BOOTSTRAP + """',
            'format' = 'json'
        )
    """
    tenv.execute_sql(sink_ddl)

    insert_dml = """
        INSERT INTO alertas_pix_cep1_sink
        SELECT
            CONCAT('PIX-CEP1-', CAST(id AS STRING)) as alerta_id,
            conta_id, id as transacao_id,
            'PIX_ALTO_VALOR' as tipo_alerta,
            valor as valor_transacao,
            CONCAT('PIX de R$ ', CAST(valor AS STRING)) as descricao,
            score_risco, data_transacao as timestamp_alerta,
            'VERIFICACAO_MANUAL_OBRIGATORIA' as recomendacao
        FROM pix_source
        WHERE tipo_transacao = 'PIX' AND valor > 50000
    """
    tenv.execute_sql(insert_dml)

    # ── Padrão 2: Múltiplos PIX rápidos (Tumbling 5 min) ──
    logger.info("CEP 2: Múltiplos PIX (Tumbling 5 min)")
    sink_ddl2 = """
        CREATE TABLE IF NOT EXISTS alertas_pix_cep2_sink (
            alerta_id STRING, conta_id BIGINT, transacao_id BIGINT,
            tipo_alerta STRING, valor_transacao DECIMAL(19,4),
            descricao STRING, score_risco FLOAT,
            timestamp_alerta TIMESTAMP(3), recomendacao STRING
        ) WITH (
            'connector' = 'kafka', 'topic' = 'alertas_pix_cep2',
            'properties.bootstrap.servers' = '""" + KAFKA_BOOTSTRAP + """',
            'format' = 'json'
        )
    """
    tenv.execute_sql(sink_ddl2)

    insert_dml2 = """
        INSERT INTO alertas_pix_cep2_sink
        SELECT
            CONCAT('PIX-CEP2-', CAST(conta_id AS STRING), '-',
                   CAST(TUMBLE_END(data_transacao, INTERVAL '5' MINUTE) AS STRING)
            ) as alerta_id,
            conta_id, 0 as transacao_id,
            'PIX_VARREDURA' as tipo_alerta,
            SUM(valor) as valor_transacao,
            CONCAT(CAST(COUNT(*) AS STRING), ' PIX em 5min') as descricao,
            0.85 as score_risco,
            TUMBLE_END(data_transacao, INTERVAL '5' MINUTE) as timestamp_alerta,
            'BLOQUEAR_PROXIMOS_PIX' as recomendacao
        FROM pix_source
        WHERE tipo_transacao = 'PIX' AND status = 'AUTORIZADA'
        GROUP BY conta_id, TUMBLE(data_transacao, INTERVAL '5' MINUTE)
        HAVING COUNT(*) >= 5
    """
    tenv.execute_sql(insert_dml2)

    # ── Padrão 3: PIX fora de horário ──
    logger.info("CEP 3: PIX fora de horário")
    sink_ddl3 = """
        CREATE TABLE IF NOT EXISTS alertas_pix_cep3_sink (
            alerta_id STRING, conta_id BIGINT, transacao_id BIGINT,
            tipo_alerta STRING, valor_transacao DECIMAL(19,4),
            descricao STRING, score_risco FLOAT,
            timestamp_alerta TIMESTAMP(3), recomendacao STRING
        ) WITH (
            'connector' = 'kafka', 'topic' = 'alertas_pix_cep3',
            'properties.bootstrap.servers' = '""" + KAFKA_BOOTSTRAP + """',
            'format' = 'json'
        )
    """
    tenv.execute_sql(sink_ddl3)

    insert_dml3 = """
        INSERT INTO alertas_pix_cep3_sink
        SELECT
            CONCAT('PIX-CEP3-', CAST(id AS STRING)) as alerta_id,
            conta_id, id as transacao_id,
            'PIX_HORARIO_ANOMALO' as tipo_alerta,
            valor as valor_transacao,
            CONCAT('PIX R$ ', CAST(valor AS STRING),
                   ' as ', CAST(EXTRACT(HOUR FROM data_transacao) AS STRING), 'h') as descricao,
            GREATEST(score_risco, 0.7) as score_risco,
            data_transacao as timestamp_alerta,
            'NOTIFICAR_CLIENTE' as recomendacao
        FROM pix_source
        WHERE tipo_transacao = 'PIX' AND valor > 5000
          AND (EXTRACT(HOUR FROM data_transacao) < 6
               OR EXTRACT(HOUR FROM data_transacao) > 22)
    """
    tenv.execute_sql(insert_dml3)

    # ── Padrão 4: Multi-cidade (clonagem) ──
    logger.info("CEP 4: Multi-cidade (Sliding 1h)")
    sink_ddl4 = """
        CREATE TABLE IF NOT EXISTS alertas_pix_cep4_sink (
            alerta_id STRING, conta_id BIGINT, transacao_id BIGINT,
            tipo_alerta STRING, valor_transacao DECIMAL(19,4),
            descricao STRING, score_risco FLOAT,
            timestamp_alerta TIMESTAMP(3), recomendacao STRING
        ) WITH (
            'connector' = 'kafka', 'topic' = 'alertas_pix_cep4',
            'properties.bootstrap.servers' = '""" + KAFKA_BOOTSTRAP + """',
            'format' = 'json'
        )
    """
    tenv.execute_sql(sink_ddl4)

    insert_dml4 = """
        INSERT INTO alertas_pix_cep4_sink
        SELECT
            CONCAT('PIX-CEP4-', CAST(conta_id AS STRING), '-',
                   CAST(HOP_END(data_transacao, INTERVAL '5' MINUTE, INTERVAL '1' HOUR) AS STRING)
            ) as alerta_id,
            conta_id, 0 as transacao_id,
            'PIX_MULTI_CIDADE' as tipo_alerta,
            SUM(valor) as valor_transacao,
            CONCAT(CAST(COUNT(DISTINCT cidade) AS STRING), ' cidades em 1h') as descricao,
            0.92 as score_risco,
            HOP_END(data_transacao, INTERVAL '5' MINUTE, INTERVAL '1' HOUR) as timestamp_alerta,
            'BLOQUEAR_CONTA' as recomendacao
        FROM pix_source
        WHERE tipo_transacao = 'PIX'
        GROUP BY conta_id,
                 HOP(data_transacao, INTERVAL '5' MINUTE, INTERVAL '1' HOUR)
        HAVING COUNT(DISTINCT cidade) >= 3
    """
    tenv.execute_sql(insert_dml4)


def criar_analytics_pix(tenv: StreamTableEnvironment):
    """Salva dados PIX processados no ClickHouse para analytics."""
    logger.info("Configurando analytics PIX → ClickHouse")

    analytics_dml = """
        INSERT INTO pix_analytics_sink
        SELECT
            id, conta_id, valor, data_transacao,
            canal, cidade, estado, score_risco,
            EXTRACT(HOUR FROM data_transacao) as hora_dia,
            CASE WHEN EXTRACT(DOW FROM data_transacao) IN (0, 6) THEN true ELSE false END as is_fim_de_semana,
            CASE WHEN EXTRACT(HOUR FROM data_transacao) BETWEEN 8 AND 18 THEN true ELSE false END as is_horario_comercial,
            CURRENT_TIMESTAMP as data_processamento
        FROM pix_source
        WHERE tipo_transacao = 'PIX' AND status = 'AUTORIZADA'
    """
    tenv.execute_sql(analytics_dml)


def executar():
    """Executa o pipeline completo de monitoramento PIX."""
    logger.info("═══ Monitoramento PIX em Tempo Real — Flink ═══")

    env = criar_env()
    tenv = StreamTableEnvironment.create(env)

    criar_tabelas(tenv)
    criar_cep_patterns(tenv)
    criar_analytics_pix(tenv)

    logger.info("Pipeline PIX iniciado. Padrões CEP ativos:")
    logger.info("  CEP1: Alto valor (> R$ 50k)")
    logger.info("  CEP2: Varredura (5+ PIX em 5min)")
    logger.info("  CEP3: Horário anômalo")
    logger.info("  CEP4: Multi-cidade (clonagem)")

    env.execute("Aurix-Realtime-PIX-Monitoring")


if __name__ == "__main__":
    executar()
