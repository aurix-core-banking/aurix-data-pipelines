"""
Aurix Data Pipelines — Detecção de Fraude em Tempo Real (Flink Streaming).

Detecta transações fraudulentas usando regras CEP e janelas temporais.
Entrada: Kafka (transacoes)
Saída: Kafka (alertas_fraude)
Janelas: Tumbling 1 min, Sliding 5 min

Uso:
    python flink/streaming/realtime_fraud.py
"""

import json
import os
import logging
from datetime import datetime
from typing import Dict

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("aurix.flink.streaming.realtime_fraud")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPICO_ENTRADA = os.getenv("TOPICO_TRANSACOES", "transacoes")
TOPICO_ALERTAS = os.getenv("TOPICO_ALERTAS_FRAUDE", "alertas_fraude")

# ─────────────────────────────────────────────────────────
# Configuração do Flink
# ─────────────────────────────────────────────────────────


def criar_env() -> StreamExecutionEnvironment:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(4)
    env.enable_checkpointing(60000)  # checkpoint a cada 60s
    return env


def criar_tabelas_kafka(tenv: StreamTableEnvironment):
    """Cria tabelas Kafka source e sink via DDL SQL."""

    # Fonte: tópico de transações
    tenv.execute_sql(f"""
        CREATE TABLE IF NOT EXISTS transacoes_source (
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
            aprovada BOOLEAN,
            WATERMARK FOR data_transacao AS data_transacao - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{TOPICO_ENTRADA}',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
            'properties.group.id' = 'aurix-flink-fraud',
            'properties.auto.offset.reset' = 'latest',
            'format' = 'json',
            'scan.startup.mode' = 'latest-offset'
        )
    """)

    # Sink: tópico de alertas de fraude
    tenv.execute_sql(f"""
        CREATE TABLE IF NOT EXISTS alertas_fraude_sink (
            alerta_id STRING,
            conta_id BIGINT,
            transacao_id BIGINT,
            tipo_fraude STRING,
            score_fraude FLOAT,
            nivel_risco STRING,
            descricao STRING,
            regras_acionadas STRING,
            valor_transacao DECIMAL(19,4),
            canal STRING,
            cidade STRING,
            estado STRING,
            timestamp_alerta TIMESTAMP(3),
            recomendacao STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{TOPICO_ALERTAS}',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
            'format' = 'json'
        )
    """)


def criar_detectao_fraude(tenv: StreamTableEnvironment):
    """Cria pipeline de detecção de fraude com regras CEP e janelas.

    Regras:
    1. Transação de alto valor (> R$ 10.000) em horário noturno
    2. Múltiplas transações da mesma conta em < 1 minuto (janela tumbling)
    3. Acúmulo de valor > R$ 20.000 em 5 minutos (janela sliding)
    4. Transação em localidade diferente da última (desvio geográfico)
    """

    # ── Regra 1: Alto valor + noturno ──
    logger.info("Configurando Regra 1: Alto valor + noturno")
    tenv.execute_sql("""
        CREATE TABLE alertas_regra1_sink (
            alerta_id STRING,
            conta_id BIGINT,
            transacao_id BIGINT,
            tipo_fraude STRING,
            score_fraude FLOAT,
            nivel_risco STRING,
            descricao STRING,
            regras_acionadas STRING,
            valor_transacao DECIMAL(19,4),
            canal STRING,
            cidade STRING,
            estado STRING,
            timestamp_alerta TIMESTAMP(3),
            recomendacao STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'alertas_fraude_regra1',
            'properties.bootstrap.servers' = '""" + KAFKA_BOOTSTRAP + """',
            'format' = 'json'
        )
    """)

    tenv.execute_sql("""
        INSERT INTO alertas_regra1_sink
        SELECT
            CONCAT('ALT-R1-', CAST(id AS STRING)) as alerta_id,
            conta_id,
            id as transacao_id,
            'ALTO_VALOR_NOTURNO' as tipo_fraude,
            0.85 as score_fraude,
            'ALTO' as nivel_risco,
            CONCAT('Transação de alto valor (R$ ', CAST(valor AS STRING), ') em horário noturno') as descricao,
            'REGRA_1_ALTO_VALOR_NOTURNO' as regras_acionadas,
            valor as valor_transacao,
            canal,
            cidade,
            estado,
            data_transacao as timestamp_alerta,
            'BLOQUEAR_E_VERIFICAR' as recomendacao
        FROM transacoes_source
        WHERE valor > 10000
          AND (EXTRACT(HOUR FROM data_transacao) < 6
               OR EXTRACT(HOUR FROM data_transacao) > 22)
    """)

    # ── Regra 2: Múltiplas transações rápidas (Tumbling 1 min) ──
    logger.info("Configurando Regra 2: Múltiplas transações rápidas (Tumbling 1 min)")
    tenv.execute_sql("""
        CREATE TABLE alertas_regra2_sink (
            alerta_id STRING,
            conta_id BIGINT,
            transacao_id BIGINT,
            tipo_fraude STRING,
            score_fraude FLOAT,
            nivel_risco STRING,
            descricao STRING,
            regras_acionadas STRING,
            valor_transacao DECIMAL(19,4),
            canal STRING,
            cidade STRING,
            estado STRING,
            timestamp_alerta TIMESTAMP(3),
            recomendacao STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'alertas_fraude_regra2',
            'properties.bootstrap.servers' = '""" + KAFKA_BOOTSTRAP + """',
            'format' = 'json'
        )
    """)

    tenv.execute_sql("""
        INSERT INTO alertas_regra2_sink
        SELECT
            CONCAT('ALT-R2-', CAST(conta_id AS STRING), '-',
                   CAST(TUMBLE_START(data_transacao, INTERVAL '1' MINUTE) AS STRING)) as alerta_id,
            conta_id,
            0 as transacao_id,
            'TRANSACOES_RAPIDAS' as tipo_fraude,
            CASE
                WHEN COUNT(*) >= 10 THEN 0.95
                WHEN COUNT(*) >= 5 THEN 0.80
                ELSE 0.60
            END as score_fraude,
            CASE
                WHEN COUNT(*) >= 10 THEN 'CRITICO'
                WHEN COUNT(*) >= 5 THEN 'ALTO'
                ELSE 'MEDIO'
            END as nivel_risco,
            CONCAT(CAST(COUNT(*) AS STRING), ' transações em 1 minuto da conta') as descricao,
            'REGRA_2_TRANSACOES_RAPIDAS' as regras_acionadas,
            SUM(valor) as valor_transacao,
            '' as canal,
            '' as cidade,
            '' as estado,
            TUMBLE_END(data_transacao, INTERVAL '1' MINUTE) as timestamp_alerta,
            'REVISAO_MANUAL' as recomendacao
        FROM transacoes_source
        WHERE aprovada = TRUE
        GROUP BY
            conta_id,
            TUMBLE(data_transacao, INTERVAL '1' MINUTE)
        HAVING COUNT(*) >= 3
    """)

    # ── Regra 3: Acúmulo alto (Sliding 5 min) ──
    logger.info("Configurando Regra 3: Acúmulo alto (Sliding 5 min)")
    tenv.execute_sql("""
        CREATE TABLE alertas_regra3_sink (
            alerta_id STRING,
            conta_id BIGINT,
            transacao_id BIGINT,
            tipo_fraude STRING,
            score_fraude FLOAT,
            nivel_risco STRING,
            descricao STRING,
            regras_acionadas STRING,
            valor_transacao DECIMAL(19,4),
            canal STRING,
            cidade STRING,
            estado STRING,
            timestamp_alerta TIMESTAMP(3),
            recomendacao STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'alertas_fraude_regra3',
            'properties.bootstrap.servers' = '""" + KAFKA_BOOTSTRAP + """',
            'format' = 'json'
        )
    """)

    tenv.execute_sql("""
        INSERT INTO alertas_regra3_sink
        SELECT
            CONCAT('ALT-R3-', CAST(conta_id AS STRING), '-',
                   CAST(HOP_END(data_transacao, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE) AS STRING)
            ) as alerta_id,
            conta_id,
            0 as transacao_id,
            'ACUMULO_VALOR' as tipo_fraude,
            CASE
                WHEN SUM(valor) > 50000 THEN 0.98
                WHEN SUM(valor) > 30000 THEN 0.90
                ELSE 0.75
            END as score_fraude,
            CASE
                WHEN SUM(valor) > 50000 THEN 'CRITICO'
                WHEN SUM(valor) > 30000 THEN 'ALTO'
                ELSE 'MEDIO'
            END as nivel_risco,
            CONCAT('Acumulo de R$ ', CAST(SUM(valor) AS STRING),
                   ' em 5 minutos da conta') as descricao,
            'REGRA_3_ACUMULO_VALOR' as regras_acionadas,
            SUM(valor) as valor_transacao,
            '' as canal,
            '' as cidade,
            '' as estado,
            HOP_END(data_transacao, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE) as timestamp_alerta,
            'BLOQUEAR_E_VERIFICAR' as recomendacao
        FROM transacoes_source
        WHERE aprovada = TRUE
        GROUP BY
            conta_id,
            HOP(data_transacao, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE)
        HAVING SUM(valor) > 20000
    """)


def executar():
    """Executa o pipeline completo de detecção de fraude."""
    logger.info("═══ Detecção de Fraude em Tempo Real — Flink ═══")

    env = criar_env()
    tenv = StreamTableEnvironment.create(env)

    criar_tabelas_kafka(tenv)
    criar_detectao_fraude(tenv)

    logger.info("Pipeline de fraude iniciado. Aguardando transações...")
    logger.info("  → Tópico entrada: %s", TOPICO_ENTRADA)
    logger.info("  → Tópicos alertas: alertas_fraude_regra1/2/3")

    env.execute("Aurix-Realtime-FraudDetection")


if __name__ == "__main__":
    executar()
