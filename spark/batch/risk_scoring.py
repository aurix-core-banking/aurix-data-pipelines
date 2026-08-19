"""
Aurix Data Pipelines — Scoring de Risco Diário (Spark Batch).

Processamento batch para calcular score de risco consolidado de clientes.
Entrada: PostgreSQL (transacoes, contas, clientes)
Saída: ClickHouse (risk_scores) + PostgreSQL (atualiza campo score_risco)

Uso:
    spark-submit --master yarn spark/batch/risk_scoring.py
    # ou local
    python spark/batch/risk_scoring.py
"""

import os
import logging
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("aurix.spark.batch.risk_scoring")

# ─────────────────────────────────────────────────────────
# Configuração
# ─────────────────────────────────────────────────────────

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB", "aurix_db")
PG_USER = os.getenv("PG_USER", "aurix")
PG_PASSWORD = os.getenv("PG_PASSWORD", "aurix123")

CH_HOST = os.getenv("CH_HOST", "clickhouse")
CH_PORT = os.getenv("CH_PORT", "8123")
CH_DB = os.getenv("CH_DB", "aurix_analytics")
CH_USER = os.getenv("CH_USER", "aurix")
CH_PASSWORD = os.getenv("CH_PASSWORD", "aurix123")

# Pesos para cálculo do score composto
PESOS = {
    "comportamento_transacional": 0.35,
    "utilizacao_credito": 0.25,
    "frequencia_atividade": 0.20,
    "concentracao_geografica": 0.10,
    "diversidade_produtos": 0.10,
}


def criar_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("Aurix-Batch-RiskScoring")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def ler_dados(spark: SparkSession):
    """Lê dados necessários para scoring de risco."""
    clientes = (
        spark.read.format("jdbc")
        .option("url", f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}")
        .option("dbtable", "clientes")
        .option("user", PG_USER).option("password", PG_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    contas = (
        spark.read.format("jdbc")
        .option("url", f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}")
        .option("dbtable", "contas")
        .option("user", PG_USER).option("password", PG_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    transacoes = (
        spark.read.format("jdbc")
        .option("url", f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}")
        .option("dbtable", f"""
            (SELECT * FROM transacoes
             WHERE data_transacao >= CURRENT_DATE - INTERVAL '90 days') AS t
        """)
        .option("user", PG_USER).option("password", PG_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    return clientes, contas, transacoes


def calcular_score_comportamento(transacoes) -> "DataFrame":
    """Score baseado em comportamento transacional (0-100).

    Fatores:
    - Regularidade de transações (menos irregular = melhor)
    - Horário das transações (horário comercial = melhor)
    - Valor médio vs mediana (consistência)
    - Presença de transações suspeitas (alto valor, madrugada)
    """
    agg = (
        transacoes
        .groupBy("conta_id")
        .agg(
            F.count("*").alias("total_transacoes"),
            F.avg("valor").alias("valor_medio"),
            F.stddev("valor").alias("valor_stddev"),
            F.sum(F.when(
                (F.hour(F.col("data_transacao").cast("timestamp")) < 6) |
                (F.hour(F.col("data_transacao").cast("timestamp")) > 22),
                1
            ).otherwise(0)).alias("transacoes_noturnas"),
            F.sum(F.when(F.col("valor") > 10000, 1).otherwise(0)).alias("transacoes_alto_valor"),
            F.countDistinct(
                F.date_trunc("day", F.col("data_transacao").cast("timestamp"))
            ).alias("dias_ativos"),
        )
    )

    # Score: regularidade (dias ativos / 90), penalidade noturna, penalidade alto valor
    scored = (
        agg
        .withColumn(
            "regularidade",
            F.when(F.col("dias_ativos") > 0,
                   F.least(F.col("dias_ativos") / 90.0, F.lit(1.0))).otherwise(0),
        )
        .withColumn(
            "pct_noturnas",
            F.when(F.col("total_transacoes") > 0,
                   F.col("transacoes_noturnas") / F.col("total_transacoes")).otherwise(0),
        )
        .withColumn(
            "pct_alto_valor",
            F.when(F.col("total_transacoes") > 0,
                   F.col("transacoes_alto_valor") / F.col("total_transacoes")).otherwise(0),
        )
        .withColumn(
            "score_comportamento",
            (F.col("regularidade") * 60 +
             (1 - F.col("pct_noturnas")) * 20 +
             (1 - F.col("pct_alto_valor")) * 20).cast("int"),
        )
        .select("conta_id", "score_comportamento")
    )
    return scored


def calcular_score_credito(contas) -> "DataFrame":
    """Score baseado em utilização de crédito (0-100).

    Fatores:
    - Utilização do limite (menor = melhor)
    - Saldo positivo
    - Contas ativas vs encerradas
    """
    scored = (
        contas
        .groupBy("cliente_id")
        .agg(
            F.sum("limite").alias("limite_total"),
            F.sum("saldo").alias("saldo_total"),
            F.count("*").alias("qtd_contas"),
            F.sum(F.when(F.col("status") == "ATIVA", 1).otherwise(0)).alias("contas_ativas"),
        )
        .withColumn(
            "utilizacao_limite",
            F.when(F.col("limite_total") > 0,
                   F.col("saldo_total") / F.col("limite_total")).otherwise(0),
        )
        .withColumn(
            "score_credito",
            F.when(F.col("utilizacao_limite") < 0.3, 90)
            .when(F.col("utilizacao_limite") < 0.5, 75)
            .when(F.col("utilizacao_limite") < 0.8, 50)
            .otherwise(25).cast("int"),
        )
        .select("cliente_id", "score_credito")
    )
    return scored


def calcular_score_atividade(transacoes) -> "DataFrame":
    """Score baseado em frequência de atividade (0-100)."""
    scored = (
        transacoes
        .groupBy("conta_id")
        .agg(
            F.count("*").alias("total_transacoes_90d"),
            F.countDistinct(F.date_trunc("day", F.col("data_transacao").cast("timestamp"))).alias("dias_ativos"),
            F.countDistinct("tipo_transacao").alias("qtd_tipos"),
            F.countDistinct("canal").alias("qtd_canais"),
        )
        .withColumn(
            "frequencia_diaria",
            F.when(F.col("dias_ativos") > 0, F.col("dias_ativos") / 90.0).otherwise(0),
        )
        .withColumn(
            "score_atividade",
            F.least(
                (F.col("frequencia_diaria") * 50 +
                 F.col("qtd_tipos") * 10 +
                 F.col("qtd_canais") * 10).cast("int"),
                F.lit(100),
            ),
        )
        .select("conta_id", "score_atividade")
    )
    return scored


def calcular_score_concentracao(transacoes) -> "DataFrame":
    """Score baseado em diversidade geográfica (0-100)."""
    scored = (
        transacoes
        .groupBy("conta_id")
        .agg(
            F.countDistinct("estado").alias("qtd_estados"),
            F.countDistinct("cidade").alias("qtd_cidades"),
        )
        .withColumn(
            "score_concentracao",
            F.when(F.col("qtd_cidades") <= 2, 90)
            .when(F.col("qtd_cidades") <= 5, 70)
            .when(F.col("qtd_cidades") <= 10, 50)
            .otherwise(30).cast("int"),
        )
        .select("conta_id", "score_concentracao")
    )
    return scored


def calcular_score_diversidade(contas) -> "DataFrame":
    """Score baseado em diversidade de produtos (0-100)."""
    scored = (
        contas
        .groupBy("cliente_id")
        .agg(
            F.countDistinct("tipo_conta").alias("qtd_tipos_conta"),
        )
        .withColumn(
            "score_diversidade",
            F.least(F.col("qtd_tipos_conta") * 25, F.lit(100)).cast("int"),
        )
        .select("cliente_id", "score_diversidade")
    )
    return scored


def calcular_score_final(clientes, contas, transacoes) -> "DataFrame":
    """Calcula score de risco final composto (0-1000)."""
    # Mapear conta_id → cliente_id
    conta_cliente = contas.select("id", "cliente_id").distinct()

    # Scores por conta
    score_comportamento = calcular_score_comportamento(transacoes)
    score_atividade = calcular_score_atividade(transacoes)
    score_concentracao = calcular_score_concentracao(transacoes)

    # Scores por cliente
    score_credito = calcular_score_credito(contas)
    score_diversidade = calcular_score_diversidade(contas)

    # Join scores por conta → cliente
    scores_conta = (
        conta_cliente
        .join(score_comportamento, conta_cliente["id"] == score_comportamento["conta_id"], "left")
        .join(score_atividade, conta_cliente["id"] == score_atividade["conta_id"], "left")
        .join(score_concentracao, conta_cliente["id"] == score_concentracao["conta_id"], "left")
        .groupBy("cliente_id")
        .agg(
            F.avg("score_comportamento").alias("score_comportamento"),
            F.avg("score_atividade").alias("score_atividade"),
            F.avg("score_concentracao").alias("score_concentracao"),
        )
    )

    # Join final
    score_final = (
        clientes.select("id", "nome", "cpf", "score_risco")
        .join(scores_conta, clientes["id"] == scores_conta["cliente_id"], "left")
        .join(score_credito, clientes["id"] == score_credito["cliente_id"], "left")
        .join(score_diversidade, clientes["id"] == score_diversidade["cliente_id"], "left")
        .fillna(50)  # Score neutro para campos nulos
        .withColumn(
            "score_risco_calculado",
            (
                F.col("score_comportamento") * PESOS["comportamento_transacional"] +
                F.col("score_credito") * PESOS["utilizacao_credito"] +
                F.col("score_atividade") * PESOS["frequencia_atividade"] +
                F.col("score_concentracao") * PESOS["concentracao_geografica"] +
                F.col("score_diversidade") * PESOS["diversidade_produtos"]
            ).cast("int"),
        )
        .withColumn(
            "nivel_risco",
            F.when(F.col("score_risco_calculado") >= 750, "BAIXO")
            .when(F.col("score_risco_calculado") >= 500, "MEDIO")
            .when(F.col("score_risco_calculado") >= 250, "ALTO")
            .otherwise("MUITO_ALTO"),
        )
        .withColumn("data_scoring", F.current_date())
        .withColumn("data_processamento", F.current_timestamp())
    )

    return score_final


def salvar_no_clickhouse(df, tabela: str):
    (
        df.write.format("jdbc")
        .option("url", f"jdbc:clickhouse://{CH_HOST}:{CH_PORT}/{CH_DB}")
        .option("dbtable", tabela)
        .option("user", CH_USER).option("password", CH_PASSWORD)
        .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
        .mode("overwrite")
        .save()
    )
    logger.info("Salvo no ClickHouse: %s", tabela)


def executar_pipeline():
    """Pipeline principal de scoring de risco."""
    logger.info("═══ Scoring de Risco Diário — Spark Batch ═══")
    spark = criar_spark_session()

    try:
        clientes, contas, transacoes = ler_dados(spark)
        logger.info("Clientes: %d | Contas: %d | Transações: %d",
                     clientes.count(), contas.count(), transacoes.count())

        scores = calcular_score_final(clientes, contas, transacoes)

        # Salvar histórico no ClickHouse
        salvar_no_clickhouse(scores, "risk_scores")

        # Atualizar score_risco na tabela de clientes (via temporária)
        (
            scores.select(
                F.col("id").alias("cliente_id"),
                "score_risco_calculado",
            )
            .write
            .format("jdbc")
            .option("url", f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}")
            .option("dbtable", "clientes_score_update")
            .option("user", PG_USER).option("password", PG_PASSWORD)
            .option("driver", "org.postgresql.Driver")
            .mode("overwrite")
            .save()
        )
        logger.info("Scores atualizados no PostgreSQL")

        logger.info("═══ Scoring de risco concluído com sucesso ═══")

    finally:
        spark.stop()


if __name__ == "__main__":
    executar_pipeline()
