"""
Aurix Data Pipelines — Agregação de Transações (Spark Batch).

Processamento batch de transações com agregações por janela de 1, 7 e 30 dias.
Entrada: PostgreSQL (aurix_db.transacoes)
Saída: ClickHouse (aurix_analytics.transacoes_agregadas)

Uso:
    spark-submit --master yarn spark/batch/transaction_aggregator.py
    # ou local
    python spark/batch/transaction_aggregator.py
"""

import os
import logging
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("aurix.spark.batch.transaction_aggregator")

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

JANUAS_WINDOW = [1, 7, 30]  # dias


def criar_spark_session() -> SparkSession:
    """Cria a sessão Spark com configurações otimizadas."""
    return (
        SparkSession.builder
        .appName("Aurix-Batch-TransactionAggregator")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def ler_transacoes_postgres(spark: SparkSession) -> "DataFrame":
    """Lê transações do PostgreSQL."""
    logger.info("Lendo transações do PostgreSQL (%s:%s/%s)", PG_HOST, PG_PORT, PG_DB)

    df = (
        spark.read
        .format("jdbc")
        .option("url", f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}")
        .option("dbtable", "(SELECT * FROM transacoes WHERE status = 'AUTORIZADA') AS t")
        .option("user", PG_USER)
        .option("password", PG_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .option("fetchsize", "10000")
        .load()
    )

    logger.info("Transações carregadas: %d registros", df.count())
    return df


def agregar_por_janela(df, janela_dias: int) -> "DataFrame":
    """Agrega transações para uma janela de N dias.

    Métricas calculadas:
    - total_transacoes, valor_total, valor_medio, valor_maximo, valor_minimo
    - taxa_aprovacao, ticket_medio
    - transacoes por tipo, canal, hora
    """
    window_spec = Window.partitionBy("conta_id").orderBy(F.col("data_transacao").cast("timestamp"))

    data_corte = F.current_date() - F.lit(janela_dias)

    df_filtrado = df.filter(F.col("data_transacao").cast("timestamp") >= data_corte)

    agregado = (
        df_filtrado
        .groupBy(
            "conta_id",
            F.date_trunc("day", F.col("data_transacao").cast("timestamp")).alias("data_ref"),
        )
        .agg(
            F.count("*").alias("total_transacoes"),
            F.sum("valor").alias("valor_total"),
            F.avg("valor").alias("valor_medio"),
            F.max("valor").alias("valor_maximo"),
            F.min("valor").alias("valor_minimo"),
            F.stddev("valor").alias("valor_desvio_padrao"),
            F.countDistinct("id").alias("qtd_transacoes_unicas"),
            # Métricas por tipo
            F.sum(F.when(F.col("tipo_transacao") == "PIX", 1).otherwise(0)).alias("qtd_pix"),
            F.sum(F.when(F.col("tipo_transacao") == "TED", 1).otherwise(0)).alias("qtd_ted"),
            F.sum(F.when(F.col("tipo_transacao") == "DOC", 1).otherwise(0)).alias("qtd_doc"),
            F.sum(F.when(F.col("tipo_transacao") == "DEBITO", 1).otherwise(0)).alias("qtd_debito"),
            F.sum(F.when(F.col("tipo_transacao") == "CREDITO", 1).otherwise(0)).alias("qtd_credito"),
            # Métricas por canal
            F.sum(F.when(F.col("canal") == "MOBILE", 1).otherwise(0)).alias("qtd_mobile"),
            F.sum(F.when(F.col("canal") == "WEB", 1).otherwise(0)).alias("qtd_web"),
            F.sum(F.when(F.col("canal") == "AGENCIA", 1).otherwise(0)).alias("qtd_agencia"),
            # Horário
            F.avg(F.hour(F.col("data_transacao").cast("timestamp"))).alias("hora_media"),
            # Score de risco
            F.avg("score_risco").alias("score_risco_medio"),
            F.max("score_risco").alias("score_risco_maximo"),
            # Valor alto (> R$ 1.000)
            F.sum(F.when(F.col("valor") > 1000, 1).otherwise(0)).alias("qtd_valor_alto"),
        )
        .withColumn("janela_dias", F.lit(janela_dias))
        .withColumn("data_processamento", F.current_timestamp())
        .withColumn(
            "ticket_medio",
            F.when(F.col("total_transacoes") > 0,
                   F.col("valor_total") / F.col("total_transacoes")).otherwise(0),
        )
    )

    return agregado


def salvar_no_clickhouse(df, tabela: str):
    """Salva DataFrame no ClickHouse via JDBC."""
    logger.info("Salvando no ClickHouse: %s", tabela)

    (
        df.write
        .format("jdbc")
        .option("url", f"jdbc:clickhouse://{CH_HOST}:{CH_PORT}/{CH_DB}")
        .option("dbtable", tabela)
        .option("user", CH_USER)
        .option("password", CH_PASSWORD)
        .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
        .mode("overwrite")
        .save()
    )

    logger.info("Salvo: %s (%d registros)", tabela, df.count())


def executar_pipeline():
    """Pipeline principal de agregação de transações."""
    logger.info("═══ Agregação de Transações — Spark Batch ═══")
    spark = criar_spark_session()

    try:
        # Ler dados do PostgreSQL
        df_transacoes = ler_transacoes_postgres(spark)

        # Agregar para cada janela
        for janela in JANUAS_WINDOW:
            logger.info("Agregando janela de %d dias...", janela)
            df_agregado = agregar_por_janela(df_transacoes, janela)

            tabela = f"transacoes_agregadas_{janela}d"
            salvar_no_clickhouse(df_agregado, tabela)

        # Agregação global (todas as transações)
        df_global = (
            df_transacoes
            .groupBy(
                F.date_trunc("day", F.col("data_transacao").cast("timestamp")).alias("data_ref"),
            )
            .agg(
                F.count("*").alias("total_transacoes"),
                F.sum("valor").alias("valor_total"),
                F.avg("valor").alias("valor_medio"),
                F.countDistinct("conta_id").alias("contas_ativas"),
                F.avg("score_risco").alias("score_risco_medio"),
            )
            .withColumn("data_processamento", F.current_timestamp())
        )

        salvar_no_clickhouse(df_global, "transacoes_agregadas_global")

        logger.info("═══ Agregação concluída com sucesso ═══")

    finally:
        spark.stop()


if __name__ == "__main__":
    executar_pipeline()
