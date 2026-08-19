"""
Aurix Data Pipelines — Perfil Consolidado do Cliente (Spark Batch).

Processamento batch para gerar perfil consolidado de cada cliente.
Entrada: PostgreSQL (clientes, contas, transacoes)
Saída: Feature Store (Feast) + ClickHouse

Uso:
    spark-submit --master yarn spark/batch/customer_profile.py
    # ou local
    python spark/batch/customer_profile.py
"""

import os
import logging
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("aurix.spark.batch.customer_profile")

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


def criar_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("Aurix-Batch-CustomerProfile")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def ler_clientes(spark: SparkSession):
    """Lê dados de clientes do PostgreSQL."""
    return (
        spark.read
        .format("jdbc")
        .option("url", f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}")
        .option("dbtable", "clientes")
        .option("user", PG_USER)
        .option("password", PG_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
    )


def ler_contas(spark: SparkSession):
    """Lê dados de contas do PostgreSQL."""
    return (
        spark.read
        .format("jdbc")
        .option("url", f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}")
        .option("dbtable", "contas")
        .option("user", PG_USER)
        .option("password", PG_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
    )


def ler_transacoes(spark: SparkSession, dias: int = 90):
    """Lê transações dos últimos N dias."""
    return (
        spark.read
        .format("jdbc")
        .option("url", f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}")
        .option("dbtable", f"""
            (SELECT * FROM transacoes
             WHERE data_transacao >= CURRENT_DATE - INTERVAL '{dias} days'
               AND status = 'AUTORIZADA') AS t
        """)
        .option("user", PG_USER)
        .option("password", PG_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
    )


def construir_perfil_cliente(clientes, contas, transacoes) -> "DataFrame":
    """Constrói perfil consolidado do cliente.

    Métricas incluídas:
    - Dados demográficos e de conta
    - Comportamento transacional (30d, 60d, 90d)
    - Score de risco consolidado
    - Flags de produto (empréstimo, cartão, investimento)
    """
    # Perfil base dos clientes
    perfil_base = (
        clientes
        .withColumn("idade", F.datediff(F.current_date(), F.col("data_nascimento")) / 365.25)
        .withColumn("tempo_como_cliente_dias", F.datediff(F.current_date(), F.col("created_at")))
        .select(
            "id",
            "cpf",
            "nome",
            "email",
            F.col("idade").cast("int").alias("idade"),
            F.col("tempo_como_cliente_dias").cast("int").alias("tempo_como_cliente_dias"),
            "score_risco",
            "status",
            F.col("created_at").alias("data_cadastro"),
        )
    )

    # Agregação de contas
    agg_contas = (
        contas
        .groupBy("cliente_id")
        .agg(
            F.count("*").alias("qtd_contas"),
            F.sum("saldo").alias("saldo_total"),
            F.avg("saldo").alias("saldo_medio"),
            F.sum("limite").alias("limite_total"),
            F.sum(F.when(F.col("status") == "ATIVA", 1).otherwise(0)).alias("contas_ativas"),
            F.sum(F.when(F.col("tipo_conta") == "CORRENTE", 1).otherwise(0)).alias("tem_conta_corrente"),
            F.sum(F.when(F.col("tipo_conta") == "POUPANCA", 1).otherwise(0)).alias("tem_poupanca"),
            F.sum(F.when(F.col("tipo_conta") == "INVESTIMENTO", 1).otherwise(0)).alias("tem_investimento"),
        )
        .withColumn(
            "utilizacao_limite_pct",
            F.when(F.col("limite_total") > 0,
                   F.col("saldo_total") / F.col("limite_total") * 100).otherwise(0),
        )
    )

    # Agregação de transações — perfil comportamental
    window_30d = F.col("data_transacao").cast("timestamp") >= F.current_date() - F.lit(30)
    window_60d = F.col("data_transacao").cast("timestamp") >= F.current_date() - F.lit(60)
    window_90d = F.col("data_transacao").cast("timestamp") >= F.current_date() - F.lit(90)

    agg_transacoes = (
        transacoes
        .groupBy("conta_id")
        .agg(
            # 30 dias
            F.sum(F.when(window_30d, 1).otherwise(0)).alias("transacoes_30d"),
            F.sum(F.when(window_30d, F.col("valor")).otherwise(0)).alias("volume_30d"),
            F.avg(F.when(window_30d, F.col("valor"))).alias("ticket_medio_30d"),
            F.countDistinct(F.when(window_30d, F.col("id"))).alias("qtd_transacoes_30d"),
            # 90 dias
            F.sum(F.when(window_90d, 1).otherwise(0)).alias("transacoes_90d"),
            F.sum(F.when(window_90d, F.col("valor")).otherwise(0)).alias("volume_90d"),
            F.avg(F.when(window_90d, F.col("valor"))).alias("ticket_medio_90d"),
            # Frequência
            F.avg(F.hour(F.col("data_transacao").cast("timestamp"))).alias("hora_media_transacoes"),
            F.avg(F.dayofweek(F.col("data_transacao").cast("timestamp"))).alias("dia_semana_medio"),
            # PIX
            F.sum(F.when(
                (window_30d) & (F.col("tipo_transacao") == "PIX"), 1
            ).otherwise(0)).alias("pix_30d"),
            # Risco
            F.avg("score_risco").alias("score_risco_medio_transacoes"),
            F.max("score_risco").alias("score_risco_max_transacoes"),
            # Diversidade
            F.countDistinct("tipo_transacao").alias("qtd_tipos_transacao"),
            F.countDistinct("canal").alias("qtd_canais_utilizados"),
            # Última transação
            F.max("data_transacao").alias("ultima_transacao"),
        )
        .withColumn(
            "frequencia_semanal",
            F.when(F.col("transacoes_90d") > 0,
                   F.col("transacoes_90d") / 13.0).otherwise(0),  # ~13 semanas em 90 dias
        )
        .withColumn(
            "dias_desde_ultima_transacao",
            F.datediff(F.current_date(), F.col("ultima_transacao")),
        )
    )

    # Join: perfil_base + contas + transações
    perfil_consolidado = (
        perfil_base
        .join(agg_contas, perfil_base["id"] == agg_contas["cliente_id"], "left")
        .join(agg_transacoes, agg_contas["conta_id"] == agg_transacoes["conta_id"], "left")
        .drop(agg_contas["cliente_id"])
        .drop(agg_transacoes["conta_id"])
        .withColumn("data_processamento", F.current_timestamp())
        .withColumn(
            "classe_risco",
            F.when(F.col("score_risco") >= 800, "A")
            .when(F.col("score_risco") >= 600, "B")
            .when(F.col("score_risco") >= 400, "C")
            .otherwise("D"),
        )
        .withColumn(
            "perfil_ativo",
            F.when(F.col("dias_desde_ultima_transacao") <= 30, True).otherwise(False),
        )
    )

    return perfil_consolidado


def salvar_no_clickhouse(df, tabela: str):
    """Salva DataFrame no ClickHouse via JDBC."""
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
    logger.info("Salvo no ClickHouse: %s", tabela)


def executar_pipeline():
    """Pipeline principal de perfil de cliente."""
    logger.info("═══ Perfil Consolidado do Cliente — Spark Batch ═══")
    spark = criar_spark_session()

    try:
        clientes = ler_clientes(spark)
        contas = ler_contas(spark)
        transacoes = ler_transacoes(spark, dias=90)

        logger.info("Clientes: %d | Contas: %d | Transações (90d): %d",
                     clientes.count(), contas.count(), transacoes.count())

        perfil = construir_perfil_cliente(clientes, contas, transacoes)

        # Salvar no ClickHouse para analytics
        salvar_no_clickhouse(perfil, "perfil_cliente_consolidado")

        # Salvar como Parquet para Feature Store
        (
            perfil.write
            .mode("overwrite")
            .parquet("/mnt/data/feature-store/customer_profile/")
        )
        logger.info("Perfil salvo como Parquet para Feature Store")

        logger.info("═══ Perfil consolidado concluído com sucesso ═══")

    finally:
        spark.stop()


if __name__ == "__main__":
    executar_pipeline()
