"""
AUREUS Data Pipeline - Processamento de Transações
Pipeline Apache Spark para processar dados de transações em tempo real
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.streaming import StreamingContext
from pyspark.sql.streaming import DataStreamWriter
import json
import os
from datetime import datetime

class TransactionsProcessor:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("AUREUS-Transactions-Processor") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        
    def define_schema(self):
        """Define schema para transações"""
        return StructType([
            StructField("id", LongType(), True),
            StructField("conta_id", LongType(), True),
            StructField("tipo_transacao", StringType(), True),
            StructField("valor", DecimalType(19, 4), True),
            StructField("data_transacao", TimestampType(), True),
            StructField("status", StringType(), True),
            StructField("canal", StringType(), True),
            StructField("dispositivo", StringType(), True),
            StructField("ip_address", StringType(), True),
            StructField("user_agent", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("cidade", StringType(), True),
            StructField("estado", StringType(), True),
            StructField("pais", StringType(), True),
            StructField("score_risco", FloatType(), True),
            StructField("aprovada", BooleanType(), True),
            StructField("tempo_processamento_ms", IntegerType(), True),
            StructField("created_at", TimestampType(), True)
        ])
    
    def read_from_kafka(self, kafka_bootstrap_servers, topic):
        """Lê dados do Kafka"""
        return self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .load()
    
    def process_transactions(self, df):
        """Processa transações e calcula métricas"""
        
        # Converter JSON para DataFrame
        schema = self.define_schema()
        
        df_parsed = df.select(
            col("key").cast("string"),
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")
        
        # Adicionar colunas calculadas
        df_processed = df_parsed.withColumn(
            "hora", hour(col("data_transacao"))
        ).withColumn(
            "dia_semana", dayofweek(col("data_transacao"))
        ).withColumn(
            "mes", month(col("data_transacao"))
        ).withColumn(
            "ano", year(col("data_transacao"))
        ).withColumn(
            "valor_alto", when(col("valor") > 1000, 1).otherwise(0)
        ).withColumn(
            "horario_comercial", when(
                (col("hora") >= 9) & (col("hora") <= 18), 1
            ).otherwise(0)
        ).withColumn(
            "fim_de_semana", when(
                col("dia_semana").isin([1, 7]), 1
            ).otherwise(0)
        )
        
        return df_processed
    
    def calculate_metrics(self, df):
        """Calcula métricas agregadas"""
        
        # Métricas por hora
        metrics_hourly = df.groupBy(
            col("hora"),
            col("tipo_transacao"),
            col("status")
        ).agg(
            count("*").alias("total_transacoes"),
            sum("valor").alias("valor_total"),
            avg("valor").alias("valor_medio"),
            avg("score_risco").alias("score_risco_medio"),
            sum("aprovada").alias("transacoes_aprovadas"),
            sum("valor_alto").alias("transacoes_valor_alto")
        ).withColumn(
            "taxa_aprovacao", 
            col("transacoes_aprovadas") / col("total_transacoes")
        )
        
        # Métricas por localização
        metrics_location = df.groupBy(
            col("estado"),
            col("cidade"),
            col("tipo_transacao")
        ).agg(
            count("*").alias("total_transacoes"),
            sum("valor").alias("valor_total"),
            avg("score_risco").alias("score_risco_medio"),
            sum("aprovada").alias("transacoes_aprovadas")
        )
        
        # Métricas por conta
        metrics_account = df.groupBy("conta_id").agg(
            count("*").alias("total_transacoes"),
            sum("valor").alias("valor_total"),
            avg("valor").alias("valor_medio"),
            max("valor").alias("valor_maximo"),
            min("valor").alias("valor_minimo"),
            avg("score_risco").alias("score_risco_medio"),
            sum("aprovada").alias("transacoes_aprovadas"),
            sum("valor_alto").alias("transacoes_valor_alto")
        ).withColumn(
            "taxa_aprovacao",
            col("transacoes_aprovadas") / col("total_transacoes")
        )
        
        return {
            "hourly": metrics_hourly,
            "location": metrics_location,
            "account": metrics_account
        }
    
    def write_to_clickhouse(self, df, table_name):
        """Escreve dados no ClickHouse (credenciais via variaveis de ambiente)."""
        clickhouse_jdbc = os.environ.get(
            "CLICKHOUSE_JDBC_URL",
            "jdbc:clickhouse://clickhouse:8123/aurix_analytics"
        )
        clickhouse_user = os.environ.get("CLICKHOUSE_USER", "aurix")
        clickhouse_password = os.environ.get("CLICKHOUSE_PASSWORD", "aurix123")
        return df.writeStream \
            .format("jdbc") \
            .option("url", clickhouse_jdbc) \
            .option("dbtable", table_name) \
            .option("user", clickhouse_user) \
            .option("password", clickhouse_password) \
            .option("driver", "ru.yandex.clickhouse.ClickHouseDriver") \
            .option("checkpointLocation", "/tmp/checkpoint/" + table_name) \
            .trigger(processingTime="30 seconds") \
            .start()
    
    def write_to_elasticsearch(self, df, index_name):
        """Escreve dados no Elasticsearch (endereco via variavel de ambiente)."""
        es_nodes = os.environ.get("ELASTICSEARCH_NODES", "elasticsearch:9200")
        return df.writeStream \
            .format("es") \
            .option("es.nodes", es_nodes) \
            .option("es.resource", index_name) \
            .option("es.mapping.id", "id") \
            .option("checkpointLocation", "/tmp/checkpoint/" + index_name) \
            .trigger(processingTime="30 seconds") \
            .start()
    
    def run_streaming_pipeline(self, kafka_bootstrap_servers, topic):
        """Executa pipeline de streaming completo"""
        
        print("Iniciando pipeline de processamento de transações...")
        
        # Ler dados do Kafka
        kafka_df = self.read_from_kafka(kafka_bootstrap_servers, topic)
        
        # Processar transações
        processed_df = self.process_transactions(kafka_df)
        
        # Calcular métricas
        metrics = self.calculate_metrics(processed_df)
        
        # Escrever dados processados
        transactions_writer = self.write_to_clickhouse(
            processed_df, "transacoes_analytics"
        )
        
        # Escrever métricas
        hourly_writer = self.write_to_clickhouse(
            metrics["hourly"], "metricas_horarias"
        )
        
        location_writer = self.write_to_clickhouse(
            metrics["location"], "metricas_localizacao"
        )
        
        account_writer = self.write_to_clickhouse(
            metrics["account"], "metricas_conta"
        )
        
        # Escrever logs no Elasticsearch
        logs_writer = self.write_to_elasticsearch(
            processed_df, "aurix-transactions"
        )
        
        print("Pipeline iniciado. Aguardando dados...")
        writers = [
            transactions_writer,
            hourly_writer,
            location_writer,
            account_writer,
            logs_writer,
        ]

        # awaitAnyTermination bloqueia enquanto qualquer query de streaming estiver
        # ativa e retorna quando uma delas terminar (evita awaitTermination sequencial
        # inalcançavel e permite parar todas as writers no KeyboardInterrupt).
        try:
            self.spark.streams.awaitAnyTermination()
        except KeyboardInterrupt:
            print("Parando pipeline...")
            for writer in writers:
                writer.stop()
            self.spark.stop()
    
    def run_batch_processing(self, input_path, output_path):
        """Executa processamento em lote"""
        
        print("Iniciando processamento em lote...")
        
        # Ler dados
        df = self.spark.read.json(input_path, schema=self.define_schema())
        
        # Processar dados
        processed_df = self.process_transactions(df)
        
        # Calcular métricas
        metrics = self.calculate_metrics(processed_df)
        
        # Salvar resultados
        processed_df.write.mode("overwrite").parquet(f"{output_path}/transacoes")
        metrics["hourly"].write.mode("overwrite").parquet(f"{output_path}/metricas_horarias")
        metrics["location"].write.mode("overwrite").parquet(f"{output_path}/metricas_localizacao")
        metrics["account"].write.mode("overwrite").parquet(f"{output_path}/metricas_conta")
        
        print(f"Processamento concluído. Resultados salvos em {output_path}")

if __name__ == "__main__":
    processor = TransactionsProcessor()
    
    # Configurações via variáveis de ambiente
    KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    TOPIC = os.environ.get("KAFKA_TRANSACTIONS_TOPIC", "transacoes")
    
    # Executar pipeline de streaming
    processor.run_streaming_pipeline(KAFKA_BOOTSTRAP_SERVERS, TOPIC)
