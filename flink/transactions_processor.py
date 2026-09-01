"""
AUREUS Data Pipeline - Processamento de Transações com Apache Flink
Pipeline Flink para processamento de dados de transações em tempo real
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, TableResult
from pyflink.datastream.functions import MapFunction
import json
import os
from datetime import datetime
from typing import List

class TransactionProcessor(MapFunction):
    """Processador de transações para Flink"""
    
    def map(self, value: str) -> str:
        """Processa uma transação individual"""
        try:
            # Parse JSON
            transaction = json.loads(value)
            
            # Adicionar campos calculados
            transaction['processed_at'] = datetime.now().isoformat()
            transaction['hour'] = datetime.fromisoformat(transaction['data_transacao']).hour
            transaction['day_of_week'] = datetime.fromisoformat(transaction['data_transacao']).weekday()
            transaction['is_high_value'] = 1 if transaction['valor'] > 1000 else 0
            transaction['is_business_hours'] = 1 if 9 <= transaction['hour'] <= 18 else 0
            transaction['is_weekend'] = 1 if transaction['day_of_week'] in [5, 6] else 0
            
            # Calcular score de risco básico
            risk_score = 0.0
            if transaction['valor'] > 5000:
                risk_score += 0.3
            if transaction['hour'] < 6 or transaction['hour'] > 22:
                risk_score += 0.2
            if transaction['is_weekend']:
                risk_score += 0.1
            if transaction['canal'] == 'MOBILE':
                risk_score += 0.1
            
            transaction['calculated_risk_score'] = min(risk_score, 1.0)
            
            return json.dumps(transaction)
        except Exception as e:
            # Log error and return original value
            print(f"Error processing transaction: {e}")
            return value

class FlinkTransactionsProcessor:
    """Processador principal de transações com Flink"""
    
    def __init__(self):
        self.env = StreamExecutionEnvironment.get_execution_environment()
        self.env.set_parallelism(4)

        self.table_env = StreamTableEnvironment.create(self.env)

        # Configuracao externalizada via variaveis de ambiente (Secrets)
        self.kafka_bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        self.clickhouse_url = os.environ.get(
            "CLICKHOUSE_URL", "clickhouse://clickhouse:8123"
        )
        self.elasticsearch_hosts = os.environ.get(
            "ELASTICSEARCH_HOSTS", "http://elasticsearch:9200"
        )
        self.kafka_props = {
            'bootstrap.servers': self.kafka_bootstrap,
            'group.id': 'aurix-flink-processor',
            'auto.offset.reset': 'latest'
        }
    
    def setup_kafka_sources(self):
        """Configura fontes e sinks Kafka via DDL SQL (PyFlink >= 1.14)"""
        # Tabela de origem: tópico de transações
        self.table_env.execute_sql(f"""
            CREATE TABLE IF NOT EXISTS kafka_transactions_source (
                id BIGINT,
                conta_id BIGINT,
                tipo_transacao STRING,
                valor DECIMAL(19,4),
                data_transacao STRING,
                status STRING,
                canal STRING,
                score_risco FLOAT
            ) WITH (
                'connector' = 'kafka',
                'topic' = 'transacoes',
                'properties.bootstrap.servers' = '{self.kafka_bootstrap}',
                'properties.group.id' = 'aurix-flink-processor',
                'properties.auto.offset.reset' = 'latest',
                'format' = 'json'
            )
        """)

        # Tabela de sink: tópico de alertas de risco
        self.table_env.execute_sql(f"""
            CREATE TABLE IF NOT EXISTS kafka_alerts_sink (
                alert_id STRING,
                account_id BIGINT,
                transaction_id BIGINT,
                risk_score FLOAT,
                alert_type STRING,
                description STRING,
                timestamp STRING,
                recommended_action STRING
            ) WITH (
                'connector' = 'kafka',
                'topic' = 'alertas_risco',
                'properties.bootstrap.servers' = '{self.kafka_bootstrap}',
                'format' = 'json'
            )
        """)

        # Tabela de sink: tópico de métricas em tempo real
        self.table_env.execute_sql(f"""
            CREATE TABLE IF NOT EXISTS kafka_metrics_sink (
                metric_type STRING,
                key STRING,
                count BIGINT,
                total_value DECIMAL(19,4),
                avg_value DECIMAL(19,4),
                max_value DECIMAL(19,4),
                timestamp STRING
            ) WITH (
                'connector' = 'kafka',
                'topic' = 'metricas_tempo_real',
                'properties.bootstrap.servers' = '{self.kafka_bootstrap}',
                'format' = 'json'
            )
        """)
    
    def _create_sinks(self):
        """Cria as tabelas de destino (ClickHouse e Elasticsearch) via DDL."""
        # Tabela de sink: ClickHouse
        self.table_env.execute_sql(f"""
            CREATE TABLE IF NOT EXISTS clickhouse_transactions_sink (
                id BIGINT,
                conta_id BIGINT,
                tipo_transacao STRING,
                valor DECIMAL(19,4),
                data_transacao TIMESTAMP(3),
                status STRING,
                canal STRING,
                score_risco FLOAT,
                calculated_risk_score FLOAT,
                processed_at STRING
            ) WITH (
                'connector' = 'clickhouse',
                'url' = '{self.clickhouse_url}',
                'database-name' = 'aurix_analytics',
                'table-name' = 'transacoes_analytics'
            )
        """)

        # Tabela de sink: Elasticsearch
        self.table_env.execute_sql(f"""
            CREATE TABLE IF NOT EXISTS elasticsearch_transactions_sink (
                id BIGINT,
                conta_id BIGINT,
                tipo_transacao STRING,
                valor DECIMAL(19,4),
                data_transacao TIMESTAMP(3),
                status STRING,
                canal STRING,
                score_risco FLOAT,
                processed_at STRING
            ) WITH (
                'connector' = 'elasticsearch-7',
                'hosts' = '{self.elasticsearch_hosts}',
                'index' = 'aurix-transactions'
            )
        """)

    def create_processing_pipeline(self) -> List[TableResult]:
        """Cria o pipeline de streaming usando somente Table API/SQL.

        Cada INSERT abaixo retorna um TableResult (job de streaming) em vez de
        codigo morto; os writes são executados como sinks de streaming a partir
        da fonte Kafka (kafka_transactions_source).
        """
        self._create_sinks()
        results: List[TableResult] = []

        # Gravar transações processadas no ClickHouse (streaming)
        results.append(self.table_env.execute_sql(
            """
            INSERT INTO clickhouse_transactions_sink
            SELECT
                id,
                conta_id,
                tipo_transacao,
                valor,
                CAST(data_transacao AS TIMESTAMP(3)) AS data_transacao,
                status,
                canal,
                score_risco,
                score_risco AS calculated_risk_score,
                CAST(CURRENT_TIMESTAMP AS STRING) AS processed_at
            FROM kafka_transactions_source
            """
        ))

        # Gravar transações processadas no Elasticsearch (streaming)
        results.append(self.table_env.execute_sql(
            """
            INSERT INTO elasticsearch_transactions_sink
            SELECT
                id,
                conta_id,
                tipo_transacao,
                valor,
                CAST(data_transacao AS TIMESTAMP(3)) AS data_transacao,
                status,
                canal,
                score_risco,
                CAST(CURRENT_TIMESTAMP AS STRING) AS processed_at
            FROM kafka_transactions_source
            """
        ))

        return results
    
    def create_windowed_analytics(self) -> TableResult:
        """Cria analytics com janelas temporais"""
        # Configurar tabela de transações
        self.table_env.execute_sql(f"""
            CREATE TABLE transactions (
                id BIGINT,
                conta_id BIGINT,
                tipo_transacao STRING,
                valor DECIMAL(19,4),
                data_transacao TIMESTAMP(3),
                status STRING,
                canal STRING,
                score_risco FLOAT,
                WATERMARK FOR data_transacao AS data_transacao - INTERVAL '5' SECOND
            ) WITH (
                'connector' = 'kafka',
                'topic' = 'transacoes',
                'properties.bootstrap.servers' = '{self.kafka_bootstrap}',
                'properties.group.id' = 'aurix-flink-analytics',
                'format' = 'json'
            )
        """)
        
        # Tabela de destino: metricas horarias no ClickHouse
        self.table_env.execute_sql(f"""
            CREATE TABLE IF NOT EXISTS hourly_analytics_sink (
                window_start TIMESTAMP(3),
                window_end TIMESTAMP(3),
                tipo_transacao STRING,
                status STRING,
                total_transacoes BIGINT,
                valor_total DECIMAL(19,4),
                valor_medio DECIMAL(19,4),
                score_risco_medio FLOAT
            ) WITH (
                'connector' = 'clickhouse',
                'url' = '{self.clickhouse_url}',
                'database-name' = 'aurix_analytics',
                'table-name' = 'metricas_horarias'
            )
        """)

        # Salvar analytics diretamente como job de streaming (sink ClickHouse)
        return self.table_env.execute_sql("""
            INSERT INTO hourly_analytics_sink
            SELECT
                TUMBLE_START(data_transacao, INTERVAL '1' HOUR) as window_start,
                TUMBLE_END(data_transacao, INTERVAL '1' HOUR) as window_end,
                tipo_transacao,
                status,
                COUNT(*) as total_transacoes,
                SUM(valor) as valor_total,
                AVG(valor) as valor_medio,
                AVG(score_risco) as score_risco_medio
            FROM transactions
            GROUP BY TUMBLE(data_transacao, INTERVAL '1' HOUR), tipo_transacao, status
        """)
    
    def run(self):
        """Executa o pipeline completo"""
        print("Iniciando pipeline Flink de processamento de transações...")
        
        # Configurar fontes
        self.setup_kafka_sources()
        
        # Criar pipeline de processing + analytics (jobs de streaming)
        jobs: List[TableResult] = []
        jobs.extend(self.create_processing_pipeline())
        jobs.append(self.create_windowed_analytics())
        
        # Aguardar os jobs rodarem (bloqueia enquanto o streaming estiver ativo)
        for job in jobs:
            job.wait()

if __name__ == "__main__":
    processor = FlinkTransactionsProcessor()
    processor.run()
