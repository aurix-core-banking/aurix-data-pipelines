"""
AUREUS Data Pipeline - Processamento de Transações com Apache Flink
Pipeline Flink para processamento de dados de transações em tempo real
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.datastream.functions import MapFunction, KeyedProcessFunction
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.time import Duration
import json
from datetime import datetime
from typing import Any, Dict, List

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

class RiskAnalyzer(KeyedProcessFunction):
    """Analisador de risco para transações"""
    
    def __init__(self):
        self.risk_threshold = 0.7
        self.suspicious_accounts = set()
    
    def process_element(self, value: str, ctx: 'KeyedProcessFunction.Context') -> None:
        """Processa elemento e detecta riscos"""
        try:
            transaction = json.loads(value)
            account_id = transaction['conta_id']
            
            # Verificar se é transação suspeita
            if transaction['calculated_risk_score'] > self.risk_threshold:
                # Adicionar à lista de contas suspeitas
                self.suspicious_accounts.add(account_id)
                
                # Emitir alerta
                alert = {
                    'alert_id': f"risk_{account_id}_{int(datetime.now().timestamp())}",
                    'account_id': account_id,
                    'transaction_id': transaction['id'],
                    'risk_score': transaction['calculated_risk_score'],
                    'alert_type': 'HIGH_RISK_TRANSACTION',
                    'description': f'Transação de alto risco detectada: R$ {transaction["valor"]}',
                    'timestamp': datetime.now().isoformat(),
                    'recommended_action': 'REVIEW_MANUAL'
                }
                
                # Enviar para tópico de alertas
                ctx.output(json.dumps(alert))
            
            # Verificar padrões suspeitos
            if self._detect_suspicious_pattern(transaction):
                pattern_alert = {
                    'alert_id': f"pattern_{account_id}_{int(datetime.now().timestamp())}",
                    'account_id': account_id,
                    'transaction_id': transaction['id'],
                    'alert_type': 'SUSPICIOUS_PATTERN',
                    'description': 'Padrão suspeito detectado na transação',
                    'timestamp': datetime.now().isoformat(),
                    'recommended_action': 'INVESTIGATE'
                }
                ctx.output(json.dumps(pattern_alert))
                
        except Exception as e:
            print(f"Error in risk analysis: {e}")
    
    def _detect_suspicious_pattern(self, transaction: Dict) -> bool:
        """Detecta padrões suspeitos na transação"""
        # Múltiplas transações em horário não comercial
        if transaction['hour'] < 6 or transaction['hour'] > 22:
            return True
        
        # Valores redondos suspeitos
        if transaction['valor'] % 1000 == 0 and transaction['valor'] > 10000:
            return True
        
        # Transações em finais de semana com valores altos
        if transaction['is_weekend'] and transaction['valor'] > 5000:
            return True
        
        return False

class MetricsAggregator(KeyedProcessFunction):
    """Agregador de métricas em tempo real"""
    
    def __init__(self):
        self.metrics = {}
    
    def process_element(self, value: str, ctx: 'KeyedProcessFunction.Context') -> None:
        """Processa elemento e agrega métricas"""
        try:
            transaction = json.loads(value)
            key = f"{transaction['tipo_transacao']}_{transaction['status']}"
            
            if key not in self.metrics:
                self.metrics[key] = {
                    'count': 0,
                    'total_value': 0.0,
                    'avg_value': 0.0,
                    'max_value': 0.0,
                    'min_value': float('inf'),
                    'last_update': datetime.now().isoformat()
                }
            
            # Atualizar métricas
            self.metrics[key]['count'] += 1
            self.metrics[key]['total_value'] += transaction['valor']
            self.metrics[key]['avg_value'] = self.metrics[key]['total_value'] / self.metrics[key]['count']
            self.metrics[key]['max_value'] = max(self.metrics[key]['max_value'], transaction['valor'])
            self.metrics[key]['min_value'] = min(self.metrics[key]['min_value'], transaction['valor'])
            self.metrics[key]['last_update'] = datetime.now().isoformat()
            
            # Emitir métricas a cada 100 transações
            if self.metrics[key]['count'] % 100 == 0:
                metric_output = {
                    'metric_type': 'TRANSACTION_AGGREGATE',
                    'key': key,
                    'metrics': self.metrics[key],
                    'timestamp': datetime.now().isoformat()
                }
                ctx.output(json.dumps(metric_output))
                
        except Exception as e:
            print(f"Error in metrics aggregation: {e}")

class FlinkTransactionsProcessor:
    """Processador principal de transações com Flink"""
    
    def __init__(self):
        self.env = StreamExecutionEnvironment.get_execution_environment()
        self.env.set_parallelism(4)
        
        self.table_env = StreamTableEnvironment.create(self.env)
        
        # Configurar Kafka
        self.kafka_props = {
            'bootstrap.servers': 'kafka:9092',
            'group.id': 'aurix-flink-processor',
            'auto.offset.reset': 'latest'
        }
    
    def setup_kafka_sources(self):
        """Configura fontes e sinks Kafka via DDL SQL (PyFlink >= 1.14)"""
        # Tabela de origem: tópico de transações
        self.table_env.execute_sql("""
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
                'properties.bootstrap.servers' = 'kafka:9092',
                'properties.group.id' = 'aurix-flink-processor',
                'properties.auto.offset.reset' = 'latest',
                'format' = 'json'
            )
        """)

        # Tabela de sink: tópico de alertas de risco
        self.table_env.execute_sql("""
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
                'properties.bootstrap.servers' = 'kafka:9092',
                'format' = 'json'
            )
        """)

        # Tabela de sink: tópico de métricas em tempo real
        self.table_env.execute_sql("""
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
                'properties.bootstrap.servers' = 'kafka:9092',
                'format' = 'json'
            )
        """)
    
    def create_processing_pipeline(self):
        """Cria pipeline de processamento usando DataStream API com fonte DDL"""
        self._save_to_clickhouse()
        self._save_to_elasticsearch()
        return self.table_env \
            .process(MetricsAggregator())

        # Salvar transações processadas no ClickHouse
        self._save_to_clickhouse(processed_stream)

        # Salvar no Elasticsearch para busca
        self._save_to_elasticsearch(processed_stream)
    
    def _save_to_clickhouse(self, stream):
        """Salva dados processados no ClickHouse"""
        # Registrar stream como tabela temporária
        ch_table = self.table_env.from_data_stream(
            stream.map(lambda x: json.loads(x)),
            DataTypes.ROW([
                DataTypes.FIELD("id", DataTypes.BIGINT()),
                DataTypes.FIELD("conta_id", DataTypes.BIGINT()),
                DataTypes.FIELD("tipo_transacao", DataTypes.STRING()),
                DataTypes.FIELD("valor", DataTypes.DECIMAL(19, 4)),
                DataTypes.FIELD("data_transacao", DataTypes.TIMESTAMP(3)),
                DataTypes.FIELD("status", DataTypes.STRING()),
                DataTypes.FIELD("canal", DataTypes.STRING()),
                DataTypes.FIELD("score_risco", DataTypes.FLOAT()),
                DataTypes.FIELD("calculated_risk_score", DataTypes.FLOAT()),
                DataTypes.FIELD("processed_at", DataTypes.STRING())
            ])
        )
        self.table_env.create_temporary_view("clickhouse_temp", ch_table)

        # Criar tabela ClickHouse via DDL
        self.table_env.execute_sql("""
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
                'url' = 'clickhouse://clickhouse:8123',
                'database-name' = 'aurix_analytics',
                'table-name' = 'transacoes_analytics'
            )
        """)

        # Inserir via SQL
        self.table_env.execute_sql("INSERT INTO clickhouse_transactions_sink SELECT * FROM clickhouse_temp")
    
    def _save_to_elasticsearch(self, stream):
        """Salva dados no Elasticsearch"""
        # Registrar stream como tabela temporária
        es_table = self.table_env.from_data_stream(
            stream.map(lambda x: json.loads(x)),
            DataTypes.ROW([
                DataTypes.FIELD("id", DataTypes.BIGINT()),
                DataTypes.FIELD("conta_id", DataTypes.BIGINT()),
                DataTypes.FIELD("tipo_transacao", DataTypes.STRING()),
                DataTypes.FIELD("valor", DataTypes.DECIMAL(19, 4)),
                DataTypes.FIELD("data_transacao", DataTypes.TIMESTAMP(3)),
                DataTypes.FIELD("status", DataTypes.STRING()),
                DataTypes.FIELD("canal", DataTypes.STRING()),
                DataTypes.FIELD("score_risco", DataTypes.FLOAT()),
                DataTypes.FIELD("processed_at", DataTypes.STRING())
            ])
        )
        self.table_env.create_temporary_view("elasticsearch_temp", es_table)

        # Criar tabela Elasticsearch via DDL
        self.table_env.execute_sql("""
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
                'hosts' = 'http://elasticsearch:9200',
                'index' = 'aurix-transactions'
            )
        """)

        # Inserir via SQL
        self.table_env.execute_sql("INSERT INTO elasticsearch_transactions_sink SELECT * FROM elasticsearch_temp")
    
    def create_windowed_analytics(self):
        """Cria analytics com janelas temporais"""
        # Configurar tabela de transações
        self.table_env.execute_sql("""
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
                'properties.bootstrap.servers' = 'kafka:9092',
                'properties.group.id' = 'aurix-flink-analytics',
                'format' = 'json'
            )
        """)
        
        # Analytics por hora
        hourly_analytics = self.table_env.execute_sql("""
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
        
        # Salvar analytics no ClickHouse
        self.table_env.execute_sql("""
            CREATE TABLE hourly_analytics_sink (
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
                'url' = 'clickhouse://clickhouse:8123',
                'database-name' = 'aurix_analytics',
                'table-name' = 'metricas_horarias'
            )
        """)
        
        hourly_analytics.insert_into("hourly_analytics_sink")
    
    def run(self):
        """Executa o pipeline completo"""
        print("Iniciando pipeline Flink de processamento de transações...")
        
        # Configurar fontes
        self.setup_kafka_sources()
        
        # Criar pipeline de processamento
        self.create_processing_pipeline()
        
        # Criar analytics com janelas
        self.create_windowed_analytics()
        
        # Executar pipeline
        self.env.execute("AUREUS-Transactions-Flink-Processor")

if __name__ == "__main__":
    processor = FlinkTransactionsProcessor()
    processor.run()
