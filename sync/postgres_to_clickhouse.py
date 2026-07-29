"""
AUREUS Data Pipeline - Sincronização PostgreSQL → ClickHouse
Pipeline para sincronizar dados do PostgreSQL para ClickHouse em tempo real
"""

import psycopg2
import clickhouse_connect
import pandas as pd
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any
import logging
import time
import threading
from queue import Queue
import schedule

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PostgresToClickHouseSync:
    """Sincronizador de dados PostgreSQL para ClickHouse"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.postgres_conn = None
        self.clickhouse_conn = None
        self.sync_queue = Queue()
        self.running = False
        
        # Configurações de conexão
        self.postgres_config = config['postgres']
        self.clickhouse_config = config['clickhouse']
        
        # Tabelas para sincronizar
        self.tables_to_sync = [
            'transacoes',
            'contas',
            'clientes',
            'solicitacoes_credito',
            'investimentos',
            'auditoria'
        ]
        
        # Mapeamento de tipos de dados
        self.type_mapping = {
            'bigint': 'Int64',
            'integer': 'Int32',
            'smallint': 'Int16',
            'decimal': 'Decimal64(4)',
            'numeric': 'Decimal64(4)',
            'real': 'Float32',
            'double precision': 'Float64',
            'varchar': 'String',
            'text': 'String',
            'timestamp': 'DateTime',
            'timestamp with time zone': 'DateTime',
            'date': 'Date',
            'boolean': 'UInt8',
            'jsonb': 'String'
        }
    
    def connect_postgres(self):
        """Conecta ao PostgreSQL"""
        try:
            self.postgres_conn = psycopg2.connect(
                host=self.postgres_config['host'],
                port=self.postgres_config['port'],
                database=self.postgres_config['database'],
                user=self.postgres_config['user'],
                password=self.postgres_config['password']
            )
            logger.info("Conectado ao PostgreSQL com sucesso")
        except Exception as e:
            logger.error(f"Erro ao conectar ao PostgreSQL: {e}")
            raise
    
    def connect_clickhouse(self):
        """Conecta ao ClickHouse"""
        try:
            self.clickhouse_conn = clickhouse_connect.get_client(
                host=self.clickhouse_config['host'],
                port=self.clickhouse_config['port'],
                database=self.clickhouse_config['database'],
                username=self.clickhouse_config['user'],
                password=self.clickhouse_config['password']
            )
            logger.info("Conectado ao ClickHouse com sucesso")
        except Exception as e:
            logger.error(f"Erro ao conectar ao ClickHouse: {e}")
            raise
    
    def get_table_schema(self, table_name: str) -> Dict[str, str]:
        """Obtém schema da tabela PostgreSQL"""
        query = """
        SELECT 
            column_name,
            data_type,
            is_nullable,
            column_default
        FROM information_schema.columns 
        WHERE table_name = %s 
        AND table_schema = 'aurix'
        ORDER BY ordinal_position
        """
        
        with self.postgres_conn.cursor() as cursor:
            cursor.execute(query, (table_name,))
            columns = cursor.fetchall()
        
        schema = {}
        for col_name, data_type, is_nullable, default in columns:
            clickhouse_type = self.type_mapping.get(data_type, 'String')
            if is_nullable == 'NO':
                clickhouse_type = clickhouse_type.replace('String', 'String NOT NULL')
            schema[col_name] = clickhouse_type
        
        return schema
    
    def create_clickhouse_table(self, table_name: str, schema: Dict[str, str]):
        """Cria tabela no ClickHouse baseada no schema PostgreSQL"""
        columns = []
        for col_name, col_type in schema.items():
            columns.append(f"    {col_name} {col_type}")
        
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name}_analytics (
            {',\\n'.join(columns)},
            created_at DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY (id, created_at)
        SETTINGS index_granularity = 8192
        """
        
        try:
            self.clickhouse_conn.command(create_table_sql)
            logger.info(f"Tabela {table_name}_analytics criada no ClickHouse")
        except Exception as e:
            logger.error(f"Erro ao criar tabela {table_name}_analytics: {e}")
            raise
    
    def sync_table(self, table_name: str, last_sync_time: datetime = None):
        """Sincroniza uma tabela específica"""
        if last_sync_time is None:
            last_sync_time = datetime.now() - timedelta(hours=1)
        
        try:
            # Obter dados do PostgreSQL
            query = f"""
            SELECT * FROM aurix.{table_name} 
            WHERE data_atualizacao > %s
            ORDER BY data_atualizacao
            """
            
            with self.postgres_conn.cursor() as cursor:
                cursor.execute(query, (last_sync_time,))
                rows = cursor.fetchall()
                
                if not rows:
                    logger.info(f"Nenhum dado novo para sincronizar na tabela {table_name}")
                    return
                
                # Obter nomes das colunas
                column_names = [desc[0] for desc in cursor.description]
                
                # Converter para DataFrame
                df = pd.DataFrame(rows, columns=column_names)
                
                # Converter tipos de dados
                df = self._convert_data_types(df, table_name)
                
                # Inserir no ClickHouse
                self._insert_to_clickhouse(table_name, df)
                
                logger.info(f"Sincronizados {len(df)} registros da tabela {table_name}")
                
        except Exception as e:
            logger.error(f"Erro ao sincronizar tabela {table_name}: {e}")
            raise
    
    def _convert_data_types(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """Converte tipos de dados para ClickHouse"""
        df = df.copy()
        
        # Converter timestamps
        timestamp_columns = ['data_transacao', 'data_atualizacao', 'data_criacao', 'created_at']
        for col in timestamp_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Converter valores decimais
        decimal_columns = ['valor', 'saldo', 'limite_credito', 'taxa_juros']
        for col in decimal_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        # Converter booleanos
        boolean_columns = ['aprovada', 'ativa', 'bloqueada']
        for col in boolean_columns:
            if col in df.columns:
                df[col] = df[col].astype(bool).astype(int)
        
        # Converter JSONB para string
        jsonb_columns = ['dados_extras', 'configuracoes', 'detalhes']
        for col in jsonb_columns:
            if col in df.columns:
                df[col] = df[col].astype(str)
        
        return df
    
    def _insert_to_clickhouse(self, table_name: str, df: pd.DataFrame):
        """Insere dados no ClickHouse"""
        try:
            # Preparar dados para inserção
            data = df.to_dict('records')
            
            # Inserir em lotes
            batch_size = 1000
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                self.clickhouse_conn.insert(
                    f"{table_name}_analytics",
                    batch
                )
                
        except Exception as e:
            logger.error(f"Erro ao inserir dados no ClickHouse: {e}")
            raise
    
    def sync_all_tables(self):
        """Sincroniza todas as tabelas"""
        logger.info("Iniciando sincronização de todas as tabelas...")
        
        for table_name in self.tables_to_sync:
            try:
                self.sync_table(table_name)
            except Exception as e:
                logger.error(f"Erro ao sincronizar {table_name}: {e}")
                continue
        
        logger.info("Sincronização concluída")
    
    def setup_initial_sync(self):
        """Configura sincronização inicial"""
        logger.info("Configurando sincronização inicial...")
        
        # Conectar aos bancos
        self.connect_postgres()
        self.connect_clickhouse()
        
        # Criar tabelas no ClickHouse
        for table_name in self.tables_to_sync:
            try:
                schema = self.get_table_schema(table_name)
                self.create_clickhouse_table(table_name, schema)
            except Exception as e:
                logger.error(f"Erro ao criar tabela {table_name}: {e}")
                continue
        
        # Sincronizar dados iniciais
        self.sync_all_tables()
    
    def start_realtime_sync(self):
        """Inicia sincronização em tempo real"""
        logger.info("Iniciando sincronização em tempo real...")
        
        self.running = True
        
        # Thread para processar fila de sincronização
        def process_sync_queue():
            while self.running:
                try:
                    if not self.sync_queue.empty():
                        table_name = self.sync_queue.get()
                        self.sync_table(table_name)
                        self.sync_queue.task_done()
                    else:
                        time.sleep(1)
                except Exception as e:
                    logger.error(f"Erro no processamento da fila: {e}")
        
        # Iniciar thread de processamento
        sync_thread = threading.Thread(target=process_sync_queue)
        sync_thread.daemon = True
        sync_thread.start()
        
        # Agendar sincronização periódica
        schedule.every(5).minutes.do(self.sync_all_tables)
        
        # Loop principal
        try:
            while self.running:
                schedule.run_pending()
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Parando sincronização...")
            self.running = False
    
    def stop_sync(self):
        """Para a sincronização"""
        self.running = False
        logger.info("Sincronização parada")
    
    def get_sync_status(self) -> Dict[str, Any]:
        """Obtém status da sincronização"""
        status = {
            'running': self.running,
            'queue_size': self.sync_queue.qsize(),
            'last_sync': datetime.now().isoformat(),
            'tables_synced': len(self.tables_to_sync)
        }
        return status

class DataQualityChecker:
    """Verificador de qualidade dos dados sincronizados"""
    
    def __init__(self, postgres_conn, clickhouse_conn):
        self.postgres_conn = postgres_conn
        self.clickhouse_conn = clickhouse_conn
    
    def check_data_consistency(self, table_name: str) -> Dict[str, Any]:
        """Verifica consistência dos dados entre PostgreSQL e ClickHouse"""
        try:
            # Contar registros no PostgreSQL
            with self.postgres_conn.cursor() as cursor:
                cursor.execute(f"SELECT COUNT(*) FROM aurix.{table_name}")
                postgres_count = cursor.fetchone()[0]
            
            # Contar registros no ClickHouse
            clickhouse_count = self.clickhouse_conn.query(f"SELECT COUNT(*) FROM {table_name}_analytics").result_rows[0][0]
            
            # Verificar diferença
            difference = abs(postgres_count - clickhouse_count)
            consistency_percentage = (1 - difference / max(postgres_count, 1)) * 100
            
            return {
                'table_name': table_name,
                'postgres_count': postgres_count,
                'clickhouse_count': clickhouse_count,
                'difference': difference,
                'consistency_percentage': consistency_percentage,
                'is_consistent': consistency_percentage > 95
            }
            
        except Exception as e:
            logger.error(f"Erro ao verificar consistência da tabela {table_name}: {e}")
            return {
                'table_name': table_name,
                'error': str(e),
                'is_consistent': False
            }
    
    def check_all_tables(self) -> List[Dict[str, Any]]:
        """Verifica consistência de todas as tabelas"""
        results = []
        tables = ['transacoes', 'contas', 'clientes', 'solicitacoes_credito', 'investimentos']
        
        for table in tables:
            result = self.check_data_consistency(table)
            results.append(result)
        
        return results

def main():
    """Função principal"""
    # Configuração
    config = {
        'postgres': {
            'host': 'localhost',
            'port': 5432,
            'database': 'aurix',
            'user': 'aurix',
            'password': 'aurix123'
        },
        'clickhouse': {
            'host': 'localhost',
            'port': 8123,
            'database': 'aurix_analytics',
            'user': 'aurix',
            'password': 'aurix123'
        }
    }
    
    # Criar sincronizador
    sync = PostgresToClickHouseSync(config)
    
    try:
        # Configurar sincronização inicial
        sync.setup_initial_sync()
        
        # Iniciar sincronização em tempo real
        sync.start_realtime_sync()
        
    except KeyboardInterrupt:
        logger.info("Parando sincronização...")
        sync.stop_sync()
    except Exception as e:
        logger.error(f"Erro na sincronização: {e}")
        raise

if __name__ == "__main__":
    main()
