"""
AUREUS Data Pipeline - Compliance e Auditoria de Dados
Sistema de compliance LGPD e auditoria de dados para o AUREUS Core Banking
"""

import pandas as pd
import numpy as np
import json
import hashlib
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import psycopg2
import clickhouse_connect
from dataclasses import dataclass
from enum import Enum
import re

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataClassification(Enum):
    """Classificação de dados conforme LGPD"""
    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"

class DataRetention(Enum):
    """Períodos de retenção de dados"""
    SHORT_TERM = 30  # 30 dias
    MEDIUM_TERM = 365  # 1 ano
    LONG_TERM = 2555  # 7 anos
    PERMANENT = -1  # Permanente

@dataclass
class DataSubject:
    """Representa um titular de dados"""
    id: str
    name: str
    email: str
    cpf: str
    phone: str
    address: str
    data_classification: DataClassification
    consent_given: bool
    consent_date: datetime
    data_retention: DataRetention

@dataclass
class DataProcessingRecord:
    """Registro de processamento de dados"""
    id: str
    data_subject_id: str
    processing_purpose: str
    legal_basis: str
    data_categories: List[str]
    processing_date: datetime
    processor: str
    retention_period: int
    anonymization_required: bool

class LGPDCompliance:
    """Sistema de compliance LGPD"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.postgres_conn = None
        self.clickhouse_conn = None
        self.data_subjects = {}
        self.processing_records = []
        
        # Mapeamento de dados sensíveis
        self.sensitive_data_patterns = {
            'cpf': r'\d{3}\.\d{3}\.\d{3}-\d{2}',
            'cnpj': r'\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}',
            'email': r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
            'phone': r'\(\d{2}\)\s\d{4,5}-\d{4}',
            'credit_card': r'\d{4}\s\d{4}\s\d{4}\s\d{4}',
            'bank_account': r'\d{4,5}-\d{1}',
            'rg': r'\d{1,2}\.\d{3}\.\d{3}-\d{1}'
        }
        
        # Bases legais para processamento
        self.legal_bases = {
            'consent': 'Consentimento do titular',
            'contract': 'Execução de contrato',
            'legal_obligation': 'Cumprimento de obrigação legal',
            'vital_interests': 'Proteção de interesses vitais',
            'public_interest': 'Interesse público',
            'legitimate_interest': 'Interesse legítimo'
        }
    
    def connect_databases(self):
        """Conecta aos bancos de dados"""
        try:
            # PostgreSQL
            self.postgres_conn = psycopg2.connect(
                host=self.config['postgres']['host'],
                port=self.config['postgres']['port'],
                database=self.config['postgres']['database'],
                user=self.config['postgres']['user'],
                password=self.config['postgres']['password']
            )
            
            # ClickHouse
            self.clickhouse_conn = clickhouse_connect.get_client(
                host=self.config['clickhouse']['host'],
                port=self.config['clickhouse']['port'],
                database=self.config['clickhouse']['database'],
                username=self.config['clickhouse']['user'],
                password=self.config['clickhouse']['password']
            )
            
            logger.info("Conectado aos bancos de dados com sucesso")
            
        except Exception as e:
            logger.error(f"Erro ao conectar aos bancos: {e}")
            raise
    
    def classify_data(self, data: Dict[str, Any]) -> DataClassification:
        """Classifica dados conforme LGPD"""
        # Verificar se contém dados sensíveis
        sensitive_count = 0
        for field, value in data.items():
            if isinstance(value, str):
                for pattern_name, pattern in self.sensitive_data_patterns.items():
                    if re.search(pattern, value):
                        sensitive_count += 1
                        break
        
        # Classificar baseado na quantidade de dados sensíveis
        if sensitive_count >= 3:
            return DataClassification.RESTRICTED
        elif sensitive_count >= 2:
            return DataClassification.CONFIDENTIAL
        elif sensitive_count >= 1:
            return DataClassification.INTERNAL
        else:
            return DataClassification.PUBLIC
    
    def anonymize_data(self, data: Dict[str, Any], fields_to_anonymize: List[str]) -> Dict[str, Any]:
        """Anonimiza dados sensíveis"""
        anonymized_data = data.copy()
        
        for field in fields_to_anonymize:
            if field in anonymized_data:
                value = str(anonymized_data[field])
                
                # Anonimizar CPF
                if field == 'cpf' and len(value) == 11:
                    anonymized_data[field] = f"***.***.***-{value[-2:]}"
                
                # Anonimizar email
                elif field == 'email' and '@' in value:
                    local, domain = value.split('@')
                    anonymized_data[field] = f"{local[:2]}***@{domain}"
                
                # Anonimizar telefone
                elif field == 'phone' and len(value) >= 10:
                    anonymized_data[field] = f"({value[:2]}) ***-{value[-4:]}"
                
                # Anonimizar nome
                elif field == 'name':
                    parts = value.split()
                    if len(parts) >= 2:
                        anonymized_data[field] = f"{parts[0]} {parts[-1][0]}***"
                    else:
                        anonymized_data[field] = f"{value[0]}***"
                
                # Hash para outros campos
                else:
                    anonymized_data[field] = hashlib.sha256(value.encode()).hexdigest()[:8]
        
        return anonymized_data
    
    def create_data_subject(self, data: Dict[str, Any]) -> DataSubject:
        """Cria um titular de dados"""
        data_classification = self.classify_data(data)
        
        # Determinar período de retenção baseado na classificação
        if data_classification == DataClassification.RESTRICTED:
            retention = DataRetention.LONG_TERM
        elif data_classification == DataClassification.CONFIDENTIAL:
            retention = DataRetention.MEDIUM_TERM
        else:
            retention = DataRetention.SHORT_TERM
        
        data_subject = DataSubject(
            id=hashlib.sha256(f"{data.get('cpf', '')}{data.get('email', '')}".encode()).hexdigest(),
            name=data.get('name', ''),
            email=data.get('email', ''),
            cpf=data.get('cpf', ''),
            phone=data.get('phone', ''),
            address=data.get('address', ''),
            data_classification=data_classification,
            consent_given=data.get('consent_given', False),
            consent_date=datetime.now(),
            data_retention=retention
        )
        
        self.data_subjects[data_subject.id] = data_subject
        return data_subject
    
    def record_data_processing(self, data_subject_id: str, processing_purpose: str, 
                             legal_basis: str, data_categories: List[str], 
                             processor: str) -> DataProcessingRecord:
        """Registra processamento de dados"""
        data_subject = self.data_subjects.get(data_subject_id)
        if not data_subject:
            raise ValueError(f"Titular de dados não encontrado: {data_subject_id}")
        
        processing_record = DataProcessingRecord(
            id=hashlib.sha256(f"{data_subject_id}{processing_purpose}{datetime.now()}".encode()).hexdigest(),
            data_subject_id=data_subject_id,
            processing_purpose=processing_purpose,
            legal_basis=legal_basis,
            data_categories=data_categories,
            processing_date=datetime.now(),
            processor=processor,
            retention_period=data_subject.data_retention.value,
            anonymization_required=data_subject.data_classification in [DataClassification.CONFIDENTIAL, DataClassification.RESTRICTED]
        )
        
        self.processing_records.append(processing_record)
        return processing_record
    
    def check_data_retention(self) -> List[Dict[str, Any]]:
        """Verifica dados que devem ser removidos por retenção"""
        expired_data = []
        current_date = datetime.now()
        
        for record in self.processing_records:
            if record.retention_period == -1:  # Permanente
                continue
            
            expiration_date = record.processing_date + timedelta(days=record.retention_period)
            
            if current_date > expiration_date:
                expired_data.append({
                    'record_id': record.id,
                    'data_subject_id': record.data_subject_id,
                    'processing_purpose': record.processing_purpose,
                    'expiration_date': expiration_date,
                    'days_overdue': (current_date - expiration_date).days
                })
        
        return expired_data
    
    def generate_data_inventory(self) -> Dict[str, Any]:
        """Gera inventário de dados"""
        inventory = {
            'total_data_subjects': len(self.data_subjects),
            'total_processing_records': len(self.processing_records),
            'classification_distribution': {},
            'retention_distribution': {},
            'legal_basis_distribution': {},
            'processing_purposes': set(),
            'data_categories': set()
        }
        
        # Distribuição por classificação
        for subject in self.data_subjects.values():
            classification = subject.data_classification.value
            inventory['classification_distribution'][classification] = inventory['classification_distribution'].get(classification, 0) + 1
        
        # Distribuição por retenção
        for subject in self.data_subjects.values():
            retention = subject.data_retention.value
            inventory['retention_distribution'][retention] = inventory['retention_distribution'].get(retention, 0) + 1
        
        # Distribuição por base legal
        for record in self.processing_records:
            legal_basis = record.legal_basis
            inventory['legal_basis_distribution'][legal_basis] = inventory['legal_basis_distribution'].get(legal_basis, 0) + 1
            inventory['processing_purposes'].add(record.processing_purpose)
            inventory['data_categories'].update(record.data_categories)
        
        # Converter sets para lists para serialização JSON
        inventory['processing_purposes'] = list(inventory['processing_purposes'])
        inventory['data_categories'] = list(inventory['data_categories'])
        
        return inventory
    
    def generate_compliance_report(self) -> Dict[str, Any]:
        """Gera relatório de compliance"""
        expired_data = self.check_data_retention()
        inventory = self.generate_data_inventory()
        
        report = {
            'report_date': datetime.now().isoformat(),
            'compliance_status': 'COMPLIANT' if len(expired_data) == 0 else 'NON_COMPLIANT',
            'data_inventory': inventory,
            'expired_data_count': len(expired_data),
            'expired_data': expired_data,
            'recommendations': self._generate_recommendations(expired_data, inventory)
        }
        
        return report
    
    def _generate_recommendations(self, expired_data: List[Dict], inventory: Dict) -> List[str]:
        """Gera recomendações de compliance"""
        recommendations = []
        
        if expired_data:
            recommendations.append(f"Remover {len(expired_data)} registros de dados expirados")
        
        if inventory['classification_distribution'].get('restricted', 0) > 100:
            recommendations.append("Considerar anonimização de dados restritos")
        
        if inventory['legal_basis_distribution'].get('consent', 0) < inventory['total_processing_records'] * 0.5:
            recommendations.append("Aumentar uso de consentimento explícito")
        
        if inventory['retention_distribution'].get(-1, 0) > inventory['total_data_subjects'] * 0.3:
            recommendations.append("Revisar política de retenção permanente")
        
        return recommendations

class DataAuditor:
    """Sistema de auditoria de dados"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.postgres_conn = None
        self.clickhouse_conn = None
        self.audit_log = []
    
    def connect_databases(self):
        """Conecta aos bancos de dados"""
        try:
            self.postgres_conn = psycopg2.connect(
                host=self.config['postgres']['host'],
                port=self.config['postgres']['port'],
                database=self.config['postgres']['database'],
                user=self.config['postgres']['user'],
                password=self.config['postgres']['password']
            )
            
            self.clickhouse_conn = clickhouse_connect.get_client(
                host=self.config['clickhouse']['host'],
                port=self.config['clickhouse']['port'],
                database=self.config['clickhouse']['database'],
                username=self.config['clickhouse']['user'],
                password=self.config['clickhouse']['password']
            )
            
            logger.info("Conectado aos bancos de dados para auditoria")
            
        except Exception as e:
            logger.error(f"Erro ao conectar para auditoria: {e}")
            raise
    
    def audit_data_access(self, user_id: str, table_name: str, operation: str, 
                         filters: Dict[str, Any] = None) -> Dict[str, Any]:
        """Audita acesso a dados"""
        audit_record = {
            'id': hashlib.sha256(f"{user_id}{table_name}{operation}{datetime.now()}".encode()).hexdigest(),
            'user_id': user_id,
            'table_name': table_name,
            'operation': operation,
            'filters': filters or {},
            'timestamp': datetime.now(),
            'ip_address': '127.0.0.1',  # Em produção, obter do contexto
            'user_agent': 'AUREUS-Audit-System',
            'success': True
        }
        
        self.audit_log.append(audit_record)
        return audit_record
    
    def audit_data_modification(self, user_id: str, table_name: str, 
                               old_data: Dict[str, Any], new_data: Dict[str, Any]) -> Dict[str, Any]:
        """Audita modificações de dados"""
        # Calcular diferenças
        changes = {}
        for key in set(old_data.keys()) | set(new_data.keys()):
            if old_data.get(key) != new_data.get(key):
                changes[key] = {
                    'old_value': old_data.get(key),
                    'new_value': new_data.get(key)
                }
        
        audit_record = {
            'id': hashlib.sha256(f"{user_id}{table_name}modify{datetime.now()}".encode()).hexdigest(),
            'user_id': user_id,
            'table_name': table_name,
            'operation': 'MODIFY',
            'changes': changes,
            'timestamp': datetime.now(),
            'ip_address': '127.0.0.1',
            'user_agent': 'AUREUS-Audit-System',
            'success': True
        }
        
        self.audit_log.append(audit_record)
        return audit_record
    
    def audit_data_deletion(self, user_id: str, table_name: str, 
                           deleted_data: Dict[str, Any]) -> Dict[str, Any]:
        """Audita exclusões de dados"""
        audit_record = {
            'id': hashlib.sha256(f"{user_id}{table_name}delete{datetime.now()}".encode()).hexdigest(),
            'user_id': user_id,
            'table_name': table_name,
            'operation': 'DELETE',
            'deleted_data': deleted_data,
            'timestamp': datetime.now(),
            'ip_address': '127.0.0.1',
            'user_agent': 'AUREUS-Audit-System',
            'success': True
        }
        
        self.audit_log.append(audit_record)
        return audit_record
    
    def generate_audit_report(self, start_date: datetime = None, 
                            end_date: datetime = None) -> Dict[str, Any]:
        """Gera relatório de auditoria"""
        if start_date is None:
            start_date = datetime.now() - timedelta(days=30)
        if end_date is None:
            end_date = datetime.now()
        
        # Filtrar registros por período
        filtered_logs = [
            log for log in self.audit_log
            if start_date <= log['timestamp'] <= end_date
        ]
        
        # Estatísticas
        total_operations = len(filtered_logs)
        operations_by_type = {}
        operations_by_user = {}
        operations_by_table = {}
        
        for log in filtered_logs:
            # Por tipo de operação
            op_type = log['operation']
            operations_by_type[op_type] = operations_by_type.get(op_type, 0) + 1
            
            # Por usuário
            user_id = log['user_id']
            operations_by_user[user_id] = operations_by_user.get(user_id, 0) + 1
            
            # Por tabela
            table_name = log['table_name']
            operations_by_table[table_name] = operations_by_table.get(table_name, 0) + 1
        
        report = {
            'report_period': {
                'start_date': start_date.isoformat(),
                'end_date': end_date.isoformat()
            },
            'total_operations': total_operations,
            'operations_by_type': operations_by_type,
            'operations_by_user': operations_by_user,
            'operations_by_table': operations_by_table,
            'audit_logs': filtered_logs
        }
        
        return report
    
    def check_data_lineage(self, table_name: str, record_id: str) -> List[Dict[str, Any]]:
        """Verifica linhagem de dados"""
        lineage = []
        
        # Buscar todas as operações relacionadas ao registro
        for log in self.audit_log:
            if (log['table_name'] == table_name and 
                (record_id in str(log.get('changes', {})) or 
                 record_id in str(log.get('deleted_data', {})))):
                lineage.append(log)
        
        return lineage

class BACENReportGenerator:
    """Gerador de relatórios para BACEN"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.postgres_conn = None
        self.clickhouse_conn = None
    
    def connect_databases(self):
        """Conecta aos bancos de dados"""
        try:
            self.postgres_conn = psycopg2.connect(
                host=self.config['postgres']['host'],
                port=self.config['postgres']['port'],
                database=self.config['postgres']['database'],
                user=self.config['postgres']['user'],
                password=self.config['postgres']['password']
            )
            
            self.clickhouse_conn = clickhouse_connect.get_client(
                host=self.config['clickhouse']['host'],
                port=self.config['clickhouse']['port'],
                database=self.config['clickhouse']['database'],
                username=self.config['clickhouse']['user'],
                password=self.config['clickhouse']['password']
            )
            
            logger.info("Conectado para geração de relatórios BACEN")
            
        except Exception as e:
            logger.error(f"Erro ao conectar para relatórios BACEN: {e}")
            raise
    
    def generate_estatisticas_mensais(self, year: int, month: int) -> Dict[str, Any]:
        """Gera relatório de estatísticas mensais a partir do ClickHouse"""
        try:
            query = f"""
            SELECT 
                tipo_transacao,
                count(*) as total_transacoes,
                sum(valor) as valor_total,
                avg(valor) as valor_medio,
                sum(case when status = 'APROVADA' then 1 else 0 end) as transacoes_aprovadas,
                sum(case when status = 'REJEITADA' then 1 else 0 end) as transacoes_rejeitadas
            FROM transacoes_analytics 
            WHERE toYear(data_transacao) = {year} AND toMonth(data_transacao) = {month}
            GROUP BY tipo_transacao
            """
            result = self.clickhouse_conn.query(query)
            results = result.result_rows

            report = {
                'periodo': f"{year}-{month:02d}",
                'data_geracao': datetime.now().isoformat(),
                'estatisticas_transacoes': []
            }
            for row in results:
                report['estatisticas_transacoes'].append({
                    'tipo_transacao': row[0],
                    'total_transacoes': row[1],
                    'valor_total': float(row[2]),
                    'valor_medio': float(row[3]),
                    'transacoes_aprovadas': row[4],
                    'transacoes_rejeitadas': row[5]
                })
            return report
        except Exception as e:
            logger.error(f"Erro ao gerar relatório de estatísticas mensais: {e}")
            raise
    
    def generate_relatorio_risco(self, year: int, month: int) -> Dict[str, Any]:
        """Gera relatório de risco a partir do ClickHouse"""
        try:
            query = f"""
            SELECT 
                tipo_evento,
                nivel_risco,
                count(*) as total_eventos,
                avg(score_risco) as score_risco_medio,
                sum(case when resolvido = 1 then 1 else 0 end) as eventos_resolvidos
            FROM eventos_risco_analytics 
            WHERE toYear(data_evento) = {year} AND toMonth(data_evento) = {month}
            GROUP BY tipo_evento, nivel_risco
            """
            result = self.clickhouse_conn.query(query)
            results = result.result_rows

            report = {
                'periodo': f"{year}-{month:02d}",
                'data_geracao': datetime.now().isoformat(),
                'eventos_risco': []
            }
            for row in results:
                report['eventos_risco'].append({
                    'tipo_evento': row[0],
                    'nivel_risco': row[1],
                    'total_eventos': row[2],
                    'score_risco_medio': float(row[3]),
                    'eventos_resolvidos': row[4]
                })
            return report
        except Exception as e:
            logger.error(f"Erro ao gerar relatório de risco: {e}")
            raise

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
    
    try:
        # Sistema de compliance LGPD
        lgpd = LGPDCompliance(config)
        lgpd.connect_databases()
        
        # Sistema de auditoria
        auditor = DataAuditor(config)
        auditor.connect_databases()
        
        # Gerador de relatórios BACEN
        bacen_reporter = BACENReportGenerator(config)
        bacen_reporter.connect_databases()
        
        # Exemplo de uso
        logger.info("Sistema de compliance e auditoria configurado com sucesso!")
        
        # Gerar relatório de compliance
        compliance_report = lgpd.generate_compliance_report()
        logger.info(f"Relatório de compliance gerado: {compliance_report['compliance_status']}")
        
        # Gerar relatório de auditoria
        audit_report = auditor.generate_audit_report()
        logger.info(f"Relatório de auditoria gerado: {audit_report['total_operations']} operações")
        
        # Gerar relatório BACEN
        bacen_report = bacen_reporter.generate_estatisticas_mensais(2024, 1)
        logger.info(f"Relatório BACEN gerado para {bacen_report['periodo']}")
        
    except Exception as e:
        logger.error(f"Erro no sistema de compliance: {e}")
        raise

if __name__ == "__main__":
    main()
