"""
AUREUS Data Pipeline - Analytics em Tempo Real
Sistema de analytics e dashboards em tempo real para o AUREUS Core Banking
"""

import pandas as pd
import numpy as np
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import logging
import threading
from queue import Queue
import websocket
import redis
import clickhouse_connect
from flask import Flask, jsonify, render_template
from flask_socketio import SocketIO, emit
import plotly.graph_objs as go
import plotly.utils

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class RealTimeAnalytics:
    """Sistema de analytics em tempo real"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.redis_client = None
        self.clickhouse_client = None
        self.analytics_queue = Queue()
        self.running = False
        
        # Métricas em tempo real
        self.metrics = {
            'transacoes_por_minuto': 0,
            'valor_total_por_minuto': 0,
            'taxa_aprovacao': 0,
            'score_risco_medio': 0,
            'transacoes_por_canal': {},
            'transacoes_por_tipo': {},
            'transacoes_por_estado': {},
            'alertas_risco': 0,
            'tempo_resposta_medio': 0,
            'erros_por_minuto': 0
        }
        
        # Histórico de métricas
        self.metrics_history = {
            'timestamp': [],
            'transacoes_por_minuto': [],
            'valor_total_por_minuto': [],
            'taxa_aprovacao': [],
            'score_risco_medio': []
        }
    
    def connect_redis(self):
        """Conecta ao Redis"""
        try:
            self.redis_client = redis.Redis(
                host=self.config['redis']['host'],
                port=self.config['redis']['port'],
                password=self.config['redis']['password'],
                decode_responses=True
            )
            logger.info("Conectado ao Redis com sucesso")
        except Exception as e:
            logger.error(f"Erro ao conectar ao Redis: {e}")
            raise
    
    def connect_clickhouse(self):
        """Conecta ao ClickHouse"""
        try:
            self.clickhouse_client = clickhouse_connect.get_client(
                host=self.config['clickhouse']['host'],
                port=self.config['clickhouse']['port'],
                database=self.config['clickhouse']['database'],
                username=self.config['clickhouse']['user'],
                password=self.config['clickhouse']['password']
            )
            logger.info("Conectado ao ClickHouse com sucesso")
        except Exception as e:
            logger.error(f"Erro ao conectar ao ClickHouse: {e}")
            raise
    
    def calculate_transaction_metrics(self, time_window: int = 60) -> Dict[str, Any]:
        """Calcula métricas de transações em tempo real"""
        try:
            # Query para métricas dos últimos minutos
            query = f"""
            SELECT 
                count(*) as total_transacoes,
                sum(valor) as valor_total,
                avg(score_risco) as score_risco_medio,
                sum(case when status = 'APROVADA' then 1 else 0 end) as transacoes_aprovadas,
                avg(tempo_processamento_ms) as tempo_resposta_medio,
                canal,
                tipo_transacao,
                estado
            FROM transacoes_analytics 
            WHERE data_transacao >= now() - toIntervalMinute({time_window})
            GROUP BY canal, tipo_transacao, estado
            """
            
            result = self.clickhouse_client.query(query)
            df = pd.DataFrame(result.result_rows, columns=[
                'total_transacoes', 'valor_total', 'score_risco_medio', 
                'transacoes_aprovadas', 'tempo_resposta_medio', 
                'canal', 'tipo_transacao', 'estado'
            ])
            
            if df.empty:
                return self.metrics
            
            # Calcular métricas agregadas
            total_transacoes = df['total_transacoes'].sum()
            valor_total = df['valor_total'].sum()
            taxa_aprovacao = df['transacoes_aprovadas'].sum() / total_transacoes if total_transacoes > 0 else 0
            score_risco_medio = df['score_risco_medio'].mean()
            tempo_resposta_medio = df['tempo_resposta_medio'].mean()
            
            # Métricas por canal
            transacoes_por_canal = df.groupby('canal')['total_transacoes'].sum().to_dict()
            
            # Métricas por tipo
            transacoes_por_tipo = df.groupby('tipo_transacao')['total_transacoes'].sum().to_dict()
            
            # Métricas por estado
            transacoes_por_estado = df.groupby('estado')['total_transacoes'].sum().to_dict()
            
            # Atualizar métricas
            self.metrics.update({
                'transacoes_por_minuto': total_transacoes,
                'valor_total_por_minuto': valor_total,
                'taxa_aprovacao': taxa_aprovacao,
                'score_risco_medio': score_risco_medio,
                'transacoes_por_canal': transacoes_por_canal,
                'transacoes_por_tipo': transacoes_por_tipo,
                'transacoes_por_estado': transacoes_por_estado,
                'tempo_resposta_medio': tempo_resposta_medio
            })
            
            # Adicionar ao histórico
            self._add_to_history()
            
            return self.metrics
            
        except Exception as e:
            logger.error(f"Erro ao calcular métricas de transações: {e}")
            return self.metrics
    
    def calculate_risk_metrics(self) -> Dict[str, Any]:
        """Calcula métricas de risco em tempo real"""
        try:
            # Query para alertas de risco
            query = """
            SELECT 
                count(*) as total_alertas,
                avg(score_risco) as score_risco_medio,
                count(case when nivel_risco = 'ALTO' then 1 end) as alertas_alto_risco,
                count(case when nivel_risco = 'MEDIO' then 1 end) as alertas_medio_risco,
                count(case when nivel_risco = 'BAIXO' then 1 end) as alertas_baixo_risco
            FROM eventos_risco_analytics 
            WHERE data_evento >= now() - toIntervalHour(1)
            """
            
            result = self.clickhouse_client.query(query)
            df = pd.DataFrame(result.result_rows, columns=[
                'total_alertas', 'score_risco_medio', 'alertas_alto_risco',
                'alertas_medio_risco', 'alertas_baixo_risco'
            ])
            
            if not df.empty:
                self.metrics['alertas_risco'] = df['total_alertas'].iloc[0]
                self.metrics['score_risco_medio'] = df['score_risco_medio'].iloc[0]
            
            return self.metrics
            
        except Exception as e:
            logger.error(f"Erro ao calcular métricas de risco: {e}")
            return self.metrics
    
    def calculate_system_metrics(self) -> Dict[str, Any]:
        """Calcula métricas do sistema em tempo real"""
        try:
            # Query para métricas de performance
            query = """
            SELECT 
                avg(tempo_resposta_ms) as tempo_resposta_medio,
                count(case when status_code >= 400 then 1 end) as erros_total,
                count(*) as requests_total
            FROM metricas_performance 
            WHERE timestamp >= now() - toIntervalMinute(1)
            """
            
            result = self.clickhouse_client.query(query)
            df = pd.DataFrame(result.result_rows, columns=[
                'tempo_resposta_medio', 'erros_total', 'requests_total'
            ])
            
            if not df.empty:
                self.metrics['tempo_resposta_medio'] = df['tempo_resposta_medio'].iloc[0]
                self.metrics['erros_por_minuto'] = df['erros_total'].iloc[0]
            
            return self.metrics
            
        except Exception as e:
            logger.error(f"Erro ao calcular métricas do sistema: {e}")
            return self.metrics
    
    def _add_to_history(self):
        """Adiciona métricas ao histórico"""
        now = datetime.now()
        self.metrics_history['timestamp'].append(now)
        self.metrics_history['transacoes_por_minuto'].append(self.metrics['transacoes_por_minuto'])
        self.metrics_history['valor_total_por_minuto'].append(self.metrics['valor_total_por_minuto'])
        self.metrics_history['taxa_aprovacao'].append(self.metrics['taxa_aprovacao'])
        self.metrics_history['score_risco_medio'].append(self.metrics['score_risco_medio'])
        
        # Manter apenas últimas 100 entradas
        if len(self.metrics_history['timestamp']) > 100:
            for key in self.metrics_history:
                self.metrics_history[key] = self.metrics_history[key][-100:]
    
    def generate_charts(self) -> Dict[str, str]:
        """Gera gráficos para dashboards"""
        charts = {}
        
        try:
            # Gráfico de transações por minuto
            fig_transacoes = go.Figure()
            fig_transacoes.add_trace(go.Scatter(
                x=self.metrics_history['timestamp'],
                y=self.metrics_history['transacoes_por_minuto'],
                mode='lines+markers',
                name='Transações por Minuto',
                line=dict(color='#1E40AF', width=2)
            ))
            fig_transacoes.update_layout(
                title='Transações por Minuto',
                xaxis_title='Tempo',
                yaxis_title='Número de Transações',
                template='plotly_white'
            )
            charts['transacoes_por_minuto'] = json.dumps(fig_transacoes, cls=plotly.utils.PlotlyJSONEncoder)
            
            # Gráfico de valor total por minuto
            fig_valor = go.Figure()
            fig_valor.add_trace(go.Scatter(
                x=self.metrics_history['timestamp'],
                y=self.metrics_history['valor_total_por_minuto'],
                mode='lines+markers',
                name='Valor Total por Minuto',
                line=dict(color='#059669', width=2)
            ))
            fig_valor.update_layout(
                title='Valor Total por Minuto',
                xaxis_title='Tempo',
                yaxis_title='Valor (R$)',
                template='plotly_white'
            )
            charts['valor_total_por_minuto'] = json.dumps(fig_valor, cls=plotly.utils.PlotlyJSONEncoder)
            
            # Gráfico de taxa de aprovação
            fig_aprovacao = go.Figure()
            fig_aprovacao.add_trace(go.Scatter(
                x=self.metrics_history['timestamp'],
                y=self.metrics_history['taxa_aprovacao'],
                mode='lines+markers',
                name='Taxa de Aprovação',
                line=dict(color='#F59E0B', width=2)
            ))
            fig_aprovacao.update_layout(
                title='Taxa de Aprovação',
                xaxis_title='Tempo',
                yaxis_title='Taxa (%)',
                template='plotly_white'
            )
            charts['taxa_aprovacao'] = json.dumps(fig_aprovacao, cls=plotly.utils.PlotlyJSONEncoder)
            
            # Gráfico de score de risco
            fig_risco = go.Figure()
            fig_risco.add_trace(go.Scatter(
                x=self.metrics_history['timestamp'],
                y=self.metrics_history['score_risco_medio'],
                mode='lines+markers',
                name='Score de Risco Médio',
                line=dict(color='#DC2626', width=2)
            ))
            fig_risco.update_layout(
                title='Score de Risco Médio',
                xaxis_title='Tempo',
                yaxis_title='Score de Risco',
                template='plotly_white'
            )
            charts['score_risco_medio'] = json.dumps(fig_risco, cls=plotly.utils.PlotlyJSONEncoder)
            
            # Gráfico de pizza - transações por canal
            fig_canal = go.Figure(data=[go.Pie(
                labels=list(self.metrics['transacoes_por_canal'].keys()),
                values=list(self.metrics['transacoes_por_canal'].values()),
                hole=0.3
            )])
            fig_canal.update_layout(
                title='Distribuição por Canal',
                template='plotly_white'
            )
            charts['transacoes_por_canal'] = json.dumps(fig_canal, cls=plotly.utils.PlotlyJSONEncoder)
            
            # Gráfico de barras - transações por tipo
            fig_tipo = go.Figure(data=[go.Bar(
                x=list(self.metrics['transacoes_por_tipo'].keys()),
                y=list(self.metrics['transacoes_por_tipo'].values()),
                marker_color='#1E40AF'
            )])
            fig_tipo.update_layout(
                title='Transações por Tipo',
                xaxis_title='Tipo de Transação',
                yaxis_title='Número de Transações',
                template='plotly_white'
            )
            charts['transacoes_por_tipo'] = json.dumps(fig_tipo, cls=plotly.utils.PlotlyJSONEncoder)
            
        except Exception as e:
            logger.error(f"Erro ao gerar gráficos: {e}")
        
        return charts
    
    def start_analytics_engine(self):
        """Inicia o motor de analytics"""
        logger.info("Iniciando motor de analytics em tempo real...")
        
        self.running = True
        
        def analytics_loop():
            while self.running:
                try:
                    # Calcular métricas
                    self.calculate_transaction_metrics()
                    self.calculate_risk_metrics()
                    self.calculate_system_metrics()
                    
                    # Salvar métricas no Redis
                    self._save_metrics_to_redis()
                    
                    # Aguardar próxima iteração
                    time.sleep(60)  # Atualizar a cada minuto
                    
                except Exception as e:
                    logger.error(f"Erro no loop de analytics: {e}")
                    time.sleep(10)
        
        # Iniciar thread de analytics
        analytics_thread = threading.Thread(target=analytics_loop)
        analytics_thread.daemon = True
        analytics_thread.start()
    
    def _save_metrics_to_redis(self):
        """Salva métricas no Redis"""
        try:
            if self.redis_client:
                # Salvar métricas atuais
                self.redis_client.set('aurix:metrics:current', json.dumps(self.metrics))
                
                # Salvar histórico
                self.redis_client.set('aurix:metrics:history', json.dumps(self.metrics_history))
                
                # Salvar timestamp da última atualização
                self.redis_client.set('aurix:metrics:last_update', datetime.now().isoformat())
                
        except Exception as e:
            logger.error(f"Erro ao salvar métricas no Redis: {e}")
    
    def get_metrics(self) -> Dict[str, Any]:
        """Obtém métricas atuais"""
        return self.metrics
    
    def get_metrics_history(self) -> Dict[str, Any]:
        """Obtém histórico de métricas"""
        return self.metrics_history
    
    def get_charts(self) -> Dict[str, str]:
        """Obtém gráficos para dashboards"""
        return self.generate_charts()
    
    def stop_analytics(self):
        """Para o motor de analytics"""
        self.running = False
        logger.info("Motor de analytics parado")

class RealTimeDashboard:
    """Dashboard em tempo real"""
    
    def __init__(self, analytics: RealTimeAnalytics):
        self.analytics = analytics
        self.app = Flask(__name__)
        self.socketio = SocketIO(self.app, cors_allowed_origins="*")
        self.setup_routes()
        self.setup_socketio()
    
    def setup_routes(self):
        """Configura rotas da API"""
        
        @self.app.route('/')
        def index():
            return render_template('dashboard.html')
        
        @self.app.route('/api/metrics')
        def get_metrics():
            return jsonify(self.analytics.get_metrics())
        
        @self.app.route('/api/metrics/history')
        def get_metrics_history():
            return jsonify(self.analytics.get_metrics_history())
        
        @self.app.route('/api/charts')
        def get_charts():
            return jsonify(self.analytics.get_charts())
        
        @self.app.route('/api/health')
        def health():
            return jsonify({
                'status': 'healthy',
                'timestamp': datetime.now().isoformat(),
                'metrics_count': len(self.analytics.get_metrics())
            })
    
    def setup_socketio(self):
        """Configura WebSocket para atualizações em tempo real"""
        
        @self.socketio.on('connect')
        def handle_connect():
            logger.info('Cliente conectado ao dashboard')
            emit('connected', {'status': 'connected'})
        
        @self.socketio.on('disconnect')
        def handle_disconnect():
            logger.info('Cliente desconectado do dashboard')
        
        @self.socketio.on('request_metrics')
        def handle_request_metrics():
            metrics = self.analytics.get_metrics()
            emit('metrics_update', metrics)
        
        @self.socketio.on('request_charts')
        def handle_request_charts():
            charts = self.analytics.get_charts()
            emit('charts_update', charts)
    
    def start_dashboard(self, host='0.0.0.0', port=5000):
        """Inicia o dashboard"""
        logger.info(f"Iniciando dashboard em http://{host}:{port}")
        self.socketio.run(self.app, host=host, port=port, debug=False)

def main():
    """Função principal"""
    # Configuração
    config = {
        'redis': {
            'host': 'localhost',
            'port': 6379,
            'password': 'redis123'
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
        # Criar sistema de analytics
        analytics = RealTimeAnalytics(config)
        
        # Conectar aos serviços
        analytics.connect_redis()
        analytics.connect_clickhouse()
        
        # Iniciar motor de analytics
        analytics.start_analytics_engine()
        
        # Criar e iniciar dashboard
        dashboard = RealTimeDashboard(analytics)
        dashboard.start_dashboard()
        
    except KeyboardInterrupt:
        logger.info("Parando sistema de analytics...")
        analytics.stop_analytics()
    except Exception as e:
        logger.error(f"Erro no sistema de analytics: {e}")
        raise

if __name__ == "__main__":
    main()
