#!/bin/bash

# AUREUS Data Pipeline - Script de Inicialização
# Inicia todos os componentes do data-pipeline

set -e

echo "============================================="
echo "AUREUS Data Pipeline - Iniciando Sistema"
echo "============================================="

# Cores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Função para log
log() {
    echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1"
}

# Função para erro
error() {
    echo -e "${RED}[ERROR]${NC} $1"
    exit 1
}

# Função para sucesso
success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

# Função para warning
warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# Verificar se Docker está rodando
if ! docker info > /dev/null 2>&1; then
    error "Docker não está rodando. Por favor, inicie o Docker primeiro."
fi

# Verificar se Docker Compose está disponível
if ! command -v docker-compose > /dev/null 2>&1; then
    error "Docker Compose não está instalado."
fi

log "Verificando dependências..."

# Verificar se Python está instalado
if ! command -v python3 > /dev/null 2>&1; then
    error "Python 3 não está instalado."
fi

# Verificar se pip está instalado
if ! command -v pip3 > /dev/null 2>&1; then
    error "pip3 não está instalado."
fi

success "Dependências verificadas"

# Instalar dependências Python
log "Instalando dependências Python..."

# Spark
if [ -f "spark/requirements.txt" ]; then
    log "Instalando dependências do Spark..."
    pip3 install -r spark/requirements.txt
fi

# Flink
if [ -f "flink/requirements.txt" ]; then
    log "Instalando dependências do Flink..."
    pip3 install -r flink/requirements.txt
fi

# Analytics
if [ -f "analytics/requirements.txt" ]; then
    log "Instalando dependências do Analytics..."
    pip3 install -r analytics/requirements.txt
fi

# Compliance
if [ -f "compliance/requirements.txt" ]; then
    log "Instalando dependências do Compliance..."
    pip3 install -r compliance/requirements.txt
fi

success "Dependências Python instaladas"

# Iniciar serviços de dados
log "Iniciando serviços de dados..."

# ClickHouse
log "Iniciando ClickHouse..."
cd ../data-platform/clickhouse
docker-compose up -d

# Aguardar ClickHouse estar pronto
log "Aguardando ClickHouse estar pronto..."
sleep 30

# Verificar se ClickHouse está rodando
if ! curl -f http://localhost:8123/ping > /dev/null 2>&1; then
    warning "ClickHouse pode não estar pronto ainda. Continuando..."
fi

# TimescaleDB
log "Iniciando TimescaleDB..."
cd ../timescaledb
docker-compose up -d

# Aguardar TimescaleDB estar pronto
log "Aguardando TimescaleDB estar pronto..."
sleep 30

# Voltar ao diretório de data-pipelines
cd ../../data-pipelines

success "Serviços de dados iniciados"

# Iniciar pipeline de sincronização
log "Iniciando pipeline de sincronização PostgreSQL → ClickHouse..."

# Criar diretório de logs se não existir
mkdir -p logs

# Iniciar sincronização em background
nohup python3 sync/postgres_to_clickhouse.py > logs/sync.log 2>&1 &
SYNC_PID=$!

# Salvar PID para parar depois
echo $SYNC_PID > logs/sync.pid

success "Pipeline de sincronização iniciado (PID: $SYNC_PID)"

# Iniciar analytics em tempo real
log "Iniciando analytics em tempo real..."

# Iniciar analytics em background
nohup python3 analytics/real_time_analytics.py > logs/analytics.log 2>&1 &
ANALYTICS_PID=$!

# Salvar PID
echo $ANALYTICS_PID > logs/analytics.pid

success "Analytics em tempo real iniciado (PID: $ANALYTICS_PID)"

# Iniciar pipeline Spark
log "Iniciando pipeline Spark..."

# Iniciar Spark em background
nohup python3 spark/transactions_processor.py > logs/spark.log 2>&1 &
SPARK_PID=$!

# Salvar PID
echo $SPARK_PID > logs/spark.pid

success "Pipeline Spark iniciado (PID: $SPARK_PID)"

# Iniciar pipeline Flink
log "Iniciando pipeline Flink..."

# Iniciar Flink em background
nohup python3 flink/transactions_processor.py > logs/flink.log 2>&1 &
FLINK_PID=$!

# Salvar PID
echo $FLINK_PID > logs/flink.pid

success "Pipeline Flink iniciado (PID: $FLINK_PID)"

# Iniciar sistema de compliance
log "Iniciando sistema de compliance..."

# Iniciar compliance em background
nohup python3 compliance/data_compliance.py > logs/compliance.log 2>&1 &
COMPLIANCE_PID=$!

# Salvar PID
echo $COMPLIANCE_PID > logs/compliance.pid

success "Sistema de compliance iniciado (PID: $COMPLIANCE_PID)"

# Aguardar um pouco para os serviços estabilizarem
log "Aguardando serviços estabilizarem..."
sleep 10

# Verificar status dos serviços
log "Verificando status dos serviços..."

# ClickHouse
if curl -f http://localhost:8123/ping > /dev/null 2>&1; then
    success "ClickHouse: OK"
else
    warning "ClickHouse: Pode não estar pronto"
fi

# TimescaleDB
if pg_isready -h localhost -p 5433 > /dev/null 2>&1; then
    success "TimescaleDB: OK"
else
    warning "TimescaleDB: Pode não estar pronto"
fi

# Analytics Dashboard
if curl -f http://localhost:5000/api/health > /dev/null 2>&1; then
    success "Analytics Dashboard: OK"
else
    warning "Analytics Dashboard: Pode não estar pronto"
fi

# Mostrar informações de acesso
echo ""
echo "============================================="
echo "AUREUS Data Pipeline - SISTEMA INICIADO"
echo "============================================="
echo ""
echo "Serviços disponíveis:"
echo "  • ClickHouse: http://localhost:8123"
echo "  • TimescaleDB: localhost:5433"
echo "  • Analytics Dashboard: http://localhost:5000"
echo ""
echo "Logs disponíveis em:"
echo "  • Sincronização: logs/sync.log"
echo "  • Analytics: logs/analytics.log"
echo "  • Spark: logs/spark.log"
echo "  • Flink: logs/flink.log"
echo "  • Compliance: logs/compliance.log"
echo ""
echo "Para parar o sistema:"
echo "  ./stop-data-pipeline.sh"
echo ""
echo "============================================="

success "AUREUS Data Pipeline iniciado com sucesso!"
