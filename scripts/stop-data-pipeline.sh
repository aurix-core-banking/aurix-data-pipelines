#!/bin/bash

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log() { echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1"; }
success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_PIPELINES_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
LOGS_DIR="$DATA_PIPELINES_DIR/logs"
PLATFORM_DIR="$(cd "$DATA_PIPELINES_DIR/../data-platform" && pwd)"

cd "$DATA_PIPELINES_DIR"

log "Parando processos do AUREUS Data Pipeline..."

stop_pid() {
    local name=$1
    local pid_file="$LOGS_DIR/$name.pid"
    if [ -f "$pid_file" ]; then
        local pid=$(cat "$pid_file")
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
            sleep 1
            if kill -0 "$pid" 2>/dev/null; then
                kill -9 "$pid" 2>/dev/null || true
            fi
            success "Processo $name (PID $pid) encerrado"
        else
            warning "Processo $name (PID $pid) já não está em execução"
        fi
        rm -f "$pid_file"
    else
        warning "Arquivo de PID não encontrado: $name"
    fi
}

stop_pid "sync"
stop_pid "analytics"
stop_pid "spark"
stop_pid "flink"
stop_pid "compliance"

log "Parando serviços de dados (Docker)..."

if [ -d "$PLATFORM_DIR/clickhouse" ]; then
    log "Parando ClickHouse..."
    cd "$PLATFORM_DIR/clickhouse"
    docker-compose down 2>/dev/null || true
    success "ClickHouse parado"
fi

if [ -d "$PLATFORM_DIR/timescaledb" ]; then
    log "Parando TimescaleDB..."
    cd "$PLATFORM_DIR/timescaledb"
    docker-compose down 2>/dev/null || true
    success "TimescaleDB parado"
fi

success "AUREUS Data Pipeline encerrado."
