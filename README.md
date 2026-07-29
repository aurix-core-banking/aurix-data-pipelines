# AURIX Data Pipeline - Sistema Completo de Processamento de Dados

## Visao Geral

O AURIX Data Pipeline e um sistema completo de processamento de dados em tempo real e batch para o AURIX Core Banking Platform. Inclui componentes para analytics, machine learning, compliance LGPD e auditoria de dados.

## Arquitetura

- **PostgreSQL** (OLTP) -> **ClickHouse** (OLAP), **TimescaleDB** (Time-series)
- **Apache Kafka** (Streaming), **Elasticsearch** (Search), **Redis** (Cache)
- **Apache Spark** (Batch), **Apache Flink** (Streaming), **ML Pipeline**

## Componentes

1. **Apache Spark** - `spark/` - Processamento batch
2. **Apache Flink** - `flink/` - Processamento em tempo real
3. **Sincronizacao** - `sync/` - PostgreSQL e ClickHouse
4. **Analytics** - `analytics/` - Dashboards tempo real
5. **Compliance** - `compliance/` - LGPD e auditoria
6. **Machine Learning** - Ver `../ml/README.md`

## Instalacao

- Linux/Mac: `chmod +x scripts/*.sh` e `./scripts/start-data-pipeline.sh`
- Windows: `scripts\start-data-pipeline.bat`

## TimescaleDB

O pipeline de ingestão do TimescaleDB é implementado como um DAG do Airflow:

**Arquivo:** `airflow/dags/timescaledb_ingestion.py`

### Tasks

| Task | Descrição | Fonte | Destino |
|------|-----------|-------|---------|
| `ingest_transaction_metrics` | Consome mensagens do Kafka (`transacoes`), agrega por janela de 1 minuto | Kafka topic `transacoes` | Hypertable `metricas_transacoes` |
| `sync_system_metrics` | Coleta métricas do Prometheus e insere no TimescaleDB | Prometheus `/metrics` | Hypertable `metricas_sistema` |

Transações com campos obrigatórios ausentes são roteadas para o tópico DLQ `timescaledb_ingestion_dlq`.

### Variáveis de Ambiente

| Variável | Padrão | Descrição |
|----------|--------|-----------|
| `TIMESCALEDB_HOST` | `localhost` | Host do TimescaleDB |
| `TIMESCALEDB_PORT` | `5433` | Porta (5433 para não conflitar com PostgreSQL 5432) |
| `TIMESCALEDB_DB` | `aurix_timeseries` | Nome do banco |
| `TIMESCALEDB_USER` | `aurix` | Usuário |
| `TIMESCALEDB_PASSWORD` | *(obrigatório)* | Senha |

Configure via `cp data/platform/timescaledb/.env.example .env`.

### Execução

```bash
# Airflow precisa estar rodando com o DAGs folder apontando para este diretório
# O DAG é automaticamente descoberto pelo Airflow
airflow dags trigger timescaledb_ingestion
```

## Documentacao adicional

- [Documentacao tecnica](../docs/README.md)
- [Arquitetura](../docs/arquitetura/visao-geral.md)
- [Banco de dados](../docs/banco-dados/README.md)

---

**AURIX Core Banking Platform** - O padrao de excelencia financeira
