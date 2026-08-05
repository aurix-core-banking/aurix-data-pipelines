# Aurix Data Pipelines

Pipelines ETL e streaming da plataforma Aurix.

## Visão Geral

Pipelines que processam transações financeiras, agregam métricas e alimentam a
camada analítica (ClickHouse) a partir do core banking (PostgreSQL).

## Stack

- Apache Spark / PySpark
- Apache Kafka
- Airflow (orquestração)
- Python

## Orquestração (Airflow)

As DAGs ficam em `airflow/dags/` e são testadas via `pytest` (dagbag) em
`airflow/tests/`. Dependências em `airflow/requirements.txt`.

### Pipeline diário

| DAG | Função | Horário |
|---|---|---|
| `ingest_postgres_to_bronze` | Ingestão incremental PostgreSQL → Bronze (watermark) | 00:00 |
| `ingest_kafka_to_bronze` | Ingestão de CDC (Debezium) Kafka → Bronze (modo batch) | a cada 15 min |
| `bronze_to_silver_dbt` | dbt Bronze → Silver (run + test) | 01:00 |
| `silver_to_gold_dbt` | dbt Silver → Gold (run + test) | 02:00 |
| `sync_clickhouse` | Sync PostgreSQL → ClickHouse (pós-Gold) | 03:00 |
| `compliance_lgpd` | Relatório LGPD + purga de dados expirados | 04:00 |
| `reconciliacao_contabil` | Reconciliação core banking vs data lake (saldos, transações, PIX/SPI) | 05:00 |

### Outras DAGs

- `market_data_ingestion` — indicadores BCB (CDI, SELIC, IPCA) e Tesouro Direto → TimescaleDB (20:00, dias úteis)
- `timescaledb_ingestion` — métricas de transações (Kafka) e métricas de sistema → TimescaleDB (a cada minuto)

### Ingestão de CDC (Debezium)

A DAG `ingest_kafka_to_bronze` consome os tópicos `cdc.aurix.*` (contas, clientes,
transacoes, pix_pagamentos) publicados pelo Kafka Connect (Debezium) e grava Parquet
no bucket `aurix-bronze` em `cdc/<tabela>/<ano>/<mês>/<dia>/`. O script é
`ingestion/kafka_to_bronze.py`:

```bash
# Modo batch (uma execução, útil no Airflow)
python ingestion/kafka_to_bronze.py --once

# Com limite de mensagens
python ingestion/kafka_to_bronze.py --once --max-messages 5000
```

Variáveis de ambiente relevantes: `KAFKA_BOOTSTRAP_SERVERS`, `CDC_TOPICS`,
`CDC_MAX_MESSAGES`, `CDC_CONSUMER_GROUP`, `BRONZE_BUCKET`, `MINIO_ENDPOINT`,
`MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`.

Os registros são enriquecidos com `_cdc_op` (c/u/d), `_cdc_ts` (ISO UTC) e
`_cdc_deleted` (true para DELETE). Os offsets são commitados após cada mensagem,
garantindo no mínimo uma vez entrega sem perda.

### Alertas

Todas as DAGs usam callbacks de falha/sucesso do módulo `alertas_aurix.py`
(Slack + e-mail). Configuração por variáveis de ambiente:

- `SLACK_WEBHOOK_URL` — webhook do Slack
- `ALERTAS_EMAIL_TO` — destinatário(s) de e-mail (separados por vírgula)

### Testes

```bash
pip install -r airflow/requirements.txt
python -m pytest airflow/tests
```

## Módulos

- `ingestion/` — PostgreSQL → Bronze (Parquet no MinIO)
- `spark/` — processamento de transações (streaming/batch)
- `dbt/` — modelos staging → silver → gold
- `sync/` — PostgreSQL → ClickHouse (replicação)
- `compliance/` — LGPD, auditoria e relatórios BACEN
- `analytics/` — dashboard analítico em tempo real
- `reconciliation/` — reconciliação contábil: exporter Prometheus e dashboard Grafana

## Reconciliação contábil

A DAG `reconciliacao_contabil` (05:00) compara core banking (PostgreSQL) vs data
lake (ClickHouse): saldos por conta, transações por dia e PIX/BACEN SPI. Quando
a divergência ultrapassa `RECONCILIACAO_LIMIAR_PCT` (padrão 0.01%), a tarefa
falha e os alertas do `alertas_aurix.py` são disparados.

Os relatórios são persistidos em `artifacts/reconciliation/*.json` e expostos
como métricas Prometheus (`aurix_reconciliacao_*`) pelo exporter:

```bash
cd reconciliation
pip install -r requirements.txt
python -m reconciliation.exporter --once          # textfile (node_exporter)
python -m reconciliation.exporter --port 9102     # HTTP scrape
```

O dashboard `reconciliation/grafana/aurix-reconciliacao-dashboard.json`
monitora status, divergências por escopo e alertas. As regras de alerta
(`ReconciliacaoDivergencias`, `ReconciliacaoAusente`) e o job `reconciliacao`
no Prometheus ficam em `aurix-infrastructure`.

## Relacionados

- [aurix-data-platform](https://github.com/aureus-platform/aurix-data-platform)
- [aurix-ml](https://github.com/aureus-platform/aurix-ml)
- [aurix-core-banking](https://github.com/aureus-platform/aurix-core-banking)
