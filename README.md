# Aurix Data Pipelines

Data lakehouse com Spark, Airflow, MinIO, dbt, Great Expectations, Feast Feature Store e OpenLineage.

## Stack

- **Apache Spark** (PySpark Structured Streaming + batch)
- **Apache Airflow** (orquestração — 13 DAGs)
- **MinIO** (S3-compatible — bronze/silver/gold)
- **dbt** (transformações SQL)
- **Great Expectations** (data quality)
- **Feast** (feature store para ML)
- **OpenLineage + Marquez** (data lineage)
- **Python** + **PySpark** + **Kafka** + **ClickHouse**

## Arquitetura Medallion

```
PostgreSQL ──┐
             ├──→ Bronze (MinIO Parquet) ──→ Silver (dbt) ──→ Gold (dbt)
Kafka CDC ───┘                                    │
                                                  ├──→ ClickHouse (OLAP)
                                                  ├──→ TimescaleDB (time-series)
                                                  └──→ Grafana (dashboards)
```

## Airflow DAGs

| DAG | Função | Horário |
|---|---|---|
| `ingest_postgres_to_bronze` | Ingestão incremental PG → Bronze | 00:00 |
| `data_quality_check` | Quality checks (Great Expectations) | 00:30 |
| `ingest_kafka_to_bronze` | CDC Debezium Kafka → Bronze | a cada 15 min |
| `bronze_to_silver_dbt` | dbt Bronze → Silver (run + test) | 01:00 |
| `silver_to_gold_dbt` | dbt Silver → Gold (run + test) | 02:00 |
| `feature_materialization` | Feast feature materialization | 03:00 |
| `sync_clickhouse` | PostgreSQL → ClickHouse | 03:30 |
| `compliance_lgpd` | LGPD report + purge | 04:00 |
| `reconciliacao_contabil` | Core banking vs data lake | 05:00 |
| `timescaledb_ingestion` | Kafka → TimescaleDB | a cada 1 min |
| `market_data_ingestion` | BCB API → TimescaleDB | 20:00 (dias úteis) |

## Módulos

| Módulo | Descrição |
|---|---|
| `ingestion/` | PostgreSQL → Bronze (Parquet), Kafka CDC → Bronze |
| `spark/` | PySpark Structured Streaming (transações analytics) |
| `dbt/` | Modelos staging/silver/gold (9 models) |
| `sync/` | PostgreSQL → ClickHouse (replicação) |
| `compliance/` | LGPD, auditoria, relatórios BACEN |
| `analytics/` | Dashboard real-time (Flask + SocketIO + Plotly) |
| `reconciliation/` | Reconciliação contábil + Prometheus exporter |
| `governance/` | DataHub ingestor (catálogo automático) |
| `data-quality/` | Great Expectations suites + DataQualityEngine |
| `lineage/` | OpenLineage + Marquez collector |
| `feature-store/` | Feast (4 feature views, 48 features ML) |

## Data Quality

```bash
# Rodar todos os checks
python data-quality/great_expectations_runner.py

# Checks específicos
python data-quality/great_expectations_runner.py --tables contas,transacoes
```

## Reconciliação

```bash
cd reconciliation
python -m reconciliation.exporter --port 9102  # HTTP scrape
```

## Testes

```bash
pip install -r airflow/requirements.txt
python -m pytest airflow/tests
python -m pytest ingestion/test_*.py
python -m pytest reconciliation/test_*.py
```

## Relacionados

- [aurix-data-platform](https://github.com/aurix-core-banking/aurix-data-platform)
- [aurix-infrastructure](https://github.com/aurix-core-banking/aurix-infrastructure)
- [aurix-ml](https://github.com/aurix-core-banking/aurix-ml)
