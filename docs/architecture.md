# Architecture

## Overview

Data pipelines process financial transactions through ETL and streaming workflows, transforming raw data into structured analytics-ready datasets.

## Pipeline Stages

1. **Ingestion** — consume events from Kafka topics (transactions, accounts, customers)
2. **Processing** — Spark jobs for aggregation, enrichment, deduplication
3. **Storage** — load into the data platform (ClickHouse / data lake)
4. **Orchestration** — Airflow DAGs manage scheduling, retries, and monitoring

## Tech Stack

- **Processing**: Apache Spark (PySpark)
- **Streaming**: Kafka Streams
- **Orchestration**: Airflow
- **Storage**: Parquet (data lake) + ClickHouse (analytics)

## Repository

```
data/pipelines/
├── dags/            # Airflow DAG definitions
├── jobs/            # Spark job source code
├── lib/             # Shared pipeline libraries
├── tests/           # Pipeline tests
└── docker/          # Pipeline Dockerfiles
```
