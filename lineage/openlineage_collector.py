"""
OpenLineage + Marquez — Data Lineage para Aurix Platform.

Rastreia de onde os dados vêm, por onde passam, e onde vão:
  PostgreSQL → Bronze (MinIO) → Silver (dbt) → Gold (dbt) → ClickHouse → Dashboard
                                        ↓
                                  Airflow DAGs
                                        ↓
                                   Grafana

Uso:
  python lineage/openlineage_collector.py --collect
  python lineage/openlineage_collector.py --report
"""

import json
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import psycopg2
import requests


class OpenLineageCollector:
    """Coleta e reporta lineage de dados via OpenLineage API (Marquez)."""

    def __init__(self, marquez_url: str = None):
        self.marquez_url = marquez_url or os.getenv(
            "MARQUEZ_URL", "http://localhost:5000"
        )
        self.namespace = "aurix-platform"
        self.run_id = None

    # ═══ Namespace ═══

    def create_namespace(self, name: str, url: str, description: str):
        """Cria namespace no Marquez."""
        payload = {
            "name": name,
            "url": url,
            "description": description,
        }
        try:
            resp = requests.post(
                f"{self.marquez_url}/api/v1/namespaces",
                json=payload,
                timeout=10,
            )
            return resp.status_code in (200, 201, 409)  # 409 = already exists
        except Exception as e:
            print(f"  [WARN] Namespace creation failed: {e}")
            return False

    # ═══ Dataset Registration ═══

    def register_dataset(self, name: str, source_name: str, schema: dict = None):
        """Registra dataset (fonte de dados) no Marquez."""
        payload = {
            "id": {
                "namespace": self.namespace,
                "name": name,
            },
            "typeName": "DB_TABLE",
            "sourceName": source_name,
            "schemaLocation": schema,
        }
        try:
            resp = requests.post(
                f"{self.marquez_url}/api/v1/datasets",
                json=payload,
                timeout=10,
            )
            return resp.status_code in (200, 201, 409)
        except Exception as e:
            print(f"  [WARN] Dataset registration failed: {e}")
            return False

    # ═══ Lineage Events ═══

    def emit_lineage_event(self, job_name: str, inputs: list, outputs: list):
        """
        Emite evento de lineage para Marquez.

        inputs:  [{"namespace": "aurix", "name": "contas", "facets": {}}]
        outputs: [{"namespace": "aurix", "name": "stg_contas", "facets": {}}]
        """
        event = {
            "eventType": "COMPLETE",
            "eventTime": datetime.utcnow().isoformat() + "Z",
            "run": {
                "runId": self.run_id or str(int(time.time())),
                "facets": {
                    "processing_engine": {
                        "version": "3.5.0",
                        "name": "spark",
                    }
                },
            },
            "job": {
                "namespace": self.namespace,
                "name": job_name,
            },
            "inputs": inputs,
            "outputs": outputs,
        }

        try:
            resp = requests.post(
                f"{self.marquez_url}/api/v1/lineage",
                json=event,
                timeout=10,
            )
            return resp.status_code in (200, 201, 202)
        except Exception as e:
            print(f"  [WARN] Lineage event failed: {e}")
            return False

    # ═══ Coleta Automática de Lineage ═══

    def collect_database_lineage(self, pg_config: dict):
        """Coleta lineage dos metadados do PostgreSQL (views, funções, triggers)."""
        print("\n[1/4] Coletando lineage de PostgreSQL...")

        conn = psycopg2.connect(**pg_config)
        cur = conn.cursor()

        # Views → dependências
        cur.execute("""
            SELECT
                table_name as view_name,
                view_definition as definition
            FROM information_schema.views
            WHERE table_schema = 'public'
        """)

        views = cur.fetchall()
        for view_name, definition in views:
            # Extrair tabelas referenciadas
            deps = []
            for table in ["contas", "clientes", "transacoes", "pix_pagamentos",
                          "credito_solicitacoes", "investimentos", "auditoria"]:
                if table in (definition or "").lower():
                    deps.append(table)

            if deps:
                inputs = [
                    {"namespace": self.namespace, "name": dep}
                    for dep in deps
                ]
                self.emit_lineage_event(
                    f"dbt:staging:{view_name}",
                    inputs=inputs,
                    outputs=[{"namespace": self.namespace, "name": view_name}],
                )

        cur.close()
        conn.close()
        print(f"  ✓ {len(views)} views analisadas")

    def collect_airflow_lineage(self):
        """Coleta lineage das DAGs do Airflow via API."""
        print("\n[2/4] Coletando lineage de Airflow DAGs...")

        airflow_url = os.getenv("AIRFLOW_URL", "http://localhost:8082")
        try:
            resp = requests.get(f"{airflow_url}/api/v1/dags", timeout=10)
            if resp.status_code != 200:
                print("  [WARN] Airflow API indisponível")
                return

            dags = resp.json().get("dags", [])

            # Mapeamento de lineage das DAGs
            lineage_map = {
                "ingest_postgres_to_bronze": {
                    "inputs": ["postgres:aurix.contas", "postgres:aurix.clientes", "postgres:aurix.transacoes"],
                    "outputs": ["minio:aurix-bronze/postgres/contas", "minio:aurix-bronze/postgres/clientes", "minio:aurix-bronze/postgres/transacoes"],
                },
                "ingest_kafka_to_bronze": {
                    "inputs": ["kafka:cdc.aurix.contas", "kafka:cdc.aurix.transacoes"],
                    "outputs": ["minio:aurix-bronze/cdc/contas", "minio:aurix-bronze/cdc/transacoes"],
                },
                "bronze_to_silver_dbt": {
                    "inputs": ["minio:aurix-bronze/postgres/*"],
                    "outputs": ["postgres:aurix.silver_contas", "postgres:aurix.silver_clientes", "postgres:aurix.silver_transacoes"],
                },
                "silver_to_gold_dbt": {
                    "inputs": ["postgres:aurix.silver_*"],
                    "outputs": ["postgres:aurix.gold_transacoes_diarias", "postgres:aurix.gold_contas_resumo", "postgres:aurix.gold_clientes_risco"],
                },
                "sync_clickhouse": {
                    "inputs": ["postgres:aurix.transacoes", "postgres:aurix.contas"],
                    "outputs": ["clickhouse:aurix_analytics.transacoes_analytics"],
                },
            }

            for dag in dags:
                dag_id = dag.get("dag_id")
                if dag_id in lineage_map:
                    mapping = lineage_map[dag_id]
                    inputs = [{"namespace": self.namespace, "name": i} for i in mapping["inputs"]]
                    outputs = [{"namespace": self.namespace, "name": o} for o in mapping["outputs"]]
                    self.emit_lineage_event(f"airflow:{dag_id}", inputs, outputs)
                    print(f"  ✓ {dag_id}: {len(mapping['inputs'])} inputs → {len(mapping['outputs'])} outputs")

        except Exception as e:
            print(f"  [WARN] Airflow lineage collection failed: {e}")

    def collect_spark_lineage(self):
        """Coleta lineage de Spark jobs."""
        print("\n[3/4] Coletando lineage de Spark jobs...")

        # Mapeamento estático do Spark processor
        spark_lineage = {
            "spark:transactions_processor": {
                "inputs": ["kafka:transacoes"],
                "outputs": [
                    "clickhouse:aurix_analytics.transacoes_analytics",
                    "clickhouse:aurix_analytics.metricas_horarias",
                    "elasticsearch:aurix-transactions",
                ],
            }
        }

        for job, mapping in spark_lineage.items():
            inputs = [{"namespace": self.namespace, "name": i} for i in mapping["inputs"]]
            outputs = [{"namespace": self.namespace, "name": o} for o in mapping["outputs"]]
            self.emit_lineage_event(job, inputs, outputs)
            print(f"  ✓ {job}: {len(mapping['inputs'])} inputs → {len(mapping['outputs'])} outputs")

    def collect_debezium_lineage(self):
        """Coleta lineage do Debezium CDC."""
        print("\n[4/4] Coletando lineage de Debezium CDC...")

        debezium_lineage = {
            "debezium:cdc_contas": {
                "inputs": ["postgres:aurix.contas"],
                "outputs": ["kafka:cdc.aurix.contas"],
            },
            "debezium:cdc_clientes": {
                "inputs": ["postgres:aurix.clientes"],
                "outputs": ["kafka:cdc.aurix.clientes"],
            },
            "debezium:cdc_transacoes": {
                "inputs": ["postgres:aurix.transacoes"],
                "outputs": ["kafka:cdc.aurix.transacoes"],
            },
            "debezium:cdc_pix": {
                "inputs": ["postgres:aurix.pix_pagamentos"],
                "outputs": ["kafka:cdc.aurix.pix_pagamentos"],
            },
        }

        for job, mapping in debezium_lineage.items():
            inputs = [{"namespace": self.namespace, "name": i} for i in mapping["inputs"]]
            outputs = [{"namespace": self.namespace, "name": o} for o in mapping["outputs"]]
            self.emit_lineage_event(job, inputs, outputs)
            print(f"  ✓ {job}: {len(mapping['inputs'])} inputs → {len(mapping['outputs'])} outputs")

    # ═══ Relatório de Lineage ═══

    def generate_lineage_report(self) -> dict:
        """Gera relatório completo de lineage da plataforma."""
        print("\n" + "=" * 60)
        print("Aurix Data Lineage Report")
        print("=" * 60)

        report = {
            "timestamp": datetime.now().isoformat(),
            "pipeline_stages": [
                {
                    "stage": "SOURCE",
                    "systems": ["PostgreSQL (aurix_db)", "Kafka (CDC)", "BACEN API"],
                    "description": "Fontes primárias de dados",
                },
                {
                    "stage": "INGESTION",
                    "systems": ["Debezium CDC", "Airflow DAGs", "pg_to_bronze.py", "kafka_to_bronze.py"],
                    "description": "Captura e ingestão para MinIO Bronze",
                },
                {
                    "stage": "BRONZE",
                    "systems": ["MinIO (aurix-bronze)", "Parquet format", "YYYY/MM/DD partitioning"],
                    "description": "Dados brutos, sem transformação",
                },
                {
                    "stage": "SILVER",
                    "systems": ["dbt models", "PostgreSQL", "Great Expectations"],
                    "description": "Dados limpos, enriquecidos, validados",
                },
                {
                    "stage": "GOLD",
                    "systems": ["dbt models", "PostgreSQL", "ClickHouse"],
                    "description": "Dados agregados para análise",
                },
                {
                    "stage": "SERVING",
                    "systems": ["ClickHouse (OLAP)", "TimescaleDB (time-series)", "Elasticsearch", "Redis"],
                    "description": "Camada de consumo para dashboards e APIs",
                },
                {
                    "stage": "CONSUMPTION",
                    "systems": ["Grafana", "Real-time Dashboard", "Open Finance APIs"],
                    "description": "Interfaces de visualização e distribuição",
                },
            ],
            "flow_map": {
                "PostgreSQL → MinIO": "pg_to_bronze.py (batch) + kafka_to_bronze.py (CDC)",
                "MinIO → PostgreSQL": "dbt run (bronze_to_silver_dbt)",
                "PostgreSQL → ClickHouse": "postgres_to_clickhouse.py (sync_clickhouse DAG)",
                "Kafka → TimescaleDB": "timescaledb_ingestion DAG",
                "ClickHouse → Grafana": "real_time_analytics.py",
                "PostgreSQL → LGPD": "compliance_lgpd.py",
                "PostgreSQL → Reconciliation": "reconciliacao_contabil.py",
            },
            "total_sources": 3,
            "total_transformations": 9,
            "total_sinks": 4,
        }

        # Salvar
        report_dir = Path(__file__).parent / "reports"
        report_dir.mkdir(exist_ok=True)
        report_path = report_dir / "lineage_report.json"
        with open(report_path, "w") as f:
            json.dump(report, f, indent=2)

        # Imprimir
        for stage in report["pipeline_stages"]:
            print(f"\n  [{stage['stage']}]")
            for system in stage["systems"]:
                print(f"    → {system}")
            print(f"    {stage['description']}")

        print(f"\n  Total: {report['total_sources']} fontes → "
              f"{report['total_transformations']} transformações → "
              f"{report['total_sinks']} destinos")

        return report

    # ═══ Main ═══

    def collect_all(self):
        """Executa coleta completa de lineage."""
        print("=" * 60)
        print("OpenLineage Collector — Aurix Platform")
        print("=" * 60)

        start = time.time()

        # Setup namespaces
        self.create_namespace(
            self.namespace,
            "https://github.com/aurix-core-banking",
            "Aurix Core Banking Platform",
        )

        # Coletar
        self.collect_debezium_lineage()
        self.collect_database_lineage({
            "host": os.getenv("PG_HOST", "localhost"),
            "port": int(os.getenv("PG_PORT", "5432")),
            "dbname": os.getenv("PG_DB", "aurix"),
            "user": os.getenv("PG_USER", "aurix"),
            "password": os.getenv("PG_PASSWORD", "aurix"),
        })
        self.collect_airflow_lineage()
        self.collect_spark_lineage()

        elapsed = time.time() - start
        print(f"\nColeta completa em {elapsed:.1f}s")

        # Gerar relatório
        report = self.generate_lineage_report()
        return report


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--collect", action="store_true", help="Coletar lineage")
    parser.add_argument("--report", action="store_true", help="Gerar relatório")
    args = parser.parse_args()

    collector = OpenLineageCollector()
    if args.collect:
        collector.collect_all()
    elif args.report:
        collector.generate_lineage_report()
    else:
        collector.collect_all()
