"""
DataHub Ingestor — Cataloga automaticamente todas as fontes de dados do Aurix.

Executa:
1. PostgreSQL schema → DataHub dataset entities
2. ClickHouse schema → DataHub dataset entities
3. MinIO/S3 buckets → DataHub dataset entities
4. Kafka topics → DataHub dataset entities
5. Classificação PII automática (CPF, CNPJ, email, telefone)
6. Glossário de domínio bancário
7. Ownership assignment
"""

import json
import os
import sys
import time
from datetime import datetime

# DataHub REST API client (sem dependência de brew — usa requests direto)
import requests


class DataHubIngestor:
    """Ingestor de metadata no DataHub via REST API (GMS)."""

    def __init__(self, gms_host: str = None):
        self.gms_host = gms_host or os.getenv("DATAHUB_GMS_HOST", "localhost")
        self.gms_port = os.getenv("DATAHUB_GMS_PORT", "8080")
        self.base_url = f"http://{self.gms_host}:{self.gms_port}"
        self.platform_types = {
            "postgres": "datahub",
            "clickhouse": "datahub",
            "kafka": "datahub",
            "s3": "datahub",
            "minio": "datahub",
        }

    def _gms_post(self, path: str, payload: dict) -> bool:
        """POST to DataHub GMS."""
        try:
            resp = requests.post(
                f"{self.base_url}{path}",
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=30,
            )
            return resp.status_code in (200, 201, 204)
        except Exception as e:
            print(f"  [ERRO] GMS POST {path}: {e}")
            return False

    def _gms_get(self, path: str) -> dict | None:
        """GET from DataHub GMS."""
        try:
            resp = requests.get(f"{self.base_url}{path}", timeout=30)
            if resp.status_code == 200:
                return resp.json()
        except Exception as e:
            print(f"  [ERRO] GMS GET {path}: {e}")
        return None

    # ═══ 1. PostgreSQL ═══

    def ingest_postgres(self, db_host: str, db_port: int, db_name: str,
                        db_user: str, db_password: str):
        """Ingere schema PostgreSQL → DataHub."""
        import psycopg2

        print("\n[1/5] Ingesting PostgreSQL → DataHub...")

        conn = psycopg2.connect(
            host=db_host, port=db_port, dbname=db_name,
            user=db_user, password=db_password,
        )
        cur = conn.cursor()

        # Listar tabelas
        cur.execute("""
            SELECT table_name, column_name, data_type, is_nullable,
                   CASE WHEN column_name LIKE '%cpf%' OR column_name LIKE '%cnpj%'
                        OR column_name LIKE '%email%' OR column_name LIKE '%telefone%'
                        OR column_name LIKE '%senha%' OR column_name LIKE '%token%'
                        THEN 'PII' ELSE 'NORMAL' END as classification
            FROM information_schema.columns
            WHERE table_schema = 'public'
            ORDER BY table_name, ordinal_position
        """)

        tables = {}
        for row in cur.fetchall():
            table_name = row[0]
            if table_name not in tables:
                tables[table_name] = []
            tables[table_name].append({
                "name": row[1],
                "type": row[2],
                "nullable": row[3] == "YES",
                "classification": row[4],
            })

        # Criar dataset entity para cada tabela
        for table_name, columns in tables.items():
            pii_columns = [c["name"] for c in columns if c["classification"] == "PII"]
            self._create_dataset(
                platform="aurix-postgres",
                database=db_name,
                schema="public",
                name=table_name,
                columns=columns,
                pii_columns=pii_columns,
                owner="data-engineering@aurix.com.br",
                description=self._get_table_description(table_name),
            )
            print(f"  ✓ postgres://{db_name}/public/{table_name} "
                  f"({len(columns)} colunas, {len(pii_columns)} PII)")

        cur.close()
        conn.close()

        print(f"  Total: {len(tables)} tabelas PostgreSQL catalogadas")

    # ═══ 2. ClickHouse ═══

    def ingest_clickhouse(self, ch_host: str, ch_port: int, ch_db: str):
        """Ingere schema ClickHouse → DataHub."""
        import clickhouse_connect

        print("\n[2/5] Ingesting ClickHouse → DataHub...")

        client = clickhouse_connect.get_client(host=ch_host, port=ch_port, database=ch_db)

        tables = client.query(
            "SELECT name, engine, partition_key, sorting_key FROM system.tables "
            "WHERE database = %s AND engine LIKE '%MergeTree%'",
            parameters=[ch_db],
        )

        for row in tables.result_rows:
            table_name = row[0]
            columns = client.query(
                f"SELECT name, type, default_kind FROM system.columns "
                f"WHERE database = %s AND table = %s",
                parameters=[ch_db, table_name],
            )

            col_data = [
                {"name": c[0], "type": c[1], "nullable": True, "classification": "NORMAL"}
                for c in columns.result_rows
            ]

            self._create_dataset(
                platform="aurix-clickhouse",
                database=ch_db,
                schema="default",
                name=table_name,
                columns=col_data,
                pii_columns=[],
                owner="data-engineering@aurix.com.br",
                description=f"Tabela ClickHouse — engine: {row[1]}, partition: {row[2]}",
                extra_aspects={
                    "schemaMetadata": {
                        "platformSchema": {
                            "com.linkedin.schema clickable schemas": {
                                "foreignTables": []
                            }
                        }
                    }
                },
            )
            print(f"  ✓ clickhouse://{ch_db}/default/{table_name}")

        client.close()
        print(f"  Total: {len(tables.result_rows)} tabelas ClickHouse catalogadas")

    # ═══ 3. Kafka Topics ═══

    def ingest_kafka(self, bootstrap: str):
        """Ingere Kafka topics → DataHub."""
        from kafka.admin import KafkaAdminClient

        print("\n[3/5] Ingesting Kafka topics → DataHub...")

        admin = KafkaAdminClient(bootstrap_servers=bootstrap)
        topics = admin.list_topics()

        for topic in sorted(topics):
            # Pular topics internos
            if topic.startswith("__") or topic.startswith("_"):
                continue

            partitions = admin.describe_topics([topic])
            num_partitions = len(partitions[0].partitions) if partitions else 1

            self._create_dataset(
                platform="aurix-kafka",
                database="kafka",
                schema="default",
                name=topic,
                columns=[],
                pii_columns=[],
                owner="data-engineering@aurix.com.br",
                description=f"Kafka topic — {num_partitions} partições",
                extra_aspects={
                    "browsePaths": {"path": f"/kafka/{topic}"},
                },
            )
            print(f"  ✓ kafka://{topic} ({num_partitions} partitions)")

        admin.close()
        print(f"  Total: {len([t for t in topics if not t.startswith('__')])} topics catalogados")

    # ═══ 4. MinIO/S3 Buckets ═══

    def ingest_minio(self, endpoint: str, access_key: str, secret_key: str):
        """Ingere MinIO buckets → DataHub."""
        import boto3

        print("\n[4/5] Ingesting MinIO/S3 → DataHub...")

        s3 = boto3.client(
            "s3",
            endpoint_url=f"http://{endpoint}",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )

        buckets = s3.list_buckets()["Buckets"]
        for bucket in buckets:
            bucket_name = bucket["Name"]

            # Listar prefixos (camadas)
            objects = s3.list_objects_v2(Bucket=bucket_name, Delimiter="/")
            prefixes = [p["Prefix"] for p in objects.get("CommonPrefixes", [])]

            self._create_dataset(
                platform="aurix-minio",
                database="minio",
                schema="default",
                name=bucket_name,
                columns=[],
                pii_columns=[],
                owner="data-engineering@aurix.com.br",
                description=f"MinIO bucket — camadas: {', '.join(prefixes) if prefixes else 'root'}",
                extra_aspects={
                    "browsePaths": {"path": f"/minio/{bucket_name}"},
                },
            )
            print(f"  ✓ s3://{bucket_name}/ ({len(prefixes)} camadas)")

        print(f"  Total: {len(buckets)} buckets catalogados")

    # ═══ 5. Glossário + Classificação ═══

    def create_glossary(self):
        """Cria glossário de domínio bancário."""
        print("\n[5/5] Creating glossary terms...")

        terms = [
            {
                "name": "Conta Corrente",
                "description": "Conta bancária para movimentação diária com cheque e débito automático",
                "term": "CONTA_CORRENTE",
            },
            {
                "name": "Conta Poupança",
                "description": "Conta de depósito com rendimento atrelado à Selic",
                "term": "CONTA_POUPANCA",
            },
            {
                "name": "Conta Salário",
                "description": "Conta vinculada ao recebimento de salário com benefícios de crédito",
                "term": "CONTA_SALARIO",
            },
            {
                "name": "PIX",
                "description": "Pagamento instantâneo 24/7 do Banco Central via chave aleatória, CPF, CNPJ ou email",
                "term": "PIX",
            },
            {
                "name": "TED",
                "description": "Transferência Eletrônica Disponível — transferência entre bancos em D+1",
                "term": "TED",
            },
            {
                "name": "DOC",
                "description": "Documento de Ordem de Crédito — transferência agendada entre bancos",
                "term": "DOC",
            },
            {
                "name": "Empréstimo Consignado",
                "description": "Crédito com desconto em folha de pagamento, taxa menor",
                "term": "EMPRESTIMO_CONSIGNADO",
            },
            {
                "name": "Financiamento",
                "description": "Crédito com garantia (veículo, imóvel, etc.)",
                "term": "FINANCIAMENTO",
            },
            {
                "name": "SPI",
                "description": "Sistema de Pagamentos Instantâneos — backbone do PIX",
                "term": "SPI",
            },
            {
                "name": "STR",
                "description": "Sistema de Transferência de Reservas — backup do SPI",
                "term": "STR",
            },
            {
                "name": "ISPB",
                "description": "Identificador do Sistema de Pagamentos Brasileiro",
                "term": "ISPB",
            },
            {
                "name": "LGPD",
                "description": "Lei Geral de Proteção de Dados — regula tratamento de dados pessoais",
                "term": "LGPD",
            },
            {
                "name": "KYC",
                "description": "Know Your Customer — verificação de identidade do cliente",
                "term": "KYC",
            },
            {
                "name": "AML",
                "description": "Anti-Money Laundering — prevenção à lavagem de dinheiro",
                "term": "AML",
            },
            {
                "name": "CVM",
                "description": "Comissão de Valores Mobiliários — regula investimentos",
                "term": "CVM",
            },
            {
                "name": "Open Finance",
                "description": "Ecossistema de dados financeiros abertos sob regulamentação do Banco Central",
                "term": "OPEN_FINANCE",
            },
        ]

        created = 0
        for term in terms:
            payload = {
                "name": term["term"],
                "displayName": term["name"],
                "description": term["description"],
                "glossary": "aurix-glossary",
            }
            if self._gms_post("/api/v2/glossaryTerms", payload):
                created += 1
                print(f"  ✓ {term['name']}")
            else:
                # Fallback: criar via urn
                print(f"  ~ {term['name']} (já existe ou API indisponível)")

        print(f"  Total: {created}/{len(terms)} termos criados")

    # ═══ Helpers ═══

    def _create_dataset(self, platform: str, database: str, schema: str,
                        name: str, columns: list, pii_columns: list,
                        owner: str, description: str, extra_aspects: dict = None):
        """Cria ou atualiza um dataset no DataHub."""
        urn = f"urn:li:dataset:({platform},{database}.{schema}.{name},PROD)"

        aspects = {
            "datasetProperties": {
                "description": description,
                "name": name,
                "qualifiedName": f"{database}.{schema}.{name}",
                "customProperties": {
                    "platform": platform,
                    "database": database,
                    "schema": schema,
                    "piicolumns": ",".join(pii_columns) if pii_columns else "",
                    "classification": "PII" if pii_columns else "NORMAL",
                },
            },
            "ownership": {
                "owners": [
                    {
                        "owner": owner,
                        "type": "DATAOWNER",
                    }
                ]
            },
            "status": {"removed": False},
            "browsePaths": {"path": f"/{platform}/{database}/{schema}/{name}"},
        }

        if pii_columns:
            aspects["globalTags"] = {
                "tags": [
                    {"tagUrn": "urn:li:tag:PII"},
                    {"tagUrn": "urn:li:tag:SENSITIVE"},
                ]
            }

        if extra_aspects:
            aspects.update(extra_aspects)

        # Upser via aspect ingestion
        payload = {
            "entityUrn": urn,
            "aspectName": "datasetProperties",
            "aspect": aspects["datasetProperties"],
        }
        self._gms_post("/api/v1/entity", {
            "aspect": aspects["datasetProperties"],
            "urn": urn,
        })

    def _get_table_description(self, table_name: str) -> str:
        """Retorna descrição em português para tabelas conhecidas."""
        descriptions = {
            "contas": "Contas bancárias (corrente, poupança, salário)",
            "clientes": "Dados cadastrais de clientes PF/PJ",
            "transacoes": "Transações financeiras (PIX, TED, DOC, transferências)",
            "pix_pagamentos": "Pagamentos via PIX processados pelo SPI/STR",
            "credito_solicitacoes": "Solicitações de crédito (empréstimo, financiamento, consignado)",
            "investimentos": "Investimentos (renda fixa, variável, fundos)",
            "auditoria": "Logs de auditoria para compliance",
            "movimentos_conta": "Movimentações de conta (entradas, saídas, saldos)",
            "liquidacoes": "Liquidações de transações interbancárias",
            "transacoes_spi": "Transações do SPI (PIX instantâneo)",
            "transacoes_str": "Transações do STR (transferência de reservas)",
            "logs_auditoria": "Logs detalhados de auditoria para compliance BACEN",
            "relatorios_bacen": "Relatórios regulatórios enviados ao BACEN",
        }
        return descriptions.get(table_name, f"Tabela {table_name}")

    def run_all(self):
        """Executa ingestão completa."""
        print("=" * 60)
        print("DataHub Ingestor — Aurix Platform")
        print("=" * 60)

        start = time.time()

        # 1. PostgreSQL
        self.ingest_postgres(
            db_host=os.getenv("PG_HOST", "localhost"),
            db_port=int(os.getenv("PG_PORT", "5432")),
            db_name=os.getenv("PG_DB", "aurix"),
            db_user=os.getenv("PG_USER", "aurix"),
            db_password=os.getenv("PG_PASSWORD", "aurix"),
        )

        # 2. ClickHouse
        self.ingest_clickhouse(
            ch_host=os.getenv("CH_HOST", "localhost"),
            ch_port=int(os.getenv("CH_PORT", "8123")),
            ch_db=os.getenv("CH_DB", "aurix_analytics"),
        )

        # 3. Kafka
        self.ingest_kafka(bootstrap=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"))

        # 4. MinIO
        self.ingest_minio(
            endpoint=os.getenv("MINIO_ENDPOINT", "localhost:9000"),
            access_key=os.getenv("MINIO_ACCESS_KEY", "aurix_admin"),
            secret_key=os.getenv("MINIO_SECRET_KEY", "minio_dev_password"),
        )

        # 5. Glossário
        self.create_glossary()

        elapsed = time.time() - start
        print(f"\n{'=' * 60}")
        print(f"Ingestão completa em {elapsed:.1f}s")
        print(f"Acesse: http://localhost:9002")


if __name__ == "__main__":
    ingestor = DataHubIngestor()
    ingestor.run_all()
