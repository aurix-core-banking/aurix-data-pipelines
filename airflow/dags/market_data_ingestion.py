"""
Market Data Ingestion DAG

Ingests real Brazilian financial market indicators from the Banco Central do Brasil
(BCB) open API and government bond prices (Tesouro Direto) into TimescaleDB.
"""

from datetime import datetime, timedelta
import os
import json
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator

import requests

log = logging.getLogger(__name__)

BCB_API_BASE = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{}/dados?formato=json"

BCB_SERIES = {
    "CDI": 4389,
    "IPCA": 433,
    "SELIC": 432,
    "USD_BRL": 1,
    "IBOVESPA": 7832,
}

TESOURO_API = "https://www.tesourodireto.com.br/json/br/com/b3/tesourodireto/service/api/titulos.json"

default_args = {
    "owner": "aurix",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def get_timescaledb_connection():
    import psycopg2
    return psycopg2.connect(
        host=os.environ.get("TIMESCALEDB_HOST", "localhost"),
        port=int(os.environ.get("TIMESCALEDB_PORT", "5433")),
        dbname=os.environ.get("TIMESCALEDB_DB", "aurix_timeseries"),
        user=os.environ.get("TIMESCALEDB_USER", "aurix"),
        password=os.environ.get("TIMESCALEDB_PASSWORD", "replace_with_secure_password"),
    )


def ingest_market_indicators():
    conn = get_timescaledb_connection()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS indicadores_mercado (
            tempo TIMESTAMPTZ NOT NULL,
            indicador VARCHAR(50) NOT NULL,
            valor NUMERIC(15,6) NOT NULL,
            labels JSONB DEFAULT '{}',
            PRIMARY KEY (tempo, indicador)
        );
    """)
    cur.execute("SELECT create_hypertable('indicadores_mercado', 'tempo', if_not_exists => TRUE);")
    conn.commit()

    now = datetime.utcnow()
    yesterday = (now - timedelta(days=1)).strftime("%d/%m/%Y")

    for name, series_id in BCB_SERIES.items():
        try:
            url = BCB_API_BASE.format(series_id)
            params = {"dataInicial": yesterday, "dataFinal": yesterday}
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()

            if data:
                valor = float(data[-1]["valor"])
                labels = json.dumps({"source": "bcb_api", "series_id": series_id})
            else:
                log.warning("No data for %s (series %s)", name, series_id)
                continue

            cur.execute(
                """
                INSERT INTO indicadores_mercado (tempo, indicador, valor, labels)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (tempo, indicador)
                DO UPDATE SET valor = EXCLUDED.valor, labels = EXCLUDED.labels;
                """,
                (now, name, valor, labels),
            )
        except Exception as e:
            log.error("Failed to fetch %s: %s", name, e)

    conn.commit()
    cur.close()
    conn.close()
    log.info("Ingested market indicators at %s", now.isoformat())


def ingest_bond_prices():
    conn = get_timescaledb_connection()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS precos_renda_fixa (
            tempo TIMESTAMPTZ NOT NULL,
            ativo_id VARCHAR(100) NOT NULL,
            tipo_investimento VARCHAR(50) NOT NULL,
            preco_unitario_compra NUMERIC(15,2) NOT NULL,
            preco_unitario_venda NUMERIC(15,2) NOT NULL,
            taxa_indicativa NUMERIC(10,6) NOT NULL,
            PRIMARY KEY (tempo, ativo_id)
        );
    """)
    cur.execute("SELECT create_hypertable('precos_renda_fixa', 'tempo', if_not_exists => TRUE);")
    conn.commit()

    now = datetime.utcnow()

    try:
        resp = requests.get(TESOURO_API, timeout=30, headers={
            "User-Agent": "AurixPlatform/1.0",
            "Accept": "application/json",
        })
        resp.raise_for_status()
        bonds = resp.json()

        tipo_map = {
            "Tesouro Selic": "TESOURO_SELIC",
            "Tesouro IPCA+": "TESOURO_IPCA",
            "Tesouro IPCA+ com Juros Semestrais": "TESOURO_IPCA",
            "Tesouro Prefixado": "TESOURO_PREFIXADO",
            "Tesouro Prefixado com Juros Semestrais": "TESOURO_PREFIXADO",
            "Tesouro RendA+": "TESOURO_RENDA_MAIS",
        }

        for bond in bonds:
            ativo_id = bond.get("nmTitulo", "")
            tipo = tipo_map.get(ativo_id, "TESOURO_OUTROS")
            pu_compra = float(bond.get("puCompraManha", 0) or 0)
            pu_venda = float(bond.get("puVendaManha", 0) or 0)
            taxa = float(bond.get("taxaCompraManha", 0) or 0) / 100.0

            if pu_compra <= 0:
                continue

            cur.execute(
                """
                INSERT INTO precos_renda_fixa (tempo, ativo_id, tipo_investimento, preco_unitario_compra, preco_unitario_venda, taxa_indicativa)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (tempo, ativo_id)
                DO UPDATE SET
                    preco_unitario_compra = EXCLUDED.preco_unitario_compra,
                    preco_unitario_venda = EXCLUDED.preco_unitario_venda,
                    taxa_indicativa = EXCLUDED.taxa_indicativa;
                """,
                (now, ativo_id, tipo, pu_compra, pu_venda, taxa),
            )
    except Exception as e:
        log.error("Failed to fetch bond prices from Tesouro Direto: %s", e)

    conn.commit()
    cur.close()
    conn.close()
    log.info("Ingested bond prices at %s", now.isoformat())


dag = DAG(
    dag_id="market_data_ingestion",
    default_args=default_args,
    schedule="0 20 * * 1-5",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["aurix", "market-data", "ingestion"],
    description=__doc__,
)

PythonOperator(
    task_id="ingest_market_indicators",
    python_callable=ingest_market_indicators,
    dag=dag,
)

PythonOperator(
    task_id="ingest_bond_prices",
    python_callable=ingest_bond_prices,
    dag=dag,
)