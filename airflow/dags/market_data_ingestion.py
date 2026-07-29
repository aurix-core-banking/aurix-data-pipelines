"""
Market Data Ingestion DAG

This DAG simulates/performs daily ingestion of Brazilian financial market indicators
and government bond prices (Tesouro Direto) into TimescaleDB.

Ingestion tasks:
1. ingest_market_indicators:
   - Fetches macroeconomic indexers (CDI, SELIC, IPCA, USD/BRL, IBOVESPA)
   - Saves time-series samples into the "indicadores_mercado" hypertable
2. ingest_bond_prices:
   - Fetches prices and yields for public bonds (Tesouro Selic, IPCA+, Prefixado)
   - Saves daily valuations into the "precos_renda_fixa" hypertable
"""

from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator

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
    import json
    import random
    from datetime import datetime

    conn = get_timescaledb_connection()
    cur = conn.cursor()

    # Create tables and hypertable for indicators
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

    # Simulated/Real data feed for market indicators (inspired by OBM values)
    indicators = {
        "CDI": round(random.uniform(14.00, 14.25) / 100.0, 6),       # 14.15% a.a.
        "IPCA": round(random.uniform(0.30, 0.40) / 100.0, 6),        # 0.32% ao mês
        "SELIC": round(14.25 / 100.0, 6),                           # 14.25% a.a.
        "USD_BRL": round(random.uniform(5.10, 5.20), 4),             # 5.14
        "IBOVESPA": float(random.randint(167000, 169000)),          # 168278 pts
    }

    labels = json.dumps({"source": "obm_api_simulation", "environment": "dev"})

    for name, value in indicators.items():
        cur.execute(
            """
            INSERT INTO indicadores_mercado (tempo, indicador, valor, labels)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (tempo, indicador)
            DO UPDATE SET
                valor = EXCLUDED.valor,
                labels = EXCLUDED.labels;
            """,
            (now, name, value, labels),
        )

    conn.commit()
    cur.close()
    conn.close()
    print(f"[ingest_market_indicators] Ingested {len(indicators)} indicators at {now.isoformat()}")


def ingest_bond_prices():
    import json
    import random
    from datetime import datetime

    conn = get_timescaledb_connection()
    cur = conn.cursor()

    # Create tables and hypertable for bond prices
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

    # Active government bonds (reference curves)
    bonds = [
        {
            "ativo_id": "Tesouro Selic 2026",
            "tipo_investimento": "TESOURO_SELIC",
            "preco_compra": 14250.00,
            "preco_venda": 14240.00,
            "taxa": 0.0005,
        },
        {
            "ativo_id": "Tesouro IPCA+ 2029",
            "tipo_investimento": "TESOURO_IPCA",
            "preco_compra": 3120.50,
            "preco_venda": 3110.00,
            "taxa": 0.0625,
        },
        {
            "ativo_id": "Tesouro Prefixado 2026",
            "tipo_investimento": "TESOURO_PREFIXADO",
            "preco_compra": 820.40,
            "preco_venda": 818.00,
            "taxa": 0.1150,
        },
    ]

    for bond in bonds:
        # Slight daily variance for realistic time-series testing
        variance_factor = random.uniform(0.999, 1.001)
        pu_compra = round(bond["preco_compra"] * variance_factor, 2)
        pu_venda = round(bond["preco_venda"] * variance_factor, 2)

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
            (
                now,
                bond["ativo_id"],
                bond["tipo_investimento"],
                pu_compra,
                pu_venda,
                bond["taxa"],
            ),
        )

    conn.commit()
    cur.close()
    conn.close()
    print(f"[ingest_bond_prices] Ingested price matrix for {len(bonds)} government bonds.")


dag = DAG(
    dag_id="market_data_ingestion",
    default_args=default_args,
    schedule="0 20 * * 1-5",  # 20:00 Monday-Friday
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
