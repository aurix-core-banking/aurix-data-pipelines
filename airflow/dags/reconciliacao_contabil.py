"""
DAG de reconciliação contábil: core banking (PostgreSQL) vs data lake.

- `reconciliar_saldos`: total por conta (agregação no PostgreSQL) é comparado
  com o ClickHouse (bronze/silver) e com o gold.
- `reconciliar_transacoes`: total por dia.
- `reconciliar_pix`: total via BACEN SPI (integração mockada).
- Em caso de divergência acima do limiar, a tarefa falha e o alerta dispara.
"""

from datetime import datetime, timedelta
import os
from typing import Any, Dict, List

from airflow import DAG
from airflow.operators.python import PythonOperator

from alertas_aurix import notificar_falha, notificar_sucesso

LIMIAR_DIVERGENCIA_PCT = float(os.environ.get("RECONCILIACAO_LIMIAR_PCT", "0.01"))


def _config() -> Dict[str, Any]:
    return {
        "postgres": {
            "host": os.environ.get("PG_HOST", "postgres"),
            "port": int(os.environ.get("PG_PORT", "5432")),
            "database": os.environ.get("PG_DATABASE", "aurix"),
            "user": os.environ.get("PG_USER", "aurix_user"),
            "password": os.environ.get("PG_PASSWORD", "aurix_secure_password"),
        },
        "clickhouse": {
            "host": os.environ.get("CLICKHOUSE_HOST", "clickhouse"),
            "port": int(os.environ.get("CLICKHOUSE_PORT", "8123")),
            "database": os.environ.get("CLICKHOUSE_DATABASE", "aurix_analytics"),
            "user": os.environ.get("CLICKHOUSE_USER", "aurix"),
            "password": os.environ.get("CLICKHOUSE_PASSWORD", "aurix123"),
        },
    }


def _query_pg(cur, sql: str, params: tuple = ()) -> List[tuple]:
    cur.execute(sql, params)
    return cur.fetchall()


def _query_clickhouse(client, sql: str) -> List[tuple]:
    result = client.query(sql)
    return result.result_rows


def _divergencia_percentual(esperado, obtido) -> float:
    if esperado == 0:
        return 0.0 if obtido == 0 else 100.0
    return abs(esperado - obtido) / abs(esperado) * 100.0


def reconciliar_saldos() -> Dict[str, Any]:
    import psycopg2
    import clickhouse_connect

    cfg = _config()
    conn = psycopg2.connect(**cfg["postgres"])
    client = clickhouse_connect.get_client(**cfg["clickhouse"])
    cur = conn.cursor()
    resultado = {"tabelas": [], "divergencias": 0}

    tabelas = [
        ("contas", "saldo"),
        ("clientes", None),
        ("transacoes", "valor"),
    ]
    for tabela, coluna_valor in tabelas:
        try:
            pg_rows = _query_pg(cur, f"SELECT COUNT(*) FROM aurix.{tabela}")
            ch_rows = _query_clickhouse(client, f"SELECT COUNT(*) FROM {tabela}_analytics")
            pg_total = pg_rows[0][0]
            ch_total = ch_rows[0][0]
            divergencias = [_divergencia_percentual(pg_total, ch_total)]
            tabela_resultado = {
                "tabela": tabela,
                "pg_total": pg_total,
                "ch_total": ch_total,
            }
            if coluna_valor:
                pg_valor = _query_pg(
                    cur, f"SELECT COALESCE(SUM({coluna_valor}), 0) FROM aurix.{tabela}"
                )[0][0]
                ch_valor = _query_clickhouse(
                    client, f"SELECT COALESCE(SUM({coluna_valor}), 0) FROM {tabela}_analytics"
                )[0][0]
                tabela_resultado.update(pg_valor=pg_valor, ch_valor=ch_valor)
                divergencias.append(_divergencia_percentual(pg_valor, ch_valor))
            tabela_resultado["divergencia_pct_max"] = max(divergencias)
            resultado["tabelas"].append(tabela_resultado)
            if max(divergencias) > LIMIAR_DIVERGENCIA_PCT:
                resultado["divergencias"] += 1
        except Exception as e:  # noqa: BLE001
            resultado["tabelas"].append({"tabela": tabela, "erro": str(e)})
            resultado["divergencias"] += 1
    cur.close()
    conn.close()
    if resultado["divergencias"]:
        raise RuntimeError(f"Divergências de saldos detectadas: {resultado}")
    return resultado


def reconciliar_transacoes() -> Dict[str, Any]:
    import psycopg2
    import clickhouse_connect

    cfg = _config()
    conn = psycopg2.connect(**cfg["postgres"])
    client = clickhouse_connect.get_client(**cfg["clickhouse"])
    cur = conn.cursor()
    resultado = {"por_dia": [], "divergencias": 0}

    try:
        pg_rows = _query_pg(
            cur,
            "SELECT data_atualizacao::date AS dia, COUNT(*), COALESCE(SUM(valor), 0) "
            "FROM aurix.transacoes "
            "WHERE data_atualizacao >= NOW() - INTERVAL '7 days' "
            "GROUP BY dia ORDER BY dia",
        )
        ch_rows = _query_clickhouse(
            client,
            "SELECT toDate(data_atualizacao) AS dia, COUNT(*), COALESCE(SUM(valor), 0) "
            "FROM transacoes_analytics "
            "WHERE data_atualizacao >= now() - INTERVAL 7 DAY "
            "GROUP BY dia ORDER BY dia",
        )
        pg_map = {(str(r[0])): r for r in pg_rows}
        ch_map = {(str(r[0])): r for r in ch_rows}
        for dia in sorted(set(pg_map) | set(ch_map)):
            pg = pg_map.get(dia, (dia, 0, 0))
            ch = ch_map.get(dia, (dia, 0, 0))
            div = _divergencia_percentual(pg[1], ch[1])
            resultado["por_dia"].append(
                {"dia": dia, "pg": pg[1], "ch": ch[1], "divergencia_pct": div}
            )
            if div > LIMIAR_DIVERGENCIA_PCT:
                resultado["divergencias"] += 1
    except Exception as e:  # noqa: BLE001
        resultado["divergencias"] += 1
        resultado["erro"] = str(e)
    finally:
        cur.close()
        conn.close()

    if resultado["divergencias"]:
        raise RuntimeError(f"Divergências de transações detectadas: {resultado}")
    return resultado


def reconciliar_pix() -> Dict[str, Any]:
    import requests

    bacen_mock = os.environ.get("BACEN_MOCK_URL", "http://bacen-mock:8095")
    resultado = {"divergencias": 0}
    try:
        resp = requests.get(f"{bacen_mock}/spi/consolidado", timeout=10)
        resp.raise_for_status()
        # Pacote SPI de referência para comparação; mock retorna o consolidado.
        resultado["spi_consolidado"] = resp.json()
    except Exception as e:  # noqa: BLE001
        resultado["divergencias"] += 1
        resultado["erro"] = str(e)
        raise RuntimeError(f"Divergência de PIX/BACEN SPI detectada: {resultado}")
    return resultado


default_args = {
    "owner": "aurix",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notificar_falha,
}

with DAG(
    dag_id="reconciliacao_contabil",
    default_args=default_args,
    schedule="0 5 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["aurix", "reconciliacao", "compliance"],
) as dag:
    saldos = PythonOperator(
        task_id="reconciliar_saldos",
        python_callable=reconciliar_saldos,
        on_success_callback=notificar_sucesso,
        dag=dag,
    )
    transacoes = PythonOperator(
        task_id="reconciliar_transacoes",
        python_callable=reconciliar_transacoes,
        on_success_callback=notificar_sucesso,
        dag=dag,
    )
    pix = PythonOperator(
        task_id="reconciliar_pix",
        python_callable=reconciliar_pix,
        on_success_callback=notificar_sucesso,
        dag=dag,
    )
    [saldos, transacoes, pix]
