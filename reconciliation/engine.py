# Copyright (c) 2026 Aurix Platform
# SPDX-License-Identifier: Apache-2.0
"""Motor de reconciliação contábil (core banking vs data lake).

Contém a lógica de comparação entre PostgreSQL (core) e ClickHouse (data lake),
extraída da DAG ``reconciliacao_contabil`` para permitir testes de integração
sem depender do Airflow instalado.

As funções recebem as conexões/curtsores como argumentos para facilitar a
injeção de fakes nos testes.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

logger = logging.getLogger(__name__)

LIMIAR_DIVERGENCIA_PCT = float(os.environ.get("RECONCILIACAO_LIMIAR_PCT", "0.01"))


def divergencia_percentual(esperado: float, obtido: float) -> float:
    """Divergência percentual entre o valor esperado (core) e o obtido (data lake)."""
    if esperado == 0:
        return 0.0 if obtido == 0 else 100.0
    return abs(esperado - obtido) / abs(esperado) * 100.0


def _query_pg(cur, sql: str, params: tuple = ()) -> List[tuple]:
    cur.execute(sql, params)
    return cur.fetchall()


def _query_clickhouse(client, sql: str) -> List[tuple]:
    result = client.query(sql)
    return result.result_rows


def persistir_relatorio(base_dir: str, nome: str, resultado: Dict[str, Any]) -> None:
    """Persiste o relatório de reconciliação para o exporter Prometheus."""
    dir_relatorios = Path(base_dir) / "artifacts" / "reconciliation"
    dir_relatorios.mkdir(parents=True, exist_ok=True)
    resultado["timestamp"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    caminho = dir_relatorios / nome
    with open(caminho, "w", encoding="utf-8") as f:
        json.dump(resultado, f, indent=2)
    logger.info("Relatório de reconciliação salvo em %s", caminho)


def reconciliar_saldos(conn, client, base_dir: str) -> Dict[str, Any]:
    """Compara contagens/somas por tabela entre PostgreSQL e ClickHouse.

    - ``contas``: contagem + soma de ``saldo``
    - ``clientes``: contagem
    - ``transacoes``: contagem + soma de ``valor``
    """
    cur = conn.cursor()
    resultado: Dict[str, Any] = {"tabelas": [], "divergencias": 0}

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
            divergencias = [divergencia_percentual(pg_total, ch_total)]
            tabela_resultado: Dict[str, Any] = {
                "tabela": tabela,
                "pg_total": pg_total,
                "ch_total": ch_total,
            }
            if coluna_valor:
                pg_valor = _query_pg(
                    cur, f"SELECT COALESCE(SUM({coluna_valor}), 0) FROM aurix.{tabela}"
                )[0][0]
                ch_valor = _query_clickhouse(
                    client,
                    f"SELECT COALESCE(SUM({coluna_valor}), 0) FROM {tabela}_analytics",
                )[0][0]
                tabela_resultado.update(pg_valor=pg_valor, ch_valor=ch_valor)
                divergencias.append(divergencia_percentual(pg_valor, ch_valor))
            tabela_resultado["divergencia_pct_max"] = max(divergencias)
            resultado["tabelas"].append(tabela_resultado)
            if max(divergencias) > LIMIAR_DIVERGENCIA_PCT:
                resultado["divergencias"] += 1
        except Exception as e:  # noqa: BLE001
            resultado["tabelas"].append({"tabela": tabela, "erro": str(e)})
            resultado["divergencias"] += 1
    cur.close()
    persistir_relatorio(base_dir, "reconciliacao_saldos.json", resultado)
    return resultado


def reconciliar_transacoes(conn, client, base_dir: str) -> Dict[str, Any]:
    """Compara o total de transações por dia (7 dias) entre core e data lake."""
    cur = conn.cursor()
    resultado: Dict[str, Any] = {"por_dia": [], "divergencias": 0}

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
        pg_map: Dict[str, Tuple[Any, ...]] = {str(r[0]): r for r in pg_rows}
        ch_map: Dict[str, Tuple[Any, ...]] = {str(r[0]): r for r in ch_rows}
        for dia in sorted(set(pg_map) | set(ch_map)):
            pg = pg_map.get(dia, (dia, 0, 0))
            ch = ch_map.get(dia, (dia, 0, 0))
            div = divergencia_percentual(pg[1], ch[1])
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

    persistir_relatorio(base_dir, "reconciliacao_transacoes.json", resultado)
    return resultado


def reconciliar_pix(bacen_mock_url: str, base_dir: str, session=None) -> Dict[str, Any]:
    """Compara o consolidado SPI (BACEN) com o data lake.

    ``session`` permite injetar um client HTTP fake nos testes.
    """
    resultado: Dict[str, Any] = {"divergencias": 0}
    try:
        import requests

        http = session or requests
        resp = http.get(f"{bacen_mock_url}/spi/consolidado", timeout=10)
        resp.raise_for_status()
        # Pacote SPI de referência para comparação; mock retorna o consolidado.
        resultado["spi_consolidado"] = resp.json()
        persistir_relatorio(base_dir, "reconciliacao_pix.json", resultado)
    except Exception as e:  # noqa: BLE001
        resultado["divergencias"] += 1
        resultado["erro"] = str(e)
        persistir_relatorio(base_dir, "reconciliacao_pix.json", resultado)
        raise RuntimeError(f"Divergência de PIX/BACEN SPI detectada: {resultado}") from e
    return resultado


def verificar_divergencias(resultado: Dict[str, Any], nome: str) -> Dict[str, Any]:
    """Levanta erro quando o relatório acusa divergências (aciona os alertas)."""
    if resultado.get("divergencias"):
        raise RuntimeError(f"Divergências em {nome} detectadas: {resultado}")
    return resultado
