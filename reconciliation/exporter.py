# Copyright (c) 2026 Aurix Platform
# SPDX-License-Identifier: Apache-2.0
"""Reconciliação contábil: exportador Prometheus das métricas de divergência.

Lê os relatórios de reconciliação (core banking vs data lake) gerados pelo DAG
`reconciliacao_contabil` em ``artifacts/reconciliation/*.json`` e os expõe como
gauges Prometheus para o dashboard de reconciliação no Grafana.

- Porta padrão: 9102 (exporter dedicado)
- Também suporta o formato ``textfile`` do node_exporter via ``--once``.

Métricas exportadas:

- ``aurix_reconciliacao_divergencias_total`` — nº de tabelas/dias com divergência
- ``aurix_reconciliacao_divergencia_pct`` — maior divergência percentual
  (rotulado por ``escopo``: saldos | transacoes | pix)
- ``aurix_reconciliacao_ok`` — 1 se a última execução não teve divergência
- ``aurix_reconciliacao_ultima_execucao`` — timestamp da última execução (epoch)

Uso:
    python -m reconciliation.exporter --config config/prometheus.yml
    python -m reconciliation.exporter --once
"""

import argparse
import json
import logging
import os
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional

from prometheus_client import CONTENT_TYPE_LATEST, Gauge, generate_latest

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

REPORTS_DIR_DEFAULT = "artifacts/reconciliation"
REPORT_FILES = ["reconciliacao_saldos.json", "reconciliacao_transacoes.json", "reconciliacao_pix.json"]

# Registro de gauges
METRICAS: Dict[str, Gauge] = {}


def _get_gauge(nome: str, descricao: str, rotulo: str = "") -> Gauge:
    chave = f"{nome}|{rotulo}"
    if chave not in METRICAS:
        if rotulo:
            METRICAS[chave] = Gauge(nome, descricao, [rotulo])
        else:
            METRICAS[chave] = Gauge(nome, descricao)
    return METRICAS[chave]


def carregar_relatorios(base_dir: Path) -> Dict[str, Any]:
    """Carrega os relatórios de reconciliação persistidos pelo DAG."""
    dir_relatorios = base_dir / REPORTS_DIR_DEFAULT
    dados: Dict[str, Any] = {}
    if not dir_relatorios.exists():
        return dados
    for nome in REPORT_FILES:
        caminho = dir_relatorios / nome
        if caminho.exists():
            with open(caminho, encoding="utf-8") as f:
                dados[nome] = json.load(f)
    return dados


def _maior_divergencia(relatorio: Dict[str, Any]) -> Optional[float]:
    """Retorna a maior divergência percentual do relatório (ou None)."""
    valores: List[float] = []
    for tabela in relatorio.get("tabelas", []):
        if "divergencia_pct_max" in tabela:
            valores.append(float(tabela["divergencia_pct_max"]))
    for dia in relatorio.get("por_dia", []):
        if "divergencia_pct" in dia:
            valores.append(float(dia["divergencia_pct"]))
    if relatorio.get("divergencias"):
        valores.append(100.0)
    if not valores:
        return None
    return max(valores)


def _contar_divergencias(relatorio: Dict[str, Any]) -> int:
    if "divergencias" in relatorio:
        return int(relatorio["divergencias"])
    return sum(1 for t in relatorio.get("tabelas", []) if t.get("divergencia_pct_max", 0) > 0)


def exportar_para_prometheus(base_dir: Path) -> None:
    """Atualiza os gauges a partir dos relatórios de reconciliação."""
    dados = carregar_relatorios(base_dir)

    escopos = {"saldos": "reconciliacao_saldos.json", "transacoes": "reconciliacao_transacoes.json", "pix": "reconciliacao_pix.json"}
    total_divergencias = 0
    execucoes = []

    for escopo, nome in escopos.items():
        relatorio = dados.get(nome, {})
        div = _contar_divergencias(relatorio)
        total_divergencias += div
        maior = _maior_divergencia(relatorio) or 0.0
        _get_gauge("aurix_reconciliacao_divergencias_total", "Nº de divergências", "escopo").labels(escopo=escopo).set(div)
        _get_gauge("aurix_reconciliacao_divergencia_pct", "Maior divergência percentual", "escopo").labels(escopo=escopo).set(maior)
        _get_gauge("aurix_reconciliacao_ok", "1 se execução sem divergência", "escopo").labels(escopo=escopo).set(1.0 if div == 0 else 0.0)
        if relatorio.get("timestamp"):
            execucoes.append(relatorio["timestamp"])

    _get_gauge("aurix_reconciliacao_divergencias_global", "Nº total de divergências").set(total_divergencias)
    _get_gauge("aurix_reconciliacao_ok_global", "1 se todas as execuções sem divergência").set(1.0 if total_divergencias == 0 else 0.0)

    ultima = max(execucoes) if execucoes else ""
    if ultima:
        _get_gauge("aurix_reconciliacao_ultima_execucao", "Timestamp da última execução").set(_to_epoch(ultima))

    # Persistência textfile (node_exporter)
    textfile_dir = Path(os.environ.get("PROMETHEUS_TEXTFILE_DIR", str(base_dir / "artifacts" / "prometheus")))
    textfile_dir.mkdir(parents=True, exist_ok=True)
    caminho = textfile_dir / "aurix_reconciliacao.prom"
    with open(caminho, "wb") as f:
        f.write(generate_latest())
    logger.info("Métricas de reconciliação exportadas para %s", caminho)


def _to_epoch(iso: str) -> float:
    """Converte timestamp ISO para epoch seconds."""
    try:
        from datetime import datetime

        return datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp()
    except Exception:  # noqa: BLE001
        return 0.0


def _setup_handler(base_dir: Path):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            if self.path == "/metrics":
                exportar_para_prometheus(base_dir)
                self.send_response(200)
                self.send_header("Content-Type", CONTENT_TYPE_LATEST)
                self.end_headers()
                self.wfile.write(generate_latest())
            elif self.path == "/health":
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"ok")
            else:
                self.send_response(404)
                self.end_headers()

        def log_message(self, *args):  # noqa: A003
            logger.info("HTTP %s", args)

    return Handler


def main():
    parser = argparse.ArgumentParser(description="Exporter Prometheus de métricas de reconciliação contábil")
    parser.add_argument("--base-dir", default=str(Path(__file__).resolve().parents[1]))
    parser.add_argument("--port", type=int, default=int(os.environ.get("RECONCILIACAO_EXPORTER_PORT", "9102")))
    parser.add_argument("--once", action="store_true", help="Exporta textfile e sai")
    args = parser.parse_args()

    base_dir = Path(args.base_dir)
    exportar_para_prometheus(base_dir)

    if args.once:
        return

    server = HTTPServer(("0.0.0.0", args.port), _setup_handler(base_dir))
    logger.info("Reconciliação exporter ouvindo em :%d/metrics", args.port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.shutdown()


if __name__ == "__main__":
    main()
