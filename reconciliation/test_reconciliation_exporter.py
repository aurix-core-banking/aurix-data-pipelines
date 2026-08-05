# Copyright (c) 2026 Aurix Platform
# SPDX-License-Identifier: Apache-2.0
"""Testes unitários do exporter Prometheus de reconciliação contábil."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from reconciliation.exporter import (  # noqa: E402
    carregar_relatorios,
    exportar_para_prometheus,
    _contar_divergencias,
    _maior_divergencia,
    _to_epoch,
)


@pytest.fixture()
def relatorios(tmp_path) -> Path:
    dir_rel = tmp_path / "artifacts" / "reconciliation"
    dir_rel.mkdir(parents=True)
    (dir_rel / "reconciliacao_saldos.json").write_text(
        json.dumps({"tabelas": [{"tabela": "contas", "divergencia_pct_max": 0.5}], "divergencias": 1})
    )
    (dir_rel / "reconciliacao_transacoes.json").write_text(
        json.dumps({"por_dia": [{"dia": "2026-08-01", "divergencia_pct": 0.0}], "divergencias": 0})
    )
    (dir_rel / "reconciliacao_pix.json").write_text(
        json.dumps({"divergencias": 0, "spi_consolidado": {}})
    )
    return tmp_path


class TestCarregarRelatorios:
    def test_sem_relatorios(self, tmp_path):
        assert carregar_relatorios(tmp_path) == {}

    def test_carrega_relatorios(self, relatorios):
        dados = carregar_relatorios(relatorios)
        assert set(dados) == {
            "reconciliacao_saldos.json",
            "reconciliacao_transacoes.json",
            "reconciliacao_pix.json",
        }


class TestMetricasAuxiliares:
    def test_maior_divergencia_por_tabela(self):
        rel = {"tabelas": [{"divergencia_pct_max": 1.2}, {"divergencia_pct_max": 0.3}]}
        assert _maior_divergencia(rel) == 1.2

    def test_maior_divergencia_por_dia(self):
        rel = {"por_dia": [{"divergencia_pct": 0.1}, {"divergencia_pct": 0.9}]}
        assert _maior_divergencia(rel) == 0.9

    def test_maior_divergencia_sem_dados(self):
        assert _maior_divergencia({}) is None

    def test_contar_divergencias(self):
        assert _contar_divergencias({"divergencias": 3}) == 3
        assert _contar_divergencias({}) == 0

    def test_to_epoch(self):
        assert _to_epoch("2026-08-04T12:00:00+00:00") == pytest.approx(1785844800)


class TestExporter:
    def test_exporta_textfile(self, relatorios, monkeypatch):
        monkeypatch.setenv("PROMETHEUS_TEXTFILE_DIR", str(relatorios / "artifacts" / "prometheus"))
        exportar_para_prometheus(relatorios)
        prom = relatorios / "artifacts" / "prometheus" / "aurix_reconciliacao.prom"
        assert prom.exists()
        conteudo = prom.read_text()
        assert "aurix_reconciliacao_divergencias_total" in conteudo
        assert "aurix_reconciliacao_divergencia_pct" in conteudo
        assert "aurix_reconciliacao_ok" in conteudo
        # Saldos: 1 divergência; transações/pix: 0
        assert 'aurix_reconciliacao_divergencias_total{escopo="saldos"} 1.0' in conteudo
        assert 'aurix_reconciliacao_divergencias_total{escopo="transacoes"} 0.0' in conteudo

    def test_exporta_sem_relatorios_nao_falha(self, tmp_path, monkeypatch):
        monkeypatch.setenv("PROMETHEUS_TEXTFILE_DIR", str(tmp_path / "prom"))
        exportar_para_prometheus(tmp_path)
        prom = tmp_path / "prom" / "aurix_reconciliacao.prom"
        assert prom.exists()
