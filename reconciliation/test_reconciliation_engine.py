# Copyright (c) 2026 Aurix Platform
# SPDX-License-Identifier: Apache-2.0
"""Testes de integração do motor de reconciliação contábil (core vs data lake).

Simula o PostgreSQL (core) e o ClickHouse (data lake) com fakes, exercitando a
lógica real de comparação, divergência e persistência dos relatórios.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from reconciliation.engine import (  # noqa: E402
    LIMIAR_DIVERGENCIA_PCT,
    divergencia_percentual,
    persistir_relatorio,
    reconciliar_pix,
    reconciliar_saldos,
    reconciliar_transacoes,
    verificar_divergencias,
)


class ResultadoFake:
    """Objeto com atributo ``result_rows`` compatível com o clickhouse-connect."""

    def __init__(self, rows):
        self.result_rows = rows


class ClickHouseFake:
    """Client ClickHouse fake: devolve linhas por SQL."""

    def __init__(self, consultas):
        self.consultas = consultas
        self.executadas = []

    def query(self, sql):
        self.executadas.append(sql)
        for prefixo, rows in self.consultas:
            if sql.startswith(prefixo):
                return ResultadoFake(rows)
        return ResultadoFake([])


class CursorFake:
    def __init__(self, consultas):
        self.consultas = consultas
        self.executadas = []
        self.fechado = False

    def execute(self, sql, params=()):
        self.executadas.append((sql, params))
        for prefixo, rows in self.consultas:
            if sql.startswith(prefixo):
                self._rows = rows
                return
        self._rows = []

    def fetchall(self):
        return self._rows

    def close(self):
        self.fechado = True


class ConexaoFake:
    def __init__(self, consultas):
        self._consultas = consultas
        self._cursor = None

    def cursor(self):
        if self._cursor is None:
            self._cursor = CursorFake(self._consultas)
        return self._cursor

    @property
    def cursor_fake(self):
        return self._cursor


@pytest.fixture()
def dir_relatorios(tmp_path):
    return str(tmp_path)


class TestDivergenciaPercentual:
    def test_zero_igual(self):
        assert divergencia_percentual(0, 0) == 0.0

    def test_zero_diferente(self):
        assert divergencia_percentual(0, 10) == 100.0

    def test_percentual(self):
        assert divergencia_percentual(1000, 1000) == 0.0
        assert divergencia_percentual(1000, 950) == pytest.approx(5.0)


class TestReconciliarSaldos:
    def test_sem_divergencias(self, dir_relatorios):
        pg = ConexaoFake([
            ("SELECT COUNT(*) FROM aurix.contas", [(100,)]),
            ("SELECT COALESCE(SUM(saldo), 0) FROM aurix.contas", [(5000.0,)]),
            ("SELECT COUNT(*) FROM aurix.clientes", [(50,)]),
            ("SELECT COUNT(*) FROM aurix.transacoes", [(1000,)]),
            ("SELECT COALESCE(SUM(valor), 0) FROM aurix.transacoes", [(25000.0,)]),
        ])
        ch = ClickHouseFake([
            ("SELECT COUNT(*) FROM contas_analytics", [(100,)]),
            ("SELECT COALESCE(SUM(saldo), 0) FROM contas_analytics", [(5000.0,)]),
            ("SELECT COUNT(*) FROM clientes_analytics", [(50,)]),
            ("SELECT COUNT(*) FROM transacoes_analytics", [(1000,)]),
            ("SELECT COALESCE(SUM(valor), 0) FROM transacoes_analytics", [(25000.0,)]),
        ])
        resultado = reconciliar_saldos(pg, ch, dir_relatorios)
        assert resultado["divergencias"] == 0
        assert {t["tabela"] for t in resultado["tabelas"]} == {"contas", "clientes", "transacoes"}
        assert all(t["divergencia_pct_max"] == 0.0 for t in resultado["tabelas"])
        assert pg.cursor_fake.fechado
        # Relatório persistido para o exporter
        rel = Path(dir_relatorios) / "artifacts" / "reconciliation" / "reconciliacao_saldos.json"
        assert rel.exists()

    def test_detecta_divergencia_de_valor(self, dir_relatorios):
        pg = ConexaoFake([
            ("SELECT COUNT(*) FROM aurix.contas", [(100,)]),
            ("SELECT COALESCE(SUM(saldo), 0) FROM aurix.contas", [(5000.0,)]),
            ("SELECT COUNT(*) FROM aurix.clientes", [(50,)]),
            ("SELECT COUNT(*) FROM aurix.transacoes", [(1000,)]),
            ("SELECT COALESCE(SUM(valor), 0) FROM aurix.transacoes", [(25000.0,)]),
        ])
        ch = ClickHouseFake([
            ("SELECT COUNT(*) FROM contas_analytics", [(100,)]),
            ("SELECT COALESCE(SUM(saldo), 0) FROM contas_analytics", [(4800.0,)]),
            ("SELECT COUNT(*) FROM clientes_analytics", [(50,)]),
            ("SELECT COUNT(*) FROM transacoes_analytics", [(1000,)]),
            ("SELECT COALESCE(SUM(valor), 0) FROM transacoes_analytics", [(25000.0,)]),
        ])
        resultado = reconciliar_saldos(pg, ch, dir_relatorios)
        assert resultado["divergencias"] == 1
        contas = next(t for t in resultado["tabelas"] if t["tabela"] == "contas")
        assert contas["divergencia_pct_max"] == pytest.approx(4.0)
        assert contas["pg_valor"] == 5000.0
        assert contas["ch_valor"] == 4800.0

    def test_erro_na_tabela_conta_como_divergencia(self, dir_relatorios):
        pg = ConexaoFake([
            ("SELECT COUNT(*) FROM aurix.contas", [(100,)]),
            ("SELECT COALESCE(SUM(saldo), 0) FROM aurix.contas", [(5000.0,)]),
            ("SELECT COUNT(*) FROM aurix.clientes", [(50,)]),
            ("SELECT COUNT(*) FROM aurix.transacoes", [(1000,)]),
            ("SELECT COALESCE(SUM(valor), 0) FROM aurix.transacoes", [(25000.0,)]),
        ])
        ch = ClickHouseFake([
            ("SELECT COUNT(*) FROM contas_analytics", [(100,)]),
            ("SELECT COUNT(*) FROM clientes_analytics", [(50,)]),
            ("SELECT COUNT(*) FROM transacoes_analytics", [(1000,)]),
            ("SELECT COALESCE(SUM(valor), 0) FROM transacoes_analytics", [(25000.0,)]),
        ])

        cur = pg.cursor()
        original = cur.execute

        def _quebrar(sql, params=()):
            if "SUM(saldo)" in sql:
                raise RuntimeError("clickhouse indisponível")
            return original(sql, params)

        cur.execute = _quebrar

        resultado = reconciliar_saldos(pg, ch, dir_relatorios)
        assert resultado["divergencias"] == 1
        assert any("erro" in t for t in resultado["tabelas"])


class TestReconciliarTransacoes:
    def _setup(self, pg_dias, ch_dias):
        pg = ConexaoFake([
            (
                "SELECT data_atualizacao::date",
                pg_dias,
            ),
        ])
        ch = ClickHouseFake([
            ("SELECT toDate(data_atualizacao)", ch_dias),
        ])
        return pg, ch

    def test_sem_divergencias(self, dir_relatorios):
        dias = [("2026-08-01", 120, 3000.0), ("2026-08-02", 95, 2400.0)]
        pg, ch = self._setup(dias, dias)
        resultado = reconciliar_transacoes(pg, ch, dir_relatorios)
        assert resultado["divergencias"] == 0
        assert len(resultado["por_dia"]) == 2
        assert all(d["divergencia_pct"] == 0.0 for d in resultado["por_dia"])
        assert pg.cursor_fake.fechado

    def test_detecta_divergencia_por_dia(self, dir_relatorios):
        pg, ch = self._setup([("2026-08-01", 120, 3000.0)], [("2026-08-01", 115, 2900.0)])
        resultado = reconciliar_transacoes(pg, ch, dir_relatorios)
        assert resultado["divergencias"] == 1
        dia = resultado["por_dia"][0]
        assert dia["pg"] == 120
        assert dia["ch"] == 115
        assert dia["divergencia_pct"] > LIMIAR_DIVERGENCIA_PCT

    def test_dia_somente_no_data_lake(self, dir_relatorios):
        pg, ch = self._setup([("2026-08-01", 120, 3000.0)], [("2026-08-02", 40, 800.0)])
        resultado = reconciliar_transacoes(pg, ch, dir_relatorios)
        assert resultado["divergencias"] >= 1
        dias = {d["dia"]: d for d in resultado["por_dia"]}
        assert dias["2026-08-02"]["pg"] == 0

    def test_erro_de_conexao_gera_divergencia(self, dir_relatorios):
        pg = ConexaoFake([])

        def _falhar(sql, params=()):
            raise RuntimeError("postgres fora do ar")

        cur = pg.cursor()
        cur.execute = _falhar
        ch = ClickHouseFake([])
        resultado = reconciliar_transacoes(pg, ch, dir_relatorios)
        assert resultado["divergencias"] == 1
        assert "erro" in resultado


class TestReconciliarPix:
    def test_sucesso_persiste_relatorio(self, dir_relatorios):
        class SessaoFake:
            def get(self, url, timeout=10):
                assert "/spi/consolidado" in url
                return _RespFake(200, {"total_pix": 5000})

        resultado = reconciliar_pix("http://bacen-mock:8095", dir_relatorios, session=SessaoFake())
        assert resultado["divergencias"] == 0
        assert resultado["spi_consolidado"]["total_pix"] == 5000
        rel = Path(dir_relatorios) / "artifacts" / "reconciliation" / "reconciliacao_pix.json"
        assert rel.exists()

    def test_falha_levanta_erro(self, dir_relatorios):
        class SessaoFalha:
            def get(self, url, timeout=10):
                return _RespFake(503, {})

        with pytest.raises(RuntimeError, match="Divergência de PIX"):
            reconciliar_pix("http://bacen-mock:8095", dir_relatorios, session=SessaoFalha())


class _RespFake:
    def __init__(self, status, body):
        self.status_code = status
        self._body = body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return self._body


class TestVerificarDivergencias:
    def test_sem_divergencias_retorna(self):
        assert verificar_divergencias({"divergencias": 0}, "saldos")["divergencias"] == 0

    def test_com_divergencias_levanta(self):
        with pytest.raises(RuntimeError, match="saldos"):
            verificar_divergencias({"divergencias": 2}, "saldos")


class TestPersistirRelatorio:
    def test_grava_json_com_timestamp(self, tmp_path):
        persistir_relatorio(str(tmp_path), "reconciliacao_saldos.json", {"divergencias": 0})
        rel = tmp_path / "artifacts" / "reconciliation" / "reconciliacao_saldos.json"
        import json

        dados = json.loads(rel.read_text())
        assert dados["divergencias"] == 0
        assert dados["timestamp"].endswith("Z")
