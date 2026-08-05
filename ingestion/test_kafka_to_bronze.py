import json
import sys
from unittest.mock import MagicMock, patch

from kafka_to_bronze import agrupar_por_topico, parse_mensagem


class MensagemFake:
    def __init__(self, topic, value, error=None):
        self._topic = topic
        self._value = value
        self._error = error

    def topic(self):
        return self._topic

    def value(self):
        return self._value

    def error(self):
        return self._error


class ConsumidorFake:
    def __init__(self, mensagens, erros=None):
        self._mensagens = list(mensagens)
        self._erros = erros or []
        self.subscribed = None
        self.commits = 0
        self.closed = False

    def subscribe(self, topicos):
        self.subscribed = topicos

    def poll(self, timeout=1.0):
        if self._mensagens:
            return self._mensagens.pop(0)
        if self._erros:
            return MensagemFake(None, None, error=self._erros.pop(0))
        return None

    def commit(self, asynchronous=False):
        self.commits += 1

    def unsubscribe(self):
        pass

    def close(self):
        self.closed = True


class ErroFake:
    def __init__(self, code, texto):
        self._code = code
        self._texto = texto

    def code(self):
        return self._code

    def __str__(self):
        return self._texto


class S3Fake:
    def __init__(self):
        self.puts = []

    def put_object(self, **kwargs):
        self.puts.append(kwargs)


def payload_debezium(op, after, ts_ms=1722830400000):
    return json.dumps(
        {
            "before": None if op != "d" else after,
            "after": None if op == "d" else after,
            "source": {"db": "aurix", "schema": "aurix", "table": "contas", "ts_ms": ts_ms},
            "op": op,
            "ts_ms": ts_ms,
            "transaction": None,
        }
    ).encode("utf-8")


class TestParseMensagem:
    def test_extrai_dados_do_after_em_creat(self):
        registro = parse_mensagem(
            payload_debezium("c", {"id": 1, "nome": "Maria", "saldo": 100.5})
        )
        assert registro["id"] == 1
        assert registro["nome"] == "Maria"
        assert registro["_cdc_op"] == "c"
        assert registro["_cdc_deleted"] is False
        assert registro["_cdc_ts"].startswith("2024-08-05")

    def test_extrai_dados_do_after_em_update(self):
        registro = parse_mensagem(
            payload_debezium("u", {"id": 2, "saldo": 200.0})
        )
        assert registro["id"] == 2
        assert registro["_cdc_op"] == "u"

    def test_delete_usa_before_e_marca_deleted(self):
        registro = parse_mensagem(
            payload_debezium("d", {"id": 3, "saldo": 0.0})
        )
        assert registro["id"] == 3
        assert registro["_cdc_deleted"] is True

    def test_payload_vazio_retorna_none(self):
        assert parse_mensagem(None) is None
        assert parse_mensagem(b"") is None

    def test_payload_invalido_retorna_none(self):
        assert parse_mensagem(b"nao-e-json") is None

    def test_payload_sem_after_retorna_none(self):
        assert (
            parse_mensagem(b'{"op": "r", "after": null, "source": {"ts_ms": 1}}')
            is None
        )


class TestAgruparPorTopico:
    def test_agrupa_preservando_ordem(self):
        registros = [
            ("cdc.aurix.contas", {"id": 1}),
            ("cdc.aurix.transacoes", {"id": 9}),
            ("cdc.aurix.contas", {"id": 2}),
        ]
        agrupado = agrupar_por_topico(registros)
        assert list(agrupado.keys()) == ["cdc.aurix.contas", "cdc.aurix.transacoes"]
        assert [r["id"] for r in agrupado["cdc.aurix.contas"]] == [1, 2]


class TestConsumirUmaVez:
    def test_consome_e_grava_parquet_por_topico(self):
        from kafka_to_bronze import consumir_uma_vez

        mensagens = [
            MensagemFake("cdc.aurix.contas", payload_debezium("c", {"id": 1, "nome": "A"})),
            MensagemFake("cdc.aurix.transacoes", payload_debezium("c", {"id": 9})),
            MensagemFake("cdc.aurix.contas", payload_debezium("u", {"id": 1, "nome": "A2"})),
        ]
        consumidor = ConsumidorFake(mensagens)
        s3 = S3Fake()

        with patch("kafka_to_bronze.escrever_bronze") as mock_escrever:
            total = consumir_uma_vez(consumidor, s3, "aurix-bronze", max_messages=100)

        assert total == 3
        assert consumidor.subscribed == [
            "cdc.aurix.contas",
            "cdc.aurix.clientes",
            "cdc.aurix.transacoes",
            "cdc.aurix.pix_pagamentos",
        ]
        assert consumidor.commits == 3
        topicos_gravados = {call.args[2] for call in mock_escrever.call_args_list}
        assert topicos_gravados == {"cdc.aurix.contas", "cdc.aurix.transacoes"}

    def test_respeita_max_messages(self):
        from kafka_to_bronze import consumir_uma_vez

        mensagens = [
            MensagemFake("cdc.aurix.contas", payload_debezium("c", {"id": i}))
            for i in range(10)
        ]
        consumidor = ConsumidorFake(mensagens)
        s3 = S3Fake()

        with patch("kafka_to_bronze.escrever_bronze"):
            total = consumir_uma_vez(consumidor, s3, "aurix-bronze", max_messages=3)

        assert total == 3

    def test_ignora_tombstone_e_mensagem_sem_dados(self):
        from kafka_to_bronze import consumir_uma_vez

        mensagens = [
            MensagemFake("cdc.aurix.contas", payload_debezium("c", {"id": 1})),
            MensagemFake("cdc.aurix.contas", None),
            MensagemFake("cdc.aurix.contas", payload_debezium("d", {"id": 1})),
        ]
        consumidor = ConsumidorFake(mensagens)
        s3 = S3Fake()

        with patch("kafka_to_bronze.escrever_bronze") as mock_escrever:
            total = consumir_uma_vez(consumidor, s3, "aurix-bronze", max_messages=100)

        assert total == 2
        registros = mock_escrever.call_args.args[3]
        assert any(r["_cdc_deleted"] for r in registros)

    def test_para_apos_eof_vazio(self):
        from kafka_to_bronze import KAFKA_EOF, consumir_uma_vez

        consumidor = ConsumidorFake([], erros=[ErroFake(KAFKA_EOF, "eof")])
        s3 = S3Fake()

        with patch("kafka_to_bronze.escrever_bronze") as mock_escrever:
            total = consumir_uma_vez(consumidor, s3, "aurix-bronze", max_messages=100)

        assert total == 0
        mock_escrever.assert_not_called()

    def test_para_com_timeout(self):
        from kafka_to_bronze import consumir_uma_vez

        consumidor = ConsumidorFake([])
        s3 = S3Fake()

        with patch("kafka_to_bronze.escrever_bronze") as mock_escrever:
            total = consumir_uma_vez(
                consumidor, s3, "aurix-bronze", max_messages=100, timeout=0.05
            )

        assert total == 0
        mock_escrever.assert_not_called()


class TestEscreverBronze:
    def test_usa_chave_e_conteudo_esperados(self):
        from kafka_to_bronze import escrever_bronze

        registros = [{"id": 1, "_cdc_op": "c"}]
        s3 = S3Fake()
        mock_pa = MagicMock()
        mock_pq = MagicMock()
        mock_buf = MagicMock()
        mock_buf.getvalue.return_value = b"parquet"
        mock_pa.Table.from_pylist.return_value = "tabela"
        mock_pa.BufferOutputStream.return_value = mock_buf

        with patch.dict(
            sys.modules, {"pyarrow": mock_pa, "pyarrow.parquet": mock_pq}
        ):
            chave = escrever_bronze(
                s3, "aurix-bronze", "cdc.aurix.contas", registros, prefixo="2026/08/05"
            )

        assert chave == "cdc/contas/2026/08/05/contas.parquet"
        assert s3.puts[0]["Bucket"] == "aurix-bronze"
        assert s3.puts[0]["Body"] == b"parquet"


class TestMain:
    def test_main_executa_fluxo_com_fakes(self):
        from kafka_to_bronze import main

        consumidor = ConsumidorFake(
            [MensagemFake("cdc.aurix.contas", payload_debezium("c", {"id": 1}))]
        )
        s3 = S3Fake()

        with patch("kafka_to_bronze.criar_consumidor", return_value=consumidor), patch(
            "kafka_to_bronze.criar_s3", return_value=s3
        ), patch("kafka_to_bronze.consumir_uma_vez") as mock_consumir:
            main()

        mock_consumir.assert_called_once()
        assert consumidor.closed is True
