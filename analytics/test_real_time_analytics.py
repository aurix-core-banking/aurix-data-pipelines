from datetime import datetime

import pytest

from real_time_analytics import RealTimeAnalytics


@pytest.fixture
def analytics():
    return RealTimeAnalytics({"redis": {}, "clickhouse": {}})


def test_constructor_initializes_metrics_with_correct_keys(analytics):
    expected_keys = {
        "transacoes_por_minuto",
        "valor_total_por_minuto",
        "taxa_aprovacao",
        "score_risco_medio",
        "transacoes_por_canal",
        "transacoes_por_tipo",
        "transacoes_por_estado",
        "alertas_risco",
        "tempo_resposta_medio",
        "erros_por_minuto",
    }
    assert set(analytics.metrics.keys()) == expected_keys


def test_constructor_initializes_metrics_history_correctly(analytics):
    expected_keys = {
        "timestamp",
        "transacoes_por_minuto",
        "valor_total_por_minuto",
        "taxa_aprovacao",
        "score_risco_medio",
    }
    assert set(analytics.metrics_history.keys()) == expected_keys
    assert analytics.metrics_history["timestamp"] == []


def test_constructor_initializes_numeric_metrics_to_zero(analytics):
    assert analytics.metrics["transacoes_por_minuto"] == 0
    assert analytics.metrics["valor_total_por_minuto"] == 0
    assert analytics.metrics["taxa_aprovacao"] == 0
    assert analytics.metrics["score_risco_medio"] == 0
    assert analytics.metrics["alertas_risco"] == 0
    assert analytics.metrics["tempo_resposta_medio"] == 0
    assert analytics.metrics["erros_por_minuto"] == 0


def test_constructor_initializes_dict_metrics_to_empty(analytics):
    assert analytics.metrics["transacoes_por_canal"] == {}
    assert analytics.metrics["transacoes_por_tipo"] == {}
    assert analytics.metrics["transacoes_por_estado"] == {}


def test_add_to_history_appends_current_metrics(analytics):
    analytics.metrics["transacoes_por_minuto"] = 10
    analytics.metrics["valor_total_por_minuto"] = 5000.0
    analytics.metrics["taxa_aprovacao"] = 0.95
    analytics.metrics["score_risco_medio"] = 0.3

    analytics._add_to_history()

    assert len(analytics.metrics_history["timestamp"]) == 1
    assert analytics.metrics_history["transacoes_por_minuto"] == [10]
    assert analytics.metrics_history["valor_total_por_minuto"] == [5000.0]
    assert analytics.metrics_history["taxa_aprovacao"] == [0.95]
    assert analytics.metrics_history["score_risco_medio"] == [0.3]


def test_add_to_history_preserves_timestamp_order(analytics):
    analytics.metrics["transacoes_por_minuto"] = 5
    analytics._add_to_history()
    before = analytics.metrics_history["timestamp"][0]

    analytics.metrics["transacoes_por_minuto"] = 8
    analytics._add_to_history()
    after = analytics.metrics_history["timestamp"][1]

    assert before < after


def test_add_to_history_trims_to_max_100_entries(analytics):
    analytics.metrics["transacoes_por_minuto"] = 1
    for _ in range(110):
        analytics._add_to_history()

    for key in analytics.metrics_history:
        assert len(analytics.metrics_history[key]) == 100


def test_add_to_history_keeps_most_recent_100_entries(analytics):
    for i in range(105):
        analytics.metrics["transacoes_por_minuto"] = i
        analytics._add_to_history()

    assert analytics.metrics_history["transacoes_por_minuto"][0] == 5
    assert analytics.metrics_history["transacoes_por_minuto"][-1] == 104


def test_constructor_queue_initialized(analytics):
    assert analytics.analytics_queue is not None


def test_constructor_running_defaults_to_false(analytics):
    assert analytics.running is False


def test_get_metrics_returns_current_metrics(analytics):
    result = analytics.get_metrics()
    assert result is analytics.metrics


def test_get_metrics_history_returns_current_history(analytics):
    result = analytics.get_metrics_history()
    assert result is analytics.metrics_history
