import json
from unittest.mock import patch

import pytest

from transactions_processor import TransactionProcessor


def test_map_valid_json_returns_enriched_json():
    input_data = {
        "id": 1,
        "conta_id": 1001,
        "tipo_transacao": "PIX",
        "valor": 500.0,
        "data_transacao": "2024-03-15T10:30:00",
        "status": "APROVADA",
        "canal": "MOBILE",
        "score_risco": 0.2,
    }
    processor = TransactionProcessor()
    result = json.loads(processor.map(json.dumps(input_data)))

    assert "processed_at" in result
    assert result["hour"] == 10
    assert result["day_of_week"] == 4
    assert result["is_high_value"] == 0
    assert result["is_business_hours"] == 1
    assert result["is_weekend"] == 0
    assert 0.0 <= result["calculated_risk_score"] <= 1.0


def test_map_malformed_json_returns_original_value():
    processor = TransactionProcessor()
    bad_input = "not-json-at-all"
    assert processor.map(bad_input) == bad_input


def test_map_missing_fields_handled_gracefully():
    processor = TransactionProcessor()
    incomplete = json.dumps({"id": 1})
    result = processor.map(incomplete)
    assert result == json.dumps({"id": 1})


def test_map_high_value_sets_flag():
    processor = TransactionProcessor()
    data = {
        "id": 2,
        "conta_id": 1002,
        "tipo_transacao": "TED",
        "valor": 1500.0,
        "data_transacao": "2024-03-15T14:00:00",
        "status": "APROVADA",
        "canal": "WEB",
        "score_risco": 0.1,
    }
    result = json.loads(processor.map(json.dumps(data)))
    assert result["is_high_value"] == 1


def test_map_low_value_does_not_set_high_value_flag():
    processor = TransactionProcessor()
    data = {
        "id": 3,
        "conta_id": 1003,
        "tipo_transacao": "PIX",
        "valor": 999.99,
        "data_transacao": "2024-03-15T14:00:00",
        "status": "APROVADA",
        "canal": "WEB",
        "score_risco": 0.1,
    }
    result = json.loads(processor.map(json.dumps(data)))
    assert result["is_high_value"] == 0


def test_map_boundary_value_exactly_1000():
    processor = TransactionProcessor()
    data = {
        "id": 4,
        "conta_id": 1004,
        "tipo_transacao": "DOC",
        "valor": 1000.0,
        "data_transacao": "2024-03-15T14:00:00",
        "status": "APROVADA",
        "canal": "WEB",
        "score_risco": 0.1,
    }
    result = json.loads(processor.map(json.dumps(data)))
    assert result["is_high_value"] == 0


def test_map_weekend_days():
    processor = TransactionProcessor()
    data = {
        "id": 5,
        "conta_id": 1005,
        "tipo_transacao": "PIX",
        "valor": 200.0,
        "data_transacao": "2024-03-16T12:00:00",
        "status": "APROVADA",
        "canal": "MOBILE",
        "score_risco": 0.0,
    }
    result = json.loads(processor.map(json.dumps(data)))
    assert result["is_weekend"] == 1
    assert result["day_of_week"] in (5, 6)


def test_map_weekday_is_not_weekend():
    processor = TransactionProcessor()
    data = {
        "id": 6,
        "conta_id": 1006,
        "tipo_transacao": "TED",
        "valor": 300.0,
        "data_transacao": "2024-03-14T12:00:00",
        "status": "APROVADA",
        "canal": "WEB",
        "score_risco": 0.0,
    }
    result = json.loads(processor.map(json.dumps(data)))
    assert result["is_weekend"] == 0


@pytest.mark.parametrize(
    "hour,expected",
    [
        (8, 0),
        (9, 1),
        (12, 1),
        (18, 1),
        (19, 0),
    ],
)
def test_map_business_hours(hour, expected):
    processor = TransactionProcessor()
    data = {
        "id": 7,
        "conta_id": 1007,
        "tipo_transacao": "PIX",
        "valor": 100.0,
        "data_transacao": f"2024-03-14T{hour:02d}:00:00",
        "status": "APROVADA",
        "canal": "WEB",
        "score_risco": 0.0,
    }
    result = json.loads(processor.map(json.dumps(data)))
    assert result["is_business_hours"] == expected


def test_map_risk_score_high_value_adds_risk():
    processor = TransactionProcessor()
    data = {
        "id": 8,
        "conta_id": 1008,
        "tipo_transacao": "PIX",
        "valor": 6000.0,
        "data_transacao": "2024-03-14T14:00:00",
        "status": "APROVADA",
        "canal": "WEB",
        "score_risco": 0.0,
    }
    result = json.loads(processor.map(json.dumps(data)))
    assert result["calculated_risk_score"] >= 0.3


def test_map_risk_score_capped_at_one():
    processor = TransactionProcessor()
    data = {
        "id": 9,
        "conta_id": 1009,
        "tipo_transacao": "PIX",
        "valor": 6000.0,
        "data_transacao": "2024-03-17T04:00:00",
        "status": "APROVADA",
        "canal": "MOBILE",
        "score_risco": 0.0,
    }
    result = json.loads(processor.map(json.dumps(data)))
    assert result["calculated_risk_score"] <= 1.0


def test_map_input_fields_preserved():
    processor = TransactionProcessor()
    input_data = {
        "id": 10,
        "conta_id": 1010,
        "tipo_transacao": "TED",
        "valor": 2500.0,
        "data_transacao": "2024-03-15T10:00:00",
        "status": "APROVADA",
        "canal": "WEB",
        "score_risco": 0.3,
    }
    result = json.loads(processor.map(json.dumps(input_data)))
    for key in input_data:
        assert result[key] == input_data[key]
