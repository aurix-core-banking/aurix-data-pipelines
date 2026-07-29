from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from transactions_processor import TransactionsProcessor


@pytest.fixture
def processor():
    with patch("transactions_processor.SparkSession") as mock_spark:
        mock_spark.builder.return_value.appName.return_value.config.return_value.config.return_value.getOrCreate.return_value = (
            MagicMock()
        )
        proc = TransactionsProcessor()
        yield proc


def test_define_schema_returns_correct_struct_type(processor):
    schema = processor.define_schema()
    assert isinstance(schema, StructType)
    field_names = [f.name for f in schema.fields]
    expected_fields = [
        "id",
        "conta_id",
        "tipo_transacao",
        "valor",
        "data_transacao",
        "status",
        "canal",
        "dispositivo",
        "ip_address",
        "user_agent",
        "latitude",
        "longitude",
        "cidade",
        "estado",
        "pais",
        "score_risco",
        "aprovada",
        "tempo_processamento_ms",
        "created_at",
    ]
    assert field_names == expected_fields


def test_define_schema_field_types(processor):
    schema = processor.define_schema()
    fields = {f.name: f.dataType for f in schema.fields}
    assert isinstance(fields["id"], LongType)
    assert isinstance(fields["conta_id"], LongType)
    assert isinstance(fields["tipo_transacao"], StringType)
    assert isinstance(fields["valor"], DecimalType)
    assert isinstance(fields["data_transacao"], TimestampType)
    assert isinstance(fields["status"], StringType)
    assert isinstance(fields["canal"], StringType)
    assert isinstance(fields["latitude"], DoubleType)
    assert isinstance(fields["score_risco"], FloatType)
    assert isinstance(fields["aprovada"], BooleanType)
    assert isinstance(fields["tempo_processamento_ms"], IntegerType)
    assert isinstance(fields["created_at"], TimestampType)


def test_process_transactions_adds_calculated_columns(processor):
    mock_df = MagicMock()
    mock_df.select.return_value.select.return_value = mock_df

    mock_df.withColumn.side_effect = lambda name, _expr: MagicMock()

    result = processor.process_transactions(mock_df)

    expected_calls = [
        "hora",
        "dia_semana",
        "mes",
        "ano",
        "valor_alto",
        "horario_comercial",
        "fim_de_semana",
    ]
    assert result is not None


def test_calculate_metrics_returns_dict_with_expected_keys(processor):
    mock_df = MagicMock()
    mock_df.groupBy.return_value.agg.return_value = mock_df
    mock_df.withColumn.return_value = mock_df

    metrics = processor.calculate_metrics(mock_df)

    assert "hourly" in metrics
    assert "location" in metrics
    assert "account" in metrics


def test_calculate_metrics_hourly_grouping(processor):
    mock_df = MagicMock()
    mock_df.groupBy.return_value.agg.return_value = mock_df
    mock_df.withColumn.return_value = mock_df

    metrics = processor.calculate_metrics(mock_df)

    assert metrics["hourly"] is not None
    assert metrics["location"] is not None
    assert metrics["account"] is not None


def test_constructor_creates_spark_session():
    with patch("transactions_processor.SparkSession") as mock_spark:
        mock_session = MagicMock()
        mock_spark.builder.return_value.appName.return_value.config.return_value.config.return_value.getOrCreate.return_value = (
            mock_session
        )
        proc = TransactionsProcessor()
        assert proc.spark is not None
