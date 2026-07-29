from unittest.mock import patch

import pytest


@patch("pg_to_bronze.pd.read_sql_table")
@patch("pg_to_bronze.create_engine")
@patch("pg_to_bronze.boto3.client")
@patch("pg_to_bronze.os.environ.get")
def test_main_reads_env_vars(mock_get, mock_boto3, mock_engine, mock_read_sql):
    env_values = {
        "PG_HOST": "test_host",
        "PG_PORT": "5432",
        "PG_USER": "test_user",
        "PG_PASSWORD": "test_pass",
        "PG_DATABASE": "test_db",
        "PG_SCHEMA": "test_schema",
        "MINIO_ENDPOINT": "http://test:9000",
        "MINIO_ACCESS_KEY": "test_key",
        "MINIO_SECRET_KEY": "test_secret",
        "BRONZE_BUCKET": "test-bronze",
    }
    mock_get.side_effect = lambda key, default=None: env_values.get(key, default)

    from pg_to_bronze import main

    main()

    engine_args = mock_engine.call_args[0][0]
    assert "test_host" in engine_args
    assert "test_user" in engine_args
    assert "test_pass" in engine_args
    assert "test_db" in engine_args


@patch("pg_to_bronze.pd.read_sql_table")
@patch("pg_to_bronze.create_engine")
@patch("pg_to_bronze.boto3.client")
@patch("pg_to_bronze.os.environ.get")
def test_main_connects_to_minio_with_env_vars(
    mock_get, mock_boto3, mock_engine, mock_read_sql
):
    env_values = {
        "MINIO_ENDPOINT": "http://minio:9000",
        "MINIO_ACCESS_KEY": "custom_key",
        "MINIO_SECRET_KEY": "custom_secret",
        "PG_HOST": "localhost",
    }
    mock_get.side_effect = lambda key, default=None: env_values.get(key, default)

    from pg_to_bronze import main

    main()

    mock_boto3.assert_called_once_with(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="custom_key",
        aws_secret_access_key="custom_secret",
        region_name="us-east-1",
    )


@patch("pg_to_bronze.pd.read_sql_table")
@patch("pg_to_bronze.create_engine")
@patch("pg_to_bronze.boto3.client")
@patch("pg_to_bronze.os.environ.get")
def test_main_uses_default_env_values(mock_get, mock_engine, mock_boto3, mock_read_sql):
    mock_get.side_effect = lambda key, default=None: default

    from pg_to_bronze import main

    main()

    engine_args = mock_engine.call_args[0][0]
    assert "localhost" in engine_args
    assert "aurix_user" in engine_args
    assert "aurix_secure_password" in engine_args
    assert "aurix" in engine_args


@patch("pg_to_bronze.pd.read_sql_table")
@patch("pg_to_bronze.create_engine")
@patch("pg_to_bronze.boto3.client")
@patch("pg_to_bronze.os.environ.get")
def test_main_iterates_over_expected_tables(
    mock_get, mock_boto3, mock_engine, mock_read_sql
):
    mock_get.side_effect = lambda key, default=None: default
    mock_read_sql.return_value = type("DF", (), {"__len__": lambda s: 0})()

    from pg_to_bronze import main

    main()

    called_tables = [call[0][0] for call in mock_read_sql.call_args_list]
    assert "contas" in called_tables
    assert "clientes" in called_tables
    assert "transacoes" in called_tables


@patch("pg_to_bronze.pd.read_sql_table")
@patch("pg_to_bronze.create_engine")
@patch("pg_to_bronze.boto3.client")
@patch("pg_to_bronze.os.environ.get")
def test_main_skips_tables_on_error(mock_get, mock_boto3, mock_engine, mock_read_sql):
    mock_get.side_effect = lambda key, default=None: default
    mock_read_sql.side_effect = [Exception("fail"), Exception("fail"), Exception("fail")]

    from pg_to_bronze import main

    main()

    assert mock_read_sql.call_count == 3


@patch("pg_to_bronze.pd.read_sql_table")
@patch("pg_to_bronze.create_engine")
@patch("pg_to_bronze.boto3.client")
@patch("pg_to_bronze.os.environ.get")
def test_main_creates_engine_with_correct_url(
    mock_get, mock_boto3, mock_engine, mock_read_sql
):
    env_values = {
        "PG_HOST": "myhost",
        "PG_PORT": "1234",
        "PG_USER": "myuser",
        "PG_PASSWORD": "mypass",
        "PG_DATABASE": "mydb",
    }
    mock_get.side_effect = lambda key, default=None: env_values.get(key, default)

    from pg_to_bronze import main

    main()

    expected_url = (
        "postgresql+psycopg2://myuser:mypass@myhost:1234/mydb"
    )
    mock_engine.assert_called_once_with(expected_url)
