import pytest

from postgres_to_clickhouse import PostgresToClickHouseSync


@pytest.fixture
def sync():
    config = {
        "postgres": {
            "host": "localhost",
            "port": 5432,
            "database": "aurix",
            "user": "aurix",
            "password": "aurix123",
        },
        "clickhouse": {
            "host": "localhost",
            "port": 8123,
            "database": "aurix_analytics",
            "user": "aurix",
            "password": "aurix123",
        },
    }
    return PostgresToClickHouseSync(config)


class TestConstructor:
    def test_initializes_with_correct_config(self, sync):
        assert sync.postgres_config["host"] == "localhost"
        assert sync.postgres_config["port"] == 5432
        assert sync.postgres_config["database"] == "aurix"
        assert sync.postgres_config["user"] == "aurix"

    def test_initializes_with_correct_clickhouse_config(self, sync):
        assert sync.clickhouse_config["host"] == "localhost"
        assert sync.clickhouse_config["port"] == 8123
        assert sync.clickhouse_config["database"] == "aurix_analytics"

    def test_initializes_connections_to_none(self, sync):
        assert sync.postgres_conn is None
        assert sync.clickhouse_conn is None

    def test_initializes_sync_queue(self, sync):
        assert sync.sync_queue is not None

    def test_initializes_running_to_false(self, sync):
        assert sync.running is False

    def test_has_expected_tables_to_sync(self, sync):
        expected_tables = [
            "transacoes",
            "contas",
            "clientes",
            "solicitacoes_credito",
            "investimentos",
            "auditoria",
        ]
        assert sync.tables_to_sync == expected_tables


class TestTypeMapping:
    def test_type_mapping_has_expected_keys(self, sync):
        expected_keys = {
            "bigint",
            "integer",
            "smallint",
            "decimal",
            "numeric",
            "real",
            "double precision",
            "varchar",
            "text",
            "timestamp",
            "timestamp with time zone",
            "date",
            "boolean",
            "jsonb",
        }
        assert set(sync.type_mapping.keys()) == expected_keys

    def test_bigint_maps_to_int64(self, sync):
        assert sync.type_mapping["bigint"] == "Int64"

    def test_integer_maps_to_int32(self, sync):
        assert sync.type_mapping["integer"] == "Int32"

    def test_smallint_maps_to_int16(self, sync):
        assert sync.type_mapping["smallint"] == "Int16"

    def test_decimal_maps_to_decimal64(self, sync):
        assert sync.type_mapping["decimal"] == "Decimal64(4)"

    def test_numeric_maps_to_decimal64(self, sync):
        assert sync.type_mapping["numeric"] == "Decimal64(4)"

    def test_real_maps_to_float32(self, sync):
        assert sync.type_mapping["real"] == "Float32"

    def test_double_precision_maps_to_float64(self, sync):
        assert sync.type_mapping["double precision"] == "Float64"

    def test_varchar_maps_to_string(self, sync):
        assert sync.type_mapping["varchar"] == "String"

    def test_text_maps_to_string(self, sync):
        assert sync.type_mapping["text"] == "String"

    def test_timestamp_maps_to_datetime(self, sync):
        assert sync.type_mapping["timestamp"] == "DateTime"

    def test_timestamp_with_tz_maps_to_datetime(self, sync):
        assert sync.type_mapping["timestamp with time zone"] == "DateTime"

    def test_date_maps_to_date(self, sync):
        assert sync.type_mapping["date"] == "Date"

    def test_boolean_maps_to_uint8(self, sync):
        assert sync.type_mapping["boolean"] == "UInt8"

    def test_jsonb_maps_to_string(self, sync):
        assert sync.type_mapping["jsonb"] == "String"


class TestGetSyncStatus:
    def test_sync_status_has_expected_keys(self, sync):
        status = sync.get_sync_status()
        assert "running" in status
        assert "queue_size" in status
        assert "last_sync" in status
        assert "tables_synced" in status

    def test_sync_status_reflects_running_state(self, sync):
        assert sync.get_sync_status()["running"] is False
        sync.running = True
        assert sync.get_sync_status()["running"] is True

    def test_sync_status_tables_synced_count(self, sync):
        status = sync.get_sync_status()
        assert status["tables_synced"] == len(sync.tables_to_sync)
