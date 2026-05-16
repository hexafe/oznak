from __future__ import annotations

import pytest

from oznak.config import load_database_profiles
from oznak.dialects import DatabaseDialect
from oznak.errors import OznakConfigurationError, OznakValidationError


def test_load_database_profiles_reads_current_yaml_shape(tmp_path):
    config_path = tmp_path / "databases.yaml"
    config_path.write_text(
        """
databases:
  database1:
    type: mysql
    host: db1.example.invalid
    port: 3306
    database: sample_process_db_1
    table: sample_measurements
  database2:
    type: mssql
    host: db2.example.invalid
    port: 1433
    database: sample_process_db_2
    table: sample_main_data
""".strip()
        + "\n",
        encoding="utf-8",
    )

    profiles = load_database_profiles(config_path)

    assert tuple(profiles.keys()) == ("database1", "database2")
    assert profiles["database1"].dialect is DatabaseDialect.MYSQL
    assert profiles["database2"].dialect is DatabaseDialect.MSSQL


def test_load_database_profiles_supports_optional_fields(tmp_path):
    config_path = tmp_path / "databases.yaml"
    config_path.write_text(
        """
databases:
  assembly:
    type: mysql
    host: db.example.invalid
    port: 3306
    database: process_db
    table: measurements
    allowed_columns: [id, status, updated_at]
    timestamp_column: updated_at
    pagination_column: id
    display_name: Assembly Line
    metadata:
      region: PL
      cell: A1
    connect_timeout_seconds: 3
    query_timeout_seconds: 7.5
    order_by_enabled: false
""".strip()
        + "\n",
        encoding="utf-8",
    )

    profile = load_database_profiles(config_path)["assembly"]

    assert profile.allowed_columns == ("id", "status", "updated_at")
    assert profile.timestamp_column == "updated_at"
    assert profile.pagination_column == "id"
    assert profile.display_name == "Assembly Line"
    assert profile.metadata["region"] == "PL"
    assert profile.metadata["cell"] == "A1"
    assert profile.connect_timeout_seconds == 3.0
    assert profile.query_timeout_seconds == 7.5
    assert profile.pool_size is None
    assert profile.max_overflow is None
    assert profile.pool_timeout_seconds is None
    assert profile.order_by_enabled is False


def test_load_database_profiles_supports_pool_tuning_fields(tmp_path):
    config_path = tmp_path / "databases.yaml"
    config_path.write_text(
        """
databases:
  assembly:
    type: mysql
    host: db.example.invalid
    port: 3306
    database: process_db
    table: measurements
    pool_size: 3
    max_overflow: 1
    pool_timeout_seconds: 4.5
""".strip()
        + "\n",
        encoding="utf-8",
    )

    profile = load_database_profiles(config_path)["assembly"]

    assert profile.pool_size == 3
    assert profile.max_overflow == 1
    assert profile.pool_timeout_seconds == 4.5


def test_load_export_profiles_reads_named_profiles(tmp_path):
    from oznak.config import load_export_profiles

    config_path = tmp_path / "databases.yaml"
    config_path.write_text(
        """
databases:
  assembly:
    type: mysql
    host: db.example.invalid
    port: 3306
    database: process_db
    table: measurements
export_profiles:
  compact_csv:
    format: csv
    delimiter: ";"
    include_metadata: true
  parquet_zstd:
    format: parquet
    compression: zstd
""".strip()
        + "\n",
        encoding="utf-8",
    )

    profiles = load_export_profiles(config_path)

    assert profiles["compact_csv"].delimiter == ";"
    assert profiles["compact_csv"].include_metadata is True
    assert profiles["parquet_zstd"].format == "parquet"
    assert profiles["parquet_zstd"].compression == "zstd"


def test_load_database_profiles_rejects_invalid_yaml(tmp_path):
    config_path = tmp_path / "databases.yaml"
    config_path.write_text("databases:\n  bad: [", encoding="utf-8")

    with pytest.raises(OznakConfigurationError, match="Invalid YAML"):
        load_database_profiles(config_path)


def test_load_database_profiles_requires_databases_section(tmp_path):
    config_path = tmp_path / "databases.yaml"
    config_path.write_text("profiles: {}", encoding="utf-8")

    with pytest.raises(OznakConfigurationError, match="databases"):
        load_database_profiles(config_path)


def test_load_database_profiles_requires_required_profile_keys(tmp_path):
    config_path = tmp_path / "databases.yaml"
    config_path.write_text(
        """
databases:
  assembly:
    type: mysql
    host: db.example.invalid
    port: 3306
    database: process_db
""".strip()
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(OznakConfigurationError, match="missing required keys: table"):
        load_database_profiles(config_path)


def test_load_database_profiles_reports_invalid_dialect_as_validation_error(tmp_path):
    config_path = tmp_path / "databases.yaml"
    config_path.write_text(
        """
databases:
  assembly:
    type: postgres
    host: db.example.invalid
    port: 5432
    database: process_db
    table: records
""".strip()
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(OznakValidationError, match="Unsupported database dialect"):
        load_database_profiles(config_path)


def test_load_database_profiles_rejects_invalid_identifiers(tmp_path):
    config_path = tmp_path / "databases.yaml"
    config_path.write_text(
        """
databases:
  assembly:
    type: mysql
    host: db.example.invalid
    port: 3306
    database: process-db
    table: records
""".strip()
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(OznakValidationError, match="Invalid database name"):
        load_database_profiles(config_path)


@pytest.mark.parametrize("value", ["10", 0, -5, False, True])
def test_load_database_profiles_rejects_invalid_connect_timeout(tmp_path, value):
    config_path = tmp_path / "databases.yaml"
    config_path.write_text(
        f"""
databases:
  assembly:
    type: mysql
    host: db.example.invalid
    port: 3306
    database: process_db
    table: records
    connect_timeout_seconds: {value!r}
""".strip()
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(OznakValidationError, match="connect_timeout_seconds"):
        load_database_profiles(config_path)


@pytest.mark.parametrize("value", ["10", 0, -5, False, True])
def test_load_database_profiles_rejects_invalid_query_timeout(tmp_path, value):
    config_path = tmp_path / "databases.yaml"
    config_path.write_text(
        f"""
databases:
  assembly:
    type: mysql
    host: db.example.invalid
    port: 3306
    database: process_db
    table: records
    query_timeout_seconds: {value!r}
""".strip()
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(OznakValidationError, match="query_timeout_seconds"):
        load_database_profiles(config_path)


@pytest.mark.parametrize("value", ["false", 0, 1, None])
def test_load_database_profiles_rejects_invalid_order_by_enabled(tmp_path, value):
    config_path = tmp_path / "databases.yaml"
    config_path.write_text(
        f"""
databases:
  assembly:
    type: mysql
    host: db.example.invalid
    port: 3306
    database: process_db
    table: records
    order_by_enabled: {value!r}
""".strip()
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(OznakValidationError, match="order_by_enabled"):
        load_database_profiles(config_path)


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("pool_size", 0),
        ("pool_size", -1),
        ("pool_size", "2"),
        ("max_overflow", -1),
        ("max_overflow", "1"),
        ("pool_timeout_seconds", 0),
        ("pool_timeout_seconds", "5"),
    ],
)
def test_load_database_profiles_rejects_invalid_pool_tuning_fields(tmp_path, field_name, value):
    config_path = tmp_path / "databases.yaml"
    config_path.write_text(
        f"""
databases:
  assembly:
    type: mysql
    host: db.example.invalid
    port: 3306
    database: process_db
    table: records
    {field_name}: {value!r}
""".strip()
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(OznakValidationError, match=field_name):
        load_database_profiles(config_path)
