from __future__ import annotations

import os
import time

import pytest
from sqlalchemy import text

from oznak.credentials import Credentials, MappingCredentialProvider
from oznak.engines import create_sqlalchemy_engine
from oznak.fetcher import fetch_records, fetch_records_chunked
from oznak.filters import QueryFilter
from oznak.profiles import DatabaseProfile
from oznak.result import FetchRequest

pytestmark = pytest.mark.integration

RUN_FLAG = "OZNAK_RUN_LIVE_DB_TESTS"
TRUTHY = {"1", "true", "yes", "on"}


@pytest.fixture(autouse=True)
def require_live_database_opt_in() -> None:
    if os.getenv(RUN_FLAG, "").strip().lower() not in TRUTHY:
        pytest.skip(f"Set {RUN_FLAG}=1 and start docker-compose.integration.yml to run live DB tests")


def test_mysql_live_fetch_and_chunked_export_contract() -> None:
    profile = _mysql_profile()
    credentials = Credentials(
        username=os.getenv("OZNAK_IT_MYSQL_USER", "oznak"),
        password=os.getenv("OZNAK_IT_MYSQL_PASSWORD", "oznak_password"),
    )
    engine = _wait_for_engine(profile, credentials)
    _seed_records(engine, dialect="mysql")

    provider = MappingCredentialProvider({profile.alias: (credentials.username, credentials.password)})

    result = fetch_records(
        FetchRequest(
            profiles=(profile,),
            filters=(QueryFilter("status", "=", "ACTIVE"),),
            columns=("id", "reference", "status", "updated_at"),
            limit=10,
            date_column="updated_at",
        ),
        credential_provider=provider,
    )

    assert result.errors == ()
    assert result.row_count == 2
    assert result.data["reference"].tolist() == ["A3", "A1"]
    assert result.data["source_database"].tolist() == [profile.alias, profile.alias]

    chunked = fetch_records_chunked(
        FetchRequest(
            profiles=(profile,),
            columns=("id", "reference", "status", "updated_at"),
        ),
        chunk_size=2,
        credential_provider=provider,
    )

    assert chunked.errors == ()
    assert chunked.row_count == 3
    assert chunked.data["id"].tolist() == [1, 2, 3]
    assert chunked.source_results[0].metadata["chunk_count"] == 2


def test_mssql_live_fetch_contract() -> None:
    _skip_without_pyodbc_driver()
    profile = _mssql_profile()
    credentials = Credentials(
        username=os.getenv("OZNAK_IT_MSSQL_USER", "sa"),
        password=os.getenv("OZNAK_IT_MSSQL_PASSWORD", "Oznak_Strong_Passw0rd!"),
    )
    engine = _wait_for_engine(profile, credentials, timeout_seconds=90)
    _seed_records(engine, dialect="mssql")

    provider = MappingCredentialProvider({profile.alias: (credentials.username, credentials.password)})
    result = fetch_records(
        FetchRequest(
            profiles=(profile,),
            filters=(QueryFilter("status", "=", "ACTIVE"),),
            columns=("id", "reference", "status", "updated_at"),
            limit=10,
            date_column="updated_at",
        ),
        credential_provider=provider,
    )

    assert result.errors == ()
    assert result.row_count == 2
    assert result.data["reference"].tolist() == ["A3", "A1"]


def _mysql_profile() -> DatabaseProfile:
    return DatabaseProfile(
        alias="mysql_it",
        dialect="mysql",
        host=os.getenv("OZNAK_IT_MYSQL_HOST", "127.0.0.1"),
        port=_env_int("OZNAK_IT_MYSQL_PORT", 3307),
        database=os.getenv("OZNAK_IT_MYSQL_DATABASE", "oznak_it"),
        table="records",
        allowed_columns=("id", "reference", "status", "updated_at"),
        timestamp_column="updated_at",
        pagination_column="id",
        connect_timeout_seconds=5,
        query_timeout_seconds=10,
        pool_size=1,
        max_overflow=0,
    )


def _mssql_profile() -> DatabaseProfile:
    return DatabaseProfile(
        alias="mssql_it",
        dialect="mssql",
        host=os.getenv("OZNAK_IT_MSSQL_HOST", "127.0.0.1"),
        port=_env_int("OZNAK_IT_MSSQL_PORT", 14333),
        database=os.getenv("OZNAK_IT_MSSQL_DATABASE", "master"),
        table="records",
        allowed_columns=("id", "reference", "status", "updated_at"),
        timestamp_column="updated_at",
        pagination_column="id",
        connect_timeout_seconds=5,
        query_timeout_seconds=10,
        pool_size=1,
        max_overflow=0,
    )


def _wait_for_engine(
    profile: DatabaseProfile,
    credentials: Credentials,
    *,
    timeout_seconds: float = 45,
):
    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        engine = create_sqlalchemy_engine(profile, credentials)
        try:
            with engine.begin() as connection:
                connection.execute(text("SELECT 1"))
            return engine
        except Exception as exc:  # pragma: no cover - exercised only with live services.
            last_error = exc
            engine.dispose()
            time.sleep(2)
    raise AssertionError(f"Timed out waiting for {profile.alias}: {last_error}")


def _seed_records(engine, *, dialect: str) -> None:
    if dialect == "mssql":
        statements = [
            "IF OBJECT_ID('records', 'U') IS NOT NULL DROP TABLE records",
            """
            CREATE TABLE records (
                id INT NOT NULL PRIMARY KEY,
                reference NVARCHAR(50) NOT NULL,
                status NVARCHAR(20) NOT NULL,
                updated_at DATETIME2 NOT NULL
            )
            """,
        ]
    else:
        statements = [
            "DROP TABLE IF EXISTS records",
            """
            CREATE TABLE records (
                id INT NOT NULL PRIMARY KEY,
                reference VARCHAR(50) NOT NULL,
                status VARCHAR(20) NOT NULL,
                updated_at DATETIME NOT NULL
            )
            """,
        ]

    rows = [
        {"id": 1, "reference": "A1", "status": "ACTIVE", "updated_at": "2026-01-01 10:00:00"},
        {"id": 2, "reference": "A2", "status": "SCRAP", "updated_at": "2026-01-01 11:00:00"},
        {"id": 3, "reference": "A3", "status": "ACTIVE", "updated_at": "2026-01-01 12:00:00"},
    ]
    with engine.begin() as connection:
        for statement in statements:
            connection.execute(text(statement))
        connection.execute(
            text(
                """
                INSERT INTO records (id, reference, status, updated_at)
                VALUES (:id, :reference, :status, :updated_at)
                """
            ),
            rows,
        )


def _skip_without_pyodbc_driver() -> None:
    pyodbc = pytest.importorskip("pyodbc")
    if "ODBC Driver 17 for SQL Server" not in pyodbc.drivers():
        pytest.skip("ODBC Driver 17 for SQL Server is required for MSSQL live integration tests")


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    return int(raw)
