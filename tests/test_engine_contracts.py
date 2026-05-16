from __future__ import annotations

import pytest

from oznak.credentials import Credentials
from oznak.engines import create_sqlalchemy_engine
from oznak.errors import OznakConfigurationError
from oznak.profiles import DatabaseProfile


def _mysql_profile() -> DatabaseProfile:
    return DatabaseProfile(
        alias="assembly_mysql",
        dialect="mysql",
        host="mysql.example.invalid",
        port=3306,
        database="process_db",
        table="records",
        connect_timeout_seconds=2,
        query_timeout_seconds=7.2,
    )


def _mssql_profile() -> DatabaseProfile:
    return DatabaseProfile(
        alias="assembly_mssql",
        dialect="mssql",
        host="mssql.example.invalid",
        port=1433,
        database="process_db",
        table="records",
        connect_timeout_seconds=4.1,
        query_timeout_seconds=9,
    )


def test_create_sqlalchemy_engine_builds_mysql_url(monkeypatch):
    calls: list[dict[str, object]] = []

    def fake_create_engine(url: str, **kwargs):
        calls.append({"url": url, "kwargs": kwargs})
        return "mysql-engine"

    monkeypatch.setattr("oznak.engines.create_engine", fake_create_engine)

    result = create_sqlalchemy_engine(
        _mysql_profile(),
        Credentials(username="svc user", password="p@ss/word"),
    )

    assert result == "mysql-engine"
    assert calls == [
        {
            "url": "mysql+pymysql://svc+user:p%40ss%2Fword@mysql.example.invalid:3306/process_db",
            "kwargs": {
                "echo": False,
                "pool_pre_ping": True,
                "connect_args": {
                    "connect_timeout": 2,
                    "read_timeout": 8,
                    "write_timeout": 8,
                },
            },
        }
    ]


def test_create_sqlalchemy_engine_builds_mssql_url(monkeypatch):
    calls: list[dict[str, object]] = []

    def fake_create_engine(url: str, **kwargs):
        calls.append({"url": url, "kwargs": kwargs})
        return "mssql-engine"

    monkeypatch.setattr("oznak.engines.create_engine", fake_create_engine)

    result = create_sqlalchemy_engine(
        _mssql_profile(),
        Credentials(username="svc.user", password="S3cret!"),
    )

    assert result == "mssql-engine"
    assert calls == [
        {
            "url": (
                "mssql+pyodbc://svc.user:S3cret%21@mssql.example.invalid:1433/process_db"
                "?driver=ODBC+Driver+17+for+SQL+Server&login_timeout=5"
            ),
            "kwargs": {"echo": False, "pool_pre_ping": True, "connect_args": {"timeout": 9}},
        }
    ]


def test_create_sqlalchemy_engine_omits_optional_timeout_parameters(monkeypatch):
    calls: list[dict[str, object]] = []

    def fake_create_engine(url: str, **kwargs):
        calls.append({"url": url, "kwargs": kwargs})
        return "mysql-engine"

    monkeypatch.setattr("oznak.engines.create_engine", fake_create_engine)

    profile = DatabaseProfile(
        alias="assembly_mysql",
        dialect="mysql",
        host="mysql.example.invalid",
        port=3306,
        database="process_db",
        table="records",
    )
    create_sqlalchemy_engine(profile, Credentials(username="svc", password="secret"))

    assert calls == [
        {
            "url": "mysql+pymysql://svc:secret@mysql.example.invalid:3306/process_db",
            "kwargs": {"echo": False, "pool_pre_ping": True},
        }
    ]


def test_create_sqlalchemy_engine_applies_pool_tuning(monkeypatch):
    calls: list[dict[str, object]] = []

    def fake_create_engine(url: str, **kwargs):
        calls.append({"url": url, "kwargs": kwargs})
        return "mysql-engine"

    monkeypatch.setattr("oznak.engines.create_engine", fake_create_engine)

    profile = DatabaseProfile(
        alias="assembly_mysql",
        dialect="mysql",
        host="mysql.example.invalid",
        port=3306,
        database="process_db",
        table="records",
        pool_size=2,
        max_overflow=0,
        pool_timeout_seconds=3.1,
    )

    create_sqlalchemy_engine(profile, Credentials(username="svc", password="secret"))

    assert calls[0]["kwargs"] == {
        "echo": False,
        "pool_pre_ping": True,
        "pool_size": 2,
        "max_overflow": 0,
        "pool_timeout": 4,
    }


def test_create_sqlalchemy_engine_requires_credentials():
    with pytest.raises(OznakConfigurationError, match="Missing credentials"):
        create_sqlalchemy_engine(_mysql_profile(), None)


def test_create_sqlalchemy_engine_redacts_failure_details(monkeypatch):
    def fake_create_engine(url: str, **kwargs):  # noqa: ARG001
        raise RuntimeError("driver initialization failed")

    monkeypatch.setattr("oznak.engines.create_engine", fake_create_engine)

    with pytest.raises(OznakConfigurationError) as exc_info:
        create_sqlalchemy_engine(
            _mysql_profile(),
            Credentials(username="svc", password="very-secret-password"),
        )

    message = str(exc_info.value)
    assert "very-secret-password" not in message
    assert "mysql+pymysql://" not in message
    assert "assembly_mysql" in message
