from __future__ import annotations

import pandas as pd
import pytest
from fastapi import HTTPException

from oznak.api import fetch, health
from oznak.diagnostics import SourceFetchDiagnostics, SourceFetchStatus
from oznak.profiles import DatabaseProfile
from oznak.result import FetchResult


def _profile(alias: str = "db_a") -> DatabaseProfile:
    return DatabaseProfile(
        alias=alias,
        dialect="mysql",
        host="db.example.invalid",
        port=3306,
        database="assembly",
        table="records",
        allowed_columns=("RefName", "Status", "CreatedAt", "TimeStamp"),
        timestamp_column="CreatedAt",
    )


def test_package_api_health():
    assert health() == {"status": "ok"}


def test_package_api_fetch_uses_package_contracts(monkeypatch):
    monkeypatch.setattr("oznak.api.load_database_profiles", lambda _config: {"db_a": _profile("db_a")})
    captured = {}

    def fake_fetch_records(request, credential_provider=None):
        captured["aliases"] = [profile.alias for profile in request.profiles]
        captured["filters"] = [(item.column, item.operator.value, item.value) for item in request.filters]
        captured["limit"] = request.limit
        captured["date_column"] = request.date_column
        captured["columns"] = request.columns
        assert credential_provider is not None
        return FetchResult(
            data=pd.DataFrame([{"RefName": "A1", "Status": "ACTIVE"}]),
            source_results=(
                SourceFetchDiagnostics(
                    source_alias="db_a",
                    status=SourceFetchStatus.SUCCESS,
                    row_count=1,
                ),
            ),
        )

    monkeypatch.setattr("oznak.api.fetch_records", fake_fetch_records)

    response = fetch(
        databases="db_a",
        config="unused.yaml",
        filters=["Status = ACTIVE"],
        last=5,
        date_col="CreatedAt",
        select_columns="RefName,Status",
    )

    assert response == {
        "rows": 1,
        "sources": [{"alias": "db_a", "status": "success", "row_count": 1}],
        "data": [{"RefName": "A1", "Status": "ACTIVE"}],
    }
    assert captured == {
        "aliases": ["db_a"],
        "filters": [("Status", "=", "ACTIVE")],
        "limit": 5,
        "date_column": "CreatedAt",
        "columns": ("RefName", "Status"),
    }


def test_package_api_fetch_rejects_invalid_request(monkeypatch):
    monkeypatch.setattr("oznak.api.load_database_profiles", lambda _config: {"db_a": _profile("db_a")})

    with pytest.raises(HTTPException) as exc_info:
        fetch(databases="bad-name", config="unused.yaml")

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail["code"] == "validation_error"


def test_package_api_fetch_wraps_structured_failures(monkeypatch):
    monkeypatch.setattr("oznak.api.load_database_profiles", lambda _config: {"db_a": _profile("db_a")})

    def fake_fetch_records(request, credential_provider=None):
        return FetchResult(
            data=pd.DataFrame(),
            source_results=(
                SourceFetchDiagnostics(
                    source_alias="db_a",
                    status=SourceFetchStatus.FAILED,
                    row_count=0,
                    error_code="query_error",
                    message="query failed",
                ),
            ),
            errors=("query failed",),
        )

    monkeypatch.setattr("oznak.api.fetch_records", fake_fetch_records)

    with pytest.raises(HTTPException) as exc_info:
        fetch(databases="db_a", config="unused.yaml")

    assert exc_info.value.status_code == 502
    assert exc_info.value.detail["code"] == "execution_error"
    assert exc_info.value.detail["details"]["sources"][0]["error_code"] == "query_error"
