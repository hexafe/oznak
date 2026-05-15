import importlib
from unittest.mock import patch

import pandas as pd
import pytest
from fastapi import HTTPException

from src.api import rest


def test_rest_module_imports():
    module = importlib.import_module("src.api.rest")
    assert hasattr(module, "app")
    assert hasattr(module, "fetch")


def test_health_endpoint():
    assert rest.health() == {"status": "ok"}


def test_fetch_handler_parses_databases_and_optional_inputs(monkeypatch):
    module = importlib.import_module("src.api.rest")
    captured = {}

    class DummyFetcher:
        def fetch(self, databases, filters, limit=None, date_column="TimeStamp", columns=None):
            captured["databases"] = databases
            captured["filters"] = filters
            captured["limit"] = limit
            captured["date_column"] = date_column
            captured["columns"] = columns
            return pd.DataFrame([{"id": 1}])

    monkeypatch.setattr(module, "fetcher", DummyFetcher())

    response = module.fetch(
        databases="db1, db2 ,,",
        time_from="2024-01-01",
        time_to="2024-02-01",
        last_n=5,
        reference="ABC123",
    )

    assert captured["databases"] == ["db1", "db2"]
    assert captured["filters"] == [
        "TimeStamp >= 2024-01-01",
        "TimeStamp <= 2024-02-01",
        "RefName = ABC123",
    ]
    assert captured["limit"] == 5
    assert captured["date_column"] == "TimeStamp"
    assert response["rows"] == 1
    assert response["data"] == [{"id": 1}]


@patch("src.api.rest.fetcher")
def test_fetch_endpoint_success(mock_fetcher):
    mock_fetcher.fetch.return_value = pd.DataFrame(
        [{"RefName": "A1", "Status": "ACTIVE"}]
    )

    response = rest.fetch(
        databases="database1,database2",
        filters=["Status = ACTIVE"],
        last=5,
        date_col="CreatedAt",
        select_columns="RefName,Status",
    )

    assert response == {
        "rows": 1,
        "data": [{"RefName": "A1", "Status": "ACTIVE"}],
    }
    mock_fetcher.fetch.assert_called_once_with(
        ["database1", "database2"],
        ["Status = ACTIVE"],
        5,
        "CreatedAt",
        ["RefName", "Status"],
    )


@patch("src.api.rest.fetcher")
def test_fetch_endpoint_can_disable_order_by(mock_fetcher):
    mock_fetcher.fetch.return_value = pd.DataFrame([{"RefName": "A1"}])

    response = rest.fetch(
        databases="database1",
        filters=["Status = ACTIVE"],
        last=5,
        order_by=False,
    )

    assert response["rows"] == 1
    mock_fetcher.fetch.assert_called_once_with(
        ["database1"],
        ["Status = ACTIVE"],
        5,
        "TimeStamp",
        None,
        order_by_enabled=False,
    )


def test_fetch_handler_rejects_invalid_database_name():
    module = importlib.import_module("src.api.rest")

    with pytest.raises(HTTPException) as exc_info:
        module.fetch(databases="db1,bad-name")

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail["code"] == "validation_error"
    assert "Invalid database name" in exc_info.value.detail["message"]
    assert exc_info.value.detail["details"] == {"field": "request"}


def test_fetch_handler_rejects_invalid_last_n():
    module = importlib.import_module("src.api.rest")

    with pytest.raises(HTTPException) as exc_info:
        module.fetch(databases="db1", last_n=-1)

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail["code"] == "validation_error"
    assert "'last' must be a positive integer" in exc_info.value.detail["message"]
    assert exc_info.value.detail["details"] == {"field": "request"}


@patch("src.api.rest.fetcher")
def test_fetch_endpoint_rejects_invalid_filters(mock_fetcher):
    with pytest.raises(HTTPException) as exc_info:
        rest.fetch(
            databases="database1",
            filters=["badfilter"],
        )

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail["code"] == "validation_error"
    assert "Invalid filter format" in exc_info.value.detail["message"]
    assert exc_info.value.detail["details"] == {"field": "request"}
    mock_fetcher.fetch.assert_not_called()


@patch("src.api.rest.fetcher")
def test_fetch_endpoint_wraps_execution_failures(mock_fetcher):
    mock_fetcher.fetch.side_effect = RuntimeError("db offline")

    with pytest.raises(HTTPException) as exc_info:
        rest.fetch(
            databases="database1",
            filters=["Status = ACTIVE"],
        )

    assert exc_info.value.status_code == 502
    assert exc_info.value.detail["code"] == "execution_error"
    assert exc_info.value.detail["message"] == "database fetch failed"
    assert exc_info.value.detail["details"] == {
        "error": "db offline",
        "databases": ["database1"],
    }
