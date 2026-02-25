import importlib

import pandas as pd
import pytest


def test_rest_module_imports():
    module = importlib.import_module("src.api.rest")
    assert hasattr(module, "app")
    assert hasattr(module, "fetch")


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
    assert response["rows"] == 1
    assert response["data"] == [{"id": 1}]


def test_fetch_handler_rejects_invalid_database_name():
    module = importlib.import_module("src.api.rest")

    with pytest.raises(module.HTTPException) as exc_info:
        module.fetch(databases="db1,bad-name")

    assert exc_info.value.status_code == 400
    assert "Invalid database name" in str(exc_info.value.detail)


def test_fetch_handler_rejects_invalid_last_n():
    module = importlib.import_module("src.api.rest")

    with pytest.raises(module.HTTPException) as exc_info:
        module.fetch(databases="db1", last_n=-1)

    assert exc_info.value.status_code == 400
    assert "'last' must be a positive integer" in str(exc_info.value.detail)
