import importlib
from unittest.mock import patch

import pandas as pd
import pytest
from typer.testing import CliRunner

from src.cli.main import app


runner = CliRunner()


@patch("src.cli.main.export")
@patch("src.cli.main.MultiDatabaseFetcher")
def test_load_command_exports_rows(mock_fetcher_class, mock_export):
    mock_fetcher = mock_fetcher_class.return_value
    mock_fetcher.fetch.return_value = pd.DataFrame([{"RefName": "A1"}])

    result = runner.invoke(
        app,
        [
            "load",
            "database1,database2",
            "--select-columns",
            "RefName",
            "--filter",
            "Status = ACTIVE",
            "--last",
            "10",
            "--date_col",
            "CreatedAt",
            "--out",
            "result.csv",
        ],
    )

    assert result.exit_code == 0
    mock_fetcher.fetch.assert_called_once_with(
        ["database1", "database2"],
        ["Status = ACTIVE"],
        10,
        "CreatedAt",
        ["RefName"],
    )
    mock_export.assert_called_once()


@patch("src.cli.main.MultiDatabaseFetcher")
def test_load_chunked_command_uses_fetch_chunked(mock_fetcher_class):
    mock_fetcher = mock_fetcher_class.return_value
    mock_fetcher.fetch_chunked.return_value = True

    result = runner.invoke(
        app,
        [
            "load-chunked",
            "database1,database2",
            "--select-columns",
            "RefName,Status",
            "--filter",
            "Status = ACTIVE",
            "--chunk-size",
            "500",
            "--pagination-column",
            "ID",
            "--out",
            "chunked.csv",
        ],
    )

    assert result.exit_code == 0
    mock_fetcher.fetch_chunked.assert_called_once_with(
        ["database1", "database2"],
        ["Status = ACTIVE"],
        500,
        "chunked.csv",
        "ID",
        ["RefName", "Status"],
    )


@patch("src.cli.main.MultiDatabaseFetcher")
def test_load_command_rejects_invalid_filter(mock_fetcher_class):
    result = runner.invoke(
        app,
        [
            "load",
            "database1",
            "--filter",
            "invalid-filter",
        ],
    )

    assert result.exit_code == 1
    assert "Invalid filters:" in result.stderr
    mock_fetcher_class.assert_not_called()


@pytest.mark.cli_integration
def test_load_command_writes_csv_and_parses_arguments(monkeypatch, tmp_path):
    module = importlib.import_module("src.cli.main")
    captured = {}

    class DummyFetcher:
        def fetch(self, databases, filters, limit=None, date_column="TimeStamp", columns=None):
            captured["databases"] = databases
            captured["filters"] = filters
            captured["limit"] = limit
            captured["date_column"] = date_column
            captured["columns"] = columns
            return pd.DataFrame(
                [
                    {"RefName": "A1", "Status": "ACTIVE"},
                    {"RefName": "B2", "Status": "PAUSED"},
                ]
            )

    monkeypatch.setattr(module, "MultiDatabaseFetcher", lambda: DummyFetcher())

    out_path = tmp_path / "result.csv"
    result = runner.invoke(
        module.app,
        [
            "load",
            "db_a, db_b",
            "--select-columns",
            "RefName,Status",
            "--filter",
            "Status = ACTIVE",
            "--last",
            "10",
            "--date_col",
            "CreatedAt",
            "--out",
            str(out_path),
        ],
    )

    assert result.exit_code == 0
    assert captured == {
        "databases": ["db_a", "db_b"],
        "filters": ["Status = ACTIVE"],
        "limit": 10,
        "date_column": "CreatedAt",
        "columns": ["RefName", "Status"],
    }
    assert out_path.exists()
    assert pd.read_csv(out_path).to_dict(orient="records") == [
        {"RefName": "A1", "Status": "ACTIVE"},
        {"RefName": "B2", "Status": "PAUSED"},
    ]
    assert "Data exported to" in result.stdout


@pytest.mark.cli_integration
def test_load_command_reports_backend_failure(monkeypatch, tmp_path):
    module = importlib.import_module("src.cli.main")

    class FailingFetcher:
        def fetch(self, databases, filters, limit=None, date_column="TimeStamp", columns=None):
            raise RuntimeError("db offline")

    monkeypatch.setattr(module, "MultiDatabaseFetcher", lambda: FailingFetcher())

    result = runner.invoke(
        module.app,
        [
            "load",
            "db_a",
            "--filter",
            "Status = ACTIVE",
            "--out",
            str(tmp_path / "result.csv"),
        ],
    )

    assert result.exit_code == 1
    assert "Error fetching data: db offline" in result.stderr


@pytest.mark.cli_integration
def test_load_chunked_command_writes_csv_and_parses_arguments(monkeypatch, tmp_path):
    module = importlib.import_module("src.cli.main")
    captured = {}

    class DummyFetcher:
        def fetch_chunked(
            self,
            databases,
            filters,
            chunk_size,
            output_path,
            pagination_column="id",
            columns=None,
        ):
            captured["databases"] = databases
            captured["filters"] = filters
            captured["chunk_size"] = chunk_size
            captured["output_path"] = output_path
            captured["pagination_column"] = pagination_column
            captured["columns"] = columns
            pd.DataFrame(
                [
                    {"RefName": "A1", "Status": "ACTIVE", "source_database": "db_a"},
                    {"RefName": "B2", "Status": "PAUSED", "source_database": "db_b"},
                ]
            ).to_csv(output_path, index=False)
            return True

    monkeypatch.setattr(module, "MultiDatabaseFetcher", lambda: DummyFetcher())

    out_path = tmp_path / "chunked.csv"
    result = runner.invoke(
        module.app,
        [
            "load-chunked",
            "db_a, db_b",
            "--select-columns",
            "RefName,Status",
            "--filter",
            "Status = ACTIVE",
            "--chunk-size",
            "250",
            "--pagination-column",
            "pk_id",
            "--out",
            str(out_path),
        ],
    )

    assert result.exit_code == 0
    assert captured == {
        "databases": ["db_a", "db_b"],
        "filters": ["Status = ACTIVE"],
        "chunk_size": 250,
        "output_path": str(out_path),
        "pagination_column": "pk_id",
        "columns": ["RefName", "Status"],
    }
    assert out_path.exists()
    assert pd.read_csv(out_path).to_dict(orient="records") == [
        {"RefName": "A1", "Status": "ACTIVE", "source_database": "db_a"},
        {"RefName": "B2", "Status": "PAUSED", "source_database": "db_b"},
    ]
    assert "Data from ['db_a', 'db_b'] loaded in chunks and exported to" in result.stdout


@pytest.mark.cli_integration
def test_load_chunked_command_reports_backend_failure(monkeypatch, tmp_path):
    module = importlib.import_module("src.cli.main")

    class FailingFetcher:
        def fetch_chunked(
            self,
            databases,
            filters,
            chunk_size,
            output_path,
            pagination_column="id",
            columns=None,
        ):
            raise RuntimeError("chunk backend offline")

    monkeypatch.setattr(module, "MultiDatabaseFetcher", lambda: FailingFetcher())

    result = runner.invoke(
        module.app,
        [
            "load-chunked",
            "db_a",
            "--filter",
            "Status = ACTIVE",
            "--out",
            str(tmp_path / "chunked.csv"),
        ],
    )

    assert result.exit_code == 1
    assert "Error fetching chunked data: chunk backend offline" in result.stderr
