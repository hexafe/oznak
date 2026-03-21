from unittest.mock import patch

import pandas as pd
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
    assert "Invalid filters:" in result.stdout
    mock_fetcher_class.assert_not_called()
