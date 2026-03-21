from unittest.mock import patch

import pandas as pd
import pytest
from fastapi import HTTPException

from src.api import rest


def test_health_endpoint():
    assert rest.health() == {"status": "ok"}


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
def test_fetch_endpoint_rejects_invalid_filters(mock_fetcher):
    with pytest.raises(HTTPException) as exc_info:
        rest.fetch(
            databases="database1",
            filters=["badfilter"],
        )

    assert exc_info.value.status_code == 400
    assert "Invalid filter format" in exc_info.value.detail
    mock_fetcher.fetch.assert_not_called()
