import pytest

from src.services.request_preprocessor import preprocess_fetch_request


def test_preprocess_fetch_request_normalizes_all_inputs():
    prepared = preprocess_fetch_request(
        databases="db1, db2",
        filters=[{"field": "Metric", "op": ">=", "value": 10}],
        last=25,
        select_columns="RefName, TimeStamp",
    )

    assert prepared == {
        "databases": ["db1", "db2"],
        "filters": ["Metric >= 10"],
        "limit": 25,
        "columns": ["RefName", "TimeStamp"],
    }


def test_preprocess_fetch_request_rejects_invalid_database_names():
    with pytest.raises(ValueError, match="Invalid database name"):
        preprocess_fetch_request(databases="db1, bad-name")


def test_preprocess_fetch_request_rejects_invalid_filter_object_shape():
    with pytest.raises(ValueError, match="missing keys: value"):
        preprocess_fetch_request(
            databases="db1",
            filters=[{"field": "RefName", "op": "="}],
        )


def test_preprocess_fetch_request_rejects_invalid_limit():
    with pytest.raises(ValueError, match="positive integer"):
        preprocess_fetch_request(databases="db1", last=0)
