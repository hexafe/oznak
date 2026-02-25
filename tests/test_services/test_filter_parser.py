import pytest

from src.services.filter_parser import (
    normalize_columns,
    normalize_databases,
    parse_filter_string,
    parse_filters,
)


@pytest.mark.parametrize(
    "filter_str,expected_operator",
    [
        ("Metric > 10", ">"),
        ("Metric <= 10", "<="),
        ("Metric NOT LIKE ABC%", "NOT LIKE"),
    ],
)
def test_parse_filter_string_accepts_distinct_comparison_operators(filter_str, expected_operator):
    _, operator, _ = parse_filter_string(filter_str)
    assert operator == expected_operator


def test_parse_filter_string_rejects_invalid_comparison_operator_token():
    with pytest.raises(ValueError, match="Invalid operator"):
        parse_filter_string("Metric ><= 10")


def test_parse_filters_raises_for_invalid_filter():
    with pytest.raises(ValueError, match="Invalid operator"):
        parse_filters(["Metric ><= 10"], last=None)


def test_parse_filters_raises_for_invalid_last_value():
    with pytest.raises(ValueError, match="positive integer"):
        parse_filters(["Metric > 10"], last=0)


def test_normalize_databases_returns_clean_names():
    assert normalize_databases("db1, db2 ,,db3") == ["db1", "db2", "db3"]


def test_normalize_databases_rejects_invalid_names():
    with pytest.raises(ValueError, match="Invalid database name"):
        normalize_databases("db1, bad-name")


def test_normalize_columns_returns_none_for_none():
    assert normalize_columns(None) is None


def test_normalize_columns_parses_and_validates_string_input():
    assert normalize_columns("RefName, TimeStamp") == ["RefName", "TimeStamp"]


def test_normalize_columns_rejects_invalid_names():
    with pytest.raises(ValueError, match="Invalid column name"):
        normalize_columns("RefName, bad.column")


def test_parse_filters_normalizes_in_values():
    parsed = parse_filters(["RefName IN A, B , ,C"], last=None)
    assert parsed["filters"] == ["RefName IN A, B, C"]


def test_parse_filters_rejects_is_with_unsupported_value():
    with pytest.raises(ValueError, match="only supports NULL, TRUE, or FALSE"):
        parse_filters(["DeletedAt IS unknown"], last=None)


def test_parse_filters_accepts_typed_filter_object():
    parsed = parse_filters([
        {"field": "RefName", "op": "LIKE", "value": "ABC%"},
        {"field": "Metric", "op": ">=", "value": "10"},
    ], last=None)
    assert parsed["filters"] == ["RefName LIKE ABC%", "Metric >= 10"]


def test_parse_filters_rejects_typed_filter_missing_keys():
    with pytest.raises(ValueError, match="missing keys: value"):
        parse_filters([{"field": "RefName", "op": "LIKE"}], last=None)


def test_parse_filters_rejects_typed_filter_extra_keys():
    with pytest.raises(ValueError, match="unsupported keys: foo"):
        parse_filters([
            {"field": "RefName", "op": "LIKE", "value": "ABC%", "foo": "bar"}
        ], last=None)


def test_parse_filters_rejects_unsupported_filter_type():
    with pytest.raises(ValueError, match="Invalid filter type"):
        parse_filters([123], last=None)

