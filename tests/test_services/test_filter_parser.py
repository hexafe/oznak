import pytest

from src.services.filter_parser import parse_filter_string


@pytest.mark.parametrize(
    "filter_str,expected_operator",
    [
        ("Metric > 10", ">"),
        ("Metric <= 10", "<="),
    ],
)
def test_parse_filter_string_accepts_distinct_comparison_operators(filter_str, expected_operator):
    _, operator, _ = parse_filter_string(filter_str)
    assert operator == expected_operator


def test_parse_filter_string_rejects_invalid_comparison_operator_token():
    with pytest.raises(ValueError, match="Invalid operator"):
        parse_filter_string("Metric ><= 10")
