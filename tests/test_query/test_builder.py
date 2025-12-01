import pytest
from src.query.builder import build_query, parse_filter_string


""" Tests for parse_filter_string """

def test_parse_filter_string_valid():
    """
    Test parsing a valid filter string with common operators
    """
    # Arrange
    filter_str = "RefName LIKE V123456"
    expected_column = "RefName"
    expected_operator = "LIKE"
    expected_value = "V123456"

    # Act
    column, operator, value = parse_filter_string(filter_str)

    # Assert
    assert column == expected_column
    assert operator == expected_operator
    assert value == expected_value

def test_parse_filter_string_valid_with_spaces_in_value():
    """
    Test parsing a valid filter string where the value contains spaces
    """
    # Arrange
    filter_str = "Description LIKE Part Number ABC 123"
    expected_column = "Description"
    expected_operator = "LIKE"
    expected_value = "Part Number ABC 123"
    
    # Act
    column, operator, value = parse_filter_string(filter_str)
    
    # Assert
    assert column == expected_column
    assert operator == expected_operator
    assert value == expected_value

def test_parse_filter_string_invalid_missing_parts():
    """
    Test parsing an invalid filter string that doesn't have all 3 parts
    """
    # Arrange
    invalid_filter_str = "RefName = "

    # Act & Assert
    with pytest.raises(ValueError, match="Invalid filter format"):
        parse_filter_string(invalid_filter_str)

def test_parse_filter_string_invalid_column_name():
    """
    Test parsing a filter string with an invalid column name
    """
    # Arrange
    invalid_filter_str = "RefName; DROP TABLE users; -- = 5"

    # Act & Assert
    with pytest.raises(ValueError, match="Invalid column name"):
        parse_filter_string(invalid_filter_str)

def test_parse_filter_string_invalid_operator():
    """
    Test parsing a filter string with an invalid operator
    """
    # Arrange
    invalid_filter_str = "Refname EXEC sp_drop_all_tables"

    # Act & Assert
    with pytest.raises(ValueError, match="Invalid operator"):
        parse_filter_string(invalid_filter_str)


""" Tests for build_query """

def test_build_query_no_filters_no_limit():
    """
    Test building a query with no filters and no limit
    """
    # Arrange
    table = "my_table"
    filters = []
    expected_query = f"SELECT * FROM `{table}`"
    expected_params = {}

    # Act
    query, params = build_query(table, filters)

    # Assert
    assert query == expected_query
    assert params == expected_params

def test_build_query_single_filter():
    """
    Test building a query with a single filter
    """
    # Arrange
    table = "my_table"
    filters = ["RefName = ABC123"]
    expected_query = f"SELECT * FROM `{table}` WHERE `RefName` = :param_0"
    expected_params = {"param_0" : "ABC123"}

    # Act
    query, params = build_query(table, filters)

    # Assert
    assert query == expected_query
    assert params == expected_params

def test_build_query_multiple_filters():
    """
    Test building a query with multiple filters
    """
    # Arrange
    table = "my_table"
    filters = ["RefName = ABC123", "Status = ACTIVE"]
    expected_query = f"SELECT * FROM `{table}` WHERE `RefName` = :param_0 AND `Status` = :param_1"
    expected_params = {"param_0": "ABC123", "param_1": "ACTIVE"}

    # Act
    query, params = build_query(table, filters)

    # Assert
    assert query == expected_query
    assert params == expected_params

def test_build_query_with_limit():
    """
    Test building a query with a limit and default date column
    """
    # Arrange
    table = "my_table"
    filters = ["Status = ACTIVE"]
    limit = 100
    date_column = "TimeStamp"
    expected_query = f"SELECT * FROM `{table}` WHERE `Status` = :param_0 ORDER BY `{date_column}` DESC LIMIT {limit}"
    expected_params = {"param_0": "ACTIVE"}

    # Act
    query, params = build_query(table, filters, limit)

    # Assert
    assert query == expected_query
    assert params == expected_params

def test_build_query_with_limit_custom_date_column():
    """
    Test building a query with a limit and custom date column
    """
    # Arrange
    table = "my_table"
    filters = ["Status = ACTIVE"]
    limit = 50
    date_column = "timestamp"
    expected_query = f"SELECT * FROM `{table}` WHERE `Status` = :param_0 ORDER BY `{date_column}` DESC LIMIT {limit}"
    expected_params = {"param_0": "ACTIVE"}

    # Act
    query, params = build_query(table, filters, limit=limit, date_column=date_column)

    # Assert
    assert query == expected_query
    assert params == expected_params

def test_build_query_like_filter():
    """
    Test building a query with a LIKE filter
    """
    # Arrange
    table = "my_table"
    filters = ["RefName LIKE V123%"]
    expected_query = f"SELECT * FROM `{table}` WHERE `RefName` LIKE :param_0"
    expected_params = {"param_0": "V123%"}

    # Act
    query, params = build_query(table, filters)

    # Assert
    assert query == expected_query
    assert params == expected_params

def test_build_query_in_filter():
    """
    Test building a query with a IN filter
    """
    # Arrange
    table = "my_table"
    filters = ["Status IN ACTIVE,PENDING"]
    expected_query = f"SELECT * FROM `{table}` WHERE `Status` IN (:param_0,:param_1)"
    expected_params = {"param_0": "ACTIVE", "param_1": "PENDING"}

    # Act
    query, params = build_query(table, filters)

    # Assert
    assert query == expected_query
    assert params == expected_params

def test_build_query_greater_than_filter():
    """
    Test building a query with a > filter
    """
    # Arrange
    table = "my_table"
    filters = ["Value > 100"]
    expected_query = f"SELECT * FROM `{table}` WHERE `Value` > :param_0"
    expected_params = {"param_0": "100"}

    # Act
    query, params = build_query(table, filters)

    # Assert
    assert query == expected_query
    assert params == expected_params

#TODO: add more tests for all operators <, >=, <=, !=, IS, IS NOT

def test_build_query_invalid_table_name():
    """
    Test building a query with an invalid table name (should raise ValueError)
    """
    # Arrange
    table = "my_table; DROP TABLE users; --"
    filters = ["Status = ACTIVE"]

    # Act & Assert
    with pytest.raises(ValueError, match="Invalid table name"):
        build_query(table, filters)

def test_build_query_invalid_date_column_name():
    """
    Test building a query with an invalid date column name (should raise ValueError)
    """
    # Arrange
    table = "my_table"
    filters = ["Status = ACTIVE"]
    limit = 100
    date_column = "Date; DROP TABLE users; --"

    # Act & Assert
    with pytest.raises(ValueError, match="Invalid date column name"):
        build_query(table, filters, limit=limit, date_column=date_column)

