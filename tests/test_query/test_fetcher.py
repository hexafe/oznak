import pytest
import pandas as pd
from unittest.mock import patch, Mock
from src.query.fetcher import fetch_data


@patch('src.query.fetcher.pd.read_sql')
def test_fetch_data_success_with_params(mock_read_sql):
    """
    Test fetch_data successfully retrieves data when params are provided
    """
    # Arrange
    mock_engine = Mock()
    query = "SELECT * FROM table WHERE col = :param_0"
    params = {"param_0": "value1"}
    expected_df = pd.DataFrame({"col1": [1, 2], "col2": ['a', 'b']})
    mock_read_sql.return_value = expected_df

    # Act
    result_df = fetch_data(mock_engine, query, params)

    # Assert
    # Check if pd.read_sql was called with the correct arguments
    mock_read_sql.assert_called_once_with(query, mock_engine, params=params)
    # Check if the returned DataFrame is the one from pd.read_sql
    pd.testing.assert_frame_equal(result_df, expected_df)

@patch('src.query.fetcher.pd.read_sql')
def test_fetch_data_success_no_params(mock_read_sql):
    """
    Test fetch_data successfully retrieves data when no parameters are provided
    """
    # Arrange
    mock_engine = Mock()
    query = "SELECT * FROM table"
    params = None
    expected_df = pd.DataFrame({"col1": [3, 4], "col2": ['c', 'd']})
    mock_read_sql.return_value = expected_df

    # Act
    result_df = fetch_data(mock_engine, query, params)

    # Assert
    # Check if pd.read_sql was called with the query and engine, but without params
    mock_read_sql.assert_called_once_with(query, mock_engine)
    # Check if the returned DataFrame is the one from pd.read_sql
    pd.testing.assert_frame_equal(result_df, expected_df)

@patch('src.query.fetcher.pd.read_sql')
def test_fetch_data_error(mock_read_sql):
    """
    Test fetch_data returns an empty DataFrame when pd.read_sql raises an exception
    """
    # Arrange
    mock_engine = Mock()
    query = "SELECT * FROM table WHERE col = :param_0"
    params = {"param_0": "value1"}
    expected_error_msg = "Database connection failed"
    mock_read_sql.side_effect = Exception(expected_error_msg)

    # Act
    result_df = fetch_data(mock_engine, query, params)

    # Assert
    # Check if pd.read_sql was called with the correct arguments
    mock_read_sql.assert_called_once_with(query, mock_engine, params=params)
    # Check if the returned DataFrame is empty
    assert result_df.empty

