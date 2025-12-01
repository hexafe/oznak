import pytest
import pandas as pd
from unittest.mock import patch, Mock, call
from src.services.multi_database_fetcher import MultiDatabaseFetcher


""" Test case: success with single database """
@patch("src.services.multi_database_fetcher.fetch_data")
@patch("src.services.multi_database_fetcher.build_query")
@patch("src.services.multi_database_fetcher.DBManager")
def test_fetch_single_database_success(mock_db_manager_class, mock_build_query, mock_fetch_data):
    """
    Test fetching data successfully from a single database
    """
    # Arrange
    mock_db_manager_instance = Mock()
    mock_db_manager_class.return_value = mock_db_manager_instance

    mock_engine = Mock()
    mock_db_manager_instance.get_engine.return_value = mock_engine
    mock_db_manager_instance.cfg = {
        "database1": {"table": "table1"}
    }

    filters = ["RefName = ABC123"]
    limit = 10
    date_column = "Date"

    expected_query = "SELECT * FROM `table1` WHERE `RefName` = :param_0 ORDER BY `Date` DESC LIMIT 10"
    expected_params = {"param_0": "ABC123"}
    expected_df_from_db = pd.DataFrame({"col1": [1, 2], "col2": ['a', 'b']})
    expected_final_df = expected_df_from_db.copy()
    expected_final_df["source_database"] = "database1"

    mock_build_query.return_value = expected_query, expected_params
    mock_fetch_data.return_value = expected_df_from_db

    fetcher = MultiDatabaseFetcher()
    fetcher.db = mock_db_manager_instance

    # Act
    result_df = fetcher.fetch(["database1"], filters, limit, date_column)

    # Assert
    # Check DBManager.get_engine was called correctly
    mock_db_manager_instance.get_engine.assert_called_once_with("database1")
    # Check build_query was called correctly
    mock_build_query.assert_called_once_with("table1", filters, limit, date_column)
    # Check fetch_data was called correctly with the engine and results from build_query
    mock_fetch_data.assert_called_once_with(mock_engine, expected_query, expected_params)
    # Check the final result DataFrame
    pd.testing.assert_frame_equal(result_df, expected_final_df)

""" Test case: success with multiple databases """
@patch("src.services.multi_database_fetcher.fetch_data")
@patch("src.services.multi_database_fetcher.build_query")
@patch("src.services.multi_database_fetcher.DBManager")
def test_fetch_multiple_databases_success(mock_db_manager_class, mock_build_query, mock_fetch_data):
    """
    Test fetching data successfully from multiple databases and concatenating
    """
    # Arrange
    mock_db_manager_instance = Mock()
    mock_db_manager_class.return_value = mock_db_manager_instance

    mock_engine1 = Mock()
    mock_engine2 = Mock()
    # Configure get_engine to return different engines for different DBs
    mock_db_manager_instance.get_engine.side_effect = lambda x: mock_engine1 if x == "database1" else mock_engine2
    mock_db_manager_instance.cfg = {
        "database1": {"table": "table1"},
        "database2": {"table": "table2"}
    }

    filters = ["Status = ACTIVE"]
    limit = 5
    date_column = "timestamp"

    expected_query = "SELECT * FROM `table1` WHERE `Status` = :param_0 ORDER BY `timestamp` DESC LIMIT 5"
    expected_params = {"param_0": "ACTIVE"}
    
    expected_df_db1 = pd.DataFrame({"col1": [1, 2], "col2": ['a', 'b']})
    expected_df_db2 = pd.DataFrame({"col1": [3, 4], "col2": ['c', 'd']})
    
    expected_final_df_db1 = expected_df_db1.copy()
    expected_final_df_db1["source_database"] = "database1"
    expected_final_df_db2 = expected_df_db2.copy()
    expected_final_df_db2["source_database"] = "database2"
    expected_combined_df = pd.concat([expected_final_df_db1, expected_final_df_db2], ignore_index=True)

    mock_build_query.return_value = expected_query, expected_params
    mock_fetch_data.side_effect = [expected_df_db1, expected_df_db2]

    fetcher = MultiDatabaseFetcher()
    fetcher.db = mock_db_manager_instance

    # Acti
    result_df = fetcher.fetch(["database1", "database2"], filters, limit, date_column)

    # Assert
    # Check DBManager.gt_engine was called for both databases
    assert mock_db_manager_instance.get_engine.call_count == 2
    mock_db_manager_instance.get_engine.assert_has_calls([call("database1"), call("database2")])
    # Check build_query was called twice (once for each database)
    assert mock_build_query.call_count == 2
    mock_build_query.assert_has_calls([
        call("table1", filters, limit, date_column),
        call("table2", filters, limit, date_column)
    ])
    # Check fetch_data was called twice, once with each engine and the query/params
    assert mock_fetch_data.call_count == 2
    mock_fetch_data.assert_has_calls([
        call(mock_engine1, expected_query, expected_params),
        call(mock_engine2, expected_query, expected_params)
    ])
    # Check the final combined result DataFrame
    pd.testing.assert_frame_equal(result_df, expected_combined_df)

""" Test case: error - connection failure for one database """
#TODO:
