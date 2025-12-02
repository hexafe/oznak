import pytest
import pandas as pd
from unittest.mock import patch, Mock, call
from src.services.multi_database_fetcher import MultiDatabaseFetcher


@patch("src.services.multi_database_fetcher.fetch_data")
@patch("src.services.multi_database_fetcher.build_query")
@patch("src.services.multi_database_fetcher.DBManager")
def test_fetch_single_database_success(mock_db_manager_class, mock_build_query, mock_fetch_data):
    # Assign
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
    mock_db_manager_instance.get_engine.assert_called_once_with("database1")
    mock_build_query.assert_called_once_with("table1", filters, limit, date_column)
    mock_fetch_data.assert_called_once_with(mock_engine, expected_query, expected_params)
    pd.testing.assert_frame_equal(result_df, expected_final_df)


@patch("src.services.multi_database_fetcher.fetch_data")
@patch("src.services.multi_database_fetcher.build_query")
@patch("src.services.multi_database_fetcher.DBManager")
def test_fetch_multiple_databases_success(mock_db_manager_class, mock_build_query, mock_fetch_data):
    # Assign
    mock_db_manager_instance = Mock()
    mock_db_manager_class.return_value = mock_db_manager_instance

    mock_engine1 = Mock()
    mock_engine2 = Mock()
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

    def mock_fetch_data_side_effect(engine, query, params):
        if engine == mock_engine1:
            return expected_df_db1
        elif engine == mock_engine2:
            return expected_df_db2
        else:
            return pd.DataFrame()

    mock_fetch_data.side_effect = mock_fetch_data_side_effect

    fetcher = MultiDatabaseFetcher()
    fetcher.db = mock_db_manager_instance

    # Act
    result_df = fetcher.fetch(["database1", "database2"], filters, limit, date_column)

    # Assert
    assert mock_db_manager_instance.get_engine.call_count == 2
    mock_db_manager_instance.get_engine.assert_has_calls([call("database1"), call("database2")], any_order=True)
    assert mock_build_query.call_count == 2
    mock_build_query.assert_has_calls([
        call("table1", filters, limit, date_column),
        call("table2", filters, limit, date_column)
    ], any_order=True)
    assert mock_fetch_data.call_count == 2
    mock_fetch_data.assert_has_calls([
        call(mock_engine1, expected_query, expected_params),
        call(mock_engine2, expected_query, expected_params)
    ], any_order=True)

    sorted_result_df = result_df.sort_values(by=result_df.columns.tolist()).reset_index(drop=True)
    sorted_expected_df = expected_combined_df.sort_values(by=expected_combined_df.columns.tolist()).reset_index(drop=True)
    pd.testing.assert_frame_equal(sorted_result_df, sorted_expected_df)


@patch("src.services.multi_database_fetcher.fetch_data")
@patch("src.services.multi_database_fetcher.build_query")
@patch("src.services.multi_database_fetcher.DBManager")
def test_fetch_connection_failure_one_db(mock_db_manager_class, mock_build_query, mock_fetch_data):
    # Assign
    mock_db_manager_instance = Mock()
    mock_db_manager_class.return_value = mock_db_manager_instance

    mock_engine1 = Mock()
    # Use a function for side_effect to raise the exception correctly inside the thread
    def get_engine_side_effect(x):
        if x == "database1":
            return mock_engine1
        else:
            raise Exception("Connection failed")

    mock_db_manager_instance.get_engine.side_effect = get_engine_side_effect
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
    expected_final_df_db1 = expected_df_db1.copy()
    expected_final_df_db1["source_database"] = "database1"

    def mock_build_query_side_effect(table, filters_arg, limit_arg, date_column_arg):
        if table == "table1":
            return expected_query, expected_params
        elif table == "table2":
            return expected_query, expected_params
        else:
            raise ValueError(f"Unexpected table: {table}")

    mock_build_query.side_effect = mock_build_query_side_effect

    def mock_fetch_data_side_effect(engine, query, params):
        if engine == mock_engine1:
            return expected_df_db1
        else:
            # This part should not be reached for database2 due to connection failure
            return pd.DataFrame()

    mock_fetch_data.side_effect = mock_fetch_data_side_effect

    fetcher = MultiDatabaseFetcher()
    fetcher.db = mock_db_manager_instance

    # Act
    result_df = fetcher.fetch(["database1", "database2"], filters, limit, date_column)

    # Assert
    assert mock_db_manager_instance.get_engine.call_count == 2
    mock_db_manager_instance.get_engine.assert_has_calls([
        call("database1"),
        call("database2")
    ], any_order=True)
    # build_query is called only for the successful database (database1) as db2's thread fails on get_engine
    assert mock_build_query.call_count == 1
    mock_build_query.assert_called_once_with("table1", filters, limit, date_column)
    # fetch_data is called only for the successful database (database1)
    mock_fetch_data.assert_called_once_with(mock_engine1, expected_query, expected_params)

    sorted_result_df = result_df.sort_values(by=result_df.columns.tolist()).reset_index(drop=True)
    sorted_expected_final_df_db1 = expected_final_df_db1.sort_values(by=expected_final_df_db1.columns.tolist()).reset_index(drop=True)
    pd.testing.assert_frame_equal(sorted_result_df, sorted_expected_final_df_db1)


@patch("src.services.multi_database_fetcher.fetch_data")
@patch("src.services.multi_database_fetcher.build_query")
@patch("src.services.multi_database_fetcher.DBManager")
def test_fetch_query_build_failure_one_db(mock_db_manager_class, mock_build_query, mock_fetch_data):
    # Assign
    mock_db_manager_instance = Mock()
    mock_db_manager_class.return_value = mock_db_manager_instance

    mock_engine1 = Mock()
    mock_engine2 = Mock()
    mock_db_manager_instance.get_engine.side_effect = lambda x: mock_engine1 if x == 'database1' else mock_engine2
    mock_db_manager_instance.cfg = {
        'database1': {'table': 'table1'},
        'database2': {'table': 'table2'}
    }

    filters = ["Status = ACTIVE"]
    limit = 5
    date_column = "timestamp"
    expected_query_db1 = "SELECT * FROM `table1` WHERE `Status` = :param_0 ORDER BY `timestamp` DESC LIMIT 5"
    expected_params_db1 = {"param_0": "ACTIVE"}

    expected_df_db1 = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
    expected_final_df_db1 = expected_df_db1.copy()
    expected_final_df_db1['source_database'] = 'database1'

    def mock_build_query_side_effect(table, filters_arg, limit_arg, date_column_arg):
        if table == "table1":
            return expected_query_db1, expected_params_db1
        elif table == "table2":
            raise ValueError("Invalid column name in filter for database2")
        else:
            raise ValueError(f"Unexpected table: {table}")

    mock_build_query.side_effect = mock_build_query_side_effect

    def mock_fetch_data_side_effect(engine, query, params):
        if engine == mock_engine1:
            return expected_df_db1
        else:
            return pd.DataFrame()

    mock_fetch_data.side_effect = mock_fetch_data_side_effect

    fetcher = MultiDatabaseFetcher()
    fetcher.db = mock_db_manager_instance

    # Act
    result_df = fetcher.fetch(['database1', 'database2'], filters, limit, date_column)

    # Assert
    assert mock_db_manager_instance.get_engine.call_count == 2
    mock_db_manager_instance.get_engine.assert_has_calls([
        call('database1'),
        call('database2')
    ], any_order=True)
    assert mock_build_query.call_count == 2
    mock_build_query.assert_has_calls([
        call('table1', filters, limit, date_column),
        call('table2', filters, limit, date_column)
    ], any_order=True)
    # fetch_data is called only for the successful database (database1) as db2's thread fails on build_query
    mock_fetch_data.assert_called_once_with(mock_engine1, expected_query_db1, expected_params_db1)

    sorted_result_df = result_df.sort_values(by=result_df.columns.tolist()).reset_index(drop=True)
    sorted_expected_final_df_db1 = expected_final_df_db1.sort_values(by=expected_final_df_db1.columns.tolist()).reset_index(drop=True)
    pd.testing.assert_frame_equal(sorted_result_df, sorted_expected_final_df_db1)


@patch("src.services.multi_database_fetcher.fetch_data")
@patch("src.services.multi_database_fetcher.build_query")
@patch("src.services.multi_database_fetcher.DBManager")
def test_fetch_data_empty_one_db(mock_db_manager_class, mock_build_query, mock_fetch_data):
    # Assign
    mock_db_manager_instance = Mock()
    mock_db_manager_class.return_value = mock_db_manager_instance

    mock_engine1 = Mock()
    mock_engine2 = Mock()
    mock_db_manager_instance.get_engine.side_effect = lambda x: mock_engine1 if x == 'database1' else mock_engine2
    mock_db_manager_instance.cfg = {
        'database1': {'table': 'table1'},
        'database2': {'table': 'table2'}
    }

    filters = ["Status = ACTIVE"]
    limit = 5
    date_column = "timestamp"
    expected_query = "SELECT * FROM `table1` WHERE `Status` = :param_0 ORDER BY `timestamp` DESC LIMIT 5"
    expected_params = {"param_0": "ACTIVE"}

    expected_df_db1 = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
    expected_df_db2_empty = pd.DataFrame()

    expected_final_df_db1 = expected_df_db1.copy()
    expected_final_df_db1['source_database'] = 'database1'
    expected_result_df = expected_final_df_db1

    mock_build_query.return_value = expected_query, expected_params

    def mock_fetch_data_side_effect(engine, query, params):
        if engine == mock_engine1:
            return expected_df_db1
        elif engine == mock_engine2:
            return expected_df_db2_empty
        else:
            return pd.DataFrame()

    mock_fetch_data.side_effect = mock_fetch_data_side_effect

    fetcher = MultiDatabaseFetcher()
    fetcher.db = mock_db_manager_instance

    # Act
    result_df = fetcher.fetch(['database1', 'database2'], filters, limit, date_column)
    sorted_result_df = result_df.sort_values(by=result_df.columns.tolist()).reset_index(drop=True)
    sorted_expected_result_df = expected_result_df.sort_values(by=expected_result_df.columns.tolist()).reset_index(drop=True)

    # Assert
    assert mock_db_manager_instance.get_engine.call_count == 2
    mock_db_manager_instance.get_engine.assert_has_calls([
        call('database1'),
        call('database2')
    ], any_order=True)
    assert mock_build_query.call_count == 2
    mock_build_query.assert_has_calls([
        call('table1', filters, limit, date_column),
        call('table2', filters, limit, date_column)
    ], any_order=True)
    assert mock_fetch_data.call_count == 2
    mock_fetch_data.assert_has_calls([
        call(mock_engine1, expected_query, expected_params),
        call(mock_engine2, expected_query, expected_params)
    ], any_order=True)
    pd.testing.assert_frame_equal(sorted_result_df, sorted_expected_result_df)


@patch("src.services.multi_database_fetcher.fetch_data")
@patch("src.services.multi_database_fetcher.build_query")
@patch("src.services.multi_database_fetcher.DBManager")
def test_fetch_no_data_any_db(mock_db_manager_class, mock_build_query, mock_fetch_data):
    # Assign
    mock_db_manager_instance = Mock()
    mock_db_manager_class.return_value = mock_db_manager_instance

    mock_engine1 = Mock()
    mock_engine2 = Mock()
    mock_db_manager_instance.get_engine.side_effect = lambda x: mock_engine1 if x == 'database1' else mock_engine2
    mock_db_manager_instance.cfg = {
        'database1': {'table': 'table1'},
        'database2': {'table': 'table2'}
    }

    filters = ["Status = INACTIVE"]
    limit = 5
    date_column = "timestamp"
    expected_query = "SELECT * FROM `table1` WHERE `Status` = :param_0 ORDER BY `timestamp` DESC LIMIT 5"
    expected_params = {"param_0": "INACTIVE"}

    expected_df_empty = pd.DataFrame()

    mock_build_query.return_value = expected_query, expected_params

    def mock_fetch_data_side_effect(engine, query, params):
        return expected_df_empty

    mock_fetch_data.side_effect = mock_fetch_data_side_effect

    fetcher = MultiDatabaseFetcher()
    fetcher.db = mock_db_manager_instance

    # Act
    result_df = fetcher.fetch(['database1', 'database2'], filters, limit, date_column)

    # Assert
    assert mock_db_manager_instance.get_engine.call_count == 2
    mock_db_manager_instance.get_engine.assert_has_calls([call('database1'), call('database2')], any_order=True)
    assert mock_build_query.call_count == 2
    mock_build_query.assert_has_calls([
        call('table1', filters, limit, date_column),
        call('table2', filters, limit, date_column)
    ], any_order=True)
    assert mock_fetch_data.call_count == 2
    mock_fetch_data.assert_has_calls([
        call(mock_engine1, expected_query, expected_params),
        call(mock_engine2, expected_query, expected_params)
    ], any_order=True)
    assert result_df.empty
    assert result_df.shape[0] == 0

