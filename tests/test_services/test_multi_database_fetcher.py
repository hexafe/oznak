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
        "database1": {"table": "table1", "type": "mysql"}
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
    mock_build_query.assert_called_once_with("table1", filters, limit, date_column, None, "mysql")
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
        "database1": {"table": "table1", "type": "mysql"},
        "database2": {"table": "table2", "type": "mysql"}
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
        call("table1", filters, limit, date_column, None, "mysql"),
        call("table2", filters, limit, date_column, None, "mysql")
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
        "database1": {"table": "table1", "type": "mysql"},
        "database2": {"table": "table2", "type": "mysql"}
    }

    filters = ["Status = ACTIVE"]
    limit = 5
    date_column = "timestamp"
    expected_query = "SELECT * FROM `table1` WHERE `Status` = :param_0 ORDER BY `timestamp` DESC LIMIT 5"
    expected_params = {"param_0": "ACTIVE"}

    expected_df_db1 = pd.DataFrame({"col1": [1, 2], "col2": ['a', 'b']})
    expected_final_df_db1 = expected_df_db1.copy()
    expected_final_df_db1["source_database"] = "database1"

    def mock_build_query_side_effect(table, filters_arg, limit_arg, date_column_arg, columns_arg = None, db_type_arg = "mysql"):
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
    result_df = fetcher.fetch(["database1", "database2"], filters, limit, date_column, None)

    # Assert
    assert mock_db_manager_instance.get_engine.call_count == 2
    mock_db_manager_instance.get_engine.assert_has_calls([
        call("database1"),
        call("database2")
    ], any_order=True)
    # build_query is called only for the successful database (database1) as db2's thread fails on get_engine
    assert mock_build_query.call_count == 1
    mock_build_query.assert_called_once_with("table1", filters, limit, date_column, None, "mysql")
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
        'database1': {'table': 'table1', 'type': 'mysql'},
        'database2': {'table': 'table2', 'type': 'mysql'}
    }

    filters = ["Status = ACTIVE"]
    limit = 5
    date_column = "timestamp"
    expected_query_db1 = "SELECT * FROM `table1` WHERE `Status` = :param_0 ORDER BY `timestamp` DESC LIMIT 5"
    expected_params_db1 = {"param_0": "ACTIVE"}

    expected_df_db1 = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
    expected_final_df_db1 = expected_df_db1.copy()
    expected_final_df_db1['source_database'] = 'database1'

    def mock_build_query_side_effect(table, filters_arg, limit_arg, date_column_arg, columns_arg = None, db_type_arg = "mysql"):
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
    result_df = fetcher.fetch(['database1', 'database2'], filters, limit, date_column, None)

    # Assert
    assert mock_db_manager_instance.get_engine.call_count == 2
    mock_db_manager_instance.get_engine.assert_has_calls([
        call('database1'),
        call('database2')
    ], any_order=True)
    assert mock_build_query.call_count == 2
    mock_build_query.assert_has_calls([
        call('table1', filters, limit, date_column, None, 'mysql'),
        call('table2', filters, limit, date_column, None, 'mysql')
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
        'database1': {'table': 'table1', 'type': 'mysql'},
        'database2': {'table': 'table2', 'type': 'mysql'}
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
    result_df = fetcher.fetch(['database1', 'database2'], filters, limit, date_column, None)
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
        call('table1', filters, limit, date_column, None, 'mysql'),
        call('table2', filters, limit, date_column, None, 'mysql')
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
        'database1': {'table': 'table1', 'type': 'mysql'},
        'database2': {'table': 'table2', 'type': 'mysql'}
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
    result_df = fetcher.fetch(['database1', 'database2'], filters, limit, date_column, None)

    # Assert
    assert mock_db_manager_instance.get_engine.call_count == 2
    mock_db_manager_instance.get_engine.assert_has_calls([call('database1'), call('database2')], any_order=True)
    assert mock_build_query.call_count == 2
    mock_build_query.assert_has_calls([
        call('table1', filters, limit, date_column, None, 'mysql'),
        call('table2', filters, limit, date_column, None, 'mysql')
    ], any_order=True)
    assert mock_fetch_data.call_count == 2
    mock_fetch_data.assert_has_calls([
        call(mock_engine1, expected_query, expected_params),
        call(mock_engine2, expected_query, expected_params)
    ], any_order=True)
    assert result_df.empty
    assert result_df.shape[0] == 0

""" Tests for chunked fetching """

@patch("src.services.multi_database_fetcher.fetch_data_chunked")
@patch("src.services.multi_database_fetcher.DBManager")
def test_fetch_database_in_chunks_single_db_success(mock_db_manager_class, mock_fetch_data_chunked, tmp_path):
    """
    Test fetching chunks from a single database
    """
    # Arrange
    mock_db_manager_instance = Mock()
    mock_db_manager_class.return_value = mock_db_manager_instance

    mock_engine = Mock()
    mock_db_manager_instance.get_engine.return_value = mock_engine
    mock_db_manager_instance.cfg = {
        "database1": {"table": "table1", "type": "mysql"}
    }
    
    database = "database1"
    filters = ["Status = ACTIVE"]
    chunk_size = 1000
    pagination_column = "timestamp"
    temp_output_file = tmp_path / "temp_test_output.csv"
    columns = None

    chunk1 = pd.DataFrame({
        "col1": [1, 2],
        "col2": ['a', 'b'],
        "timestamp": ["2025-01-01 10:00:00", "2025-01-01 10:00:01"]
    })
    chunk2 = pd.DataFrame({
        "col1": [3, 4],
        "col2": ['c', 'd'],
        "timestamp": ["2025-01-01 10:00:02", "2025-01-01 10:00:03"]
    })
    mock_fetch_data_chunked.return_value = iter([chunk1, chunk2])

    fetcher = MultiDatabaseFetcher()
    fetcher.db = mock_db_manager_instance

    # Act
    result = fetcher.fetch_database_in_chunks(database, filters, chunk_size, str(temp_output_file), pagination_column, columns)

    # Assert
    mock_fetch_data_chunked.assert_called_once_with(mock_engine, "table1", filters, chunk_size, pagination_column, columns, "mysql")
    assert result is True

    exported_df = pd.read_csv(temp_output_file)
    assert "source_database" in exported_df.columns
    assert exported_df["source_database"].tolist() == ["database1", "database1", "database1", "database1"]

@patch("src.services.multi_database_fetcher.fetch_data_chunked")
@patch("src.services.multi_database_fetcher.DBManager")
def test_fetch_chunked_streams_multiple_databases_without_pagination_column_in_output(mock_db_manager_class, mock_fetch_data_chunked, tmp_path):
    mock_db_manager_instance = Mock()
    mock_db_manager_class.return_value = mock_db_manager_instance

    mock_engine1 = Mock()
    mock_engine2 = Mock()
    mock_db_manager_instance.get_engine.side_effect = lambda db: mock_engine1 if db == "database1" else mock_engine2
    mock_db_manager_instance.cfg = {
        "database1": {"table": "table1", "type": "mysql"},
        "database2": {"table": "table2", "type": "mysql"},
    }

    db1_chunk = pd.DataFrame({"id": [1, 2], "value": ["a", "b"]})
    db2_chunk = pd.DataFrame({"id": [3], "value": ["c"]})
    mock_fetch_data_chunked.side_effect = [iter([db1_chunk]), iter([db2_chunk])]

    output_file = tmp_path / "chunked_output.csv"

    fetcher = MultiDatabaseFetcher()
    fetcher.db = mock_db_manager_instance

    result = fetcher.fetch_chunked(
        ["database1", "database2"],
        ["Status = ACTIVE"],
        1000,
        str(output_file),
        pagination_column="id",
        columns=["value"],
    )

    assert result is True
    exported_df = pd.read_csv(output_file)
    assert exported_df.columns.tolist() == ["value", "source_database"]
    assert exported_df.to_dict(orient="records") == [
        {"value": "a", "source_database": "database1"},
        {"value": "b", "source_database": "database1"},
        {"value": "c", "source_database": "database2"},
    ]

@patch("src.services.multi_database_fetcher.fetch_data_chunked")
@patch("src.services.multi_database_fetcher.DBManager")
def test_fetch_chunked_returns_false_when_no_chunks_are_exported(mock_db_manager_class, mock_fetch_data_chunked, tmp_path):
    mock_db_manager_instance = Mock()
    mock_db_manager_class.return_value = mock_db_manager_instance

    mock_engine = Mock()
    mock_db_manager_instance.get_engine.return_value = mock_engine
    mock_db_manager_instance.cfg = {
        "database1": {"table": "table1", "type": "mysql"},
    }

    mock_fetch_data_chunked.return_value = iter([])
    output_file = tmp_path / "empty_chunked_output.csv"

    fetcher = MultiDatabaseFetcher()
    fetcher.db = mock_db_manager_instance

    result = fetcher.fetch_chunked(
        ["database1"],
        ["Status = ACTIVE"],
        1000,
        str(output_file),
        pagination_column="id",
        columns=["value"],
    )

    assert result is False
    assert output_file.exists() is False

@patch("src.services.multi_database_fetcher.fetch_data_chunked")
@patch("src.services.multi_database_fetcher.DBManager")
def test_fetch_chunked_rejects_non_csv_output(mock_db_manager_class, mock_fetch_data_chunked, tmp_path):
    mock_db_manager_instance = Mock()
    mock_db_manager_class.return_value = mock_db_manager_instance
    mock_db_manager_instance.cfg = {"database1": {"table": "table1", "type": "mysql"}}

    fetcher = MultiDatabaseFetcher()
    fetcher.db = mock_db_manager_instance

    with pytest.raises(ValueError, match="supports only CSV"):
        fetcher.fetch_chunked(
            ["database1"],
            [],
            1000,
            str(tmp_path / "chunked_output.xlsx"),
            pagination_column="id",
            columns=None,
        )
