import pytest
from unittest.mock import patch, Mock
import pandas as pd
from sqlalchemy import text


""" Test 1: can we import the main modules without errors? """
def test_import_core_modules():
    try:
        import src.db.manager
        import src.query.builder
        import src.query.fetcher
        import src.services.multi_database_fetcher
        import src.cli.main
        import src.storage.exporter
        #TODO: add other modues after implementation
    except ImportError as e:
        pytest.fail(f"Failed to import core module: {e}")

""" Test 2: can we instantiate key classes without errors? """
def test_instantiate_core_classes():
    try:
        from src.services.multi_database_fetcher import MultiDatabaseFetcher
        from src.db.manager import DBManager

        fetcher_instance = MultiDatabaseFetcher()
        # db_manager_instance = DBManager()
        
        assert isinstance(fetcher_instance, MultiDatabaseFetcher)
    except Exception as e:
        pytest.fail(f"Failed to instantiate core class: {e}")

""" Test 3: can we call key methods with mocked dependencies without errors? """
@patch("src.services.multi_database_fetcher.DBManager")
@patch("src.query.fetcher.pd.read_sql")
def test_multi_database_fetcher_integration(mock_read_sql, mock_db_manager_class):
    # Arrange
    mock_db_manager_instance = mock_db_manager_class.return_value
    mock_engine = Mock()
    mock_db_manager_instance.get_engine.return_value = mock_engine
    mock_db_manager_instance.cfg = {
        "database1": {"table": "test_table"}
    }

    # Mock the return value of pd.read_sql (which is called by fetch_data)
    mock_read_sql.return_value = pd.DataFrame({"col1": [1], "col2": ["test"]})

    # Instantiate the fetcher and inject the mocked DB manager
    from src.services.multi_database_fetcher import MultiDatabaseFetcher
    fetcher = MultiDatabaseFetcher()

    # Act
    filters = ["test_col = test_val"]
    result_df = fetcher.fetch(["database1"], filters, limit=1, date_column="Date")

    # Assert
    mock_db_manager_instance.get_engine.assert_called_once_with("database1")
    assert mock_read_sql.call_count == 1
    
    call_args = mock_read_sql.call_args
    args, kwargs = call_args
    assert "params" in kwargs

""" Test 4: can the CLI app be loaded without errors? """
def test_cli_app_loads():
    try:
        from src.cli.main import app
        from typer.main import Typer
        assert isinstance(app, Typer)
    except Exception as e:
        pytest.fail(f"Failed to load or validate CLI app: {e}")

#TODO: add more integration tests for other critical paths after implementation

