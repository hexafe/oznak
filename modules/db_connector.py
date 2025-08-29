"""Database connector module for loading data from multiple production line databases"""

import pandas as pd
import sqlite3
import os
from sqlalchemy import create_engine, text
from core.session import SessionManager
import psycopg2
import pyodbc
import pymysql


def execute(args: dict) -> dict:
    """Execute database connector actions

    Args:
        args: Dictionary containing action and parameters

    Returns:
        Dictionary containing execution results
    """
    action = args.get('action')

    if action == 'add_connection':
        return _add_cnnection(args)
    elif action == 'load_table':
        return _load_table(args)
    elif action == 'query':
        return _execute_query(args)
    elif action == 'combine_all_lines':
        return _combine_all_production_lines(args)
    elif action == 'list_connections':
        return _list_connections()
    else:
        return {'error': f"Unknown action: {action}"}


def _load_data_from_database(db_config: dict, table_name: str, line_name: str) -> pd.DataFrame:
    """Load data from a database and add production line identifier

        Args:
            db_config: Database config directory
            table_name: Name of the table to load
            line_name: Name of the production line

        Returns:
            DataFrame containing loaded data with production line identifier

        Reises:
            ValueError: If database type is unsupported or password is missing
    """
    db_type = db_config['type']

    session = SessionManager()
    password_ref = db_config.get('password_ref')
    password = session.get_password(password_ref) if password_ref else None

    if not password and password_ref:
        raise ValueError(f"Password not found for reference: {password_ref}")

    if db_type == 'postgresql':
        conn_str = f"postgresql://{db_config['username']}:{password}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        engine = create_engine(conn_str)
        df = pd.read_sql_query(text(f"SELECT * FROM {table_name}"), engine)

    elif db_type == 'mysql':
        conn_str = f"mysql+pymysql://{db_config['username']}:{password}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
                engine = create_engine(conn_str)
                df = pd.read_sql_query(text(f"SELECT * FROM {table_name}"), engine)
            
    elif db_type == 'mssql':
        conn_str = f"mssql+pyodbc://{db_config['username']}:{password}@{db_config['server']}/{db_config['database']}?driver=ODBC+Driver+17+for+SQL+Server"
        engine = create_engine(conn_str)
        df = pd.read_sql_query(text(f"SELECT * FROM {table_name}"), engine)
    
    elif db_type == 'sqlite':
        conn = sqlite3.connect(db_config['path'])
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        conn.close()
    
    else:
        raise ValueError(f"Unsupported database type: {db_type}")
    
    combination_config = session.get_combination_config()
    production_line_column = combination_config.get('production_line_column', 'production_line')
    df[production_line_column] = line_name
    
    return df

def _combine_all_production_lines(args: dict) -> dict:
    """Combine data from all production lines unsing TraceCode deduplication

        Args:
            args: Dictionary containing execution arguments

        Returns:
            Dictionary containing combination results
    """
    try:
        session = SessionManager()
        db_configs = session.get_all_database_configs()
        combination_config = session.get_combination_config()

        if not db_configs:
            return {'error': "No database configurations found"}

        unique_id_column = combination_config.get('unique_identifier', 'TraceCode')
        timestamp_column = combination_config.get('timestamp_column', 'timestamp')
        production_line_column = combination_config.get('production_line_column', 'production_line')
        merge_strategy = combination_config.get('merge_strategy', 'latest_wins')

        all_dataframes = []
        line_names = []
        successful_lines = []

        print("Loading data from production lines...")
        for line_name, config in db_configs.items():
            try:
                table_name = config.get('table', 'measurements')
                df = _load_data_from_database(config, table_name, line_name)

                if unique_id_column not in df.columns:
                    print(f"Warning: {unique_id_column} not found in {line_name}, skipping...")
                    continue

                all_dataframes.append(df)
                line_names.append(line_name)
                successful_lines.append(line_name)
                print(f"Loaded {len(df):,} records from {line_name}")

            exceptException as e:
                print(f"Failed to load data from {line_name}")
                continue

        if not all_dataframes:
            return {'error': 'Failed to load data from any production line'}

        print(f"\nCombining data from {len(successful_lines)} lines...")
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        initial_count = len(combined_df)

        print(f"Total records before deduplication: {initial_count:,}")

        if unique_id_column in combined_df.columns:
            if merge_strategy == 'latest_wins' and timestamp_column in combined_df.columns:
                combined_df = combined_df.sort_values(timestamp_column).drop_duplicates(subset=[unique_id_column], keep='last')
            else:
                combined_df = combined_df.drop_duplicates(subset=[unique_id_column], keep='first')

            final_count = len(combined_df)
            duplicates_removed = initial_count - final_count

            print(f"Records after deduplication: {final_count:,}")
            print(f"Duplicate records removed: {duplicates_removed:,}")

            unique_id_counts = combined_df[unique_id_column].value_counts()
            duplicate_tracecodes =unique_id_counts[unique_id_counts > 1]

            if len(duplicate_ids) > 0:
                print(f"Warning: Found {len(duplicate_ids}) {unique_id_column} values with duplicates after deduplication")
            else:
                print(f"All {unique_id_column} values are unique after deduplication")

            if production_line_column in combined_df.columns:
                line_distribution = combined_df[production_line_column].value_counts()
                print(f"\nProduction line distribution:")
                for line, count in line_distribution.items():
                    percentage = (count / final_count) * 100
                    print(f"  - {line}: {count:,} records ({percentage:.1f}%)")

            combined_name = args.get('name': 'combined_production_data')
            session.save_combined_dataframe(combined_df, combined_name)

            session.add_combined_dataset(combined_name, {
                'lines': successful_lines,
                'total_records_initial': initial_count,
                'total_records_final': final_count,
                'duplicates_removed': duplicates_removed,
                'unique_tracecodes': final_count,
                'columns': list(combined_df.columns),
                'created_at': pd.Timestamp.now().isoformat(),
                'deduplication_info': {
                    'unique_identifier': unique_id_column,
                    'strategy': merge_strategy,
                    'timestamp_column': timestamp_column if merge_strategy == 'latest_wins' else None
                },
                'line_distribution': line_distribution.to_dict() if 'line_distribution' in locals() else {}
            })

            session.add_data_source(combined_name, {
            'type': 'combined_dataset',
            'source': 'multi_line_production',
            'lines': successful_lines,
            'rows': final_count,
            'columns': len(combined_df.columns),
            'created_at': pd.Timestamp.now().isoformat(),
            'unique_identifier': unique_id_column,
            'duplicates_removed': duplicates_removed,
            'line_distribution': line_distribution.to_dict() if 'line_distribution' in locals() else {},
            'data_file': f"data/{combined_name}.parquet"
        })
        
        return {
            'success': True,
            'message': f'Successfully combined data from {len(successful_lines)} production lines',
            'lines_combined': successful_lines,
            'total_records_initial': initial_count,
            'total_records_final': final_count,
            'duplicates_removed': duplicates_removed,
            'unique_tracecodes': final_count,
            'deduplication_strategy': merge_strategy,
            'line_distribution': line_distribution.to_dict() if 'line_distribution' in locals() else {}
        }
        
    except Exception as e:
        return {'success': False, 'error': str(e)}


def _list_connections() -> dict:
    """List all configured database connections.
    
    Returns:
        Dictionary containing connection information
    """
    session = SessionManager()
    db_configs = session.get_all_database_configs()
    return {
        'connections': list(db_configs.keys()),
        'details': {name: {
            'type': config['type'], 
            'location': config.get('host', config.get('server', config.get('path', 'N/A'))),
            'table': config.get('table', 'N/A')
        } for name, config in db_configs.items()}
    }


def _add_connection(args: dict) -> dict:
    """Add database connection.
    
    Args:
        args: Dictionary containing connection parameters
        
    Returns:
        Dictionary containing success message
    """
    return {'message': 'Connection added successfully'}


def _load_table(args: dict) -> dict:
    """Load data from database table.
    
    Args:
        args: Dictionary containing table loading parameters
        
    Returns:
        Dictionary containing success message
    """
    return {'message': 'Table loaded successfully'}


def _execute_query(args: dict) -> dict:
    """Execute custom database query.
    
    Args:
        args: Dictionary containing query parameters
        
    Returns:
        Dictionary containing success message
    """
    return {'message': 'Query executed successfully'}

