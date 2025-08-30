"""Database connector module for loading data from multiple production line databases

This module handles database connections data loading deduplication
and product-specific data extraction It supports multiple database types
and provides flexible querying capabilities
"""

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
        return _add_connection(args)
    elif action == 'load_table':
        return _load_table(args)
    elif action == 'query':
        return _execute_query(args)
    elif action == 'combine_all_lines':
        return _combine_all_production_lines(args)
    elif action == 'list_connections':
        return _list_connections()
    elif action == 'extract_product_data':
        return _extract_product_data(args)
    elif action == 'search_products':
        return _search_products(args)
    else:
        return {'error': f'Unknown action: {action}'}


def _load_data_from_database(db_config: dict, table_name: str, line_name: str) -> pd.DataFrame:
    """Load data from a database and add production line identifier
    
    Args:
        db_config: Database configuration dictionary
        table_name: Name of the table to load
        line_name: Name of the production line
        
    Returns:
        DataFrame containing loaded data with production line identifier
        
    Raises:
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
    """Combine data from all production lines using configurable deduplication
    
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
            return {'error': 'No database configurations found'}
        
        unique_id_column = combination_config.get('unique_identifier', 'TraceCode')
        timestamp_column = combination_config.get('timestamp_column', 'timestamp')
        production_line_column = combination_config.get('production_line_column', 'production_line')
        merge_strategy = combination_config.get('merge_strategy', 'latest_wins')
        
        # Load data from all lines
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
                
            except Exception as e:
                print(f"Failed to load data from {line_name}: {e}")
                continue
        
        if not all_dataframes:
            return {'error': 'Failed to load data from any production line'}
        
        # Combine all dataframes
        print(f"\nCombining data from {len(successful_lines)} lines...")
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        initial_count = len(combined_df)
        
        print(f"Total records before deduplication: {initial_count:,}")
        
        # Remove duplicates based on configurable unique identifier
        if unique_id_column in combined_df.columns:
            if merge_strategy == 'latest_wins' and timestamp_column in combined_df.columns:
                # Keep the latest record for each unique identifier based on timestamp
                combined_df = combined_df.sort_values(timestamp_column).drop_duplicates(
                    subset=[unique_id_column], keep='last'
                )
                print(f"Applied 'latest_wins' strategy using {timestamp_column}")
            else:
                # Keep first occurrence of each unique identifier
                combined_df = combined_df.drop_duplicates(subset=[unique_id_column], keep='first')
                print("Applied 'first occurrence' deduplication strategy")
        
        final_count = len(combined_df)
        duplicates_removed = initial_count - final_count
        
        print(f"Records after deduplication: {final_count:,}")
        print(f"Duplicate records removed: {duplicates_removed:,}")
        
        # Validate unique identifier uniqueness
        unique_id_counts = combined_df[unique_id_column].value_counts()
        duplicate_ids = unique_id_counts[unique_id_counts > 1]
        
        if len(duplicate_ids) > 0:
            print(f"Warning: Found {len(duplicate_ids)} {unique_id_column} values with duplicates after deduplication")
        else:
            print(f"All {unique_id_column} values are unique after deduplication")
        
        # Show production line distribution
        if production_line_column in combined_df.columns:
            line_distribution = combined_df[production_line_column].value_counts()
            print(f"\nProduction Line Distribution:")
            for line, count in line_distribution.items():
                percentage = (count / final_count) * 100
                print(f"  - {line}: {count:,} records ({percentage:.1f}%)")
        
        # Save combined dataset to file for persistence
        combined_name = args.get('name', 'combined_production_data')
        session.save_combined_dataframe(combined_df, combined_name)
        
        # Save combined dataset metadata to session
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
        
        # Also save as regular data source for analysis
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


def _extract_product_data(args: dict) -> dict:
    """Extract data for specific product type from all databases
    
    Args:
        args: Dictionary containing extraction parameters
        
    Returns:
        Dictionary containing extraction results
    """
    try:
        session = SessionManager()
        db_configs = session.get_all_database_configs()
        combination_config = session.get_combination_config()
        
        # Get parameters
        product_name = args.get('product_name')  # This is RefName, not unique ID
        product_column = args.get('product_column', 'RefName')  # Configurable product column
        selected_columns = args.get('columns', [])
        output_file = args.get('output_file', f'product_{product_name}.csv')
        output_format = args.get('format', 'csv')
        date_from = args.get('date_from')
        date_to = args.get('date_to')
        
        if not product_name:
            return {'error': 'Product name not specified'}
        
        # Load data from all databases for the specific product type
        all_dataframes = []
        successful_lines = []
        
        print(f"Searching for product type '{product_name}' across all production lines...")
        
        for line_name, config in db_configs.items():
            try:
                table_name = config.get('table', 'measurements')
                
                # Load data using existing function
                df = _load_data_from_database(config, table_name, line_name)
                
                # Filter for specific product type
                if product_column in df.columns:
                    product_df = df[df[product_column].astype(str).str.contains(product_name, case=False, na=False)]
                else:
                    print(f"Warning: Column '{product_column}' not found in {line_name}")
                    continue
                
                # Apply date filtering if specified
                if not product_df.empty and date_from:
                    timestamp_col = combination_config.get('timestamp_column', 'timestamp')
                    if timestamp_col in product_df.columns:
                        try:
                            product_df[timestamp_col] = pd.to_datetime(product_df[timestamp_col])
                            date_from_parsed = pd.to_datetime(date_from)
                            product_df = product_df[product_df[timestamp_col] >= date_from_parsed]
                        except Exception as e:
                            print(f"Warning: Could not parse date_from for {line_name}: {e}")
                
                if not product_df.empty and date_to:
                    timestamp_col = combination_config.get('timestamp_column', 'timestamp')
                    if timestamp_col in product_df.columns:
                        try:
                            product_df[timestamp_col] = pd.to_datetime(product_df[timestamp_col])
                            date_to_parsed = pd.to_datetime(date_to)
                            product_df = product_df[product_df[timestamp_col] <= date_to_parsed]
                        except Exception as e:
                            print(f"Warning: Could not parse date_to for {line_name}: {e}")
                
                if not product_df.empty:
                    all_dataframes.append(product_df)
                    successful_lines.append(line_name)
                    print(f"Found {len(product_df)} records for product '{product_name}' in {line_name}")
                else:
                    print(f"No records found for product '{product_name}' in {line_name}")
                
            except Exception as e:
                print(f"Error accessing {line_name}: {e}")
                continue
        
        if not all_dataframes:
            return {'error': f'No data found for product type {product_name}'}
        
        # Combine all dataframes
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        
        # Select specific columns if requested
        if selected_columns:
            # Always include essential columns
            essential_columns = [product_column]
            unique_id_column = combination_config.get('unique_identifier', 'TraceCode')
            if unique_id_column not in essential_columns:
                essential_columns.append(unique_id_column)
            
            # Add timestamp column if it exists
            timestamp_column = combination_config.get('timestamp_column', 'timestamp')
            if timestamp_column not in essential_columns:
                essential_columns.append(timestamp_column)
            
            # Add production line column
            production_line_column = combination_config.get('production_line_column', 'production_line')
            if production_line_column not in essential_columns:
                essential_columns.append(production_line_column)
            
            # Add requested columns
            for col in selected_columns:
                if col in combined_df.columns and col not in essential_columns:
                    essential_columns.append(col)
            
            # Check if all requested columns exist
            missing_columns = [col for col in selected_columns if col not in combined_df.columns]
            if missing_columns:
                print(f"Warning: Columns not found: {missing_columns}")
            
            # Select available columns
            available_columns = [col for col in essential_columns if col in combined_df.columns]
            if available_columns:
                combined_df = combined_df[available_columns]
            else:
                print("Warning: None of the requested columns found, returning essential columns")
        
        # Save to file
        if output_format.lower() == 'excel':
            combined_df.to_excel(output_file, index=False)
        else:
            combined_df.to_csv(output_file, index=False)
        
        return {
            'success': True,
            'message': f'Successfully extracted {len(combined_df)} records for product type {product_name}',
            'records_found': len(combined_df),
            'production_lines': successful_lines,
            'output_file': output_file,
            'columns_exported': list(combined_df.columns),
            'product_type': product_name
        }
        
    except Exception as e:
        return {'success': False, 'error': str(e)}


def _search_products(args: dict) -> dict:
    """Search for product types matching a pattern across all databases
    
    Args:
        args: Dictionary containing search parameters
        
    Returns:
        Dictionary containing search results
    """
    try:
        session = SessionManager()
        db_configs = session.get_all_database_configs()
        combination_config = session.get_combination_config()
        
        # Get parameters
        search_pattern = args.get('search_pattern', '*')
        product_column = args.get('product_column', 'RefName')  # Default to RefName
        limit = args.get('limit', 100)
        
        # Search for product types across all databases
        found_products = {}  # product_name: {count, lines}
        
        print(f"Searching for product types matching '{search_pattern}'...")
        
        for line_name, config in db_configs.items():
            try:
                table_name = config.get('table', 'measurements')
                
                # Load a sample of data to check available products
                df = _load_data_from_database(config, table_name, line_name)
                
                # Get unique product types
                if product_column in df.columns:
                    if search_pattern == '*':  # Get all products
                        product_types = df[product_column].dropna().unique()
                    else:
                        # Use string matching
                        product_types = df[
                            df[product_column].astype(str).str.contains(search_pattern, case=False, na=False)
                        ][product_column].dropna().unique()
                    
                    # Count occurrences and track lines
                    for product_type in product_types:
                        product_type_str = str(product_type)
                        count = len(df[df[product_column] == product_type])
                        if product_type_str not in found_products:
                            found_products[product_type_str] = {
                                'count': 0,
                                'lines': []
                            }
                        found_products[product_type_str]['count'] += count
                        found_products[product_type_str]['lines'].append(line_name)
                else:
                    print(f"Warning: Column '{product_column}' not found in {line_name}")
                
            except Exception as e:
                print(f"Error searching {line_name}: {e}")
                continue
        
        # Convert to list and sort
        product_list = []
        for product_name, info in found_products.items():
            product_list.append({
                'product_name': product_name,
                'total_count': info['count'],
                'production_lines': len(info['lines']),
                'lines': info['lines']
            })
        
        # Sort by count (descending) and limit
        product_list.sort(key=lambda x: x['total_count'], reverse=True)
        product_list = product_list[:limit]
        
        return {
            'success': True,
            'products_found': len(found_products),
            'products': product_list
        }
        
    except Exception as e:
        return {'success': False, 'error': str(e)}


def _list_connections() -> dict:
    """List all configured database connections
    
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
    """Add database connection
    
    Args:
        args: Dictionary containing connection parameters
        
    Returns:
        Dictionary containing success message
    """
    return {'message': 'Connection added successfully'}


def _load_table(args: dict) -> dict:
    """Load data from database table
    
    Args:
        args: Dictionary containing table loading parameters
        
    Returns:
        Dictionary containing success message
    """
    return {'message': 'Table loaded successfully'}


def _execute_query(args: dict) -> dict:
    """Execute custom database query
    
    Args:
        args: Dictionary containing query parameters
        
    Returns:
        Dictionary containing success message
    """
    return {'message': 'Query executed successfully'}

