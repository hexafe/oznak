"""Command-line interface for the Oznak application

This module implements the command-line interface for Oznak
It provides user-friendly commands for data management analysis
and reporting operations
"""

import click
import pandas as pd
from core.module_manager import ModuleManager
from core.session import SessionManager


@click.group()
@click.version_option(version='1.0.0')
def cli() -> None:
    """Oznak - Multi-Database Production Data Analysis System"""
    pass


@cli.command()
def setup() -> None:
    """Setup Oznak configuration files"""
    import os
    import shutil
    
    config_dir = 'config'
    if not os.path.exists(config_dir):
        os.makedirs(config_dir)
    
    template_files = {
        'databases_template.yaml': """# Database connection configuration
databases:
  line_a:
    type: postgresql
    host: 192.168.1.10
    port: 5432
    database: production_line_a
    username: production_reader
    password_ref: line_a_password
    table: measurements
    
  line_b:
    type: mysql
    host: 192.168.1.11
    port: 3306
    database: production_line_b
    username: mysql_reader
    password_ref: line_b_password
    table: measurements

# Data combination settings
combination:
  unique_identifier: TraceCode      # Column used for deduplication
  timestamp_column: timestamp       # Column used for latest-wins strategy
  merge_strategy: latest_wins       # or 'first_occurrence'
  production_line_column: production_line
  product_name_column: RefName       # Column containing product names

# Analysis configuration
analysis:
  outlier_method: iqr               # or 'zscore'
  outlier_factor: 1.5               # IQR multiplier
  decimal_precision: 4              # Decimal places in output
  default_capability_threshold: 1.33

# Data processing settings
data_processing:
  date_formats:
    - "%Y-%m-%d"
    - "%Y-%m-%d %H:%M:%S"
    - "%m/%d/%Y"
  numeric_tolerance: 0.01
  missing_value_strategy: drop      # or 'fill_mean', 'fill_median', 'fill_mode'

# Filter configuration
filters:
  default_date_range_days: 30       # 0 or -1 for all data, positive for days
  case_sensitive_filters: false
  filter_combination: and           # or 'or'

# Output formatting
output:
  thousands_separator: ","
  decimal_separator: "."
  date_display_format: "%Y-%m-%d %H:%M:%S"

# Quality thresholds
quality:
  stable_process_threshold: 10      # % relative std dev
  unstable_process_threshold: 25    # % relative std dev
  outlier_warning_threshold: 1.0    # % of data
  outlier_critical_threshold: 5.0   # % of data
  cpk_warning_threshold: 1.0
  cpk_critical_threshold: 0.67

# Database defaults
database_defaults:
  connection_timeout: 30
  query_timeout: 300
  max_retries: 3
  retry_delay: 5
""",
        'passwords_template.yaml': """# Database passwords - keep this file secure!
passwords:
  line_a_password: "your_password_for_line_a"
  line_b_password: "your_password_for_line_b"
  line_c_password: "your_password_for_line_c"

"""
    }
    
    for filename, content in template_files.items():
        template_path = os.path.join(config_dir, filename)
        if not os.path.exists(template_path):
            with open(template_path, 'w') as f:
                f.write(content)
    
    user_files = {
        'databases.yaml': 'databases_template.yaml',
        'passwords.yaml': 'passwords_template.yaml'
    }
    
    for user_file, template_file in user_files.items():
        user_path = os.path.join(config_dir, user_file)
        template_path = os.path.join(config_dir, template_file)
        
        if not os.path.exists(user_path) and os.path.exists(template_path):
            shutil.copy2(template_path, user_path)


@cli.command()
def check_config() -> None:
    """Check configuration files"""
    session = SessionManager()
    db_configs = session.get_all_database_configs()
    passwords = session.passwords
    
    if db_configs:
        print("Database configurations found:")
        for name, config in db_configs.items():
            password_ref = config.get('password_ref', 'N/A')
            has_password = password_ref in passwords if password_ref != 'N/A' else True
            status = "OK" if has_password or config['type'] == 'sqlite' else "MISSING"
            print(f"  {status} {name}: {config['type']} @ {config.get('host', config.get('server', config.get('path', 'N/A')))}")
    else:
        print("No database configurations found")


@cli.command()
@click.argument('file_path', type=click.Path(exists=True))
@click.option('-n', '--name', help='Name for this data source')
@click.option('--separator', default=',', help='CSV separator')
@click.option('--encoding', default='utf-8', help='File encoding')
def load_file(file_path: str, name: str, separator: str, encoding: str) -> None:
    """Load data from file
    
    Args:
        file_path: Path to the file to load
        name: Name for this data source
        separator: CSV separator character
        encoding: File encoding
    """
    module_manager = ModuleManager()
    result = module_manager.execute_module('file_loader', {
        'action': 'load_file',
        'file_path': file_path,
        'name': name,
        'separator': separator,
        'encoding': encoding
    })
    
    if result.success:
        print(result.data.get('message', 'File loaded successfully'))
    else:
        print(f"Error: {result.error}")


@cli.command()
@click.argument('file_paths', type=click.Path(exists=True), nargs=-1)
@click.option('-n', '--names', help='Comma-separated names for each file')
@click.option('--separator', default=',', help='CSV separator')
@click.option('--encoding', default='utf-8', help='File encoding')
def load_files(file_paths: tuple, names: str, separator: str, encoding: str) -> None:
    """Load multiple files at once
    
    Args:
        file_paths: Paths to the files to load
        names: Comma-separated names for each file
        separator: CSV separator character
        encoding: File encoding
    """
    if not file_paths:
        print("No files specified")
        return
    
    name_list = names.split(',') if names else None
    
    module_manager = ModuleManager()
    result = module_manager.execute_module('file_loader', {
        'action': 'load_multiple_files',
        'file_paths': list(file_paths),
        'names': name_list,
        'separator': separator,
        'encoding': encoding
    })
    
    if result.success:
        print(result.data.get('message', 'Files loaded successfully'))
    else:
        print(f"Error: {result.error}")


@cli.command()
@click.argument('file_paths', type=click.Path(exists=True), nargs=-1)
@click.option('-n', '--name', help='Name for combined dataset')
def combine_files(file_paths: tuple, name: str) -> None:
    """Combine multiple files into single dataset
    
    Args:
        file_paths: Paths to the files to combine
        name: Name for the combined dataset
    """
    if not file_paths:
        print("No files specified")
        return
    
    module_manager = ModuleManager()
    result = module_manager.execute_module('file_loader', {
        'action': 'combine_multiple_files',
        'file_paths': list(file_paths),
        'combined_name': name
    })
    
    if result.success:
        data = result.data
        print("FILES COMBINATION REPORT")
        print("=" * 30)
        print(f"Combined {len(file_paths)} files")
        print(f"Total records: {data.get('total_rows', 0):,}")
        print(f"Dataset name: {data.get('combined_name', 'N/A')}")
        
        # Set as current dataset
        session_manager = SessionManager()
        session_manager.session['current_source'] = data.get('combined_name')
        session_manager._save_session()
        print(f"Combined dataset is now active for analysis")
        
    else:
        print(f"Error: {result.error}")


@cli.command()
@click.argument('db_name')
@click.argument('table_name')
@click.option('-n', '--name', help='Name for this data source')
def load_db(db_name: str, table_name: str, name: str) -> None:
    """Load data from database table
    
    Args:
        db_name: Name of the database connection
        table_name: Name of the table to load
        name: Name for this data source
    """
    module_manager = ModuleManager()
    result = module_manager.execute_module('db_connector', {
        'action': 'load_table',
        'db_name': db_name,
        'table_name': table_name,
        'name': name
    })
    
    if result.success:
        print(result.data.get('message', 'Data loaded successfully'))
    else:
        print(f"Error: {result.error}")


@cli.command()
def combine_lines() -> None:
    """Combine data from all configured production lines using configurable deduplication"""
    module_manager = ModuleManager()
    result = module_manager.execute_module('db_connector', {
        'action': 'combine_all_lines'
    })
    
    if result.success:
        data = result.data
        print("PRODUCTION LINE DATA COMBINATION REPORT")
        print("=" * 50)
        print(f"{data.get('message', 'Lines combined successfully')}")
        print(f"Lines processed: {', '.join(data.get('lines_combined', []))}")
        print(f"Initial records: {data.get('total_records_initial', 0):,}")
        print(f"Final records: {data.get('total_records_final', 0):,}")
        print(f"Duplicates removed: {data.get('duplicates_removed', 0):,}")
        print(f"Unique identifiers: {data.get('unique_tracecodes', 0):,}")
        print(f"Strategy: {data.get('deduplication_strategy', 'N/A')}")
        
        line_dist = data.get('line_distribution', {})
        if line_dist:
            print(f"\nProduction Line Distribution:")
            total_records = data.get('total_records_final', 0)
            for line, count in line_dist.items():
                percentage = (count / total_records) * 100
                print(f"  - {line}: {count:,} records ({percentage:.1f}%)")
        
        session_manager = SessionManager()
        session_manager.session['current_source'] = 'combined_production_data'
        session_manager._save_session()
        print(f"\nCombined dataset is now active for analysis")
        print(f"Use 'oznak analyze <column>' to begin analysis")
        
    else:
        print(f"Error: {result.error}")


@cli.command()
def list_lines() -> None:
    """List all configured production lines and their settings"""
    module_manager = ModuleManager()
    result = module_manager.execute_module('db_connector', {
        'action': 'list_connections'
    })
    
    if result.success:
        connections = result.data.get('connections', [])
        details = result.data.get('details', {})
        if connections:
            print("CONFIGURED PRODUCTION LINES")
            print("=" * 40)
            for conn in connections:
                detail = details.get(conn, {})
                print(f"{conn}")
                print(f"  Type: {detail.get('type', 'unknown')}")
                print(f"  Location: {detail.get('location', 'N/A')}")
                print(f"  Table: {detail.get('table', 'N/A')}")
                print()
        else:
            print("No production lines configured")
            print("Configure lines in config/databases.yaml")
    else:
        print(f"Error: {result.error}")


@cli.command()
def line_distribution() -> None:
    """Show production line distribution in combined dataset"""
    session = SessionManager()
    current_source = session.get_current_source()
    
    if not current_source or current_source.get('type') != 'combined_dataset':
        print("No combined dataset loaded. Run 'combine-lines' first.")
        return
    
    line_dist = current_source.get('line_distribution', {})
    total_records = current_source.get('rows', 0)
    
    if line_dist:
        print("PRODUCTION LINE DISTRIBUTION")
        print("=" * 40)
        for line, count in line_dist.items():
            percentage = (count / total_records) * 100
            print(f"{line}: {count:,} records ({percentage:.1f}%)")
        print(f"Total: {total_records:,} records")
    else:
        print("No line distribution data available")


@cli.command()
@click.argument('source_name')
def use(source_name: str) -> None:
    """Set current working dataset
    
    Args:
        source_name: Name of the dataset to use
    """
    session_manager = SessionManager()
    if session_manager.set_current_source(source_name):
        print(f"Using dataset: {source_name}")
    else:
        print(f"Dataset '{source_name}' not found")


@cli.command()
def list_sources() -> None:
    """List all loaded data sources"""
    session = SessionManager()
    sources = session.get_data_sources()
    if sources:
        print("Loaded data sources:")
        for name, info in sources.items():
            current_marker = " (current)" if name == session.session.get('current_source') else ""
            print(f"  - {name}{current_marker}: {info.get('type', 'unknown')} ({info.get('rows', 0):,} rows)")
    else:
        print("No data sources loaded")


@cli.command()
def columns() -> None:
    """List all columns in the current dataset"""
    session = SessionManager()
    current = session.get_current_source()
    
    if not current:
        print("No dataset selected. Use 'use' command first.")
        return
    
    try:
        # Load the actual data to get real column names
        if current.get('type') == 'combined_dataset':
            df = session.load_combined_dataframe()
        elif current.get('type') == 'file':
            file_path = current.get('path')
            if file_path.endswith('.csv'):
                df = pd.read_csv(file_path, nrows=0)
            elif file_path.endswith(('.xlsx', '.xls')):
                df = pd.read_excel(file_path, nrows=0)
            elif file_path.endswith('.parquet'):
                df = pd.read_parquet(file_path)
            else:
                print("Unsupported file format for column listing")
                return
        else:
            print("Column listing not available for this data source type")
            return
            
        print("Available columns in current dataset:")
        print("=" * 40)
        for i, column in enumerate(df.columns, 1):
            print(f"  {i:2d}. {column}")
            
    except Exception as e:
        print(f"Error loading column information: {e}")


@cli.command()
@click.argument('column')
@click.option('--usl', type=float, help='Upper specification limit')
@click.option('--lsl', type=float, help='Lower specification limit')
@click.option('--date-column', help='Column to filter by date')
@click.option('--date-from', help='Start date (YYYY-MM-DD)')
@click.option('--date-to', help='End date (YYYY-MM-DD)')
@click.option('--filter-column', help='Column to filter by value')
@click.option('--filter-value', help='Value to filter by')
def analyze(column: str, usl: float, lsl: float, date_column: str, 
           date_from: str, date_to: str, filter_column: str, filter_value: str) -> None:
    """Perform statistical analysis on column
    
    Args:
        column: Column to analyze
        usl: Upper specification limit
        lsl: Lower specification limit
        date_column: Column to filter by date
        date_from: Start date for filtering
        date_to: End date for filtering
        filter_column: Column to filter by value
        filter_value: Value to filter by
    """
    module_manager = ModuleManager()
    result = module_manager.execute_module('analyzer', {
        'action': 'analyze',
        'column': column,
        'usl': usl,
        'lsl': lsl,
        'date_column': date_column,
        'date_from': date_from,
        'date_to': date_to,
        'filter_column': filter_column,
        'filter_value': filter_value
    })
    
    if result.success:
        data = result.data
        column_name = data.get('column_analyzed', column)
        
        # Basic Statistics
        stats = data.get('statistics', {})
        if stats:
            print(f"\nBasic Statistics for {column_name}:")
            print("=" * 40)
            for key, value in stats.items():
                if isinstance(value, float):
                    print(f"  {key}: {value:.4f}")
                else:
                    print(f"  {key}: {value}")
        
        # Quality Analysis
        quality = data.get('quality', {})
        if quality:
            print(f"\nQuality Analysis:")
            print("=" * 25)
            for key, value in quality.items():
                if isinstance(value, float):
                    print(f"  {key}: {value:.4f}")
                else:
                    print(f"  {key}: {value}")
        
        # Insights
        insights = data.get('insights', {})
        if insights:
            print(f"\nInsights:")
            print("=" * 15)
            for key, value in insights.items():
                if isinstance(value, dict):
                    print(f"  {key}:")
                    for sub_key, sub_value in value.items():
                        print(f"    {sub_key}: {sub_value}")
                else:
                    print(f"  {key}: {value}")
        
        # Filtering Info
        filtering_info = data.get('filtering_info', {})
        if filtering_info:
            print(f"\nFiltering Information:")
            print("=" * 25)
            print(f"  Original records: {filtering_info.get('original_records', 0):,}")
            print(f"  After filtering: {filtering_info.get('records_after_filtering', 0):,}")
            removed = filtering_info.get('records_removed_by_filtering', 0)
            if removed > 0:
                print(f"  Removed by filters: {removed:,}")
        
        print(f"\nAnalyzed {data.get('analyzed_records', 0):,} records")
        
    else:
        print(f"Error: {result.error}")


@cli.command()
def info() -> None:
    """Show information about current dataset"""
    session = SessionManager()
    current = session.get_current_source()
    if current:
        print("Current dataset information:")
        print("=" * 35)
        print(f"  Name: {session.session.get('current_source')}")
        print(f"  Type: {current.get('type', 'unknown')}")
        print(f"  Rows: {current.get('rows', 0):,}")
        print(f"  Columns: {current.get('columns', 0)}")
        print(f"  Created: {current.get('created_at', 'N/A')}")
        
        if current.get('type') == 'combined_dataset':
            print(f"  Source: {current.get('source', 'N/A')}")
            print(f"  Lines: {', '.join(current.get('lines', []))}")
            print(f"  Duplicates removed: {current.get('duplicates_removed', 0):,}")
            
            # Try to show actual columns
            try:
                df = session.load_combined_dataframe()
                print(f"  Available columns: {list(df.columns)}")
            except:
                pass
        elif current.get('type') == 'file':
            print(f"  File path: {current.get('path', 'N/A')}")
    else:
        print("No current dataset selected")
       
