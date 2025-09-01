"""File loader module for loading data from various file formats"""

import pandas as pd
import os
from core.session import SessionManager
from datetime import datetime
import re


def execute(args: dict) -> dict:
    """Execute file loader actions
    
    Args:
        args: Dictionary containing action and parameters
        
    Returns:
        Dictionary containing execution results
    """
    action = args.get('action')
    
    if action == 'load_file':
        return _load_file(args)
    elif action == 'load_multiple_files':
        return _load_multiple_files(args)
    elif action == 'combine_multiple_files':
        return _combine_multiple_files(args)
    elif action == 'list_sources':
        return _list_sources()
    elif action == 'info':
        return _info()
    else:
        return {'error': f'Unknown action: {action}'}


def _detect_csv_separators(file_path: str) -> tuple:
    """Auto-detect CSV separator and decimal separator
    
    Args:
        file_path: Path to the CSV file
        
    Returns:
        Tuple of (separator, decimal_separator)
    """
    try:
        # Read first few lines to detect separators
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            first_lines = [f.readline().strip() for _ in range(5) if f.readline().strip()]
        
        if not first_lines:
            return ',', '.'  # Default fallback
        
        first_line = first_lines[0]
        
        # Count common separators
        separators = [',', ';', '\t', '|']
        separator_counts = {}
        
        for sep in separators:
            separator_counts[sep] = first_line.count(sep)
        
        # Determine most likely separator
        if separator_counts:
            separator = max(separator_counts, key=separator_counts.get)
            # If no separators found, default to comma
            if separator_counts[separator] == 0:
                separator = ','
        else:
            separator = ','
        
        # Detect decimal separator by looking at number patterns
        decimal_separator = '.'
        
        # Look for patterns that suggest European format
        sample_data = ' '.join(first_lines[:2])
        
        # Count numbers with commas vs dots
        comma_number_pattern = r'\b\d+,\d+\b'
        dot_number_pattern = r'\b\d+\.\d+\b'
        
        comma_matches = len(re.findall(comma_number_pattern, sample_data))
        dot_matches = len(re.findall(dot_number_pattern, sample_data))
        
        # If more numbers have commas, likely European format
        if comma_matches > dot_matches and comma_matches > 0:
            decimal_separator = ','
        
        # Semicolon separator often indicates European format
        if separator == ';':
            decimal_separator = ','
        
        print(f"DEBUG: Detected separator='{separator}', decimal='{decimal_separator}'")
        return separator, decimal_separator
        
    except Exception as e:
        print(f"DEBUG: Error detecting CSV format: {e}")
        return ',', '.'  # Default fallback


def _load_csv_european_format(file_path: str, separator: str, encoding: str) -> pd.DataFrame:
    """Load CSV with proper handling of European formats
    
    Args:
        file_path: Path to the CSV file
        separator: CSV column separator
        encoding: File encoding
        
    Returns:
        Loaded DataFrame
    """
    try:
        print(f"DEBUG: Loading European CSV with separator='{separator}', encoding='{encoding}'")
        
        # For European formats, use appropriate settings
        if separator == ';' or ',' in encoding:  # Suspect European format
            # Try loading with European format settings
            df = pd.read_csv(
                file_path,
                sep=separator,
                encoding=encoding,
                decimal=',' if separator == ';' else '.',  # European decimal separator
                thousands=None  # Disable thousands separator to avoid conflicts
            )
            print(f"DEBUG: Successfully loaded European CSV format")
            return df
        else:
            # Standard format
            df = pd.read_csv(file_path, sep=separator, encoding=encoding)
            print(f"DEBUG: Successfully loaded standard CSV format")
            return df
            
    except Exception as e1:
        print(f"DEBUG: CSV loading failed: {e1}")
        
        # Try alternative approaches for European formats
        try:
            # Try with different decimal separator
            df = pd.read_csv(
                file_path,
                sep=separator,
                encoding=encoding,
                decimal=',' if separator == ';' else '.',
                thousands=None
            )
            print(f"DEBUG: Successfully loaded with alternative decimal separator")
            return df
            
        except Exception as e2:
            print(f"DEBUG: Alternative loading failed: {e2}")
            
            # Try with different encoding
            encodings = ['utf-8', 'latin1', 'cp1252', 'iso-8859-1']
            for enc in encodings:
                if enc != encoding:  # Don't retry the same encoding
                    try:
                        df = pd.read_csv(file_path, sep=separator, encoding=enc)
                        print(f"DEBUG: Successfully loaded with encoding {enc}")
                        return df
                    except:
                        continue
            
            # If all else fails, re-raise the original exception
            raise e1


def _get_file_format(file_path: str) -> str:
    """Get file format
    
    Args:
        file_path: Path to the file
        
    Returns:
        File format string
    """
    if file_path.endswith('.csv'):
        return 'csv'
    elif file_path.endswith(('.xlsx', '.xls')):
        return 'excel'
    elif file_path.endswith('.json'):
        return 'json'
    elif file_path.endswith('.parquet'):
        return 'parquet'
    return 'unknown'


def _load_file(args: dict) -> dict:
    """Load data from file
    
    Args:
        args: Dictionary containing file loading parameters
        
    Returns:
        Dictionary containing load results
    """
    try:
        file_path = args['file_path']
        name = args.get('name') or os.path.splitext(os.path.basename(file_path))[0]
        separator = args.get('separator', ',')
        encoding = args.get('encoding', 'utf-8')
        
        print(f"DEBUG: Loading file: {file_path}")
        print(f"DEBUG: Requested separator: '{separator}'")
        print(f"DEBUG: Encoding: {encoding}")
        
        # Handle spaces in file path (remove quotes if present)
        if file_path.startswith("'") and file_path.endswith("'"):
            file_path = file_path[1:-1]
        elif file_path.startswith('"') and file_path.endswith('"'):
            file_path = file_path[1:-1]
        
        print(f"DEBUG: Cleaned file path: {file_path}")
        
        # Check if file exists
        if not os.path.exists(file_path):
            return {'error': f'File not found: {file_path}'}
        
        # File loading based on format
        if file_path.endswith('.csv'):
            # For CSV, auto-detect separator if not explicitly specified
            if separator == ',' and args.get('separator') is None:
                print("DEBUG: Auto-detecting CSV format...")
                detected_separator, decimal_sep = _detect_csv_separators(file_path)
                separator = detected_separator
                print(f"DEBUG: Will use separator: '{separator}'")
            
            # Load CSV with proper handling for European format
            try:
                df = _load_csv_european_format(file_path, separator, encoding)
                print(f"DEBUG: CSV loaded successfully with {len(df)} rows")
                
                # Check if DataFrame is empty or has very few rows (might indicate parsing issue)
                if len(df) == 0:
                    print("DEBUG: WARNING - Loaded DataFrame is empty!")
                    # Try alternative approach
                    df = pd.read_csv(file_path, sep=None, engine='python', encoding=encoding, on_bad_lines='skip')
                    print(f"DEBUG: Python engine gave {len(df)} rows")
                
                # Check for parsing issues
                if len(df.columns) == 1 and separator != ',':
                    print("DEBUG: Suspecting separator issue, trying comma separator...")
                    df = pd.read_csv(file_path, sep=',', encoding=encoding, on_bad_lines='skip')
                    print(f"DEBUG: Comma separator gave {len(df)} rows with {len(df.columns)} columns")
                
            except Exception as csv_error:
                print(f"DEBUG: CSV loading failed: {csv_error}")
                return {'error': f'Failed to load CSV file: {str(csv_error)}'}
            
        elif file_path.endswith(('.xlsx', '.xls')):
            sheet_name = args.get('sheet_name', 0)  # First sheet by default
            df = pd.read_excel(file_path, sheet_name=sheet_name)
            print(f"DEBUG: Excel loaded with {len(df)} rows")
            
        elif file_path.endswith('.json'):
            df = pd.read_json(file_path)
            print(f"DEBUG: JSON loaded with {len(df)} rows")
            
        elif file_path.endswith('.parquet'):
            df = pd.read_parquet(file_path)
            print(f"DEBUG: Parquet loaded with {len(df)} rows")
            
        else:
            return {'error': f'Unsupported file format: {file_path}'}
        
        print(f"DEBUG: Final DataFrame shape: {df.shape}")
        print(f"DEBUG: Final DataFrame columns: {list(df.columns)}")
        
        # Validate that we actually loaded meaningful data
        if len(df) == 0:
            return {'error': 'Loaded file contains no data rows'}
        
        if len(df.columns) == 0:
            return {'error': 'Loaded file contains no columns'}
        
        # Save to session
        session = SessionManager()
        source_info = {
            'type': 'file',
            'path': file_path,
            'rows': len(df),
            'columns': len(df.columns),
            'loaded_at': datetime.now().isoformat(),
            'file_format': _get_file_format(file_path),
            'separator': separator if file_path.endswith('.csv') else None
        }
        
        session.add_data_source(name, source_info)
        print(f"DEBUG: Data source added to session successfully")
        
        return {
            'message': f'SUCCESSFULLY_LOADED: {len(df)} rows, {len(df.columns)} columns from {file_path} as {name}'
        }
        
    except Exception as e:
        import traceback
        print(f"DEBUG: Exception in _load_file: {e}")
        print(f"DEBUG: Traceback: {traceback.format_exc()}")
        return {'error': f'CRITICAL_ERROR_LOADING_FILE: {str(e)}'}


def _load_multiple_files(args: dict) -> dict:
    """Load multiple files
    
    Args:
        args: Dictionary containing file loading parameters
        
    Returns:
        Dictionary containing load results
    """
    try:
        file_paths = args.get('file_paths', [])
        names = args.get('names', [])
        separator = args.get('separator', ',')
        encoding = args.get('encoding', 'utf-8')
        
        results = []
        for i, file_path in enumerate(file_paths):
            # Handle spaces in file path
            if file_path.startswith("'") and file_path.endswith("'"):
                file_path = file_path[1:-1]
            elif file_path.startswith('"') and file_path.endswith('"'):
                file_path = file_path[1:-1]
                
            name = names[i] if i < len(names) and names[i] else None
            result = _load_file({
                'file_path': file_path,
                'name': name,
                'separator': separator,
                'encoding': encoding
            })
            results.append(result)
        
        successful = sum(1 for r in results if r.get('message') and 'SUCCESSFULLY_LOADED' in r['message'])
        return {
            'message': f'LOADED_FILES: {successful} out of {len(file_paths)} files successfully',
            'results': results
        }
        
    except Exception as e:
        return {'error': str(e)}


def _combine_multiple_files(args: dict) -> dict:
    """Combine multiple files into single dataset
    
    Args:
        args: Dictionary containing file combination parameters
        
    Returns:
        Dictionary containing combination results
    """
    try:
        file_paths = args.get('file_paths', [])
        combined_name = args.get('combined_name', f"combined_files_{len(file_paths)}")
        separator = args.get('separator', ',')
        encoding = args.get('encoding', 'utf-8')
        
        dataframes = []
        file_info = []
        
        for i, file_path in enumerate(file_paths):
            # Handle spaces in file path
            if file_path.startswith("'") and file_path.endswith("'"):
                file_path = file_path[1:-1]
            elif file_path.startswith('"') and file_path.endswith('"'):
                file_path = file_path[1:-1]
                
            # Load each file
            if file_path.endswith('.csv'):
                if separator == ',' and args.get('separator') is None:
                    sep, _ = _detect_csv_separators(file_path)
                else:
                    sep = separator
                df = _load_csv_european_format(file_path, sep, encoding)
            elif file_path.endswith(('.xlsx', '.xls')):
                df = pd.read_excel(file_path)
            elif file_path.endswith('.json'):
                df = pd.read_json(file_path)
            elif file_path.endswith('.parquet'):
                df = pd.read_parquet(file_path)
            else:
                continue
                
            # Add source identifier
            source_name = f"source_{i}"
            df['source_file'] = source_name
            dataframes.append(df)
            file_info.append({
                'file': file_path,
                'source_name': source_name,
                'rows': len(df)
            })
        
        if not dataframes:
            return {'error': 'No valid files to combine'}
        
        # Combine all dataframes
        combined_df = pd.concat(dataframes, ignore_index=True)
        
        # Save combined dataset
        session = SessionManager()
        session.save_combined_dataframe(combined_df, combined_name)
        
        return {
            'message': f'COMBINED_FILES: {len(file_paths)} files into {len(combined_df)} rows',
            'combined_name': combined_name,
            'file_details': file_info,
            'total_rows': len(combined_df)
        }
        
    except Exception as e:
        return {'error': str(e)}


def _list_sources() -> dict:
    """List all loaded data sources
    
    Returns:
        Dictionary containing list of data sources
    """
    session = SessionManager()
    sources = list(session.get_data_sources().keys())
    return {'sources': sources}


def _info() -> dict:
    """Get information about current data source
    
    Returns:
        Dictionary containing current data source information
    """
    session = SessionManager()
    current = session.get_current_source()
    if current:
        return current
    else:
        return {}
    
