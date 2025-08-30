"""File loader module for loading data from various file formats

This module handles loading data from CSV Excel JSON and Parquet files
It supports various file encodings and separators and provides
flexible data loading capabilities
"""

import pandas as pd
import os
from core.session import SessionManager
from datetime import datetime


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


def _detect_csv_separator(file_path: str) -> str:
    """Auto-detect CSV separator
    
    Args:
        file_path: Path to the CSV file
        
    Returns:
        Detected separator character
    """
    try:
        # Read first few lines to detect separator
        with open(file_path, 'r', encoding='utf-8') as f:
            first_lines = [f.readline() for _ in range(3)]
        
        # Count common separators
        separators = [',', ';', '\t', '|']
        separator_counts = {}
        
        for sep in separators:
            count = sum(line.count(sep) for line in first_lines)
            separator_counts[sep] = count
        
        # Return separator with highest count
        if separator_counts:
            return max(separator_counts, key=separator_counts.get)
        return ','  # Default fallback
    except:
        return ','  # Default fallback


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
        
        # CSV file loading with custom separator
        if file_path.endswith('.csv'):
            # Try to auto-detect separator if not specified
            if separator == ',' and args.get('separator') is None:
                separator = _detect_csv_separator(file_path)
            
            df = pd.read_csv(file_path, sep=separator, encoding=encoding)
            
        elif file_path.endswith(('.xlsx', '.xls')):
            sheet_name = args.get('sheet_name', 0)  # First sheet by default
            df = pd.read_excel(file_path, sheet_name=sheet_name)
            
        elif file_path.endswith('.json'):
            df = pd.read_json(file_path)
            
        elif file_path.endswith('.parquet'):
            df = pd.read_parquet(file_path)
            
        else:
            return {'error': f'Unsupported file format: {file_path}'}
        
        # Save to session
        session = SessionManager()
        session.add_data_source(name, {
            'type': 'file',
            'path': file_path,
            'rows': len(df),
            'columns': len(df.columns),
            'loaded_at': datetime.now().isoformat(),
            'file_format': _get_file_format(file_path),
            'separator': separator if file_path.endswith('.csv') else None
        })
        
        return {
            'message': f'Loaded {len(df)} rows, {len(df.columns)} columns from {file_path} as {name}'
        }
        
    except Exception as e:
        return {'error': str(e)}


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
            name = names[i] if i < len(names) and names[i] else None
            result = _load_file({
                'file_path': file_path,
                'name': name,
                'separator': separator,
                'encoding': encoding
            })
            results.append(result)
        
        successful = sum(1 for r in results if r.get('message'))
        return {
            'message': f'Loaded {successful} out of {len(file_paths)} files successfully',
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
            # Load each file
            if file_path.endswith('.csv'):
                if separator == ',' and args.get('separator') is None:
                    sep = _detect_csv_separator(file_path)
                else:
                    sep = separator
                df = pd.read_csv(file_path, sep=sep, encoding=encoding)
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
            'message': f'Combined {len(file_paths)} files into {len(combined_df)} rows',
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

