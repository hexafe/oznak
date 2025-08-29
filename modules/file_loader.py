"""File loader module for loading data from various file formats"""

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
    elif action == 'list_sources':
        return _list_sources()
    elif action == 'info':
        return _info()
    else:
        return {'error': f'Unknown action: {action}'}


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
        
        if file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
        elif file_path.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(file_path)
        elif file_path.endswith('.json'):
            df = pd.read_json(file_path)
        elif file_path.endswith('.parquet'):
            df = pd.read_parquet(file_path)
        else:
            return {'error': f'Unsupported file format: {file_path}'}
        
        session = SessionManager()
        session.add_data_source(name, {
            'type': 'file',
            'path': file_path,
            'rows': len(df),
            'columns': len(df.columns),
            'loaded_at': datetime.now().isoformat()
        })
        
        return {
            'message': f'Loaded {len(df)} rows, {len(df.columns)} columns from {file_path} as {name}'
        }
        
    except Exception as e:
        return {'error': str(e)}


def _list_sources() -> dict:
    """List all loaded data sources
    
    Returns: Dictionary containing list of data sources
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

