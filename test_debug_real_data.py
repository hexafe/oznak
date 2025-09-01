"""Debug script to test with your real data."""

import os
import pandas as pd
from core.module_manager import ModuleManager
from core.session import SessionManager

def debug_real_data():
    print("=== DEBUGGING WITH YOUR REAL DATA ===")
    
    # Check if the file exists
    file_path = r'.\input_data\dane.csv'
    print(f"File exists: {os.path.exists(file_path)}")
    
    if not os.path.exists(file_path):
        print("ERROR: File not found!")
        return
    
    # Inspect the file
    print("\n--- Inspecting file ---")
    try:
        # Read first few lines to understand format
        with open(file_path, 'r', encoding='utf-8') as f:
            first_lines = [f.readline().strip() for _ in range(3)]
        
        print("First 3 lines:")
        for i, line in enumerate(first_lines, 1):
            print(f"  Line {i}: {repr(line)}")
            
        # Try to detect separator
        first_line = first_lines[0]
        separators = [',', ';', '\t']
        separator_counts = {}
        for sep in separators:
            separator_counts[sep] = first_line.count(sep)
        
        print(f"Separator counts: {separator_counts}")
        detected_separator = max(separator_counts, key=separator_counts.get)
        print(f"Detected separator: '{detected_separator}' (count: {separator_counts[detected_separator]})")
        
    except Exception as e:
        print(f"Error inspecting file: {e}")
    
    # Try loading with pandas
    print("\n--- Loading with pandas ---")
    try:
        # Try different approaches
        df = pd.read_csv(file_path)
        print(f"SUCCESS: Loaded with default comma separator")
        print(f"Shape: {df.shape}")
        print(f"Columns: {list(df.columns)}")
        print(f"First few rows:")
        print(df.head())
        return df
        
    except Exception as e1:
        print(f"Failed with comma separator: {e1}")
        
        try:
            df = pd.read_csv(file_path, sep=';')
            print(f"SUCCESS: Loaded with semicolon separator")
            print(f"Shape: {df.shape}")
            print(f"Columns: {list(df.columns)}")
            print(f"First few rows:")
            print(df.head())
            return df
            
        except Exception as e2:
            print(f"Failed with semicolon separator: {e2}")
            
            # Try auto-detect
            try:
                df = pd.read_csv(file_path, sep=None, engine='python')
                print(f"SUCCESS: Loaded with auto-detect")
                print(f"Shape: {df.shape}")
                print(f"Columns: {list(df.columns)}")
                print(f"First few rows:")
                print(df.head())
                return df
                
            except Exception as e3:
                print(f"Failed with auto-detect: {e3}")
                return None

def test_file_loading_with_real_data():
    print("\n=== TESTING FILE LOADING WITH REAL DATA ===")
    
    file_path = r'.\input_data\dane.csv'
    
    if not os.path.exists(file_path):
        print("ERROR: File not found!")
        return
    
    # Test the exact sequence that's failing
    print("\n--- Testing file loading ---")
    module_manager = ModuleManager()
    result = module_manager.execute_module('file_loader', {
        'action': 'load_file',
        'file_path': file_path,
        'name': 'real_test_data',
        'separator': ',',
        'encoding': 'utf-8'
    })
    
    print(f"Load result success: {result.success}")
    print(f"Load result  {result.data}")
    print(f"Load result error: {result.error}")
    
    # Check session file
    print("\n--- Checking session file ---")
    session_file = 'oznak_session.json'
    print(f"Session file exists: {os.path.exists(session_file)}")
    if os.path.exists(session_file):
        with open(session_file, 'r') as f:
            import json
            try:
                content = json.load(f)
                print(f"Session file content: {content}")
            except Exception as e:
                print(f"Error reading session file: {e}")
                with open(session_file, 'r') as f2:
                    print(f"Raw content: {f2.read()}")
    
    # Test session manager directly
    print("\n--- Testing session manager ---")
    session = SessionManager()
    sources = session.get_data_sources()
    print(f"Session sources: {sources}")

if __name__ == "__main__":
    # First debug the data file
    df = debug_real_data()
    
    # Then test the loading sequence
    if df is not None:
        test_file_loading_with_real_data()