"""Test the exact CLI sequence with your real data."""

import os
import json
from core.module_manager import ModuleManager
from core.session import SessionManager

def test_cli_like_sequence():
    print("=== TESTING CLI-LIKE SEQUENCE ===")
    
    # Test 1: Load file
    print("\n--- STEP 1: Loading file ---")
    file_path = r'.\input_data\dane.csv'
    
    if not os.path.exists(file_path):
        print(f"ERROR: File {file_path} not found!")
        return
    
    module_manager = ModuleManager()
    result = module_manager.execute_module('file_loader', {
        'action': 'load_file',
        'file_path': file_path,
        'name': 'cli_test_data',
        'separator': ',',
        'encoding': 'utf-8'
    })
    
    print(f"File load result - Success: {result.success}")
    if result.success:
        print(f"Message: {result.data.get('message', 'No message')}")
    else:
        print(f"Error: {result.error}")
    
    # Test 2: Check session file immediately
    print("\n--- STEP 2: Checking session file ---")
    session_file = 'oznak_session.json'
    print(f"Session file exists: {os.path.exists(session_file)}")
    
    if os.path.exists(session_file):
        try:
            with open(session_file, 'r') as f:
                content = json.load(f)
                print(f"Session file content:")
                print(json.dumps(content, indent=2))
        except Exception as e:
            print(f"Error reading session file: {e}")
            with open(session_file, 'r') as f:
                print(f"Raw content: {f.read()}")
    else:
        print("Session file does not exist")
    
    # Test 3: List sources (simulate second CLI command)
    print("\n--- STEP 3: Listing sources ---")
    session = SessionManager()
    sources = session.get_data_sources()
    print(f"Sources from session: {sources}")
    
    if sources:
        print("Loaded data sources:")
        current_source_name = session.session.get('current_source')
        for name, info in sources.items():
            current_marker = " (current)" if name == current_source_name else ""
            print(f"  - {name}{current_marker}: {info.get('type', 'unknown')} ({info.get('rows', 0):,} rows)")
    else:
        print("No data sources loaded")

def manual_session_test():
    print("\n=== MANUAL SESSION TEST ===")
    
    # Manually create and save session data
    test_session_data = {
        'data_sources': {
            'manual_test': {
                'type': 'file',
                'path': r'.\input_data\dane.csv',
                'rows': 1000,
                'columns': 10,
                'loaded_at': '2023-01-01T12:00:00'
            }
        },
        'current_source': 'manual_test'
    }
    
    # Save to file
    session_file = 'oznak_session.json'
    try:
        with open(session_file, 'w') as f:
            json.dump(test_session_data, f, indent=2)
        print(f"Manual session saved to {session_file}")
        
        # Read it back
        with open(session_file, 'r') as f:
            read_data = json.load(f)
        print(f"Read back: {read_data}")
        print(f"Data matches: {read_data == test_session_data}")
        
    except Exception as e:
        print(f"Error in manual session test: {e}")

if __name__ == "__main__":
    test_cli_like_sequence()
    manual_session_test()