# test_csv_detection.py
"""Test CSV separator detection."""

from modules.file_loader import _detect_csv_separators, _load_csv_with_locale_handling

def test_csv_detection():
    print("=== Testing CSV Separator Detection ===")
    
    # Test with your file
    file_path = r'.\input_data\dane.csv'
    
    if os.path.exists(file_path):
        separator, decimal_sep = _detect_csv_separators(file_path)
        print(f"Detected separator: '{separator}'")
        print(f"Detected decimal separator: '{decimal_sep}'")
        
        # Try loading with detected separators
        try:
            df = _load_csv_with_locale_handling(file_path, separator, 'utf-8', decimal_sep)
            print(f"Successfully loaded {len(df)} rows, {len(df.columns)} columns")
            print("First few rows:")
            print(df.head())
        except Exception as e:
            print(f"Error loading file: {e}")
    else:
        print(f"File not found: {file_path}")

if __name__ == "__main__":
    import os
    test_csv_detection()