"""Inspect your CSV file format."""

import pandas as pd
import csv

def inspect_csv_format():
    print("=== INSPECTING CSV FORMAT ===")
    
    file_path = r'.\input_data\dane.csv'
    
    # Method 1: Try different encodings and separators
    encodings = ['utf-8', 'latin1', 'cp1252']
    separators = [',', ';', '\t']
    
    for encoding in encodings:
        print(f"\n--- Trying encoding: {encoding} ---")
        for separator in separators:
            try:
                # Read just the header to check
                df = pd.read_csv(file_path, sep=separator, encoding=encoding, nrows=5)
                print(f"SUCCESS with separator '{separator}':")
                print(f"  Shape: {df.shape}")
                print(f"  Columns: {list(df.columns)}")
                print(f"  First row: {df.iloc[0].to_dict()}")
                print(f"  Data types: {df.dtypes.to_dict()}")
                return df, separator, encoding
            except Exception as e:
                print(f"  Failed with separator '{separator}': {str(e)[:100]}")
    
    print("All combinations failed!")
    return None, None, None

def detailed_csv_inspection():
    print("\n=== DETAILED CSV INSPECTION ===")
    
    file_path = r'.\input_data\dane.csv'
    
    # Read file in binary mode to check encoding
    try:
        with open(file_path, 'rb') as f:
            raw_data = f.read(1000)  # Read first 1000 bytes
            print(f"Raw data (first 100 bytes): {raw_data[:100]}")
            
            # Try to decode with different encodings
            for encoding in ['utf-8', 'latin1', 'cp1252']:
                try:
                    decoded = raw_data.decode(encoding)
                    print(f"Decoded with {encoding}: {repr(decoded[:200])}")
                except Exception as e:
                    print(f"Failed to decode with {encoding}: {e}")
    except Exception as e:
        print(f"Error reading file: {e}")
    
    # Try to read with csv module to understand structure
    print("\n--- Using csv module ---")
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            for i, row in enumerate(reader):
                if i < 5:  # Show first 5 rows
                    print(f"Row {i}: {row}")
                else:
                    break
    except Exception as e:
        print(f"Error with csv reader: {e}")

if __name__ == "__main__":
    df, separator, encoding = inspect_csv_format()
    detailed_csv_inspection()
    
    if df is not None:
        print(f"\n=== RECOMMENDATION ===")
        print(f"Use separator: '{separator}'")
        print(f"Use encoding: '{encoding}'")
        print(f"Command: python oznak.py load-file \"{r'.\input_data\dane.csv'}\" --separator \"{separator}\" --encoding \"{encoding}\"")