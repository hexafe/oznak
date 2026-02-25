

def export(df, path: str):
    try:
        if path.endswith(".csv"):
            df.to_csv(path, index=False)
            print(f"Data exported to {path}")
        elif path.endswith((".xlsx", ".xls")):
            df.to_excel(path, index=False)
            print(f"Data exported to {path}")
        else:
            raise ValueError("Unsupported format. Use .csv or .xlsx/.xls")
    except Exception as e:
        print(f"Error exporting: {e}")

def export_chunks_streaming(chunks_generator, path: str, write_header: bool = True):
    """
    Export DataFrames yielded by a generator to a single file (CSV) by appending
    Assumes the first chunk determines the column structure
    
    Args:
        chunks_generator: A generator yielding pandas.DataFrames
        path: Path to the output CSV file
        write_header: If True, writes the header for the first chunk. Subsequent chunks append without header
    """
    if not path.endswith(".csv"):
        raise ValueError("Streaming export is currently only supported for CSV format")

    first_chunk = True
    mode = 'w'
    for i, chunk_df in enumerate(chunks_generator):
        print(f"Exporting chunk {i+1} ({len(chunk_df)} records) to {path}...")

        header = write_header if first_chunk else False
        if not first_chunk:
            mode = 'a'

        chunk_df.to_csv(path, mode=mode, header=header, index=False)
        first_chunk = False
        mode = 'a'

    print(f"Chunks exported to {path} successfully")

