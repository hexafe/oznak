import pandas as pd


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

def export_chunks_streaming(chunks_generator, path: str, write_header: bool = True, mode: str = "w"):
    """
    Export DataFrames yielded by a generator to a single file (CSV) by appending
    Assumes the first chunk determines the column structure
    
    Args:
        chunks_generator: A generator yielding pandas.DataFrames
        path: Path to the output CSV file
        write_header: If True, writes the header for the first chunk. Subsequent chunks append without header
    """
    if not path.endswith(".csv"):
        raise ValueError(f"Streaming export is currently only supported for CSV format")

    first_chunk = True
    current_mode = mode
    wrote_any_chunks = False

    for i, chunk_df in enumerate(chunks_generator):
        if chunk_df.empty:
            continue

        print(f"Exporting chunk {i+1} ({len(chunk_df)} records) to {path}...")

        header = write_header if first_chunk else False
        chunk_df.to_csv(path, mode=current_mode, header=header, index=False)
        first_chunk = False
        current_mode = 'a'
        wrote_any_chunks = True

    if wrote_any_chunks:
        print(f"Chunks exported to {path} successfully")
    else:
        print(f"No chunks exported to {path}")

    return wrote_any_chunks
