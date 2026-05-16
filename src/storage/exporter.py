
from src._legacy import warn_legacy_module
from oznak.exporter import export_output

warn_legacy_module("src.storage.exporter", "oznak.exporter")


def export(df, path: str):
    try:
        export_output(df, path)
        print(f"Data exported to {path}")
    except Exception as e:
        print(f"Error exporting: {e}")


def export_chunks_streaming(chunks_generator, path: str, write_header: bool = True, mode: str = "w"):
    if not path.endswith(".csv"):
        raise ValueError("Streaming export is currently only supported for CSV format")

    first_chunk = True
    current_mode = mode
    wrote_any_chunks = False

    for chunk_df in chunks_generator:
        if chunk_df.empty:
            continue

        header = write_header if first_chunk else False
        chunk_df.to_csv(path, mode=current_mode, header=header, index=False)
        first_chunk = False
        current_mode = "a"
        wrote_any_chunks = True

    return wrote_any_chunks
