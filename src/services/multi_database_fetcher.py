from concurrent.futures import ThreadPoolExecutor, as_completed
import os

import pandas as pd

from src._legacy import warn_legacy_module
from src.db.manager import DBManager
from src.query.builder import build_query
from src.query.fetcher import fetch_data, fetch_data_chunked
from src.storage.exporter import export_chunks_streaming

warn_legacy_module("src.services.multi_database_fetcher", "oznak.fetcher and oznak.chunked")


def _fetch_single_database(database, filters, limit, date_column, columns, db_manager_instance, order_by_enabled=True):
    """
    Helper function to fetch data from a single database within a thread
    Returns the DataFrame with 'source_database' column or None if it fails
    """
    try:
        print(f"    Thread fetching database: {database}")
        engine = db_manager_instance.get_engine(database)

        cfg = db_manager_instance.cfg[database]
        table = cfg["table"]
        db_type = cfg["type"]

        if order_by_enabled:
            query, params = build_query(table, filters, limit, date_column, columns, db_type)
        else:
            query, params = build_query(
                table,
                filters,
                limit,
                date_column,
                columns,
                db_type,
                order_by_enabled=False,
            )
        print(f"   └── Query: {query[:50]}...") # Could be too much spam on the terminal :(
        
        df = fetch_data(engine, query, params)
        if not df.empty:
            df["source_database"] = database
            print(f"    Thread fetched {len(df)} records from {database}")
            return df
        else:
            print(f"    Thread fetched no data from {database}")
            return None
    except Exception as e:
        print(f"    Thread failed to fetch from {database}: {e}")
        return None


def _prepare_chunk_for_export(chunk_df, database: str, pagination_column: str, columns: list = None):
    prepared_chunk = chunk_df.copy()
    prepared_chunk["source_database"] = database

    if columns is None:
        return prepared_chunk

    requested_columns = list(columns)
    if pagination_column not in requested_columns and pagination_column in prepared_chunk.columns:
        prepared_chunk = prepared_chunk.drop(columns=[pagination_column])

    ordered_columns = list(requested_columns)
    if "source_database" not in ordered_columns:
        ordered_columns.append("source_database")

    available_columns = [column for column in ordered_columns if column in prepared_chunk.columns]
    return prepared_chunk[available_columns]


class MultiDatabaseFetcher:
    def __init__(self):
        self.db = DBManager()

    def fetch(
        self,
        databases: list,
        filters: list,
        limit: int = None,
        date_column: str = "TimeStamp",
        columns: list = None,
        order_by_enabled: bool = True,
    ):
        frames = []

        # Use ThreadPoolExecutor to fetch from multiple databases concurrently
        # max_workers could be configurable, or default to number of CPUs
        with ThreadPoolExecutor() as executor:
            future_to_database = {
                executor.submit(
                    _fetch_single_database,
                    db,
                    filters,
                    limit,
                    date_column,
                    columns,
                    self.db,
                    order_by_enabled,
                ): db for db in databases
            }

            for future in as_completed(future_to_database):
                database = future_to_database[future]
                try:
                    df = future.result()
                    if df is not None and not df.empty:
                        frames.append(df)
                except Exception as e:
                    print(f"Unexpected error processing result for {database}: {e}")

        if not frames:
            print("No data fetched from any database")
            return pd.DataFrame()

        print(f"Combining data from {len(frames)} databases...")
        combined_df = pd.concat(frames, ignore_index=True)
        print(f"Combined {len(combined_df)} records from {len(frames)} databases")

        return combined_df

    def fetch_database_in_chunks(
        self,
        database: str,
        filters: list,
        chunk_size: int,
        temp_output_path: str,
        pagination_column: str = "id",
        columns: list = None,
        write_header: bool = True,
        mode: str = "w",
    ):
        """
        Fetches data in chunks from a single database and exports them directly to the output file
        """
        print(f"Fetching chunks from database: {database}")
        engine = self.db.get_engine(database)
        cfg = self.db.cfg[database]
        table = cfg["table"]
        db_type = cfg["type"]

        chunk_generator = fetch_data_chunked(
            engine,
            table,
            filters,
            chunk_size,
            pagination_column,
            columns,
            db_type,
        )
        prepared_generator = (
            _prepare_chunk_for_export(chunk_df, database, pagination_column, columns)
            for chunk_df in chunk_generator
        )
        return export_chunks_streaming(
            prepared_generator,
            temp_output_path,
            write_header=write_header,
            mode=mode,
        )

    def fetch_chunked(self, databases: list, filters: list, chunk_size: int, output_path: str, pagination_column: str = "id", columns: list = None):
        """
        Fetches data in chunks from multiple databases and streams them into the final output file
        """
        print(f"Starting chunked fetch for databases: {databases}")
        if not output_path.endswith(".csv"):
            raise ValueError("Chunked export currently supports only CSV output")

        if os.path.exists(output_path):
            os.unlink(output_path)

        wrote_any_data = False

        for database in databases:
            database_wrote_data = self.fetch_database_in_chunks(
                database,
                filters,
                chunk_size,
                output_path,
                pagination_column,
                columns,
                write_header=not wrote_any_data,
                mode="a" if wrote_any_data else "w",
            )
            if database_wrote_data:
                wrote_any_data = True

        if not wrote_any_data:
            print("No data fetched in chunked mode")
            return False

        print(f"Chunked fetch and export completed. Output file: {output_path}")
        return True
