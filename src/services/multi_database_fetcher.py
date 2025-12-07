from src.db.manager import DBManager
from src.query.builder import build_query
from src.query.fetcher import fetch_data, fetch_data_chunked
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import tempfile
import os
from src.storage.exporter import export_chunks_streaming


def _fetch_single_database(database, filters, limit, date_column, columns, db_manager_instance):
    """
    Helper function to fetch data from a single database within a thread
    Returns the DataFrame with 'source_database' column or None if it fails
    """
    try:
        print(f"    Thread fetching database: {database}")
        engine = db_manager_instance.get_engine(database)

        cfg = db_manager_instance.cfg[database]
        table = cfg["table"]

        query, params = build_query(table, filters, limit, date_column, columns)
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


class MultiDatabaseFetcher:
    def __init__(self):
        self.db = DBManager()

    def fetch(self, databases: list, filters: list, limit: int = None, date_column: str = "TimeStamp", columns: list = None):
        frames = []

        # Use ThreadPoolExecutor to fetch from multiple databases concurrently
        # max_workers could be configurable, or default to number of CPUs
        with ThreadPoolExecutor() as executor:
            future_to_database = {
                executor.submit(_fetch_single_database, db, filters, limit, date_column, columns, self.db): db for db in databases
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
            print(f"No data fetched from any database")
            return pd.DataFrame()

        print(f"Combining data from {len(frames)} databases...")
        combined_df = pd.concat(frames, ignore_index=True)
        print(f"Combined {len(combined_df)} records from {len(frames)} databases")

        return combined_df

    def fetch_database_in_chunks(self, database: str, filters: list, chunk_size: int, temp_output_path: str, pagination_column: str = "id", columns: list = None):
        """
        Fetches data in chunks from a single database and exports them directly to a temporary file
        """
        print(f"Fetching chunks from database: {database}")
        engine = self.db.get_engine(database)
        cfg = self.db.cfg[database]
        table = cfg["table"]

        chunk_generator = fetch_data_chunked(engine, table, filters, chunk_size, pagination_column, columns)
        export_chunks_streaming(chunk_generator, temp_output_path)
        print(f"   └── Chunks from {database} exported to {temp_output_path}")

    def fetch_chunked(self, databases: list, filters: list, chunk_size: int, output_path: str, pagination_column: str = "id", columns: list = None):
        """
        Fetches data in chunks from multiple databases and combines them into the final output file
        Uses temporary files per database to manage memory
        """
        print(f"Starting chunked fetch for databases: {databases}")
        temp_files = []

        # Fetch chunks from each database into separate temporary files
        for database in databases:
            # Create temp file for database chunks
            temp_db_file = tempfile.NamedTemporaryFile(mode='w', suffix=".csv", delete=False)
            temp_db_path = temp_db_file.name
            temp_db_file.close()
            temp_files.append(temp_db_path)

            # Fetch and export chunks fro this database
            self.fetch_database_in_chunks(database, filters, chunk_size, temp_db_path, pagination_column, columns)

        # Combine temporary files into the final output file
        print(f"Combining data from {len(temp_files)} temp files tinto {output_path}...")
        combined_df = pd.DataFrame()
        for temp_path in temp_files:
            temp_df = pd.read_csv(temp_path)
            combined_df = pd.concat([combined_df, temp_df], ignore_index=True)
            os.unlink(temp_path) # Delete temp file after reading

        # Export the final combined DataFrame
        if output_path.endswith(".csv"):
            combined_df.to_csv(output_path, index=False)
        elif output_path.endswith((".parquet", ".parq")):
            combined_df.to_parquet(output_path, index=False)
        else:
            raise ValueError(f"Unsupported output format for output file: {output_path}")

        print(f"Chunked fetch and export completed. Output file: {output_path}")

