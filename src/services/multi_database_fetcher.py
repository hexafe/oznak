from src.db.manager import DBManager
from src.query.builder import build_query
from src.query.fetcher import fetch_data
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed


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

