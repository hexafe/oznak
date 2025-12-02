from src.db.manager import DBManager
from src.query.builder import build_query
from src.query.fetcher import fetch_data
import pandas as pd


class MultiDatabaseFetcher:
    def __init__(self):
        self.db = DBManager()

    def fetch(self, databases: list, filters: list, limit: int = None, date_column: str = "TimeStamp"):
        frames = []

        for i, database in enumerate(databases):
            print(f"Processing database {i+1}/{len(databases)}: {database}")
            try:
                engine = self.db.get_engine(database)
            except Exception as e:
                print(f"Could not connect to {database}, skipping... Error: {e}")
                continue

            cfg = self.db.cfg[database]
            table = cfg["table"]

            # Build a query safely with generic filters
            try:
                query, params = build_query(table, filters, limit, date_column)
                print(f"   └── Query: {query[:50]}...")
            except ValueError as e:
                print(f"Error building query for {database}: {e}")
                continue

            df = fetch_data(engine, query, params)
            if not df.empty:
                df["source_database"] = database
                frames.append(df)
            else:
                print(f"   └── No data returned for this database")

        if not frames:
            print(f"No data fetched from any database")
            return pd.DataFrame()

        print(f"Combining data from {len(frames)} databases...")
        combined_df = pd.concat(frames, ignore_index=True)
        print(f"Combined {len(combined_df)} records from {len(frames)} databases")

        return combined_df

