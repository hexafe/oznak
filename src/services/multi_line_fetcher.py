from src.db.manager import DBManager
from src.query.builder import build_query
from src.query.fetcher import fetch_data
import pandas as pd


class MultiLineFetcher:
    def __init__(self):
        self.db = DBManager()

    def fetch(self, lines: list, filters: list, limit: int = None):
        frames = []

        for line in lines:
            conn = self.db.connect(line)
            if not conn:
                print("Could not connect to {line}, skipping...")
                continue

            cfg = self.db.cfg[line]
            table = cfg["table"]

            # Build a query safely with generic filters
            try:
                query, params = build_query(table, filters, limit)
            except ValueError as e:
                print(f"Error building query for {line}: {e}")
                continue

            df = fetch_data(conn, query, tuple(params) if params else None)
            df["source_line"] = line
            frames.append(df)

            conn.close()

        if not frames:
            print(f"No data fetched from any line")
            return pd.DataFrame()

        combined_df = pd.concat(frames, ignore_index=True)
        print(f"Combined {len(combined_df)} records from {len(frames)} lines")

        return combined_df

