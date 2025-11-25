import pandas as pd


def fetch_data(connection, query: str, params: tuple = None):
    try:
        # Use pandas with parameters to prevent SQL injection
        if params:
            df = pd.read_sql(query, connection, params=params)
        else:
            df = pd.read_sql(query, connection)
        print("Fetched {len(df)} records from query: {query[:50]}...")
        return df
    except Exception as e:
        print(f"Error executing query: {e}")
        return pd.DataFrame()

