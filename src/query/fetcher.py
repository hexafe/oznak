import pandas as pd
from sqlalchemy import text


def fetch_data(engine, query: str, params: dict = None):
    """
    Fetch data using SQLAlchemy engine and return a pandas DataFrame
    Expects query string with :param_name placeholders and a params dictionary
    Uses sqlalchemy.text() for the query and params= keyword for pandas
    """
    try:
        print(f"Executing query on database...")
        # Use pandas with the SQLAlchemy engine
        if params:
            df = pd.read_sql(text(query), engine, params=params)
        else:
            df = pd.read_sql(text(query), engine)
        print(f"Fetched {len(df)} records from database")
        return df
    except Exception as e:
        print(f"Error executing query: {e}")
        return pd.DataFrame()

