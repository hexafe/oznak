from typing import Any

import pandas as pd
from sqlalchemy import text

from src.query.builder import build_chunked_query


def fetch_data(engine: Any, query: str, params: dict[str, Any] | None = None) -> pd.DataFrame:
    """
    Fetch data using SQLAlchemy engine and return a pandas DataFrame
    Expects query string with :param_name placeholders and a params dictionary
    Uses sqlalchemy.text() for the query and params= keyword for pandas
    """
    try:
        print("Executing query on database...")
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

def fetch_data_chunked(engine: Any, table: str, filters: list[str], chunk_size: int, pagination_column: str = "id", columns: list[str] | None = None):
    """
    Fetches data in chunks using unique, indexed column for pagination

    Args:
        engine: SQLAlchemy engine
        table: Table name
        filters: List of filters
        chunk_size: Number of rows per chunk
        pagination_column: Column for pagination
        columns: Optional list of columns for SELECT

    Yields:
        pandas.DataFrame: A chunk of data
    """
    last_value = None
    while True:
        query, params = build_chunked_query(table, filters, chunk_size, last_value, pagination_column, columns)
        print(f"Fetching chunk with query: {query[:50]}...")

        try:
            if params:
                df = pd.read_sql(text(query), engine, params=params)
            else:
                df = pd.read_sql(text(query), engine)
        except Exception as e:
            print(f"Error executing chunk query: {e}")
            break

        if df.empty:
            print("Reached end of data")
            break

        print(f"Fetched {len(df)} reacords in this chunk")
        yield df

        last_value = df[pagination_column].iloc[-1]

        print(f"   └── Last {pagination_column} in chunk: {last_value}")
