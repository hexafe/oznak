from typing import Any

import pandas as pd
from sqlalchemy import text

from src._legacy import warn_legacy_module
from src.query.builder import build_chunked_query

warn_legacy_module("src.query.fetcher", "oznak.fetcher and oznak.chunked")


def fetch_data(engine: Any, query: str, params: dict[str, Any] | None = None) -> pd.DataFrame:
    try:
        print("Executing query on database...")
        if params:
            df = pd.read_sql(text(query), engine, params=params)
        else:
            df = pd.read_sql(text(query), engine)
        print(f"Fetched {len(df)} records from database")
        return df
    except Exception as e:
        print(f"Error executing query: {e}")
        return pd.DataFrame()


def fetch_data_chunked(
    engine: Any,
    table: str,
    filters: list[str],
    chunk_size: int,
    pagination_column: str = "id",
    columns: list[str] | None = None,
    db_type: str = "mysql",
):
    last_value = None
    while True:
        query, params = build_chunked_query(
            table,
            filters,
            chunk_size,
            last_value,
            pagination_column,
            columns,
            db_type,
        )
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

        if pagination_column not in df.columns:
            raise ValueError(
                f"Pagination column '{pagination_column}' is missing from chunk results"
            )
        if not df[pagination_column].is_unique:
            raise ValueError(
                f"Pagination column '{pagination_column}' must be unique within each chunk"
            )

        print(f"Fetched {len(df)} records in this chunk")
        yield df

        last_value = df[pagination_column].iloc[-1]
        print(f"   └── Last {pagination_column} in chunk: {last_value}")
