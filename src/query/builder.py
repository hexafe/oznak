import re

from src.services.filter_parser import parse_filter_string


def build_query(table: str, filters: list[str], limit: int = None, date_column: str = "TimeStamp", columns: list[str] = None):
    """
    Build a SQL query with generic filters
    filters: list of filter strings like ["RefName LIKE V123456", "Date >= 2025-01-01"]
    date_column: name of the date/timestamp column for default ordering (when using LIMIT)
    columns: list of columns for SELECT statement
    """
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", table):
        raise ValueError(f"Invalid table name: {table}")

    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", date_column):
        raise ValueError(f"Invalid date column name: {date_column}")

    if columns is not None:
        validated_columns = []
        for col in columns:
            if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", col):
                raise ValueError(f"Invalid column name: {col}")
            validated_columns.append(f"`{col}`")
        select_clause = f"SELECT {', '.join(validated_columns)}"
    else:
        select_clause = "SELECT *"

    safe_table = f"`{table}`"
    safe_date_col = f"`{date_column}`"

    where_conditions = []
    params = {}
    param_counter = 0

    for filter_str in filters:
        column, operator, value = parse_filter_string(filter_str)
        safe_column = f"`{column}`"

        if operator in ["LIKE", "NOT LIKE"]:
            param_name = f"param_{param_counter}"
            where_conditions.append(f"{safe_column} {operator} :{param_name}")
            params[param_name] = value
            param_counter += 1
        elif operator in ["IN", "NOT IN"]:
            values = [v.strip() for v in value.split(",") if v.strip()]
            if not values:
                raise ValueError(f"{operator} operator requires at least one value")
            placeholders = []
            for v in values:
                param_name = f"param_{param_counter}"
                placeholders.append(f":{param_name}")
                params[param_name] = v
                param_counter += 1
            where_conditions.append(f"{safe_column} {operator} ({','.join(placeholders)})")
        elif operator in ["IS", "IS NOT"]:
            where_conditions.append(f"{safe_column} {operator} {value}")
        else:
            param_name = f"param_{param_counter}"
            where_conditions.append(f"{safe_column} {operator} :{param_name}")
            params[param_name] = value
            param_counter += 1

    if where_conditions:
        base_query = f"{select_clause} FROM {safe_table} WHERE {' AND '.join(where_conditions)}"
    else:
        base_query = f"{select_clause} FROM {safe_table}"

    if limit:
        if not isinstance(limit, int) or limit <= 0:
            raise ValueError("LIMIT must be a positive integer")
        base_query = f"{base_query} ORDER BY {safe_date_col} DESC LIMIT {limit}"

    return base_query, params


def build_chunked_query(
    table: str,
    filters: list[str],
    chunk_size: int,
    last_value_for_pagination: any = None,
    pagination_column: str = "id",
    columns: list[str] = None,
):
    """
    Builds a query to fetch a specific chunk of data based on a unique, indexed column.
    """
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", table):
        raise ValueError(f"Invalid table name: {table}")
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", pagination_column):
        raise ValueError(f"Invalid pagination column name: {pagination_column}")
    if columns:
        for col in columns:
            if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", col):
                raise ValueError(f"Invalid column name: {col}")

    safe_table = f"`{table}`"
    safe_pagination_col = f"`{pagination_column}`"

    if columns:
        select_clause = f"SELECT {', '.join([f'`{col}`' for col in columns])}"
    else:
        select_clause = "SELECT *"

    where_conditions = []
    params = {}
    param_counter = 0

    for filter_str in filters:
        column, operator, value = parse_filter_string(filter_str)
        safe_column = f"`{column}`"

        if operator in ["LIKE", "NOT LIKE"]:
            param_name = f"param_{param_counter}"
            where_conditions.append(f"{safe_column} {operator} :{param_name}")
            params[param_name] = value
            param_counter += 1
        elif operator in ["IN", "NOT IN"]:
            values = [v.strip() for v in value.split(",") if v.strip()]
            if not values:
                raise ValueError(f"{operator} operator requires at least one value")
            placeholders = []
            for v in values:
                param_name = f"param_{param_counter}"
                placeholders.append(f":{param_name}")
                params[param_name] = v
                param_counter += 1
            where_conditions.append(f"({safe_column} {operator} ({','.join(placeholders)}))")
        elif operator in ["IS", "IS NOT"]:
            where_conditions.append(f"{safe_column} {operator} {value}")
        else:
            param_name = f"param_{param_counter}"
            where_conditions.append(f"{safe_column} {operator} :{param_name}")
            params[param_name] = value
            param_counter += 1

    if last_value_for_pagination is not None:
        where_conditions.append(f"{safe_pagination_col} > :pagination_param")
        params["pagination_param"] = last_value_for_pagination

    full_where = f"WHERE {' AND '.join(where_conditions)}" if where_conditions else ""
    query = f"{select_clause} FROM {safe_table} {full_where} ORDER BY {safe_pagination_col} ASC LIMIT {chunk_size}"

    return query, params
