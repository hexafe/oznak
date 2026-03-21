import re
from typing import Any


def _quote_identifier(identifier: str, db_type: str) -> str:
    if db_type == "mssql":
        return f"[{identifier}]"
    return f"`{identifier}`"


def _build_select_clause(columns: list[str] | None = None, db_type: str = "mysql", limit: int | None = None) -> str:
    if columns is not None:
        validated_columns = [_quote_identifier(col, db_type) for col in columns]
        select_target = ", ".join(validated_columns)
    else:
        select_target = "*"

    if db_type == "mssql" and limit is not None:
        return f"SELECT TOP {limit} {select_target}"

    return f"SELECT {select_target}"


def parse_filter_string(filter_str: str) -> tuple[str, str, str]:
    normalized_filter = filter_str.strip()
    if not normalized_filter:
        raise ValueError(f"Invalid filter format: {filter_str}. Expected: 'column operator value'")

    pattern = re.compile(
        r"^(?P<column>[a-zA-Z_][a-zA-Z0-9_]*)\s+"
        r"(?P<operator>IS\s+NOT|NOT\s+LIKE|NOT\s+IN|IS|LIKE|IN|!=|<>|<=|>=|=|<|>)\s+"
        r"(?P<value>.+)$",
        re.IGNORECASE,
    )
    match = pattern.match(normalized_filter)
    if not match:
        parts = normalized_filter.split()
        if len(parts) >= 3 and re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", parts[0]):
            raise ValueError(f"Invalid operator: {parts[1].upper()}")
        raise ValueError(f"Invalid filter format: {filter_str}. Expected: 'column operator value'")

    column = match.group("column")
    operator = re.sub(r"\s+", " ", match.group("operator").upper()).strip()
    value = match.group("value").strip()

    allowed_operators = {
        "=", "!=", "<>", "<=", ">=", "<", ">",
        "LIKE", "NOT LIKE", "IN", "NOT IN",
        "IS", "IS NOT",
    }
    if operator not in allowed_operators:
        raise ValueError(f"Invalid operator: {operator}. Allowed: {', '.join(allowed_operators)}")
    if not value:
        raise ValueError(f"Invalid filter format: {filter_str}. Expected: 'column operator value'")

    return column, operator, value


def build_query(
    table: str,
    filters: list[str],
    limit: int | None = None,
    date_column: str = "TimeStamp",
    columns: list[str] | None = None,
    db_type: str = "mysql",
) -> tuple[str, dict[str, Any]]:
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", table):
        raise ValueError(f"Invalid table name: {table}")
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", date_column):
        raise ValueError(f"Invalid date column name: {date_column}")
    if db_type not in {"mysql", "mssql"}:
        raise ValueError(f"Unsupported DB type: {db_type}")

    if columns is not None:
        for col in columns:
            if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", col):
                raise ValueError(f"Invalid column name: {col}")

    if limit:
        if not isinstance(limit, int) or limit <= 0:
            raise ValueError("LIMIT must be a positive integer")

    select_clause = _build_select_clause(columns, db_type, limit)
    safe_table = _quote_identifier(table, db_type)
    safe_date_col = _quote_identifier(date_column, db_type)

    where_conditions = []
    params: dict[str, Any] = {}
    param_counter = 0

    for filter_str in filters:
        column, operator, value = parse_filter_string(filter_str)
        safe_column = _quote_identifier(column, db_type)

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
            for item in values:
                param_name = f"param_{param_counter}"
                placeholders.append(f":{param_name}")
                params[param_name] = item
                param_counter += 1
            where_conditions.append(f"{safe_column} {operator} ({','.join(placeholders)})")
        elif operator in ["IS", "IS NOT"]:
            where_conditions.append(f"{safe_column} {operator} {value}")
        else:
            param_name = f"param_{param_counter}"
            where_conditions.append(f"{safe_column} {operator} :{param_name}")
            params[param_name] = value
            param_counter += 1

    base_query = f"{select_clause} FROM {safe_table}"
    if where_conditions:
        base_query = f"{base_query} WHERE {' AND '.join(where_conditions)}"

    if limit:
        if db_type == "mssql":
            base_query = f"{base_query} ORDER BY {safe_date_col} DESC"
        else:
            base_query = f"{base_query} ORDER BY {safe_date_col} DESC LIMIT {limit}"

    return base_query, params


def build_chunked_query(
    table: str,
    filters: list[str],
    chunk_size: int,
    last_value_for_pagination: Any = None,
    pagination_column: str = "id",
    columns: list[str] | None = None,
    db_type: str = "mysql",
) -> tuple[str, dict[str, Any]]:
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", table):
        raise ValueError(f"Invalid table name: {table}")
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", pagination_column):
        raise ValueError(f"Invalid pagination column name: {pagination_column}")
    if db_type not in {"mysql", "mssql"}:
        raise ValueError(f"Unsupported DB type: {db_type}")
    if columns:
        for col in columns:
            if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", col):
                raise ValueError(f"Invalid column name: {col}")

    safe_table = _quote_identifier(table, db_type)
    safe_pagination_col = _quote_identifier(pagination_column, db_type)

    if columns:
        requested_columns = list(columns)
        if pagination_column not in requested_columns:
            requested_columns.append(pagination_column)
        select_clause = _build_select_clause(
            requested_columns,
            db_type,
            chunk_size if db_type == "mssql" else None,
        )
    else:
        select_clause = _build_select_clause(None, db_type, chunk_size if db_type == "mssql" else None)

    where_conditions = []
    params: dict[str, Any] = {}
    param_counter = 0

    for filter_str in filters:
        column, operator, value = parse_filter_string(filter_str)
        safe_column = _quote_identifier(column, db_type)

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
            for item in values:
                param_name = f"param_{param_counter}"
                placeholders.append(f":{param_name}")
                params[param_name] = item
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
    if db_type == "mssql":
        query = f"{select_clause} FROM {safe_table} {full_where} ORDER BY {safe_pagination_col} ASC"
    else:
        query = f"{select_clause} FROM {safe_table} {full_where} ORDER BY {safe_pagination_col} ASC LIMIT {chunk_size}"

    return query, params
