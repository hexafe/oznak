import re


def _quote_identifier(identifier: str, db_type: str) -> str:
    if db_type == "mssql":
        return f"[{identifier}]"
    return f"`{identifier}`"


def _build_select_clause(columns: list = None, db_type: str = "mysql", limit: int = None) -> str:
    if columns is not None:
        validated_columns = [_quote_identifier(col, db_type) for col in columns]
        select_target = ", ".join(validated_columns)
    else:
        select_target = "*"

    if db_type == "mssql" and limit is not None:
        return f"SELECT TOP {limit} {select_target}"

    return f"SELECT {select_target}"

def parse_filter_string(filter_str: str):
    """
    Parse filter string like "RefName LIKE V123456" into (column, operator, value)
    """
    # Split by spaces but handle quoted values
    parts = filter_str.split()
    if len(parts) < 3:
        raise ValueError(f"Invalid filter format: {filter_str}. Expected: 'column operator value'")
    
    column = parts[0]
    operator = parts[1].upper()
    value = " ".join(parts[2:])  # Join remaining parts as value
    
    # Validate column name (SQL injection protection)
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", column):
        raise ValueError(f"Invalid column name: {column}")
    
    # Validate operator (only allow safe operators)
    allowed_operators = {
        '=', '!=', '<>', '<=', '>=', '<', '>', # > and < to be removed? due to some weird bug on windows shell
        'LIKE', 'NOT LIKE', 'IN', 'NOT IN',
        'IS', 'IS NOT'
    }
    if operator not in allowed_operators:
        raise ValueError(f"Invalid operator: {operator}. Allowed: {', '.join(allowed_operators)}")
    
    return column, operator, value

def build_query(table: str, filters: list, limit: int = None, date_column: str = "TimeStamp", columns: list = None, db_type: str = "mysql"):
    """
    Build a SQL query with generic filters
    filters: list of filter strings like ["RefName LIKE V123456", "Date >= 2025-01-01"]
    date_column: name of the date/timestamp column for default ordering (when using LIMIT)
    columns: list of columns for SELECT statement
    """
    # Validate table name (SQL injection protection)
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", table):
        raise ValueError(f"Invalid table name: {table}")

    # Validate date_column name (SQL injection protection)
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
    params = {}
    param_counter = 0
    
    for filter_str in filters:
        column, operator, value = parse_filter_string(filter_str)
        safe_column = _quote_identifier(column, db_type)
        
        # Handle different operators
        if operator in ['LIKE', 'NOT LIKE']:
            param_name = f"param_{param_counter}"
            where_conditions.append(f"{safe_column} {operator} :{param_name}")
            params[param_name] = value
            param_counter += 1
        elif operator in ['IN', 'NOT IN']:
            # For IN clauses, value should be comma-separated like "A,B,C"
            values = [v.strip() for v in value.split(',')]
            placeholders = []
            for v in values:
                param_name = f"param_{param_counter}"
                placeholders.append(f":{param_name}")
                params[param_name] = v
                param_counter += 1
            where_clause_part = f"{safe_column} {operator} ({','.join(placeholders)})"
            where_conditions.append(where_clause_part)
        elif operator in ['IS', 'IS NOT']:
            # For IS/IS NOT, value should be NULL, NOT NULL, etc.
            where_conditions.append(f"{safe_column} {operator} {value}")
        else:
            # For =, !=, <>, <, >, <=, >=
            param_name = f"param_{param_counter}"
            where_conditions.append(f"{safe_column} {operator} :{param_name}")
            params[param_name] = value
            param_counter += 1

    if not where_conditions:
        base_query = f"{select_clause} FROM {safe_table}"
    else:
        where_clause = " AND ".join(where_conditions)
        base_query = f"{select_clause} FROM {safe_table} WHERE {where_clause}"

    if limit:
        if db_type == "mssql":
            base_query = f"{base_query} ORDER BY {safe_date_col} DESC"
        else:
            base_query = f"{base_query} ORDER BY {safe_date_col} DESC LIMIT {limit}"

    return base_query, params

def build_chunked_query(table: str, filters: list, chunk_size: int, last_value_for_pagination: any = None, pagination_column: str = "id", columns: list = None, db_type: str = "mysql"):
    """
    Builds a query to fetch a specific chunk of data based on a unique, indexed column

    Args:
        table: The table name
        filters: List of filters
        chunk_size: Number of rows to fetch per chunk
        last_value_for_pagination: The value of pagination column for the last value of previous chunk (None for first chunk)
        pagination_column: The column to use for pagination (should be unique and indexed)
        columns: Optional list of column names for SELECT

    Returns:
        A tuple (query_string, params_dict) for the current chunk
    """
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
        select_clause = _build_select_clause(requested_columns, db_type, chunk_size if db_type == "mssql" else None)
    else:
        select_clause = _build_select_clause(None, db_type, chunk_size if db_type == "mssql" else None)

    where_conditions = []
    params = {}
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
            values = [v.strip() for v in value.split(',')]
            placeholders = []
            for v in values:
                param_name = f"param_{param_counter}"
                placeholders.append(f":{param_name}")
                params[param_name] = v
                param_counter += 1
            where_clause_part = f"({safe_column} {operator} ({','.join(placeholders)}))"
            where_conditions.append(where_clause_part)
        elif operator in ["IS", "IS NOT"]:
            where_conditions.append(f"{safe_column} {operator} {value}")
        else:
            param_name = f"param_{param_counter}"
            where_conditions.append(f"{safe_column} {operator} :{param_name}")
            params[param_name] = value
            param_counter += 1

    if last_value_for_pagination is not None:
        pagination_param_name = f"pagination_param"
        where_conditions.append(f"{safe_pagination_col} > :{pagination_param_name}")
        params[pagination_param_name] = last_value_for_pagination

    if where_conditions:
        where_clause = " AND ".join(where_conditions)
        full_where = f"WHERE {where_clause}"
    else:
        full_where = ""

    if db_type == "mssql":
        query = f"{select_clause} FROM {safe_table} {full_where} ORDER BY {safe_pagination_col} ASC"
    else:
        query = f"{select_clause} FROM {safe_table} {full_where} ORDER BY {safe_pagination_col} ASC LIMIT {chunk_size}"

    return query, params
