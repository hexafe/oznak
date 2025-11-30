import re

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
        '=', '!=', '<>', '<', '>', '<=', '>=', 
        'LIKE', 'NOT LIKE', 'IN', 'NOT IN',
        'IS', 'IS NOT'
    }
    if operator not in allowed_operators:
        raise ValueError(f"Invalid operator: {operator}. Allowed: {', '.join(allowed_operators)}")
    
    return column, operator, value

def build_query(table: str, filters: list, limit: int = None, date_column: str = "TimeStamp"):
    """
    Build a SQL query with generic filters
    filters: list of filter strings like ["RefName LIKE V123456", "Date >= 2025-01-01"]
    date_column: name of the date/timestamp column for default ordering (when using LIMIT)
    """
    # Validate table name (SQL injection protection)
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", table):
        raise ValueError(f"Invalid table name: {table}")

    # Validate date_column name (SQL injection protection)
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", date_column):
        raise ValueError(f"Invalid date column name: {date_column}")
    
    safe_table = f"`{table}`"
    safe_date_col = f"`{date_column}`"

    where_conditions = []
    params = {}
    param_counter = 0
    
    for filter_str in filters:
        column, operator, value = parse_filter_string(filter_str)
        safe_column = f"`{column}`"
        
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
        base_query = f"SELECT * FROM {safe_table}"
    else:
        where_clause = " AND ".join(where_conditions)
        base_query = f"SELECT * FROM {safe_table} WHERE {where_clause}"

    # Add LIMIT if specified
    if limit:
        if not isinstance(limit, int) or limit <= 0:
            raise ValueError("LIMIT must be a positive integer")
        base_query = f"{base_query} ORDER BY {safe_date_col} DESC LIMIT {limit}"
    
    return base_query, params

