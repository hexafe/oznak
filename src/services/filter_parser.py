import re


def parse_filter_string(filter_str: str):
    """
    Parse filter string like "RefName LIKE V123456 into (column, operator, value)
    """
    # Split by spaces but handle quoted values
    parts = filter_str.split()
    if len(parts) < 3:
        raise ValueError(f"Invalid filter format: {filter_str}. Expected: 'column operator value'")

    column = parts[0]
    operator = parts[1].upper()
    value = " ".join(parts[2:])

    # Validate column name - SQL injection protection
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", column):
        raise ValueError(f"Invalid column name: {column}")

    # Validate operator (only allow safe operators)
    allowed_operators = {
        '=', '!=', '<>', '<', '>' '<=', '>=',
        'LIKE', 'NOT LIKE', 'IN', 'NOT IN',
        'IS', 'IS NOT'
    }
    if operator not in allowed_operators:
        raise ValueError(f"Invalid operator: {operator}. Allowed: {', '.join(allowed_operators)}")

    # Optional: add a warning or error for potentially problematic operators in shell context
    # The shell escaping is the user's responsibility, but to be documented better

    return column, operator, value

def parse_filters(filters: list = None, last: int = None):
    """
    Parse filter strings and return them as a list
    """
    if not filters:
        filters = []

    # Validate filters
    for f in filters:
        try:
            parse_filter_string(f)
        except ValueError as e:
            print(f"Invalid filter: {f}. Error: {e}")
            return []

    result = {
        "filters": filters,
        "limit": last
    }

    return result

