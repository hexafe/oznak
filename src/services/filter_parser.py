import re


IDENTIFIER_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
OPERATOR_PATTERN = re.compile(
    r"^(?P<column>[a-zA-Z_][a-zA-Z0-9_]*)\s+"
    r"(?P<operator>IS\s+NOT|NOT\s+LIKE|NOT\s+IN|IS|LIKE|IN|!=|<>|<=|>=|=|<|>)\s+"
    r"(?P<value>.+)$",
    re.IGNORECASE,
)


def parse_filter_string(filter_str: str):
    """
    Parse filter string like "RefName LIKE V123456 into (column, operator, value)
    """
    normalized_filter = filter_str.strip()
    if not normalized_filter:
        raise ValueError(f"Invalid filter format: {filter_str}. Expected: 'column operator value'")

    match = OPERATOR_PATTERN.match(normalized_filter)
    if not match:
        parts = normalized_filter.split()
        if len(parts) >= 3 and IDENTIFIER_PATTERN.match(parts[0]):
            bad_operator = parts[1].upper()
            raise ValueError(f"Invalid operator: {bad_operator}")
        raise ValueError(f"Invalid filter format: {filter_str}. Expected: 'column operator value'")

    column = match.group("column")
    operator = re.sub(r"\s+", " ", match.group("operator").upper()).strip()
    value = match.group("value").strip()

    # Validate column name - SQL injection protection
    if not IDENTIFIER_PATTERN.match(column):
        raise ValueError(f"Invalid column name: {column}")

    # Validate operator (only allow safe operators)
    allowed_operators = {
        '=', '!=', '<>', '<', '>', '<=', '>=',
        'LIKE', 'NOT LIKE', 'IN', 'NOT IN',
        'IS', 'IS NOT'
    }
    if operator not in allowed_operators:
        raise ValueError(f"Invalid operator: {operator}. Allowed: {', '.join(allowed_operators)}")

    if not value:
        raise ValueError(f"Invalid filter format: {filter_str}. Expected: 'column operator value'")

    return column, operator, value


def normalize_databases(databases: str | list[str]) -> list[str]:
    if isinstance(databases, str):
        candidates = [database.strip() for database in databases.split(",")]
    else:
        candidates = [str(database).strip() for database in databases]

    normalized = [database for database in candidates if database]
    invalid = [database for database in normalized if not IDENTIFIER_PATTERN.match(database)]
    if invalid:
        raise ValueError(f"Invalid database name(s): {', '.join(invalid)}")

    return normalized


def normalize_columns(columns: str | list[str] | None) -> list[str] | None:
    if columns is None:
        return None

    if isinstance(columns, str):
        candidates = [column.strip() for column in columns.split(",")]
    else:
        candidates = [str(column).strip() for column in columns]

    normalized = [column for column in candidates if column]
    invalid = [column for column in normalized if not IDENTIFIER_PATTERN.match(column)]
    if invalid:
        raise ValueError(f"Invalid column name(s): {', '.join(invalid)}")

    return normalized

def _normalize_filter_value(operator: str, value: str) -> str:
    if operator in {"IN", "NOT IN"}:
        values = [candidate.strip() for candidate in value.split(",") if candidate.strip()]
        if not values:
            raise ValueError(f"{operator} operator requires at least one value")
        return ", ".join(values)

    if operator in {"IS", "IS NOT"}:
        normalized_value = value.upper()
        if normalized_value not in {"NULL", "TRUE", "FALSE"}:
            raise ValueError(f"{operator} operator only supports NULL, TRUE, or FALSE values")
        return normalized_value

    return value


def parse_filters(filters: list[str] | None = None, last: int = None):
    """
    Parse filter strings and return them as a list
    """
    if not filters:
        filters = []

    normalized_filters = []
    for filter_value in filters:
        column, operator, value = parse_filter_string(filter_value)
        normalized_value = _normalize_filter_value(operator, value)
        normalized_filters.append(f"{column} {operator} {normalized_value}")

    if last is not None and (not isinstance(last, int) or last <= 0):
        raise ValueError("'last' must be a positive integer")

    result = {
        "filters": normalized_filters,
        "limit": last
    }

    return result
