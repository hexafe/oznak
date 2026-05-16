import re

from src._legacy import warn_legacy_module
from src.query.builder import parse_filter_string

warn_legacy_module("src.services.filter_parser", "oznak.request and oznak.filters")


IDENTIFIER_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
OPERATOR_PATTERN = re.compile(
    r"^(?P<column>[a-zA-Z_][a-zA-Z0-9_]*)\s+"
    r"(?P<operator>IS\s+NOT|NOT\s+LIKE|NOT\s+IN|IS|LIKE|IN|!=|<>|<=|>=|=|<|>)\s+"
    r"(?P<value>.+)$",
    re.IGNORECASE,
)


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
        if normalized_value != "NULL":
            raise ValueError(f"{operator} operator only supports NULL values")
        return normalized_value

    return value


def _normalize_typed_filter(filter_item: dict[str, object]) -> str:
    required_keys = {"field", "op", "value"}
    missing_keys = sorted(required_keys - set(filter_item.keys()))
    if missing_keys:
        raise ValueError(f"Invalid filter object: missing keys: {', '.join(missing_keys)}")

    extra_keys = sorted(set(filter_item.keys()) - required_keys)
    if extra_keys:
        raise ValueError(f"Invalid filter object: unsupported keys: {', '.join(extra_keys)}")

    field = filter_item["field"]
    operator = filter_item["op"]
    value = filter_item["value"]

    if not isinstance(field, str) or not field.strip():
        raise ValueError("Invalid filter object: field must be a non-empty string")
    if not isinstance(operator, str) or not operator.strip():
        raise ValueError("Invalid filter object: op must be a non-empty string")

    field = field.strip()
    operator = operator.strip()
    normalized_operator = re.sub(r"\s+", " ", operator.upper()).strip()

    if isinstance(value, (dict, tuple, set)):
        raise ValueError("Invalid filter object: value must be a scalar or list")

    if isinstance(value, list):
        if normalized_operator not in {"IN", "NOT IN"}:
            raise ValueError(
                f"Invalid filter object: operator {normalized_operator} does not support list values"
            )
        normalized_values = [str(item).strip() for item in value if str(item).strip()]
        if not normalized_values:
            raise ValueError(
                f"Invalid filter object: operator {normalized_operator} requires at least one value"
            )
        value = ", ".join(normalized_values)
    elif value is None:
        value = "NULL"
    else:
        value = str(value).strip()

    if not value:
        raise ValueError("Invalid filter object: value must be non-empty")

    return f"{field} {operator} {value}"


def parse_filters(filters: list[str | dict[str, object]] | None = None, last: int | None = None) -> dict[str, list[str] | int | None]:
    if not filters:
        filters = []

    normalized_filters = []
    for filter_value in filters:
        if isinstance(filter_value, dict):
            candidate_filter = _normalize_typed_filter(filter_value)
        elif isinstance(filter_value, str):
            candidate_filter = filter_value
        else:
            raise ValueError(
                f"Invalid filter type: {type(filter_value).__name__}. "
                "Expected string or object with field/op/value"
            )

        column, operator, value = parse_filter_string(candidate_filter)
        normalized_value = _normalize_filter_value(operator, value)
        normalized_filters.append(f"{column} {operator} {normalized_value}")

    if last is not None and (not isinstance(last, int) or last <= 0):
        raise ValueError("'last' must be a positive integer")

    return {
        "filters": normalized_filters,
        "limit": last,
    }
