import re
from typing import Any

from oznak.filters import QueryFilter
from oznak.profiles import DatabaseProfile, validate_identifier
from oznak.query_builder import QuerySpec, compile_query


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
    query_filters = [_legacy_filter_to_query_filter(filter_str) for filter_str in filters]
    profile = _legacy_profile(
        table=table,
        db_type=db_type,
        columns=columns,
        filters=query_filters,
        date_column=date_column if limit is not None else None,
    )
    compiled = compile_query(
        profile,
        QuerySpec(
            filters=tuple(query_filters),
            columns=tuple(columns) if columns is not None else None,
            limit=limit,
            date_column=date_column if limit is not None else None,
        ),
    )
    return compiled.sql, dict(compiled.params)


def build_chunked_query(
    table: str,
    filters: list[str],
    chunk_size: int,
    last_value_for_pagination: Any = None,
    pagination_column: str = "id",
    columns: list[str] | None = None,
    db_type: str = "mysql",
) -> tuple[str, dict[str, Any]]:
    query_filters = [_legacy_filter_to_query_filter(filter_str) for filter_str in filters]
    requested_columns = _chunk_columns(columns, pagination_column)
    profile = _legacy_profile(
        table=table,
        db_type=db_type,
        columns=requested_columns,
        filters=query_filters,
        pagination_column=pagination_column,
    )
    compiled = compile_query(
        profile,
        QuerySpec(
            filters=tuple(query_filters),
            columns=tuple(requested_columns) if requested_columns is not None else None,
            chunk_size=chunk_size,
            pagination_column=pagination_column,
            last_pagination_value=last_value_for_pagination,
        ),
    )
    return compiled.sql, dict(compiled.params)


def _legacy_filter_to_query_filter(filter_str: str) -> QueryFilter:
    column, operator, value = parse_filter_string(filter_str)
    if operator == "IS":
        if value.upper() != "NULL":
            raise ValueError("IS operator only supports NULL")
        return QueryFilter.is_null(column)
    if operator == "IS NOT":
        if value.upper() != "NULL":
            raise ValueError("IS NOT operator only supports NULL")
        return QueryFilter.is_not_null(column)
    if operator in {"IN", "NOT IN"}:
        values = tuple(candidate.strip() for candidate in value.split(",") if candidate.strip())
        return QueryFilter(column, operator, values)
    return QueryFilter(column, operator, value)


def _legacy_profile(
    *,
    table: str,
    db_type: str,
    columns: list[str] | tuple[str, ...] | None = None,
    filters: list[QueryFilter] | None = None,
    date_column: str | None = None,
    pagination_column: str | None = None,
) -> DatabaseProfile:
    allowed_columns: list[str] = []
    for column in columns or []:
        allowed_columns.append(validate_identifier(column, field_name="column name"))
    for query_filter in filters or []:
        allowed_columns.append(query_filter.column)
    if date_column is not None:
        allowed_columns.append(validate_identifier(date_column, field_name="date column name"))
    if pagination_column is not None:
        allowed_columns.append(validate_identifier(pagination_column, field_name="pagination column name"))

    return DatabaseProfile(
        alias="legacy",
        dialect=db_type,
        host="db.example.invalid",
        port=1433 if db_type == "mssql" else 3306,
        database="legacy_query",
        table=validate_identifier(table, field_name="table name"),
        allowed_columns=tuple(dict.fromkeys(allowed_columns)),
        timestamp_column=date_column,
        pagination_column=pagination_column,
    )


def _chunk_columns(columns: list[str] | None, pagination_column: str) -> list[str] | None:
    if columns is None:
        return None
    requested_columns = list(columns)
    if pagination_column not in requested_columns:
        requested_columns.append(pagination_column)
    return requested_columns
