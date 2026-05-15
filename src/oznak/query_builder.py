from __future__ import annotations

from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Mapping

from oznak.dialects import DatabaseDialect
from oznak.errors import OznakValidationError
from oznak.filters import QueryFilter, QueryOperator
from oznak.profiles import DatabaseProfile, validate_identifier


@dataclass(frozen=True)
class QuerySpec:
    filters: tuple[QueryFilter, ...] = field(default_factory=tuple)
    columns: tuple[str, ...] | None = None
    limit: int | None = None
    date_column: str | None = None
    chunk_size: int | None = None
    pagination_column: str | None = None
    last_pagination_value: Any = None
    order_by_enabled: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(self, "filters", tuple(self.filters))

        if self.columns is not None:
            columns = tuple(dict.fromkeys(self.columns))
            for column in columns:
                validate_identifier(column, field_name="selected column")
            object.__setattr__(self, "columns", columns)

        if self.date_column is not None:
            validate_identifier(self.date_column, field_name="date column")
        if self.pagination_column is not None:
            validate_identifier(self.pagination_column, field_name="pagination column")
        if type(self.order_by_enabled) is not bool:
            raise OznakValidationError("QuerySpec.order_by_enabled must be a boolean")


@dataclass(frozen=True)
class CompiledQuery:
    sql: str
    params: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "params", MappingProxyType(dict(self.params)))


def compile_query(profile: DatabaseProfile, spec: QuerySpec) -> CompiledQuery:
    limit = _validate_positive_int(spec.limit, field_name="limit")
    chunk_size = _validate_positive_int(spec.chunk_size, field_name="chunk_size")
    if limit is not None and chunk_size is not None:
        raise OznakValidationError("QuerySpec cannot combine limit and chunk_size")

    selected_columns = _allowed_columns(profile, spec.columns)
    filter_clauses, params = _compile_filters(profile, spec.filters)

    if chunk_size is not None:
        return _compile_chunked_query(
            profile,
            selected_columns=selected_columns,
            filter_clauses=filter_clauses,
            params=params,
            chunk_size=chunk_size,
            pagination_column=spec.pagination_column,
            last_pagination_value=spec.last_pagination_value,
            order_by_enabled=spec.order_by_enabled,
        )

    return _compile_limited_query(
        profile,
        selected_columns=selected_columns,
        filter_clauses=filter_clauses,
        params=params,
        limit=limit,
        date_column=spec.date_column,
        order_by_enabled=spec.order_by_enabled,
    )


def _compile_limited_query(
    profile: DatabaseProfile,
    *,
    selected_columns: tuple[str, ...] | None,
    filter_clauses: list[str],
    params: dict[str, Any],
    limit: int | None,
    date_column: str | None,
    order_by_enabled: bool,
) -> CompiledQuery:
    order_column = None
    if order_by_enabled:
        order_column = date_column or (profile.timestamp_column if limit is not None else None)
    if order_column is not None:
        order_column = profile.require_allowed_column(order_column)

    select_clause = _select_clause(
        profile.dialect,
        selected_columns=selected_columns,
        top=limit if profile.dialect is DatabaseDialect.MSSQL else None,
    )
    sql_parts = [select_clause, "FROM", _quote_identifier(profile.table, profile.dialect)]
    _append_where(sql_parts, filter_clauses)

    if order_column is not None:
        sql_parts.extend(["ORDER BY", _quote_identifier(order_column, profile.dialect), "DESC"])
    if limit is not None and profile.dialect is DatabaseDialect.MYSQL:
        sql_parts.extend(["LIMIT", str(limit)])

    return CompiledQuery(sql=" ".join(sql_parts), params=params)


def _compile_chunked_query(
    profile: DatabaseProfile,
    *,
    selected_columns: tuple[str, ...] | None,
    filter_clauses: list[str],
    params: dict[str, Any],
    chunk_size: int,
    pagination_column: str | None,
    last_pagination_value: Any,
    order_by_enabled: bool,
) -> CompiledQuery:
    if not order_by_enabled:
        raise OznakValidationError("Chunked fetch requires ORDER BY for deterministic pagination")

    resolved_pagination_column = pagination_column or profile.pagination_column
    if resolved_pagination_column is None:
        raise OznakValidationError("QuerySpec.chunk_size requires a pagination column")
    resolved_pagination_column = profile.require_allowed_column(resolved_pagination_column)

    where_clauses = list(filter_clauses)
    if last_pagination_value is not None:
        where_clauses.append(
            f"{_quote_identifier(resolved_pagination_column, profile.dialect)} > :pagination_param"
        )
        params["pagination_param"] = last_pagination_value

    select_clause = _select_clause(
        profile.dialect,
        selected_columns=selected_columns,
        top=chunk_size if profile.dialect is DatabaseDialect.MSSQL else None,
    )
    sql_parts = [select_clause, "FROM", _quote_identifier(profile.table, profile.dialect)]
    _append_where(sql_parts, where_clauses)
    sql_parts.extend(["ORDER BY", _quote_identifier(resolved_pagination_column, profile.dialect), "ASC"])
    if profile.dialect is DatabaseDialect.MYSQL:
        sql_parts.extend(["LIMIT", str(chunk_size)])

    return CompiledQuery(sql=" ".join(sql_parts), params=params)


def _compile_filters(profile: DatabaseProfile, filters: tuple[QueryFilter, ...]) -> tuple[list[str], dict[str, Any]]:
    clauses: list[str] = []
    params: dict[str, Any] = {}
    param_index = 0

    for query_filter in filters:
        column = profile.require_allowed_column(query_filter.column)
        quoted_column = _quote_identifier(column, profile.dialect)
        operator = query_filter.operator

        if operator is QueryOperator.IS_NULL:
            clauses.append(f"{quoted_column} IS NULL")
            continue
        if operator is QueryOperator.IS_NOT_NULL:
            clauses.append(f"{quoted_column} IS NOT NULL")
            continue
        if operator in {QueryOperator.IN, QueryOperator.NOT_IN}:
            placeholders: list[str] = []
            for value in query_filter.value:
                param_name = f"param_{param_index}"
                placeholders.append(f":{param_name}")
                params[param_name] = value
                param_index += 1
            clauses.append(f"{quoted_column} {operator.value} ({','.join(placeholders)})")
            continue

        param_name = f"param_{param_index}"
        clauses.append(f"{quoted_column} {operator.value} :{param_name}")
        params[param_name] = query_filter.value
        param_index += 1

    return clauses, params


def _allowed_columns(profile: DatabaseProfile, columns: tuple[str, ...] | None) -> tuple[str, ...] | None:
    if columns is None:
        return None
    return profile.require_columns(columns)


def _select_clause(
    dialect: DatabaseDialect,
    *,
    selected_columns: tuple[str, ...] | None,
    top: int | None = None,
) -> str:
    select_target = "*"
    if selected_columns is not None:
        select_target = ", ".join(_quote_identifier(column, dialect) for column in selected_columns)
    if top is not None:
        return f"SELECT TOP {top} {select_target}"
    return f"SELECT {select_target}"


def _append_where(sql_parts: list[str], filter_clauses: list[str]) -> None:
    if filter_clauses:
        sql_parts.extend(["WHERE", " AND ".join(filter_clauses)])


def _quote_identifier(identifier: str, dialect: DatabaseDialect) -> str:
    if dialect is DatabaseDialect.MSSQL:
        return f"[{identifier}]"
    return f"`{identifier}`"


def _validate_positive_int(value: int | None, *, field_name: str) -> int | None:
    if value is None:
        return None
    if type(value) is not int or value <= 0:
        raise OznakValidationError(f"QuerySpec.{field_name} must be a positive integer")
    return value
