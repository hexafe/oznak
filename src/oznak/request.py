from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from oznak.errors import OznakConfigurationError, OznakValidationError
from oznak.filters import QueryFilter, parse_legacy_filter
from oznak.profiles import DatabaseProfile, validate_identifier
from oznak.result import FetchRequest


def _normalize_aliases(databases: str | Sequence[str]) -> tuple[str, ...]:
    if isinstance(databases, str):
        candidates = databases.split(",")
    else:
        candidates = databases
    aliases = tuple(str(alias).strip() for alias in candidates if str(alias).strip())
    if not aliases:
        raise OznakValidationError("At least one database alias is required")
    return tuple(validate_identifier(alias, field_name="profile alias") for alias in aliases)


def _normalize_columns(columns: str | Sequence[str] | None) -> tuple[str, ...] | None:
    if columns is None:
        return None
    if isinstance(columns, str):
        candidates = columns.split(",")
    else:
        candidates = columns
    normalized = tuple(dict.fromkeys(str(column).strip() for column in candidates if str(column).strip()))
    if not normalized:
        return None
    return tuple(validate_identifier(column, field_name="selected column") for column in normalized)


def _normalize_filters(filters: Sequence[str | QueryFilter] | None) -> tuple[QueryFilter, ...]:
    if filters is None:
        return ()

    normalized: list[QueryFilter] = []
    for value in filters:
        if isinstance(value, QueryFilter):
            normalized.append(value)
            continue
        normalized.append(parse_legacy_filter(str(value)))
    return tuple(normalized)


def _normalize_positive_int(value: int | None, *, field_name: str) -> int | None:
    if value is None:
        return None
    if type(value) is not int or value <= 0:
        raise OznakValidationError(f"{field_name} must be a positive integer")
    return value


def _normalize_positive_float(value: float | int | None, *, field_name: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (float, int)):
        raise OznakValidationError(f"{field_name} must be positive")
    normalized = float(value)
    if normalized <= 0:
        raise OznakValidationError(f"{field_name} must be positive")
    return normalized


@dataclass(frozen=True)
class QueryRequest:
    profile_aliases: tuple[str, ...]
    filters: tuple[QueryFilter, ...] = field(default_factory=tuple)
    columns: tuple[str, ...] | None = None
    limit: int | None = None
    date_column: str | None = None
    order_by_enabled: bool | None = None
    timeout_seconds: float | None = None
    chunk_size: int | None = None
    pagination_column: str | None = None
    max_workers: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "profile_aliases", _normalize_aliases(self.profile_aliases))
        object.__setattr__(self, "filters", tuple(self.filters))
        if self.columns is not None:
            object.__setattr__(self, "columns", _normalize_columns(self.columns))
        object.__setattr__(self, "limit", _normalize_positive_int(self.limit, field_name="limit"))
        object.__setattr__(self, "chunk_size", _normalize_positive_int(self.chunk_size, field_name="chunk_size"))
        object.__setattr__(self, "max_workers", _normalize_positive_int(self.max_workers, field_name="max_workers"))
        object.__setattr__(
            self,
            "timeout_seconds",
            _normalize_positive_float(self.timeout_seconds, field_name="timeout_seconds"),
        )
        if self.date_column is not None:
            validate_identifier(self.date_column, field_name="date column")
        if self.pagination_column is not None:
            validate_identifier(self.pagination_column, field_name="pagination column")
        if self.order_by_enabled is not None and type(self.order_by_enabled) is not bool:
            raise OznakValidationError("order_by_enabled must be a boolean when provided")
        if self.limit is not None and self.chunk_size is not None:
            raise OznakValidationError("QueryRequest cannot combine limit and chunk_size")

    @classmethod
    def from_inputs(
        cls,
        *,
        databases: str | Sequence[str],
        filters: Sequence[str | QueryFilter] | None = None,
        select_columns: str | Sequence[str] | None = None,
        last: int | None = None,
        date_col: str | None = None,
        order_by_enabled: bool | None = None,
        timeout_seconds: float | int | None = None,
        chunk_size: int | None = None,
        pagination_column: str | None = None,
        max_workers: int | None = None,
    ) -> "QueryRequest":
        normalized_limit = _normalize_positive_int(last, field_name="last")
        normalized_chunk_size = _normalize_positive_int(chunk_size, field_name="chunk_size")
        normalized_date_column = date_col if normalized_limit is not None else None
        if normalized_date_column is not None:
            validate_identifier(normalized_date_column, field_name="date column")
        normalized_pagination_column = pagination_column if normalized_chunk_size is not None else None
        if normalized_pagination_column is not None:
            validate_identifier(normalized_pagination_column, field_name="pagination column")

        return cls(
            profile_aliases=_normalize_aliases(databases),
            filters=_normalize_filters(filters),
            columns=_normalize_columns(select_columns),
            limit=normalized_limit,
            date_column=normalized_date_column,
            order_by_enabled=order_by_enabled,
            timeout_seconds=timeout_seconds,
            chunk_size=normalized_chunk_size,
            pagination_column=normalized_pagination_column,
            max_workers=max_workers,
        )

    def resolve_profiles(self, loaded_profiles: Mapping[str, DatabaseProfile]) -> tuple[DatabaseProfile, ...]:
        missing = [alias for alias in self.profile_aliases if alias not in loaded_profiles]
        if missing:
            raise OznakConfigurationError(f"Unknown database alias(es) in config: {', '.join(missing)}")
        return tuple(loaded_profiles[alias] for alias in self.profile_aliases)

    def to_fetch_request(self, loaded_profiles: Mapping[str, DatabaseProfile]) -> FetchRequest:
        return FetchRequest(
            profiles=self.resolve_profiles(loaded_profiles),
            filters=self.filters,
            columns=self.columns,
            limit=self.limit,
            date_column=self.date_column,
            order_by_enabled=self.order_by_enabled,
            timeout_seconds=self.timeout_seconds,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "profile_aliases": list(self.profile_aliases),
            "filters": [
                {
                    "column": item.column,
                    "operator": item.operator.value,
                    "value": item.value,
                }
                for item in self.filters
            ],
            "columns": list(self.columns) if self.columns is not None else None,
            "limit": self.limit,
            "date_column": self.date_column,
            "order_by_enabled": self.order_by_enabled,
            "timeout_seconds": self.timeout_seconds,
            "chunk_size": self.chunk_size,
            "pagination_column": self.pagination_column,
            "max_workers": self.max_workers,
        }


__all__ = ["QueryRequest"]
