from __future__ import annotations

import re
from dataclasses import dataclass, field
from math import isfinite
from types import MappingProxyType
from typing import Any, Mapping

from oznak.dialects import DatabaseDialect, normalize_dialect
from oznak.errors import OznakValidationError

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def validate_identifier(value: str, *, field_name: str = "identifier") -> str:
    if not isinstance(value, str) or not _IDENTIFIER_RE.fullmatch(value):
        raise OznakValidationError(f"Invalid {field_name}: {value!r}")
    return value


def _normalize_allowed_columns(columns: tuple[str, ...] | list[str] | set[str] | frozenset[str]) -> tuple[str, ...]:
    normalized = tuple(dict.fromkeys(str(column).strip() for column in columns))
    for column in normalized:
        validate_identifier(column, field_name="allowed column")
    return normalized


def _normalize_timeout_seconds(value: float | int | None, *, field_name: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise OznakValidationError(f"{field_name} must be a positive number of seconds")

    normalized = float(value)
    if not isfinite(normalized) or normalized <= 0:
        raise OznakValidationError(f"{field_name} must be a positive number of seconds")
    return normalized


@dataclass(frozen=True)
class DatabaseProfile:
    alias: str
    dialect: DatabaseDialect | str
    host: str
    port: int
    database: str
    table: str
    allowed_columns: tuple[str, ...] | list[str] | set[str] | frozenset[str] = field(default_factory=tuple)
    timestamp_column: str | None = None
    pagination_column: str | None = None
    display_name: str | None = None
    connect_timeout_seconds: float | int | None = None
    query_timeout_seconds: float | int | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        validate_identifier(self.alias, field_name="profile alias")
        validate_identifier(self.database, field_name="database name")
        validate_identifier(self.table, field_name="table name")

        if not isinstance(self.host, str) or not self.host.strip():
            raise OznakValidationError("Database host must be a non-empty string")
        if not isinstance(self.port, int) or self.port <= 0:
            raise OznakValidationError("Database port must be a positive integer")

        dialect = normalize_dialect(self.dialect)
        allowed_columns = _normalize_allowed_columns(self.allowed_columns)

        if self.timestamp_column is not None:
            validate_identifier(self.timestamp_column, field_name="timestamp column")
        if self.pagination_column is not None:
            validate_identifier(self.pagination_column, field_name="pagination column")
        connect_timeout_seconds = _normalize_timeout_seconds(
            self.connect_timeout_seconds, field_name="connect_timeout_seconds"
        )
        query_timeout_seconds = _normalize_timeout_seconds(
            self.query_timeout_seconds, field_name="query_timeout_seconds"
        )

        object.__setattr__(self, "dialect", dialect)
        object.__setattr__(self, "host", self.host.strip())
        object.__setattr__(self, "allowed_columns", allowed_columns)
        object.__setattr__(self, "connect_timeout_seconds", connect_timeout_seconds)
        object.__setattr__(self, "query_timeout_seconds", query_timeout_seconds)
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))

    def require_allowed_column(self, column: str) -> str:
        validate_identifier(column, field_name="column")
        if self.allowed_columns and column not in self.allowed_columns:
            raise OznakValidationError(
                f"Column '{column}' is not allowed for database profile '{self.alias}'"
            )
        return column

    def require_columns(self, columns: tuple[str, ...] | list[str] | set[str] | frozenset[str]) -> tuple[str, ...]:
        return tuple(self.require_allowed_column(column) for column in columns)

    @property
    def redacted_location(self) -> str:
        return f"{self.dialect.value}://<redacted-host>:<redacted-port>/{self.database}.{self.table}"
