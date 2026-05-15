from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from oznak.diagnostics import SourceFetchDiagnostics, SourceFetchStatus
from oznak.filters import QueryFilter
from oznak.profiles import DatabaseProfile, validate_identifier


@dataclass(frozen=True)
class FetchRequest:
    profiles: tuple[DatabaseProfile, ...]
    filters: tuple[QueryFilter, ...] = field(default_factory=tuple)
    columns: tuple[str, ...] | None = None
    limit: int | None = None
    date_column: str | None = None
    order_by_enabled: bool | None = None
    timeout_seconds: float | None = None

    def __post_init__(self) -> None:
        if not self.profiles:
            raise ValueError("FetchRequest requires at least one database profile")
        if self.limit is not None and self.limit <= 0:
            raise ValueError("FetchRequest.limit must be a positive integer")
        if self.timeout_seconds is not None and self.timeout_seconds <= 0:
            raise ValueError("FetchRequest.timeout_seconds must be positive")
        if self.order_by_enabled is not None and type(self.order_by_enabled) is not bool:
            raise ValueError("FetchRequest.order_by_enabled must be a boolean when provided")
        if self.columns is not None:
            columns = tuple(dict.fromkeys(self.columns))
            for column in columns:
                validate_identifier(column, field_name="selected column")
            object.__setattr__(self, "columns", columns)
        if self.date_column is not None:
            validate_identifier(self.date_column, field_name="date column")


@dataclass(frozen=True)
class FetchResult:
    data: Any = None
    source_results: tuple[SourceFetchDiagnostics, ...] = field(default_factory=tuple)
    warnings: tuple[str, ...] = field(default_factory=tuple)
    errors: tuple[str, ...] = field(default_factory=tuple)

    @property
    def row_count(self) -> int:
        return sum(source.row_count for source in self.source_results)

    @property
    def has_errors(self) -> bool:
        return bool(self.errors) or any(source.failed for source in self.source_results)

    @property
    def partial_success(self) -> bool:
        statuses = {source.status for source in self.source_results}
        return any(status in {SourceFetchStatus.SUCCESS, SourceFetchStatus.NO_ROWS} for status in statuses) and any(
            status in {SourceFetchStatus.FAILED, SourceFetchStatus.TIMEOUT, SourceFetchStatus.CANCELLED}
            for status in statuses
        )
