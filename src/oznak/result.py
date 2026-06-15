from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
import json
import math
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

    def to_json_records(self) -> list[dict[str, Any]]:
        """Return records safe for JSON responses without mutating ``data``."""

        return _data_to_json_records(self.data)

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


def _data_to_json_records(data: Any) -> list[dict[str, Any]]:
    if data is None:
        return []

    to_json = getattr(data, "to_json", None)
    if callable(to_json):
        try:
            records = json.loads(to_json(orient="records", date_format="iso"))
        except (TypeError, ValueError):
            records = None
        if isinstance(records, list):
            return [_json_safe_record(record) for record in records]

    to_dict = getattr(data, "to_dict", None)
    if callable(to_dict):
        try:
            records = to_dict(orient="records")
        except TypeError:
            records = to_dict()
        return _records_from_value(records)

    return _records_from_value(data)


def _records_from_value(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, Mapping):
        return [_json_safe_record(value)]
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        return [_json_safe_record(item) for item in value]
    return [{"value": _json_safe_value(value)}]


def _json_safe_record(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return {str(key): _json_safe_value(nested) for key, nested in value.items()}
    return {"value": _json_safe_value(value)}


def _json_safe_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, bool | str | int):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, Decimal):
        return str(value) if value.is_finite() else None
    if _is_missing_scalar(value):
        return None
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _json_safe_value(nested) for key, nested in value.items()}
    if isinstance(value, set | frozenset):
        return [_json_safe_value(item) for item in sorted(value, key=repr)]
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        return [_json_safe_value(item) for item in value]

    item = getattr(value, "item", None)
    if callable(item):
        try:
            return _json_safe_value(item())
        except (TypeError, ValueError):
            pass

    tolist = getattr(value, "tolist", None)
    if callable(tolist):
        try:
            return _json_safe_value(tolist())
        except (TypeError, ValueError):
            pass

    isoformat = getattr(value, "isoformat", None)
    if callable(isoformat):
        try:
            return str(isoformat())
        except (TypeError, ValueError):
            pass

    return str(value)


def _is_missing_scalar(value: Any) -> bool:
    try:
        if value != value:
            return True
    except (TypeError, ValueError):
        pass

    if not type(value).__module__.startswith(("numpy", "pandas")):
        return False
    try:
        import pandas as pd
    except Exception:
        return False
    try:
        missing = pd.isna(value)
    except (TypeError, ValueError):
        return False
    if isinstance(missing, bool):
        return missing
    if type(missing).__module__.startswith("numpy") and type(missing).__name__ == "bool_":
        return bool(missing)
    return False
