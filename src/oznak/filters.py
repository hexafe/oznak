from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from typing import Any, Sequence

from oznak.errors import OznakValidationError
from oznak.profiles import validate_identifier

_FILTER_PATTERN = re.compile(
    r"^(?P<column>[a-zA-Z_][a-zA-Z0-9_]*)\s+"
    r"(?P<operator>IS\s+NOT|NOT\s+LIKE|NOT\s+IN|IS|LIKE|IN|!=|<>|<=|>=|=|<|>)\s+"
    r"(?P<value>.+)$",
    re.IGNORECASE,
)


class QueryOperator(str, Enum):
    EQ = "="
    NE = "!="
    LT = "<"
    LTE = "<="
    GT = ">"
    GTE = ">="
    LIKE = "LIKE"
    NOT_LIKE = "NOT LIKE"
    IN = "IN"
    NOT_IN = "NOT IN"
    IS_NULL = "IS NULL"
    IS_NOT_NULL = "IS NOT NULL"

    @classmethod
    def normalize(cls, value: "QueryOperator | str") -> "QueryOperator":
        if isinstance(value, QueryOperator):
            return value
        normalized = " ".join(str(value).strip().upper().split())
        aliases = {
            "==": cls.EQ,
            "<>": cls.NE,
            "IS": cls.IS_NULL,
            "IS NOT": cls.IS_NOT_NULL,
        }
        if normalized in aliases:
            return aliases[normalized]
        try:
            return cls(normalized)
        except ValueError as exc:
            supported = ", ".join(operator.value for operator in cls)
            raise OznakValidationError(f"Unsupported query operator '{value}'. Supported: {supported}") from exc


@dataclass(frozen=True)
class QueryFilter:
    column: str
    operator: QueryOperator | str
    value: Any = None

    def __post_init__(self) -> None:
        validate_identifier(self.column, field_name="filter column")
        operator = QueryOperator.normalize(self.operator)

        if operator in {QueryOperator.IS_NULL, QueryOperator.IS_NOT_NULL}:
            if self.value not in (None, "NULL", "null"):
                raise OznakValidationError(f"{operator.value} does not accept a non-NULL value")
            object.__setattr__(self, "value", None)
        elif operator in {QueryOperator.IN, QueryOperator.NOT_IN}:
            values = _normalize_sequence(self.value, operator=operator)
            object.__setattr__(self, "value", values)
        elif self.value is None:
            raise OznakValidationError(f"{operator.value} requires a value")

        object.__setattr__(self, "operator", operator)

    @classmethod
    def is_null(cls, column: str) -> "QueryFilter":
        return cls(column=column, operator=QueryOperator.IS_NULL)

    @classmethod
    def is_not_null(cls, column: str) -> "QueryFilter":
        return cls(column=column, operator=QueryOperator.IS_NOT_NULL)


def _normalize_sequence(value: Any, *, operator: QueryOperator) -> tuple[Any, ...]:
    if isinstance(value, str) or not isinstance(value, Sequence):
        raise OznakValidationError(f"{operator.value} requires a non-string sequence of values")
    values = tuple(item for item in value if item is not None)
    if not values:
        raise OznakValidationError(f"{operator.value} requires at least one value")
    return values


def parse_legacy_filter(filter_str: str) -> QueryFilter:
    normalized_filter = str(filter_str).strip()
    if not normalized_filter:
        raise OznakValidationError("Invalid filter format. Expected: 'column operator value'")

    match = _FILTER_PATTERN.match(normalized_filter)
    if not match:
        parts = normalized_filter.split()
        if len(parts) >= 3 and re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", parts[0]):
            raise OznakValidationError(f"Invalid operator: {parts[1].upper()}")
        raise OznakValidationError("Invalid filter format. Expected: 'column operator value'")

    column = match.group("column")
    operator = " ".join(match.group("operator").upper().split())
    value = match.group("value").strip()
    if not value:
        raise OznakValidationError("Invalid filter format. Expected: 'column operator value'")

    if operator in {"IN", "NOT IN"}:
        values = tuple(candidate.strip() for candidate in value.split(",") if candidate.strip())
        return QueryFilter(column=column, operator=operator, value=values)
    if operator in {"IS", "IS NOT"}:
        if value.upper() != "NULL":
            raise OznakValidationError(f"{operator} operator only supports NULL")
        return QueryFilter(column=column, operator=operator, value="NULL")
    return QueryFilter(column=column, operator=operator, value=value)
