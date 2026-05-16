from __future__ import annotations

from enum import Enum


class DatabaseDialect(str, Enum):
    MYSQL = "mysql"
    MSSQL = "mssql"


def normalize_dialect(value: DatabaseDialect | str) -> DatabaseDialect:
    if isinstance(value, DatabaseDialect):
        return value

    normalized = str(value).strip().lower()
    try:
        return DatabaseDialect(normalized)
    except ValueError as exc:
        supported = ", ".join(dialect.value for dialect in DatabaseDialect)
        raise ValueError(f"Unsupported database dialect '{value}'. Supported: {supported}") from exc
