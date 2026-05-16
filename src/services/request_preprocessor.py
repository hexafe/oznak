from typing import Any

from src._legacy import warn_legacy_module
from src.services.filter_parser import normalize_columns, normalize_databases, parse_filters

warn_legacy_module("src.services.request_preprocessor", "oznak.request.QueryRequest")


def preprocess_fetch_request(
    *,
    databases: str | list[str],
    filters: list[str | dict[str, object]] | None = None,
    last: int | None = None,
    select_columns: str | list[str] | None = None,
) -> dict[str, Any]:
    """Normalize inbound fetch inputs into a single validated payload."""
    parsed_filters = parse_filters(filters, last)

    return {
        "databases": normalize_databases(databases),
        "filters": parsed_filters["filters"],
        "limit": parsed_filters["limit"],
        "columns": normalize_columns(select_columns),
    }
