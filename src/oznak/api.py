from __future__ import annotations

from typing import Optional

from fastapi import FastAPI, Query
from fastapi import HTTPException

from oznak.config import load_database_profiles
from oznak.credentials import EnvironmentCredentialProvider
from oznak.errors import OznakConfigurationError, OznakValidationError
from oznak.fetcher import fetch_records
from oznak.filters import parse_legacy_filter
from oznak.profiles import DatabaseProfile, validate_identifier
from oznak.result import FetchRequest

app = FastAPI(title="Oznak API")


def api_error(status_code: int, code: str, message: str, details: object | None = None) -> HTTPException:
    return HTTPException(
        status_code=status_code,
        detail={
            "code": code,
            "message": message,
            "details": details,
        },
    )


def validation_error(message: str, details: object | None = None) -> HTTPException:
    return api_error(400, "validation_error", message, details)


def execution_error(message: str, details: object | None = None) -> HTTPException:
    return api_error(502, "execution_error", message, details)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/fetch")
def fetch(
    databases: str = Query(..., description="Comma-separated database profile aliases"),
    config: str = Query("config/databases.yaml", description="Path to database profiles YAML"),
    filters: Optional[list[str]] = Query(default=None),
    last: Optional[int] = Query(default=None, gt=0),
    date_col: str = Query(default="TimeStamp"),
    select_columns: Optional[str] = Query(default=None),
    time_from: Optional[str] = None,
    time_to: Optional[str] = None,
    last_n: Optional[int] = None,
    reference: Optional[str] = None,
) -> dict[str, object]:
    try:
        loaded_profiles = load_database_profiles(config)
        aliases = _parse_aliases(databases)
        selected_profiles = _resolve_profiles(loaded_profiles, aliases)
        selected_columns = _parse_columns(select_columns if isinstance(select_columns, str) else None)
        normalized_filters = filters if isinstance(filters, list) else None
        normalized_time_from = time_from if isinstance(time_from, str) else None
        normalized_time_to = time_to if isinstance(time_to, str) else None
        normalized_reference = reference if isinstance(reference, str) else None
        combined_filters = _combine_filters(
            normalized_filters,
            time_from=normalized_time_from,
            time_to=normalized_time_to,
            reference=normalized_reference,
        )
        parsed_filters = tuple(parse_legacy_filter(item) for item in combined_filters)

        effective_last = last if isinstance(last, int) else last_n if isinstance(last_n, int) else None
        if effective_last is not None:
            if not isinstance(effective_last, int) or effective_last <= 0:
                raise OznakValidationError("'last' must be a positive integer")
            validate_identifier(date_col, field_name="date column")

        request = FetchRequest(
            profiles=selected_profiles,
            filters=parsed_filters,
            columns=selected_columns,
            limit=effective_last,
            date_column=date_col if effective_last is not None else None,
        )
    except (OznakConfigurationError, OznakValidationError, ValueError) as exc:
        raise validation_error(str(exc), {"field": "request"}) from exc

    result = fetch_records(request, credential_provider=EnvironmentCredentialProvider())
    if result.has_errors:
        raise execution_error(
            "database fetch failed",
            {
                "errors": list(result.errors),
                "sources": [
                    {
                        "alias": diagnostic.source_alias,
                        "status": diagnostic.status.value,
                        "error_code": diagnostic.error_code,
                        "message": diagnostic.message,
                    }
                    for diagnostic in result.source_results
                ],
            },
        )

    return {
        "rows": int(result.row_count),
        "sources": [
            {
                "alias": diagnostic.source_alias,
                "status": diagnostic.status.value,
                "row_count": diagnostic.row_count,
            }
            for diagnostic in result.source_results
        ],
        "data": result.data.to_dict(orient="records"),
    }


def _parse_aliases(raw_aliases: str) -> tuple[str, ...]:
    aliases = tuple(alias.strip() for alias in raw_aliases.split(",") if alias.strip())
    if not aliases:
        raise OznakValidationError("databases is required")
    return tuple(validate_identifier(alias, field_name="profile alias") for alias in aliases)


def _resolve_profiles(loaded_profiles: dict[str, DatabaseProfile], aliases: tuple[str, ...]) -> tuple[DatabaseProfile, ...]:
    missing = [alias for alias in aliases if alias not in loaded_profiles]
    if missing:
        raise OznakConfigurationError(f"Unknown database alias(es) in config: {', '.join(missing)}")
    return tuple(loaded_profiles[alias] for alias in aliases)


def _parse_columns(raw_columns: str | None) -> tuple[str, ...] | None:
    if raw_columns is None:
        return None
    columns = tuple(column.strip() for column in raw_columns.split(",") if column.strip())
    if not columns:
        return None
    return tuple(validate_identifier(column, field_name="selected column") for column in columns)


def _combine_filters(
    filters: list[str] | None,
    *,
    time_from: str | None,
    time_to: str | None,
    reference: str | None,
) -> list[str]:
    combined_filters = list(filters) if filters else []
    if time_from:
        combined_filters.append(f"TimeStamp >= {time_from}")
    if time_to:
        combined_filters.append(f"TimeStamp <= {time_to}")
    if reference:
        combined_filters.append(f"RefName = {reference}")
    return combined_filters
