from __future__ import annotations

from typing import Optional

from fastapi import FastAPI, Query
from fastapi import HTTPException

from oznak.config import load_database_profiles
from oznak.credentials import EnvironmentCredentialProvider
from oznak.errors import OznakConfigurationError, OznakValidationError
from oznak.fetcher import fetch_records
from oznak.request import QueryRequest

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
    order_by: bool = True,
    max_workers: Optional[int] = Query(default=None, gt=0),
) -> dict[str, object]:
    try:
        loaded_profiles = load_database_profiles(config)
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

        effective_last = last if isinstance(last, int) else last_n if isinstance(last_n, int) else None
        query_request = QueryRequest.from_inputs(
            databases=databases,
            filters=combined_filters,
            select_columns=select_columns if isinstance(select_columns, str) else None,
            last=effective_last,
            date_col=date_col,
            order_by_enabled=order_by,
            max_workers=max_workers if isinstance(max_workers, int) else None,
        )
        request = query_request.to_fetch_request(loaded_profiles)
    except (OznakConfigurationError, OznakValidationError, ValueError) as exc:
        raise validation_error(str(exc), {"field": "request"}) from exc

    fetch_kwargs = {"credential_provider": EnvironmentCredentialProvider()}
    if query_request.max_workers is not None:
        fetch_kwargs["max_workers"] = query_request.max_workers
    result = fetch_records(request, **fetch_kwargs)
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
        "data": result.to_json_records(),
    }


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
