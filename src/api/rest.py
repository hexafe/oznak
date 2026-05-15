from typing import Optional

from fastapi import FastAPI, Query

from src.api.errors import execution_error, validation_error
from src.services.multi_database_fetcher import MultiDatabaseFetcher
from src.services.request_preprocessor import preprocess_fetch_request

app = FastAPI(title="Oznak API")
fetcher = MultiDatabaseFetcher()


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/fetch")
def fetch(
    databases: str = Query(..., description="Comma-separated database names"),
    filters: Optional[list[str]] = Query(default=None),
    last: Optional[int] = Query(default=None, gt=0),
    date_col: str = Query(default="TimeStamp"),
    select_columns: Optional[str] = Query(default=None),
    time_from: Optional[str] = None,
    time_to: Optional[str] = None,
    last_n: Optional[int] = None,
    reference: Optional[str] = None,
    order_by: bool = True,
):
    combined_filters = list(filters) if isinstance(filters, list) else []
    if time_from:
        combined_filters.append(f"TimeStamp >= {time_from}")
    if time_to:
        combined_filters.append(f"TimeStamp <= {time_to}")
    if reference:
        combined_filters.append(f"RefName = {reference}")

    effective_last = last if isinstance(last, int) else last_n

    normalized_select_columns = select_columns if isinstance(select_columns, (str, list)) else None
    normalized_date_col = date_col if isinstance(date_col, str) else "TimeStamp"

    try:
        prepared = preprocess_fetch_request(
            databases=databases,
            filters=combined_filters,
            last=effective_last,
            select_columns=normalized_select_columns,
        )
    except ValueError as exc:
        raise validation_error(str(exc), {"field": "request"}) from exc

    if not prepared["databases"]:
        raise validation_error("databases is required", {"field": "databases"})

    try:
        if order_by:
            df = fetcher.fetch(
                prepared["databases"],
                prepared["filters"],
                prepared["limit"],
                normalized_date_col,
                prepared["columns"],
            )
        else:
            df = fetcher.fetch(
                prepared["databases"],
                prepared["filters"],
                prepared["limit"],
                normalized_date_col,
                prepared["columns"],
                order_by_enabled=False,
            )
    except Exception as exc:
        raise execution_error(
            "database fetch failed",
            {"error": str(exc), "databases": prepared["databases"]},
        ) from exc

    return {"rows": len(df), "data": df.to_dict(orient="records")}
