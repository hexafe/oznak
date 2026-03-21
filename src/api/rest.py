from typing import Optional

from fastapi import FastAPI, HTTPException, Query

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
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    if not prepared["databases"]:
        raise HTTPException(status_code=400, detail="databases is required")

    df = fetcher.fetch(
        prepared["databases"],
        prepared["filters"],
        prepared["limit"],
        normalized_date_col,
        prepared["columns"],
    )

    return {"rows": len(df), "data": df.to_dict(orient="records")}
