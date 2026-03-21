from typing import Optional

from fastapi import FastAPI, HTTPException, Query

from src.services.filter_parser import parse_filters
from src.services.multi_database_fetcher import MultiDatabaseFetcher

app = FastAPI(title="Oznak API")
fetcher = MultiDatabaseFetcher()


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/fetch")
def fetch(
    databases: str = Query(..., description="Comma-separated database names"),
    filters: list[str] = Query(default=[]),
    last: Optional[int] = Query(default=None, gt=0),
    date_col: str = Query(default="TimeStamp"),
    select_columns: Optional[str] = Query(default=None),
):
    databases_list = [db.strip() for db in databases.split(",") if db.strip()]
    if not databases_list:
        raise HTTPException(status_code=400, detail="databases is required")

    try:
        parsed = parse_filters(filters, last)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    columns = [col.strip() for col in select_columns.split(",")] if select_columns else None
    df = fetcher.fetch(databases_list, parsed["filters"], parsed["limit"], date_col, columns)

    return {"rows": len(df), "data": df.to_dict(orient="records")}
