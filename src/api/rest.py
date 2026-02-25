from fastapi import FastAPI, HTTPException, Query
from typing import Optional
from src.services.multi_database_fetcher import MultiDatabaseFetcher
from src.services.filter_parser import normalize_databases, parse_filters

app = FastAPI(title='Oznak MVP API')
fetcher = MultiDatabaseFetcher()

@app.get('/fetch')
def fetch(databases: str = Query(..., description='Comma-separated databases'),
          time_from: Optional[str] = None,
          time_to: Optional[str] = None,
          last_n: Optional[int] = None,
          reference: Optional[str] = None):
    try:
        databases_list = normalize_databases(databases)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    if not databases_list:
        raise HTTPException(status_code=400, detail='databases is required')

    filters: list[str] = []
    if time_from:
        filters.append(f"TimeStamp >= {time_from}")
    if time_to:
        filters.append(f"TimeStamp <= {time_to}")
    if reference:
        filters.append(f"RefName = {reference}")

    try:
        parsed = parse_filters(filters, last_n)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    df = fetcher.fetch(databases_list, parsed["filters"], parsed["limit"])
    return {'rows': len(df), 'data': df.to_dict(orient='records')}
