from fastapi import FastAPI, HTTPException, Query
from typing import Optional
from src.services.multi_database_fetcher import MultiDatabaseFetcher
from src.services.request_preprocessor import preprocess_fetch_request

app = FastAPI(title='Oznak MVP API')
fetcher = MultiDatabaseFetcher()

@app.get('/fetch')
def fetch(databases: str = Query(..., description='Comma-separated databases'),
          time_from: Optional[str] = None,
          time_to: Optional[str] = None,
          last_n: Optional[int] = None,
          reference: Optional[str] = None):
    filters: list[str] = []
    if time_from:
        filters.append(f"TimeStamp >= {time_from}")
    if time_to:
        filters.append(f"TimeStamp <= {time_to}")
    if reference:
        filters.append(f"RefName = {reference}")

    try:
        prepared = preprocess_fetch_request(
            databases=databases,
            filters=filters,
            last=last_n,
            select_columns=None,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    if not prepared["databases"]:
        raise HTTPException(status_code=400, detail='databases is required')

    df = fetcher.fetch(prepared["databases"], prepared["filters"], prepared["limit"])
    return {'rows': len(df), 'data': df.to_dict(orient='records')}
