from fastapi import FastAPI, HTTPException, Query
from typing import Optional
from src.services.multi_database_fetcher import MultiDatabaseFetcher

app = FastAPI(title='Oznak MVP API')
fetcher = MultiDatabaseFetcher()

@app.get('/fetch')
def fetch(databases: str = Query(..., description='Comma-separated databases'),
          time_from: Optional[str] = None,
          time_to: Optional[str] = None,
          last_n: Optional[int] = None,
          reference: Optional[str] = None):
    databases_list = [db.strip() for db in databases.split(',') if db.strip()]
    if not databases_list:
        raise HTTPException(status_code=400, detail='databases is required')
    filters = []
    if time_from:
        filters.append(f"TimeStamp >= {time_from}")
    if time_to:
        filters.append(f"TimeStamp <= {time_to}")
    if last_n:
        limit = last_n
    else:
        limit = None
    if reference:
        filters.append(f"RefName = {reference}")
    df = fetcher.fetch(databases_list, filters, limit)
    return {'rows': len(df), 'data': df.to_dict(orient='records')}
