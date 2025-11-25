from fastapi import FastAPI, HTTPException, Query
from typing import Optional
from src.services.multi_line_fetcher import MultiLineFetcher

app = FastAPI(title='Oznak MVP API')
fetcher = MultiLineFetcher()

@app.get('/fetch')
def fetch(lines: str = Query(..., description='Comma-separated lines'),
          time_from: Optional[str] = None,
          time_to: Optional[str] = None,
          last_n: Optional[int] = None,
          reference: Optional[str] = None):
    lines_list = [l.strip() for l in lines.split(',') if l.strip()]
    if not lines_list:
        raise HTTPException(status_code=400, detail='lines is required')
    filters = {}
    if time_from:
        filters['time_from'] = time_from
    if time_to:
        filters['time_to'] = time_to
    if last_n:
        filters['last_n'] = last_n
    if reference:
        filters['reference'] = reference
    df = fetcher.fetch(lines_list, filters)
    return {'rows': len(df), 'data': df.to_dict(orient='records')}
