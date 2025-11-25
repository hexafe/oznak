# Oznak — MVP (multi-db loader)

Minimal MVP for loading and aggregating production data from multiple databases (multi-line support).
Features:
- multi-db config (YAML)
- credentials via `.env`
- connectors for MySQL and MSSQL
- query builder with filters (time range, last N, reference LIKE)
- multi-line fetcher (concatenate results, add source_line)
- minimal FastAPI `/fetch` endpoint
- CLI using Typer

Run:
1. Copy `.env.example` -> `.env` and fill credentials.
2. Adjust `config/databases.yaml` with your lines.
3. `pip install -r requirements.txt`
4. `uvicorn src.api.rest:app --reload`
5. Example: GET /fetch?lines=line1,line2&last_n=1000&reference=ABC

