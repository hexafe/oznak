# Oznak

Oznak is the Hexafe industrial database access layer. It connects to configured MySQL/MSSQL process databases, fetches selected records through validated query contracts, and returns structured data plus diagnostics for CLI, TUI, API, and library consumers.

Oznak is independent at runtime. Consumers should import `oznak.*`, not legacy `src.*` modules.

## Current Status

- Package namespace: `oznak`
- Version: `0.1.0`
- Supported dialects: MySQL and MSSQL
- Default tests use synthetic data only
- Standalone surfaces: CLI and minimal prompt-based TUI
- Reuse surface: typed package API with profiles, filters, fetch results, diagnostics, cancellation, and chunked fetch

## Install For Development

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
```

## Verify

```bash
python -m pytest -q
python -m ruff check .
python -m compileall -q -x '^\./\.git/' .
python -m build --sdist --wheel
```

## Configuration

Use `config/databases.yaml` as a safe template. Public examples must stay synthetic and must not contain real hosts, credentials, database dumps, query logs, or plant-derived data.

Each profile supports:

- `type`: `mysql` or `mssql`
- `host`
- `port`
- `database`
- `table`
- optional `allowed_columns`
- optional `timestamp_column`
- optional `pagination_column`
- optional `connect_timeout_seconds`
- optional `query_timeout_seconds`
- optional `order_by_enabled` (defaults to `true`; set `false` to omit server-side sorting)
- optional `display_name`
- optional `metadata`

Credentials are resolved by alias from environment variables:

```bash
DATABASE1_USER=
DATABASE1_PASSWORD=
```

Core package modules do not load `.env` at import time.

## CLI

```bash
oznak version
oznak profiles --config config/databases.yaml
oznak load database1 --config config/databases.yaml --filter "Status = ACTIVE" --out output.csv
```

Common `load` options:

```bash
oznak load database1,database2 \
  --select-columns "reference,status,updated_at" \
  --filter "status = ACTIVE" \
  --last 100 \
  --date-col updated_at \
  --no-order-by \
  --out output.csv
```

## TUI

```bash
oznak tui --config config/databases.yaml
```

The TUI is a minimal terminal prompt flow for selecting profiles, columns, filters, limits, and output path. It uses the same package fetch path as the CLI.

## API

The package-native FastAPI app is optional:

```bash
uvicorn oznak.api:app --host 127.0.0.1 --port 8000
```

CLI and TUI are the primary standalone surfaces; the API is for integration/deployment scenarios.

## Library API

```python
from oznak import (
    DatabaseProfile,
    EnvironmentCredentialProvider,
    FetchRequest,
    QueryFilter,
    fetch_records,
)

profile = DatabaseProfile(
    alias="database1",
    dialect="mysql",
    host="db1.example.invalid",
    port=3306,
    database="sample_process_db_1",
    table="sample_measurements",
    allowed_columns=("id", "status", "updated_at"),
    timestamp_column="updated_at",
    pagination_column="id",
    order_by_enabled=True,
)

request = FetchRequest(
    profiles=(profile,),
    filters=(QueryFilter("status", "=", "ACTIVE"),),
    columns=("id", "status", "updated_at"),
    limit=100,
    date_column="updated_at",
)

result = fetch_records(request, credential_provider=EnvironmentCredentialProvider())
```

Use `fetch_records_chunked(...)` for chunked reads with a pagination column. Both fetch paths return `FetchResult` with per-source diagnostics.

## Safety Notes

- Filters are typed and parameterized.
- `IS` and `IS NOT` are restricted to `NULL` predicates.
- Columns are validated against configured profile allowlists when allowlists are present.
- Profile timeout fields are typed and converted to safe dialect-specific engine options.
- Diagnostics redact credentials and connection strings.
- Default tests do not require live databases.
- Live database tests should be opt-in integration tests only.
