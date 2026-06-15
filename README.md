# Oznak

Oznak is the Hexafe industrial database access layer. It connects to configured MySQL/MSSQL process databases, fetches selected records through validated query contracts, and returns structured data plus diagnostics for CLI, TUI, API, and library consumers.

Oznak is independent at runtime. Consumers should import `oznak.*`, not legacy `src.*` modules.

## Current Status

- Package namespace: `oznak`
- Version: `0.2.0rc2`
- Supported dialects: MySQL and MSSQL
- Default tests use synthetic data only
- Standalone surfaces: CLI and minimal prompt-based TUI
- Reuse surface: typed package API with profiles, normalized query requests, filters, bounded concurrency, fetch results, diagnostics, cancellation, chunked fetch, streaming chunk events, export profiles, and a no-DB synthetic chunk benchmark

## Install For Development

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
```

Install the optional Parquet writer when Parquet export is needed:

```bash
python -m pip install -e ".[dev,parquet]"
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
- optional `pool_size`
- optional `max_overflow`
- optional `pool_timeout_seconds`
- optional `order_by_enabled` (defaults to `true`; set `false` to omit server-side sorting)
- optional `display_name`
- optional `metadata`

The same YAML file may define reusable export profiles:

```yaml
export_profiles:
  semicolon_csv:
    format: csv
    delimiter: ";"
    include_metadata: true
  parquet_zstd:
    format: parquet
    compression: zstd
```

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
  --max-workers 2 \
  --no-order-by \
  --out output.csv
```

Chunked output uses the same package contracts and streams CSV rows as chunks
are fetched. With `--max-workers`, ready chunks from parallel sources are emitted
through a bounded queue so slow sources do not block fast sources from writing.
Use one worker when source-grouped output order is required.

```bash
oznak load-chunked database1,database2 \
  --chunk-size 10000 \
  --pagination-column id \
  --max-workers 2 \
  --export-profile semicolon_csv \
  --out output.csv
```

Benchmark chunked orchestration without live database access:

```bash
oznak benchmark-chunked \
  --sources 4 \
  --rows-per-source 5000 \
  --chunk-size 1000 \
  --workers 1 \
  --workers 2 \
  --delay-ms 5
```

The benchmark uses synthetic profiles and an injected `read_sql` replacement.
It exercises the same chunked iterator and optional streaming CSV exporter used
by `load-chunked`, so it can compare sequential and parallel chunk plumbing on
developer machines and CI without credentials or database services. Add
`--export-dir <path>` to include streaming CSV writes in the measurement.

## TUI

```bash
oznak tui --config config/databases.yaml
```

The TUI is a minimal terminal prompt flow for selecting profiles, columns,
filters, limits, request timeout, output path, and named export profiles when
they exist in the config. It uses the same package fetch path as the CLI and
prints a compact per-source fetch summary before export.

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
    QueryRequest,
    QueryFilter,
    fetch_records,
    fetch_records_chunked,
    iter_records_chunked,
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
json_records = result.to_json_records()
```

Use `QueryRequest.from_inputs(...)` to normalize CLI/API/TUI-style inbound arguments before creating `FetchRequest`. Use `fetch_records(..., max_workers=N)` for bounded parallel source fetches. `FetchResult.data` remains the original pandas DataFrame; use `FetchResult.to_json_records()` when returning records through JSON APIs. Use `fetch_records_chunked(...)` for a deterministic `FetchResult`, or `iter_records_chunked(..., max_workers=N)` when a caller wants to stream ready chunk frames to an exporter through a bounded queue. Use `run_synthetic_chunked_benchmark(...)` when a machine has no database access but still needs to verify chunk orchestration and export overhead.

## Safety Notes

- Filters are typed and parameterized.
- `IS` and `IS NOT` are restricted to `NULL` predicates.
- Columns are validated against configured profile allowlists when allowlists are present.
- Profile timeout fields are typed and converted to safe dialect-specific engine options.
- Profile pool fields are typed before they reach SQLAlchemy.
- See `docs/SECURITY_PERFORMANCE.md` for SQL safety boundaries and
  performance/memory controls.

## Release Notes For Maintainers

- Run the release gates in `docs/RELEASE_PROCESS.md` before tagging.
- Keep `pyproject.toml` and `oznak.__version__` synchronized.
- Refresh `THIRD_PARTY_NOTICES.md` before publishing a release or changing
  dependencies.
- MSSQL deployments need Microsoft ODBC Driver 17 for SQL Server or a compatible
  `pyodbc` driver available on the target system.
- Diagnostics redact credentials and connection strings.
- Default tests do not require live databases.
- Live database tests should be opt-in integration tests only.
- Use `docs/LIVE_DB_INTEGRATION.md` for disposable MySQL/MSSQL integration
  checks when Docker and database drivers are available.
- Legacy `src.*` modules are deprecated compatibility shims; new consumers
  should import from `oznak.*`.
