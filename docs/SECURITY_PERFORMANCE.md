# Security And Performance Notes

## SQL Safety

Use the package-native `oznak.*` API for new code. The package path avoids raw
SQL concatenation for user values:

- database/table/column identifiers are validated before rendering;
- configured `allowed_columns` are enforced when present;
- filter values are passed as bound SQL parameters;
- `IS` and `IS NOT` only accept `NULL`;
- `IN` and `NOT IN` require non-empty value lists;
- diagnostics and examples avoid credentials and real host details.

Legacy `src.*` modules are deprecated compatibility shims. They emit
`DeprecationWarning` and should not be used by new consumers.

## Performance And Memory Controls

The package exposes several controls for large or slow database reads:

- `order_by_enabled: false` in a profile disables default server-side ordering.
- `QueryRequest(..., order_by_enabled=False)` disables ordering for a request.
- CLI `--no-order-by` disables ordering for `oznak load`.
- `max_workers` bounds parallel source fetches.
- SQLAlchemy pool fields (`pool_size`, `max_overflow`, `pool_timeout_seconds`)
  bound database connection pressure.
- `connect_timeout_seconds`, `query_timeout_seconds`, and request-level
  `timeout_seconds` bound long-running operations.
- `fetch_records_chunked(...)` and `iter_records_chunked(...)` avoid loading
  all rows at once.
- Parallel chunked iteration uses a bounded queue so ready chunks can stream
  without unbounded buffering.
- `export_chunks_streaming(...)` writes chunked CSV output incrementally.
- Optional Parquet export is available for compact columnar output.

Use `oznak benchmark-chunked` for no-DB throughput smoke checks and
`docs/LIVE_DB_INTEGRATION.md` for disposable live MySQL/MSSQL validation.
