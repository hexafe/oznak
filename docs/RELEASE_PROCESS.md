# Oznak Release Process

_Last updated: 2026-05-16_

This process is required before tagging or publishing a release.

## 1. Required quality gates

Run all required gates from the repository root:

- `python -m pytest -q`
- `python -m ruff check .`
- `python -m build --sdist --wheel`
- `python -m compileall -q -x '^\./\.git/' .`

All commands must pass without local workarounds.
Run the build from a clean generated-artifact state so stale `build/` bytecode
cannot be copied into the wheel.

## 2. CLI smoke gate

Run a minimal CLI smoke pass:

- `oznak version`
- `oznak profiles --config config/databases.yaml`
- `oznak load --help`
- `oznak load-chunked --help`
- `oznak benchmark-chunked --sources 2 --rows-per-source 10 --chunk-size 5 --workers 1 --workers 2`
- `oznak tui --help`

If validating Parquet export for a release candidate, install the optional extra
with `python -m pip install -e ".[dev,parquet]"` and rerun the export tests.

## 3. Integration-test policy

Live database tests are optional and opt-in only. They must be marked with
`@pytest.mark.integration` and excluded from the default release gate unless the
release owner explicitly enables them.

Use `oznak benchmark-chunked` as the no-DB benchmark performance smoke path. It verifies
chunk iteration, parallel worker plumbing, and optional streaming CSV export
without credentials, but it is not a substitute for opt-in live MySQL/MSSQL
validation before a deployment that depends on real database behavior.

Example explicit run:

- `python -m pytest -q -m integration`
- `docker compose -f docker-compose.integration.yml up -d`
- `OZNAK_RUN_LIVE_DB_TESTS=1 python -m pytest -q -m integration`
- `docker compose -f docker-compose.integration.yml down -v`

See `docs/LIVE_DB_INTEGRATION.md` for driver prerequisites and environment
variable overrides.

## 4. Configuration and data hygiene gate

Before release:

- Confirm there is no real production data in the repository.
- Confirm there are no real credentials, secrets, or connection strings.
- Keep `.env` and other credential-bearing files out of commits.
- Keep public config as template-only inputs.
- Redact credentials and host details in diagnostics and examples.

## 5. Third-party notice and driver prerequisite gate

Before tagging a release:

- Refresh `THIRD_PARTY_NOTICES.md` against the exact dependency set being
  published.
- Confirm `mysql-connector-python` license terms are acceptable for the
  intended distribution.
- Confirm MSSQL users have Microsoft ODBC Driver 17 for SQL Server, or a
  compatible `pyodbc` driver, installed on target systems.
- Confirm MySQL users have the selected Python driver installed and network
  access to configured hosts.
- If Parquet support is advertised, install `.[parquet]` and confirm the
  optional Parquet engine metadata is included in the notice review.
- Review `docs/SECURITY_PERFORMANCE.md` when changing query rendering,
  identifier validation, concurrency, chunking, or export behavior.

## 6. Version and tag policy

- Keep `pyproject.toml` `project.version` and `oznak.__version__` synchronized.
- Tag final releases as `vX.Y.Z` and prereleases as `vX.Y.ZrcN`, matching the
  package version exactly.
- Do not publish a tag while generated artifacts, local credentials, or
  production-derived exports are present in the working tree.
- Prefer a pinned Git commit for downstream smoke validation before creating a
  formal public tag.

## 7. Release checklist handoff

Use `docs/TEST_VERIFICATION_CHECKLIST.md` to record the exact verification run
for the release candidate.
