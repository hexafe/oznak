# Oznak Test Verification Checklist

_Last updated: 2026-05-16_

## 1. Required release gates

- Run `python -m pytest -q`
- Run `python -m ruff check .`
- Run `python -m build --sdist --wheel`
- Run `python -m compileall -q -x '^\./\.git/' .`
- Expected result: all commands pass with no local exceptions

## 2. CLI entrypoint verification

- Run `oznak version`
- Run `oznak profiles --config config/databases.yaml`
- Run `oznak load --help`
- Run `oznak load-chunked --help`
- Run `oznak tui --help`
- Run `pytest -q -m cli_integration`
- Expected result: all commands render help successfully
- Expected result: CLI integration tests pass in isolation

## 3. Optional live DB integration verification (opt-in)

- Live DB tests are optional and never part of default release gating.
- Mark live DB tests with `@pytest.mark.integration`.
- Run only when explicitly requested:
  `python -m pytest -q -m integration`

## 3a. Optional Parquet verification

- Install optional export support with `python -m pip install -e ".[dev,parquet]"`.
- Run `python -m pytest -q tests/test_exporter_contracts.py`.
- Expected result: Parquet roundtrip test is executed instead of skipped.

## 4. Config and data hygiene verification

- Verify no real plant/production data exists in repository files.
- Verify no real credentials, secrets, or connection strings are committed.
- Verify diagnostics and examples redact credentials and host details.
- Verify config files committed to git are template-safe only.

## 5. Release documentation and branch state

- Confirm `README.md` matches current CLI/API behavior.
- Confirm `docs/RELEASE_PROCESS.md` and this checklist are current.
- Confirm `THIRD_PARTY_NOTICES.md` was refreshed for the exact dependencies
  being released.
- Confirm `pyproject.toml` version matches `oznak.__version__`.
- Confirm release tags use `vX.Y.Z` and match the package version.
- Confirm MSSQL runtime notes mention Microsoft ODBC Driver 17 for SQL Server
  or a compatible `pyodbc` driver.
- Confirm branch is clean before merge or push
