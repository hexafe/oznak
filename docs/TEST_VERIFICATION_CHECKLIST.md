# Oznak Test Verification Checklist

_Last updated: 2026-03-21_

## 1. Baseline local verification

- Run `pytest -q`
- Expected result: all tests pass on the `roadmap` branch

## 2. CLI entrypoint verification

- Run `python -m src.main --help`
- Run `python -m src.main load --help`
- Run `python -m src.main load-chunked --help`
- Expected result: all commands render help successfully

## 3. API smoke verification

- Start API with `uvicorn src.api.rest:app --host 127.0.0.1 --port 8000`
- Run `curl http://127.0.0.1:8000/health`
- Run one invalid request:
  `curl "http://127.0.0.1:8000/fetch?databases=database1&filters=badfilter"`
- Run one valid request if a reachable database is configured
- Expected result:
  - `/health` returns `{"status":"ok"}`
  - invalid filter request returns `400`

## 4. DB manager verification

- Verify supported MySQL config builds an engine
- Verify supported MSSQL config builds an engine
- Verify unsupported `type` fails with `Unsupported DB type`
- Verify missing credentials fail with `Missing credentials`

## 5. Real MySQL verification

- Run a bounded non-chunked fetch with `load`
- Run a bounded chunked fetch with `load-chunked`
- Run `load-chunked` with `--select-columns` that omit the pagination column
- Compare row count and columns between chunked and non-chunked outputs

## 6. Real MSSQL verification

- Run a bounded non-chunked fetch with `load`
- Run a bounded chunked fetch with `load-chunked`
- Verify filters using `=`, `LIKE`, and `IN`
- Confirm the generated SQL executes with the configured driver and schema

## 7. Chunking correctness verification

- Use a known unique and indexed pagination column
- Compare chunked export against a bounded non-chunked export
- Verify no skipped rows and no duplicated rows
- Verify `source_database` is present for multi-database chunked output

## 8. Container verification

- Run `docker compose build`
- Run `docker compose up`
- Call `curl http://127.0.0.1:8000/health`
- Expected result: container starts and API answers health checks

## 9. Release readiness verification

- Confirm `README.md` matches actual CLI/API behavior
- Confirm `docs/IMPLEMENTATION_ROADMAP.md` reflects current implementation status
- Confirm branch is clean before merge or push
