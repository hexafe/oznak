# Oznak Implementation Roadmap

_Last updated: 2026-05-16_

## Repository review summary

Current strengths:
- Installable package metadata exists in `pyproject.toml`.
- Canonical package namespace exists at `src/oznak/`.
- Public contracts cover profiles, credentials, normalized query requests, typed filters, query compilation, fetch requests/results, diagnostics, cancellation, bounded concurrency, and chunked fetch.
- CLI entry point exists as `oznak`.
- Minimal prompt-based TUI exists as `oznak tui`.
- Query generation is typed, parameterized, database-aware for MySQL/MSSQL, and allowlist-aware.
- Package fetch paths return structured per-source diagnostics and support opt-in `max_workers`.
- Chunked reads expose bounded queue-backed streaming events as well as the compatibility `FetchResult` wrapper.
- A no-DB synthetic chunk benchmark exercises the same chunk iterator and optional streaming CSV exporter.
- SQLAlchemy engine creation supports typed pool tuning fields.
- Package export profiles support CSV, XLSX, and optional Parquet.
- Legacy `src.cli.main` and `src.api.rest` delegate to package-native surfaces.
- Public configuration examples are synthetic-only.
- Unit, package CLI/TUI, query, fetch, config, hygiene, API, legacy compatibility, and smoke tests run under `pytest -q`.

Current gaps identified:
- Live MySQL/MSSQL validation remains opt-in future work.
- Legacy `src.*` service/query/db/storage modules still exist for compatibility and should be retired gradually.
- TUI cancellation and advanced diagnostics drill-down can still be polished.

## Phase plan

| Phase | Scope | Status | Next step |
|---|---|---|---|
| Phase 0 | Package foundation and quality guardrails | ✅ Completed | Keep package build/import smoke in CI and release checks. |
| Phase 1 | Query subsystem hardening | ✅ Completed | Add live dialect validation later as opt-in integration tests. |
| Phase 2 | Multi-database orchestration resilience | ✅ Completed | Add live DB validation later as opt-in integration tests. |
| Phase 3 | CLI and TUI productization | 🟢 In Progress | Improve cancellation ergonomics and advanced diagnostics drill-down. |
| Phase 4 | Data export and post-processing workflow | ✅ Completed | Keep optional Parquet smoke coverage active where `pyarrow` is installed. |
| Phase 5 | Deployment, API, and release readiness | ✅ Completed | Use the release process before tagging; live DB tests remain opt-in future validation. |


## Backlog mapping

The normalized backlog in `TODO` is the source of truth for cross-phase, non-PR-sized work items. Keep roadmap planning and backlog IDs synchronized by updating both files together whenever priorities or phase targeting changes.

| Backlog ID | Current roadmap phase alignment | Synchronization note |
|---|---|---|
| BL-001 | Phase 3 — CLI and TUI productization | TUI is now the primary standalone interactive surface; web/API is optional integration. |
| BL-002 | Phase 1 — Query subsystem hardening | `QueryRequest` preprocessing is implemented; keep backlog item only for future live-dialect validation and advanced filter groups. |

## Planned phase delivery slices

### Next PR Queue

**PR3.4 — TUI cancellation polish**
- Objective: Make cancellation and interrupted fetch handling easier to operate from `oznak tui`.
- Scope: `src/oznak/tui.py`, `src/oznak/runtime.py`, package TUI tests.
- Acceptance criteria:
  - Cancellation prompt/keyboard handling is predictable.
  - Cancelled and timed-out sources are clearly separated from validation errors.
  - Existing compact fetch summaries remain covered by tests.

### Completed Delivery Slices

**PR4.2 — TUI export profile polish**
- Objective: Make named export profiles selectable in `oznak tui` without making the prompt flow noisy.
- Scope: `src/oznak/tui.py`, package TUI tests.
- Status: ✅ Completed.
- Acceptance criteria:
  - TUI can choose an export profile when profiles exist in config.
  - Existing suffix-driven CSV/XLSX/Parquet behavior remains compatible.
  - Prompt sequence remains short and covered by tests.

**PR5.1 — Release readiness**
- Objective: Make a pinned Git commit/tag safe for downstream consumers.
- Scope: release checklist, version policy, third-party notices, optional integration-test marker, README/roadmap.
- Status: ✅ Completed.
- Acceptance criteria:
  - `python -m build` and import smoke are documented release gates.
  - Live DB tests are marked opt-in and excluded from default tests.
  - Windows ODBC/MySQL driver prerequisites are documented.
  - Third-party notice obligations are documented.
  - Version tag policy is documented.

**PR2.3 — Bounded parallel chunk streaming**
- Objective: Let parallel chunked reads stream ready chunks without buffering all
  events from each source first.
- Scope: `src/oznak/chunked.py`, chunked fetch tests, README/roadmap.
- Status: ✅ Completed.
- Acceptance criteria:
  - `iter_records_chunked(..., max_workers=N)` yields ready chunks while slower
    sources are still reading.
  - Worker output uses a bounded queue to avoid unbounded per-source buffering.
  - `fetch_records_chunked(...)` keeps deterministic source-grouped result
    ordering for compatibility callers.

**PR3.3 — No-DB benchmark and TUI summary**
- Objective: Give developers a real performance/streaming smoke path when no
  database access is available, and make TUI fetch outcomes easier to scan.
- Scope: `src/oznak/benchmarks.py`, `src/oznak/cli.py`, `src/oznak/tui.py`,
  package CLI/TUI tests, README/release docs.
- Status: ✅ Completed.
- Acceptance criteria:
  - `oznak benchmark-chunked` compares worker counts using synthetic profiles
    and injected `read_sql`.
  - Optional `--export-dir` includes streaming CSV export overhead.
  - `oznak tui` prints source count, row count, and per-status totals before
    export or error handling.

### Future Backlog

**PR2.4 — Opt-in live DB integration harness**
- Objective: Validate package behavior and performance against disposable MySQL
  and MSSQL instances without making live services part of default CI.
- Scope: `tests/integration/`, container/test fixtures, release checklist.
- Acceptance criteria:
  - Integration tests are marked `@pytest.mark.integration`.
  - Default `python -m pytest -q` remains synthetic-only.
  - MySQL and MSSQL query/fetch/export paths are covered when explicitly run.
  - Chunked sequential versus parallel fetch timing is measured on disposable
    database fixture data and compared with the synthetic benchmark baseline.

## Feature-by-feature execution policy

For each new feature merged into the repository:
1. Add or update tests (unit and/or integration) for both success and failure paths.
2. Wire tests into CI so feature behavior is enforced on pull requests.
3. Document rollout status in this roadmap table by updating `Status` and `Next step`.

## Tracking cadence

- Update roadmap on every feature PR.
- Re-evaluate priorities at least once per sprint.
- Promote any blocked phase with explicit blocker notes and mitigation steps.
- Use `docs/TEST_VERIFICATION_CHECKLIST.md` as the default manual verification sequence for release candidates.

## Evidence checked

- `.github/workflows/ci.yml`
- `pytest.ini`
- `requirements.txt`
- `tests/test_smoke.py`
- `docs/TEST_VERIFICATION_CHECKLIST.md`
