# Oznak Implementation Roadmap

_Last updated: 2026-05-10_

## Repository review summary

Current strengths:
- Installable package metadata exists in `pyproject.toml`.
- Canonical package namespace exists at `src/oznak/`.
- Public contracts cover profiles, credentials, typed filters, query compilation, fetch requests/results, diagnostics, cancellation, and chunked fetch.
- CLI entry point exists as `oznak`.
- Minimal prompt-based TUI exists as `oznak tui`.
- Query generation is typed, parameterized, database-aware for MySQL/MSSQL, and allowlist-aware.
- Package fetch paths return structured per-source diagnostics.
- Public configuration examples are synthetic-only.
- Unit, package CLI/TUI, query, fetch, config, hygiene, API, legacy compatibility, and smoke tests run under `pytest -q`.

Current gaps identified:
- Live MySQL/MSSQL validation remains opt-in future work.
- Legacy `src.*` modules still exist for compatibility and should be retired gradually.
- Legacy FastAPI still exists under `src.api`, but package-native FastAPI exists at `oznak.api`.
- Third-party notices and version-tag policy need finalization before a public release tag.

## Phase plan

| Phase | Scope | Status | Next step |
|---|---|---|---|
| Phase 0 | Package foundation and quality guardrails | ✅ Completed | Keep package build/import smoke in CI and release checks. |
| Phase 1 | Query subsystem hardening | ✅ Completed | Add live dialect validation later as opt-in integration tests. |
| Phase 2 | Multi-database orchestration resilience | ✅ Completed | Add live DB validation later as opt-in integration tests. |
| Phase 3 | CLI and TUI productization | 🟢 In Progress | Improve TUI ergonomics and add richer diagnostics views. |
| Phase 4 | Data export and post-processing workflow | 🟢 In Progress | Add export profiles and metadata once CLI/TUI workflows stabilize. |
| Phase 5 | Deployment, API, and release readiness | 🟢 In Progress | Add third-party notices and version-tag policy before public release tag. |


## Backlog mapping

The normalized backlog in `TODO` is the source of truth for cross-phase, non-PR-sized work items. Keep roadmap planning and backlog IDs synchronized by updating both files together whenever priorities or phase targeting changes.

| Backlog ID | Current roadmap phase alignment | Synchronization note |
|---|---|---|
| BL-001 | Phase 3 — CLI and TUI productization | TUI is now the primary standalone interactive surface; web/API is optional integration. |
| BL-002 | Phase 1 — Query subsystem hardening | Typed query compiler is complete; keep backlog item only for future live-dialect validation and advanced filter groups. |

## Planned phase delivery slices

### Next PR Queue

**PR3.3 — TUI diagnostics polish**
- Objective: Improve `oznak tui` ergonomics without expanding scope into a full GUI.
- Scope: `src/oznak/tui.py`, package TUI tests.
- Acceptance criteria:
  - Source diagnostics are easier to scan.
  - Validation errors explain which profile/field failed.
  - Cancellation prompt/keyboard handling is predictable.

**PR4.1 — Export profile configuration**
- Objective: Add reusable export settings for output format, delimiter, and metadata.
- Scope: package export helper module, CLI/TUI integration, tests.
- Acceptance criteria:
  - CLI/TUI can use named export defaults.
  - Export metadata can include source aliases and diagnostic summary.
  - Existing plain CSV/XLSX behavior remains compatible.

**PR5.1 — Release readiness**
- Objective: Make a pinned Git commit/tag safe for downstream consumers.
- Scope: release checklist, version policy, third-party notices, optional integration-test marker, README/roadmap.
- Acceptance criteria:
  - `python -m build` and import smoke are documented release gates.
  - Live DB tests are marked opt-in and excluded from default tests.
  - Windows ODBC/MySQL driver prerequisites are documented.
  - Third-party notice obligations are documented.
  - Version tag policy is documented.

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
