# Oznak Implementation Roadmap

_Last updated: 2026-02-25 (refreshed)_

## Repository review summary

Current strengths:
- Core modular structure is already present (`db`, `query`, `services`, `cli`, `api`, `storage`).
- Unit and smoke tests exist for query and multi-database fetcher flows.
- CI pipeline is present and running tests in `.github/workflows/ci.yml`.
- Test invocation is stable via `PYTHONPATH: .` in CI and `pytest.ini`.

Current gaps identified:
- No lint/type-check stage is configured in CI (for example `ruff`, `mypy`, or `pyright`).
- No API contract-test coverage is present (schema/contract regression checks are missing).
- CLI and API regression suites are not yet implemented beyond current smoke-level checks.

## Phase plan

| Phase | Scope | Status | Next step |
|---|---|---|---|
| Phase 0 | Baseline quality guardrails (CI + reliable test invocation) | ✅ Completed | Add lint/type checks to CI and establish API/CLI regression coverage as the next quality gate. |
| Phase 1 | Query subsystem hardening (filter parsing, SQL safety, edge-case behavior) | 🟡 Planned | Add typed validation layer and expand test matrix for malformed filters and unsupported operators. |
| Phase 2 | Multi-database orchestration resilience (timeouts, partial failures, observability) | 🟡 Planned | Introduce structured logging and per-database execution metrics surfaced in CLI/API output. |
| Phase 3 | API and CLI productization (stable contracts, error semantics, UX polish) | 🟡 Planned | Publish explicit API schema examples and add CLI golden-path + failure-path integration tests. |
| Phase 4 | Data export and post-processing workflow (CSV/Excel ergonomics, metadata, extensibility) | 🟡 Planned | Add export profile configuration and tests for column typing, ordering, and file naming strategy. |
| Phase 5 | Deployment and operations readiness (container hardening, env templates, release process) | 🟡 Planned | Add release checklist, semantic version workflow, and deployment docs for dev/staging/prod environments. |

## Planned phase delivery slices

### Phase 1 — Query subsystem hardening

#### Next PR queue

**PR1.1 — Typed filter validation foundation**
- Objective: Add a typed validation layer that rejects malformed filter payloads before SQL generation.
- Scope: `query/filter_parser.py`, `query/errors.py`, `tests/query/`.
- Acceptance criteria:
  - Invalid filter shapes (missing field/op/value) return deterministic validation errors.
  - Unsupported operators are rejected with explicit operator names in error messages.
  - Existing valid filter payload tests continue to pass unchanged.
- Risk/rollback: Medium parser-surface risk; rollback by feature-flagging typed validation and restoring legacy parser path.
- Dependencies: None.

**PR1.2 — SQL safety and edge-case matrix expansion**
- Objective: Expand parser/generator tests to lock SQL safety behavior for nulls, mixed types, and nested logical groups.
- Scope: `query/sql_builder.py`, `tests/query/test_filters.py`, `tests/query/test_sql_builder.py`.
- Acceptance criteria:
  - Parameterized SQL output is used for all dynamic values (no string interpolation of raw user values).
  - New edge-case fixtures cover null comparisons, empty lists, and nested `AND`/`OR` groups.
  - Regression tests prove unsupported combinations fail with stable error semantics.
- Risk/rollback: Low-medium risk of test brittleness; rollback by reverting only new fixtures/cases that over-constrain implementation.
- Dependencies: PR1.1 must land before PR1.2.

### Phase 2 — Multi-database orchestration resilience

#### Next PR queue

**PR2.1 — Per-database timeout and cancellation controls**
- Objective: Add per-backend timeout controls so slow databases fail fast without blocking aggregate results.
- Scope: `services/multi_db_fetcher.py`, `services/config.py`, `tests/services/`.
- Acceptance criteria:
  - Timeout can be configured per database target.
  - Timed-out backends are marked failed while other backends continue processing.
  - Timeout behavior is covered by deterministic tests (mocked clock or controlled delay stubs).
- Risk/rollback: Medium operational risk if defaults are too aggressive; rollback by restoring previous global timeout/default behavior.
- Dependencies: None.

**PR2.2 — Partial-failure result contract**
- Objective: Standardize partial-failure payloads so callers always receive data + failure metadata in a consistent shape.
- Scope: `services/multi_db_fetcher.py`, `api/schemas.py`, `tests/services/`, `tests/api/`.
- Acceptance criteria:
  - Aggregated response includes `results`, `failures`, and backend status metadata.
  - API and CLI callers can distinguish complete failure vs partial success without string parsing.
  - Contract tests verify backward-compatible fields for existing consumers.
- Risk/rollback: Medium compatibility risk; rollback by preserving old response fields and gating new structure behind an opt-in flag.
- Dependencies: PR2.1 must land before PR2.2.

### Phase 3 — API and CLI productization

#### Next PR queue

**PR3.1 — API error semantics normalization**
- Objective: Normalize API error payloads into a stable schema with code/message/details fields.
- Scope: `api/routes/`, `api/errors.py`, `api/schemas.py`, `tests/api/`.
- Acceptance criteria:
  - Validation, dependency, and execution errors map to documented status codes.
  - Error bodies include machine-readable `code` and human-readable `message`.
  - API tests assert schema shape and status mapping for representative failure paths.
- Risk/rollback: Medium client-impact risk; rollback by keeping legacy error envelope available behind compatibility mode.
- Dependencies: None.

**PR3.2 — CLI golden-path and failure-path integration tests**
- Objective: Add CLI integration coverage for successful runs and structured handling of partial/error responses.
- Scope: `cli/commands/`, `tests/cli/`, `tests/fixtures/`.
- Acceptance criteria:
  - Golden-path tests validate expected table/output artifacts for representative queries.
  - Failure-path tests verify exit codes and stderr formatting for validation and backend errors.
  - CI executes CLI integration tests in a dedicated stage.
- Risk/rollback: Low-medium CI runtime risk; rollback by splitting heavy scenarios into smoke + nightly tiers.
- Dependencies: PR3.1 must land before PR3.2.

### Phase 4 — Data export and post-processing workflow

#### Next PR queue

**PR4.1 — Export profile configuration**
- Objective: Introduce named export profiles for output format, delimiter, and file naming defaults.
- Scope: `storage/export_config.py`, `cli/commands/export.py`, `docs/` config reference, `tests/storage/`.
- Acceptance criteria:
  - Users can select a profile by name via CLI/API configuration.
  - Profile settings override defaults predictably with clear precedence rules.
  - Unit tests validate profile parsing and invalid profile handling.
- Risk/rollback: Low risk; rollback by defaulting to current static export settings when profile lookup fails.
- Dependencies: None.

**PR4.2 — Column typing/order and metadata guarantees**
- Objective: Lock deterministic export column order/types and attach dataset metadata in output artifacts.
- Scope: `storage/exporters/csv_exporter.py`, `storage/exporters/excel_exporter.py`, `tests/storage/`.
- Acceptance criteria:
  - Exported columns follow explicit ordering rules independent of source backend order.
  - Numeric/date/string typing is preserved in both CSV and Excel outputs.
  - Metadata (generated timestamp, source backends, row count) is emitted and tested.
- Risk/rollback: Medium compatibility risk for downstream parsers; rollback by retaining legacy column ordering behind a config switch.
- Dependencies: PR4.1 must land before PR4.2.

### Phase 5 — Deployment and operations readiness

#### Next PR queue

**PR5.1 — Release checklist + versioning workflow docs**
- Objective: Define a repeatable release process with semantic version bump rules and verification gates.
- Scope: `docs/RELEASE_PROCESS.md`, `.github/` release workflow docs/config, `docs/IMPLEMENTATION_ROADMAP.md` cross-links.
- Acceptance criteria:
  - Release checklist includes pre-release tests, changelog update, tag creation, and rollback steps.
  - Semantic version policy is documented with concrete examples for patch/minor/major changes.
  - Maintainers can run the documented process without undocumented manual steps.
- Risk/rollback: Low risk (docs/process); rollback by reverting workflow doc changes and using current ad-hoc release flow.
- Dependencies: None.

**PR5.2 — Environment templates and container hardening baseline**
- Objective: Add hardened runtime defaults and environment templates for dev/staging/prod parity.
- Scope: `Dockerfile`, `docker-compose*.yml`, `.env.example` (or environment template files), `docs/deployment/`.
- Acceptance criteria:
  - Container runs as non-root with least-privilege defaults.
  - Environment templates enumerate required variables with safe defaults/placeholders.
  - Deployment docs cover startup, health checks, and rollback for each environment tier.
- Risk/rollback: Medium deployment risk; rollback by publishing hardened image as optional tag while retaining prior runtime image.
- Dependencies: PR5.1 should land before PR5.2 to align with release gating.

## Feature-by-feature execution policy

For each new feature merged into the repository:
1. Add or update tests (unit and/or integration) for both success and failure paths.
2. Wire tests into CI so feature behavior is enforced on pull requests.
3. Document rollout status in this roadmap table by updating `Status` and `Next step`.

## Tracking cadence

- Update roadmap on every feature PR.
- Re-evaluate priorities at least once per sprint.
- Promote any blocked phase with explicit blocker notes and mitigation steps.

## Evidence checked

- `.github/workflows/ci.yml`
- `pytest.ini`
- `requirements.txt`
- `tests/test_smoke.py`
