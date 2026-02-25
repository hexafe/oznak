# Oznak Implementation Roadmap

_Last updated: 2026-02-25_

## Repository review summary

Current strengths:
- Core modular structure is already present (`db`, `query`, `services`, `cli`, `api`, `storage`).
- Unit and smoke tests exist for query and multi-database fetcher flows.

Current gaps identified:
- No CI pipeline in repository to run tests automatically.
- Test discovery/import requires path setup (`src` is not on `PYTHONPATH` by default).
- Project backlog exists (`TODO`) but no structured phased delivery plan with ownership-ready next actions.

## Phase plan

| Phase | Scope | Status | Next step |
|---|---|---|---|
| Phase 0 | Baseline quality guardrails (CI + reliable test invocation) | ✅ Completed | Extend CI with linting (`ruff`) and formatting checks once tooling is added to dependencies. |
| Phase 1 | Query subsystem hardening (filter parsing, SQL safety, edge-case behavior) | 🟡 Planned | Add typed validation layer and expand test matrix for malformed filters and unsupported operators. |
| Phase 2 | Multi-database orchestration resilience (timeouts, partial failures, observability) | 🟡 Planned | Introduce structured logging and per-database execution metrics surfaced in CLI/API output. |
| Phase 3 | API and CLI productization (stable contracts, error semantics, UX polish) | 🟡 Planned | Publish explicit API schema examples and add CLI golden-path + failure-path integration tests. |
| Phase 4 | Data export and post-processing workflow (CSV/Excel ergonomics, metadata, extensibility) | 🟡 Planned | Add export profile configuration and tests for column typing, ordering, and file naming strategy. |
| Phase 5 | Deployment and operations readiness (container hardening, env templates, release process) | 🟡 Planned | Add release checklist, semantic version workflow, and deployment docs for dev/staging/prod environments. |

## Feature-by-feature execution policy

For each new feature merged into the repository:
1. Add or update tests (unit and/or integration) for both success and failure paths.
2. Wire tests into CI so feature behavior is enforced on pull requests.
3. Document rollout status in this roadmap table by updating `Status` and `Next step`.

## Tracking cadence

- Update roadmap on every feature PR.
- Re-evaluate priorities at least once per sprint.
- Promote any blocked phase with explicit blocker notes and mitigation steps.
