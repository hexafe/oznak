# Oznak Release Process

_Last updated: 2026-05-10_

This process is required before tagging or publishing a release.

## 1. Required quality gates

Run all required gates from the repository root:

- `python -m pytest -q`
- `python -m ruff check .`
- `python -m compileall -q -x '^\./\.git/' .`
- `python -m build --sdist --wheel`

All commands must pass without local workarounds.

## 2. CLI smoke gate

Run a minimal CLI smoke pass:

- `oznak version`
- `oznak profiles --config config/databases.yaml`
- `oznak load --help`
- `oznak tui --help`

## 3. Integration-test policy

Live database tests are optional and opt-in only. They must be marked with
`@pytest.mark.integration` and excluded from the default release gate unless the
release owner explicitly enables them.

Example explicit run:

- `python -m pytest -q -m integration`

## 4. Configuration and data hygiene gate

Before release:

- Confirm there is no real production data in the repository.
- Confirm there are no real credentials, secrets, or connection strings.
- Keep `.env` and other credential-bearing files out of commits.
- Keep public config as template-only inputs.
- Redact credentials and host details in diagnostics and examples.

## 5. Release checklist handoff

Use `docs/TEST_VERIFICATION_CHECKLIST.md` to record the exact verification run
for the release candidate.
