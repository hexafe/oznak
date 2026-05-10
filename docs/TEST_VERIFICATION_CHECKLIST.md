# Oznak Test Verification Checklist

_Last updated: 2026-05-10_

## 1. Required release gates

- Run `python -m pytest -q`
- Run `python -m ruff check .`
- Run `python -m compileall -q -x '^\./\.git/' .`
- Run `python -m build --sdist --wheel`
- Expected result: all commands pass with no local exceptions

## 2. CLI entrypoint verification

- Run `oznak version`
- Run `oznak profiles --config config/databases.yaml`
- Run `oznak load --help`
- Run `oznak tui --help`
- Run `pytest -q -m cli_integration`
- Expected result: all commands render help successfully
- Expected result: CLI integration tests pass in isolation

## 3. Optional live DB integration verification (opt-in)

- Live DB tests are optional and never part of default release gating.
- Mark live DB tests with `@pytest.mark.integration`.
- Run only when explicitly requested:
  `python -m pytest -q -m integration`

## 4. Config and data hygiene verification

- Verify no real plant/production data exists in repository files.
- Verify no real credentials, secrets, or connection strings are committed.
- Verify diagnostics and examples redact credentials and host details.
- Verify config files committed to git are template-safe only.

## 5. Release documentation and branch state

- Confirm `README.md` matches current CLI/API behavior.
- Confirm `docs/RELEASE_PROCESS.md` and this checklist are current.
- Confirm branch is clean before merge or push
