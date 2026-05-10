from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RELEASE_PROCESS = ROOT / "docs" / "RELEASE_PROCESS.md"
TEST_CHECKLIST = ROOT / "docs" / "TEST_VERIFICATION_CHECKLIST.md"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8").lower()


def test_release_process_includes_required_gates() -> None:
    text = _read(RELEASE_PROCESS)

    assert "python -m pytest -q" in text
    assert "python -m ruff check ." in text
    assert "python -m compileall -q -x '^\\./\\.git/' ." in text
    assert "python -m build --sdist --wheel" in text
    assert "oznak version" in text


def test_release_docs_require_opt_in_live_db_tests() -> None:
    process_text = _read(RELEASE_PROCESS)
    checklist_text = _read(TEST_CHECKLIST)

    assert "@pytest.mark.integration" in process_text
    assert "live database tests are optional and opt-in only" in process_text
    assert "@pytest.mark.integration" in checklist_text
    assert "live db tests are optional and never part of default release gating" in checklist_text


def test_release_docs_require_no_real_data_or_credentials() -> None:
    process_text = _read(RELEASE_PROCESS)
    checklist_text = _read(TEST_CHECKLIST)

    for required in (
        "no real production data",
        "no real credentials",
        "no real plant/production data",
        "no real credentials, secrets, or connection strings",
    ):
        assert required in f"{process_text}\n{checklist_text}"
