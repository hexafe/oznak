import tomllib
from pathlib import Path

import oznak

ROOT = Path(__file__).resolve().parents[1]
PYPROJECT = ROOT / "pyproject.toml"
RELEASE_PROCESS = ROOT / "docs" / "RELEASE_PROCESS.md"
TEST_CHECKLIST = ROOT / "docs" / "TEST_VERIFICATION_CHECKLIST.md"
THIRD_PARTY_NOTICES = ROOT / "THIRD_PARTY_NOTICES.md"
LIVE_DB_INTEGRATION = ROOT / "docs" / "LIVE_DB_INTEGRATION.md"
SECURITY_PERFORMANCE = ROOT / "docs" / "SECURITY_PERFORMANCE.md"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8").lower()


def test_release_process_includes_required_gates() -> None:
    text = _read(RELEASE_PROCESS)

    assert "python -m pytest -q" in text
    assert "python -m ruff check ." in text
    assert "python -m compileall -q -x '^\\./\\.git/' ." in text
    assert "python -m build --sdist --wheel" in text
    assert "oznak version" in text
    assert "oznak benchmark-chunked" in text


def test_release_docs_require_opt_in_live_db_tests() -> None:
    process_text = _read(RELEASE_PROCESS)
    checklist_text = _read(TEST_CHECKLIST)
    live_db_text = _read(LIVE_DB_INTEGRATION)

    assert "@pytest.mark.integration" in process_text
    assert "live database tests are optional and opt-in only" in process_text
    assert "@pytest.mark.integration" in checklist_text
    assert "live db tests are optional and never part of default release gating" in checklist_text
    assert "oznak_run_live_db_tests=1" in live_db_text
    assert "no-db benchmark" in process_text
    assert "no-db benchmark" in checklist_text
    assert "docker compose -f docker-compose.integration.yml up -d" in process_text
    assert "oznak_run_live_db_tests=1 python -m pytest -q -m integration" in checklist_text


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


def test_release_docs_cover_version_tags_and_driver_prerequisites() -> None:
    process_text = _read(RELEASE_PROCESS)
    checklist_text = _read(TEST_CHECKLIST)

    combined = f"{process_text}\n{checklist_text}"
    assert "vx.y.z" in combined
    assert "vx.y.zrcn" in combined
    assert "oznak.__version__" in combined
    assert "microsoft odbc driver 17 for sql server" in combined
    assert "compatible `pyodbc` driver" in combined


def test_security_performance_notes_cover_core_controls() -> None:
    text = _read(SECURITY_PERFORMANCE)

    assert "bound sql parameters" in text
    assert "allowed_columns" in text
    assert "order_by_enabled: false" in text
    assert "max_workers" in text
    assert "fetch_records_chunked" in text


def test_package_version_matches_pyproject() -> None:
    raw_pyproject = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))

    assert raw_pyproject["project"]["version"] == oznak.__version__


def test_third_party_notices_cover_runtime_dependencies() -> None:
    raw_pyproject = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))
    notices = _read(THIRD_PARTY_NOTICES)

    for dependency in raw_pyproject["project"]["dependencies"]:
        package_name = (
            dependency.split("[", 1)[0]
            .split(";", 1)[0]
            .split("=", 1)[0]
            .split("<", 1)[0]
            .split(">", 1)[0]
            .strip()
        )
        assert package_name.lower() in notices

    assert "mysql-connector-python" in notices
    assert "gplv2 with foss license exception" in notices
    assert "pyarrow" in notices


def test_third_party_notices_are_included_as_license_files() -> None:
    raw_pyproject = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))

    assert "THIRD_PARTY_NOTICES.md" in raw_pyproject["project"]["license-files"]
