from __future__ import annotations

import pandas as pd
import pytest
from typer.testing import CliRunner

from oznak import __version__
from oznak.cli import app
from oznak.diagnostics import SourceFetchDiagnostics, SourceFetchStatus
from oznak.profiles import DatabaseProfile
from oznak.result import FetchResult

runner = CliRunner()


def _profile(alias: str = "db_a") -> DatabaseProfile:
    return DatabaseProfile(
        alias=alias,
        dialect="mysql",
        host="db.example.invalid",
        port=3306,
        database="assembly",
        table="records",
        allowed_columns=("status", "updated_at", "refname"),
        timestamp_column="updated_at",
    )


@pytest.mark.cli_integration
def test_version_command_prints_package_version() -> None:
    result = runner.invoke(app, ["version"])
    assert result.exit_code == 0
    assert result.stdout.strip() == __version__


def test_profiles_command_loads_yaml_and_prints_aliases_and_dialects(tmp_path) -> None:
    config_path = tmp_path / "databases.yaml"
    config_path.write_text(
        """
databases:
  alpha:
    type: mysql
    host: alpha.internal.example
    port: 3306
    database: process_a
    table: records
  beta:
    type: mssql
    host: beta.internal.example
    port: 1433
    database: process_b
    table: records
""".strip(),
        encoding="utf-8",
    )

    result = runner.invoke(app, ["profiles", "--config", str(config_path)])

    assert result.exit_code == 0
    lines = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    assert lines == ["alpha\tmysql", "beta\tmssql"]
    assert "internal.example" not in result.stdout


@pytest.mark.cli_integration
def test_load_command_exports_csv(monkeypatch, tmp_path) -> None:
    out_path = tmp_path / "result.csv"
    loaded_profiles = {"db_a": _profile("db_a")}

    monkeypatch.setattr("oznak.cli.load_database_profiles", lambda _config: loaded_profiles)

    def fake_fetch_records(request, credential_provider=None):
        assert [profile.alias for profile in request.profiles] == ["db_a"]
        assert request.columns == ("refname",)
        assert request.limit == 10
        assert request.date_column == "updated_at"
        assert len(request.filters) == 1
        assert request.filters[0].column == "status"
        assert request.filters[0].operator.value == "="
        assert request.filters[0].value == "ACTIVE"
        assert credential_provider is not None
        return FetchResult(
            data=pd.DataFrame([{"refname": "A1", "status": "ACTIVE"}]),
            source_results=(
                SourceFetchDiagnostics(
                    source_alias="db_a",
                    status=SourceFetchStatus.SUCCESS,
                    row_count=1,
                ),
            ),
        )

    monkeypatch.setattr("oznak.cli.fetch_records", fake_fetch_records)

    result = runner.invoke(
        app,
        [
            "load",
            "db_a",
            "--config",
            "unused.yaml",
            "--select-columns",
            "refname",
            "--filter",
            "status = ACTIVE",
            "--last",
            "10",
            "--date-col",
            "updated_at",
            "--out",
            str(out_path),
        ],
    )

    assert result.exit_code == 0
    assert out_path.exists()
    assert pd.read_csv(out_path).to_dict(orient="records") == [{"refname": "A1", "status": "ACTIVE"}]


def test_load_command_can_disable_server_order_by(monkeypatch, tmp_path) -> None:
    out_path = tmp_path / "result.csv"
    monkeypatch.setattr("oznak.cli.load_database_profiles", lambda _config: {"db_a": _profile("db_a")})

    def fake_fetch_records(request, credential_provider=None):
        assert request.order_by_enabled is False
        return FetchResult(
            data=pd.DataFrame([{"refname": "A1"}]),
            source_results=(
                SourceFetchDiagnostics(
                    source_alias="db_a",
                    status=SourceFetchStatus.SUCCESS,
                    row_count=1,
                ),
            ),
        )

    monkeypatch.setattr("oznak.cli.fetch_records", fake_fetch_records)

    result = runner.invoke(
        app,
        [
            "load",
            "db_a",
            "--config",
            "unused.yaml",
            "--last",
            "10",
            "--no-order-by",
            "--out",
            str(out_path),
        ],
    )

    assert result.exit_code == 0


def test_load_command_passes_max_workers_and_export_profile(monkeypatch, tmp_path) -> None:
    out_path = tmp_path / "result.csv"
    config_path = tmp_path / "databases.yaml"
    config_path.write_text("databases: {}\nexport_profiles:\n  semicolon:\n    format: csv\n    delimiter: ';'\n", encoding="utf-8")
    monkeypatch.setattr("oznak.cli.load_database_profiles", lambda _config: {"db_a": _profile("db_a")})
    captured: dict[str, object] = {}

    def fake_fetch_records(request, credential_provider=None, max_workers=None):
        captured["max_workers"] = max_workers
        return FetchResult(
            data=pd.DataFrame([{"refname": "A1", "status": "ACTIVE"}]),
            source_results=(
                SourceFetchDiagnostics(
                    source_alias="db_a",
                    status=SourceFetchStatus.SUCCESS,
                    row_count=1,
                ),
            ),
        )

    monkeypatch.setattr("oznak.cli.fetch_records", fake_fetch_records)

    result = runner.invoke(
        app,
        [
            "load",
            "db_a",
            "--config",
            str(config_path),
            "--max-workers",
            "2",
            "--export-profile",
            "semicolon",
            "--out",
            str(out_path),
        ],
    )

    assert result.exit_code == 0
    assert captured["max_workers"] == 2
    assert out_path.read_text(encoding="utf-8").splitlines()[0] == "refname;status"


@pytest.mark.cli_integration
def test_load_chunked_command_streams_export(monkeypatch, tmp_path) -> None:
    out_path = tmp_path / "chunked.csv"
    monkeypatch.setattr("oznak.cli.load_database_profiles", lambda _config: {"db_a": _profile("db_a")})

    from oznak.chunked import ChunkedFetchEvent

    def fake_iter_records_chunked(request, **kwargs):
        assert kwargs["chunk_size"] == 50
        assert kwargs["max_workers"] == 2
        yield ChunkedFetchEvent(
            source_alias="db_a",
            frame=pd.DataFrame([{"refname": "A1", "source_database": "db_a"}]),
        )
        yield ChunkedFetchEvent(
            source_alias="db_a",
            diagnostics=SourceFetchDiagnostics(
                source_alias="db_a",
                status=SourceFetchStatus.SUCCESS,
                row_count=1,
            ),
        )

    monkeypatch.setattr("oznak.cli.iter_records_chunked", fake_iter_records_chunked)

    result = runner.invoke(
        app,
        [
            "load-chunked",
            "db_a",
            "--chunk-size",
            "50",
            "--max-workers",
            "2",
            "--out",
            str(out_path),
        ],
    )

    assert result.exit_code == 0
    assert pd.read_csv(out_path).to_dict(orient="records") == [
        {"refname": "A1", "source_database": "db_a"}
    ]


def test_benchmark_chunked_command_reports_synthetic_runs() -> None:
    result = runner.invoke(
        app,
        [
            "benchmark-chunked",
            "--sources",
            "2",
            "--rows-per-source",
            "5",
            "--chunk-size",
            "2",
            "--workers",
            "1",
            "--workers",
            "2",
        ],
    )

    assert result.exit_code == 0
    assert "workers\tseconds\trows\tchunks\tqueries\trows_per_second\texport" in result.stdout
    assert "\t10\t6\t8\t" in result.stdout


def test_benchmark_chunked_command_can_export_csv(tmp_path) -> None:
    result = runner.invoke(
        app,
        [
            "benchmark-chunked",
            "--sources",
            "1",
            "--rows-per-source",
            "3",
            "--chunk-size",
            "2",
            "--workers",
            "1",
            "--export-dir",
            str(tmp_path),
        ],
    )

    assert result.exit_code == 0
    out_path = tmp_path / "synthetic-chunked-w1.csv"
    assert out_path.exists()
    assert pd.read_csv(out_path)["id"].tolist() == [1, 2, 3]


def test_load_command_rejects_unsafe_is_filter(monkeypatch) -> None:
    monkeypatch.setattr("oznak.cli.load_database_profiles", lambda _config: {"db_a": _profile("db_a")})

    called = {"fetch": False}

    def fake_fetch_records(request, credential_provider=None):
        called["fetch"] = True
        return FetchResult()

    monkeypatch.setattr("oznak.cli.fetch_records", fake_fetch_records)

    result = runner.invoke(
        app,
        [
            "load",
            "db_a",
            "--config",
            "unused.yaml",
            "--filter",
            "deleted_at IS TRUE",
        ],
    )

    assert result.exit_code == 1
    assert "only supports NULL" in result.stderr
    assert called["fetch"] is False


def test_load_command_returns_nonzero_on_fetch_failure(monkeypatch) -> None:
    monkeypatch.setattr("oznak.cli.load_database_profiles", lambda _config: {"db_a": _profile("db_a")})

    def fake_fetch_records(request, credential_provider=None):
        return FetchResult(
            data=pd.DataFrame(),
            source_results=(
                SourceFetchDiagnostics(
                    source_alias="db_a",
                    status=SourceFetchStatus.FAILED,
                    row_count=0,
                    error_code="query_error",
                    message="query failed",
                ),
            ),
            errors=("Source 'db_a' query failed",),
        )

    monkeypatch.setattr("oznak.cli.fetch_records", fake_fetch_records)

    result = runner.invoke(
        app,
        [
            "load",
            "db_a",
            "--config",
            "unused.yaml",
            "--filter",
            "status = ACTIVE",
        ],
    )

    assert result.exit_code == 1
    assert "Fetch error: Source 'db_a' query failed" in result.stderr


@pytest.mark.cli_integration
def test_tui_command_delegates_to_package_tui(monkeypatch) -> None:
    called: dict[str, str] = {}

    def fake_run_tui(*, config_path: str, prompt=input, output=print) -> int:
        called["config_path"] = config_path
        assert callable(prompt)
        assert callable(output)
        return 0

    monkeypatch.setattr("oznak.cli.run_tui", fake_run_tui)

    result = runner.invoke(app, ["tui", "--config", "cfg.yaml"])

    assert result.exit_code == 0
    assert called["config_path"] == "cfg.yaml"
