from __future__ import annotations

from pathlib import Path

import pandas as pd

from oznak.credentials import EnvironmentCredentialProvider
from oznak.diagnostics import SourceFetchDiagnostics, SourceFetchStatus
from oznak.result import FetchResult
from oznak.runtime import CancellationToken
from oznak.tui import run_tui


def _write_config(path: Path) -> None:
    path.write_text(
        """
databases:
  db_a:
    type: mysql
    host: alpha.internal.example
    port: 3306
    database: assembly
    table: records
    allowed_columns:
      - refname
      - status
      - updated_at
""".strip(),
        encoding="utf-8",
    )


def test_run_tui_fetches_and_exports_csv(monkeypatch, tmp_path) -> None:
    config_path = tmp_path / "databases.yaml"
    out_path = tmp_path / "out.csv"
    _write_config(config_path)

    answers = iter(
        [
            "1",
            "refname,status",
            "status = ACTIVE",
            "",
            "5",
            "updated_at",
            "",
            str(out_path),
            "y",
        ]
    )
    messages: list[str] = []

    def fake_fetch_records(
        request,
        credential_provider=None,
        cancellation_token=None,
        progress_callback=None,
        **kwargs,
    ):
        assert kwargs == {}
        assert [profile.alias for profile in request.profiles] == ["db_a"]
        assert request.columns == ("refname", "status")
        assert request.limit == 5
        assert request.date_column == "updated_at"
        assert request.order_by_enabled is True
        assert len(request.filters) == 1
        assert request.filters[0].column == "status"
        assert request.filters[0].operator.value == "="
        assert request.filters[0].value == "ACTIVE"
        assert isinstance(credential_provider, EnvironmentCredentialProvider)
        assert isinstance(cancellation_token, CancellationToken)
        assert progress_callback is not None
        progress_callback(
            SourceFetchDiagnostics(
                source_alias="db_a",
                status=SourceFetchStatus.SUCCESS,
                row_count=1,
                elapsed_seconds=0.12,
                message="Fetched 1 rows from source 'db_a'",
            )
        )
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

    monkeypatch.setattr("oznak.tui.fetch_records", fake_fetch_records)

    exit_code = run_tui(
        config_path=str(config_path),
        prompt=lambda _question: next(answers),
        output=messages.append,
    )

    assert exit_code == 0
    assert out_path.exists()
    assert pd.read_csv(out_path).to_dict(orient="records") == [{"refname": "A1", "status": "ACTIVE"}]
    assert any("[success] db_a rows=1 elapsed=0.12s" in msg for msg in messages)


def test_run_tui_aborts_before_fetch(monkeypatch, tmp_path) -> None:
    config_path = tmp_path / "databases.yaml"
    _write_config(config_path)

    answers = iter(
        [
            "db_a",
            "",
            "",
            "",
            "output.csv",
            "n",
        ]
    )
    called = {"fetch": False}
    messages: list[str] = []

    def fake_fetch_records(*_args, **_kwargs):
        called["fetch"] = True
        return FetchResult()

    monkeypatch.setattr("oznak.tui.fetch_records", fake_fetch_records)

    exit_code = run_tui(
        config_path=str(config_path),
        prompt=lambda _question: next(answers),
        output=messages.append,
    )

    assert exit_code == 1
    assert called["fetch"] is False
    assert any("Aborted by user" in msg for msg in messages)


def test_run_tui_can_disable_server_order_by(monkeypatch, tmp_path) -> None:
    config_path = tmp_path / "databases.yaml"
    out_path = tmp_path / "out.csv"
    _write_config(config_path)

    answers = iter(
        [
            "db_a",
            "",
            "",
            "5",
            "updated_at",
            "n",
            str(out_path),
            "y",
        ]
    )

    def fake_fetch_records(
        request,
        credential_provider=None,
        cancellation_token=None,
        progress_callback=None,
        **kwargs,
    ):
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

    monkeypatch.setattr("oznak.tui.fetch_records", fake_fetch_records)

    exit_code = run_tui(
        config_path=str(config_path),
        prompt=lambda _question: next(answers),
        output=lambda _message: None,
    )

    assert exit_code == 0
