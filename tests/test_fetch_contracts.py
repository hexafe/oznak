from __future__ import annotations

from threading import Event, Lock

import pandas as pd
import pytest

from oznak.credentials import Credentials, MappingCredentialProvider
from oznak.diagnostics import SourceFetchStatus
from oznak.errors import OznakValidationError
from oznak.fetcher import fetch_records
from oznak.profiles import DatabaseProfile
from oznak.result import FetchRequest
from oznak.runtime import CancellationToken


def _profile(alias: str, *, allowed_columns: tuple[str, ...] = ("id", "updated_at", "value")) -> DatabaseProfile:
    return DatabaseProfile(
        alias=alias,
        dialect="mysql",
        host=f"{alias}.db.example.invalid",
        port=3306,
        database="process_db",
        table="records",
        allowed_columns=allowed_columns,
        timestamp_column="updated_at",
    )


def _request(*profiles: DatabaseProfile, **kwargs) -> FetchRequest:
    defaults = {
        "profiles": tuple(profiles),
        "columns": ("id", "value"),
        "limit": 10,
        "date_column": "updated_at",
    }
    defaults.update(kwargs)
    return FetchRequest(**defaults)


def test_fetch_records_success_concatenates_rows_in_profile_order():
    profile_a = _profile("alpha")
    profile_b = _profile("beta")
    provider = MappingCredentialProvider(
        {
            "alpha": Credentials("alpha_user", "alpha_secret"),
            "beta": Credentials("beta_user", "beta_secret"),
        }
    )
    read_calls: list[tuple[str, str, dict[str, object] | None]] = []

    def fake_engine_factory(profile: DatabaseProfile, credentials: Credentials | None) -> str:
        assert credentials is not None
        return f"engine:{profile.alias}:{credentials.username}"

    def fake_read_sql(sql: str, engine: str, params: dict[str, object] | None = None) -> pd.DataFrame:
        read_calls.append((sql, engine, params))
        if "alpha" in engine:
            return pd.DataFrame([{"id": 1, "value": "A1"}, {"id": 2, "value": "A2"}])
        return pd.DataFrame([{"id": 10, "value": "B1"}])

    result = fetch_records(
        _request(profile_a, profile_b),
        credential_provider=provider,
        engine_factory=fake_engine_factory,
        read_sql=fake_read_sql,
    )

    assert [item.status for item in result.source_results] == [SourceFetchStatus.SUCCESS, SourceFetchStatus.SUCCESS]
    assert [item.source_alias for item in result.source_results] == ["alpha", "beta"]
    assert result.errors == ()
    assert result.warnings == ()
    assert result.data["source_alias"].tolist() == ["alpha", "alpha", "beta"]
    assert result.data["source_database"].tolist() == ["alpha", "alpha", "beta"]
    assert len(read_calls) == 2
    assert [call[1] for call in read_calls] == ["engine:alpha:alpha_user", "engine:beta:beta_user"]
    assert all("<redacted-host>" in item.query_summary for item in result.source_results)


def test_fetch_records_can_disable_order_by_per_request():
    profile = _profile("alpha")
    read_calls: list[str] = []

    def fake_engine_factory(profile: DatabaseProfile, credentials: Credentials | None) -> str:
        return "engine"

    def fake_read_sql(sql: str, engine: str, params: dict[str, object] | None = None) -> pd.DataFrame:
        read_calls.append(sql)
        return pd.DataFrame([{"id": 1, "value": "A1"}])

    result = fetch_records(
        _request(profile, order_by_enabled=False),
        engine_factory=fake_engine_factory,
        read_sql=fake_read_sql,
    )

    assert result.errors == ()
    assert result.row_count == 1
    assert read_calls == ["SELECT `id`, `value` FROM `records` LIMIT 10"]


def test_fetch_records_marks_no_rows_as_no_rows_with_warning():
    profile = _profile("alpha")

    def fake_engine_factory(profile: DatabaseProfile, credentials: Credentials | None) -> str:
        return "engine"

    def fake_read_sql(sql: str, engine: str, params: dict[str, object] | None = None) -> pd.DataFrame:
        return pd.DataFrame(columns=["id", "value"])

    result = fetch_records(_request(profile), engine_factory=fake_engine_factory, read_sql=fake_read_sql)

    assert result.source_results[0].status is SourceFetchStatus.NO_ROWS
    assert result.source_results[0].row_count == 0
    assert result.warnings == ("Source 'alpha' returned no rows",)
    assert result.errors == ()
    assert result.data.empty


def test_fetch_records_partial_success_when_one_source_fails():
    profile_a = _profile("alpha")
    profile_b = _profile("beta")
    provider = MappingCredentialProvider({"alpha": ("user", "secret"), "beta": ("user", "secret")})

    def fake_engine_factory(profile: DatabaseProfile, credentials: Credentials | None) -> str:
        return profile.alias

    def fake_read_sql(sql: str, engine: str, params: dict[str, object] | None = None) -> pd.DataFrame:
        if engine == "alpha":
            return pd.DataFrame([{"id": 7, "value": "ok"}])
        raise RuntimeError("read failed")

    result = fetch_records(
        _request(profile_a, profile_b),
        credential_provider=provider,
        engine_factory=fake_engine_factory,
        read_sql=fake_read_sql,
    )

    assert [item.status for item in result.source_results] == [SourceFetchStatus.SUCCESS, SourceFetchStatus.FAILED]
    assert result.partial_success is True
    assert result.data["source_alias"].tolist() == ["alpha"]
    assert len(result.errors) == 1
    assert result.source_results[1].error_code == "query_error"


def test_fetch_records_all_failed_returns_empty_data_and_errors():
    profile_a = _profile("alpha")
    profile_b = _profile("beta")
    provider = MappingCredentialProvider({"alpha": ("user", "secret"), "beta": ("user", "secret")})

    def fake_engine_factory(profile: DatabaseProfile, credentials: Credentials | None) -> str:
        return profile.alias

    def fake_read_sql(sql: str, engine: str, params: dict[str, object] | None = None) -> pd.DataFrame:
        raise RuntimeError(f"{engine} unavailable")

    result = fetch_records(
        _request(profile_a, profile_b),
        credential_provider=provider,
        engine_factory=fake_engine_factory,
        read_sql=fake_read_sql,
    )

    assert [item.status for item in result.source_results] == [SourceFetchStatus.FAILED, SourceFetchStatus.FAILED]
    assert result.partial_success is False
    assert result.data.empty
    assert len(result.errors) == 2


def test_fetch_records_compile_validation_failure_is_captured_per_source():
    profile = _profile("alpha", allowed_columns=("id", "value"))
    provider = MappingCredentialProvider({"alpha": ("user", "secret")})
    called = {"engine": False, "read": False}

    def fake_engine_factory(profile: DatabaseProfile, credentials: Credentials | None) -> str:
        called["engine"] = True
        return "engine"

    def fake_read_sql(sql: str, engine: str, params: dict[str, object] | None = None) -> pd.DataFrame:
        called["read"] = True
        return pd.DataFrame([{"id": 1, "value": "X"}])

    result = fetch_records(
        _request(profile),
        credential_provider=provider,
        engine_factory=fake_engine_factory,
        read_sql=fake_read_sql,
    )

    assert result.source_results[0].status is SourceFetchStatus.FAILED
    assert result.source_results[0].error_code == "compile_validation_error"
    assert len(result.errors) == 1
    assert called == {"engine": False, "read": False}


def test_fetch_records_reports_credential_and_engine_errors():
    profile = _profile("alpha")
    missing_provider = MappingCredentialProvider({})

    credential_failure = fetch_records(_request(profile), credential_provider=missing_provider, engine_factory=lambda p, c: "x")
    assert credential_failure.source_results[0].error_code == "credential_error"
    assert credential_failure.source_results[0].status is SourceFetchStatus.FAILED

    engine_failure = fetch_records(_request(profile), read_sql=lambda *a, **k: pd.DataFrame())
    assert engine_failure.source_results[0].error_code == "engine_error"
    assert engine_failure.source_results[0].status is SourceFetchStatus.FAILED
    assert "Missing credentials" in engine_failure.errors[0]


def test_fetch_records_default_engine_factory_uses_sqlalchemy_engine(monkeypatch):
    profile = _profile("alpha")
    provider = MappingCredentialProvider({"alpha": ("user", "secret")})
    created: list[tuple[str, str]] = []

    def fake_create_sqlalchemy_engine(profile: DatabaseProfile, credentials: Credentials | None) -> str:
        assert credentials is not None
        created.append((profile.alias, credentials.username))
        return "engine"

    def fake_read_sql(sql: str, engine: str, params: dict[str, object] | None = None) -> pd.DataFrame:
        return pd.DataFrame([{"id": 1, "value": "X"}])

    monkeypatch.setattr("oznak.fetcher.create_sqlalchemy_engine", fake_create_sqlalchemy_engine)

    result = fetch_records(
        _request(profile),
        credential_provider=provider,
        read_sql=fake_read_sql,
    )

    assert created == [("alpha", "user")]
    assert result.source_results[0].status is SourceFetchStatus.SUCCESS


def test_fetch_records_reports_cancellation_before_next_source():
    profile_a = _profile("alpha")
    profile_b = _profile("beta")
    token = CancellationToken()
    progress_statuses: list[SourceFetchStatus] = []

    def fake_engine_factory(profile: DatabaseProfile, credentials: Credentials | None) -> str:
        return profile.alias

    def fake_read_sql(sql: str, engine: str, params: dict[str, object] | None = None) -> pd.DataFrame:
        token.cancel()
        return pd.DataFrame([{"id": 1, "value": engine}])

    result = fetch_records(
        _request(profile_a, profile_b),
        engine_factory=fake_engine_factory,
        read_sql=fake_read_sql,
        cancellation_token=token,
        progress_callback=lambda diagnostics: progress_statuses.append(diagnostics.status),
    )

    assert [item.status for item in result.source_results] == [
        SourceFetchStatus.CANCELLED,
        SourceFetchStatus.CANCELLED,
    ]
    assert progress_statuses == [SourceFetchStatus.CANCELLED, SourceFetchStatus.CANCELLED]
    assert result.data.empty


def test_fetch_records_reports_timeout_after_source_exceeds_timeout(monkeypatch):
    profile = _profile("alpha")
    times = iter([0.0, 2.0, 2.0])

    monkeypatch.setattr("oznak.fetcher.perf_counter", lambda: next(times))

    def fake_engine_factory(profile: DatabaseProfile, credentials: Credentials | None) -> str:
        return "engine"

    def fake_read_sql(sql: str, engine: str, params: dict[str, object] | None = None) -> pd.DataFrame:
        return pd.DataFrame([{"id": 1, "value": "late"}])

    result = fetch_records(
        _request(profile, timeout_seconds=1.0),
        engine_factory=fake_engine_factory,
        read_sql=fake_read_sql,
    )

    assert result.source_results[0].status is SourceFetchStatus.TIMEOUT
    assert result.source_results[0].error_code == "execute_timeout"
    assert result.data.empty


def test_fetch_records_can_run_sources_with_bounded_parallelism():
    profile_a = _profile("alpha")
    profile_b = _profile("beta")
    active = 0
    peak_active = 0
    lock = Lock()
    first_reader_started = Event()
    release_readers = Event()

    def fake_engine_factory(profile: DatabaseProfile, credentials: Credentials | None) -> str:
        return profile.alias

    def fake_read_sql(sql: str, engine: str, params: dict[str, object] | None = None) -> pd.DataFrame:
        nonlocal active, peak_active
        with lock:
            active += 1
            peak_active = max(peak_active, active)
        if engine == "alpha":
            first_reader_started.set()
            assert release_readers.wait(timeout=2)
        else:
            assert first_reader_started.wait(timeout=2)
            release_readers.set()
        with lock:
            active -= 1
        return pd.DataFrame([{"id": 1, "value": engine}])

    result = fetch_records(
        _request(profile_a, profile_b),
        engine_factory=fake_engine_factory,
        read_sql=fake_read_sql,
        max_workers=2,
    )

    assert peak_active == 2
    assert result.data["source_alias"].tolist() == ["alpha", "beta"]


@pytest.mark.parametrize("bad_max_workers", (0, -1, 1.5, "2", True))
def test_fetch_records_rejects_invalid_max_workers(bad_max_workers):
    with pytest.raises(OznakValidationError, match="max_workers"):
        fetch_records(_request(_profile("alpha")), max_workers=bad_max_workers)
