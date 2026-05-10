from __future__ import annotations

import pandas as pd
import pytest

from oznak.chunked import fetch_records_chunked
from oznak.diagnostics import SourceFetchStatus
from oznak.errors import OznakValidationError
from oznak.profiles import DatabaseProfile
from oznak.result import FetchRequest
from oznak.runtime import CancellationToken


def _profile(alias: str, *, pagination_column: str = "id") -> DatabaseProfile:
    return DatabaseProfile(
        alias=alias,
        dialect="mysql",
        host=f"{alias}.db.example.invalid",
        port=3306,
        database="process_db",
        table="records",
        allowed_columns=("id", "value", "updated_at"),
        pagination_column=pagination_column,
    )


def _request(*profiles: DatabaseProfile, columns: tuple[str, ...] = ("id", "value")) -> FetchRequest:
    return FetchRequest(profiles=tuple(profiles), columns=columns)


def test_fetch_records_chunked_multi_chunk_success_in_deterministic_order():
    profile_a = _profile("alpha")
    profile_b = _profile("beta")
    per_engine_call_index: dict[str, int] = {}
    read_calls: list[tuple[str, str, dict[str, object] | None]] = []

    def fake_engine_factory(profile: DatabaseProfile, credentials: object | None) -> str:
        return profile.alias

    def fake_read_sql(sql: str, engine: str, params: dict[str, object] | None = None) -> pd.DataFrame:
        read_calls.append((sql, engine, params))
        call_idx = per_engine_call_index.get(engine, 0)
        per_engine_call_index[engine] = call_idx + 1

        if engine == "alpha":
            if call_idx == 0:
                assert params is None
                return pd.DataFrame([{"id": 1, "value": "A1"}, {"id": 2, "value": "A2"}])
            if call_idx == 1:
                assert params == {"pagination_param": 2}
                return pd.DataFrame([{"id": 3, "value": "A3"}])
            assert params == {"pagination_param": 3}
            return pd.DataFrame(columns=["id", "value"])

        if call_idx == 0:
            assert params is None
            return pd.DataFrame([{"id": 10, "value": "B1"}])
        assert params == {"pagination_param": 10}
        return pd.DataFrame(columns=["id", "value"])

    result = fetch_records_chunked(
        _request(profile_a, profile_b),
        chunk_size=2,
        engine_factory=fake_engine_factory,
        read_sql=fake_read_sql,
    )

    assert [item.status for item in result.source_results] == [SourceFetchStatus.SUCCESS, SourceFetchStatus.SUCCESS]
    assert [item.source_alias for item in result.source_results] == ["alpha", "beta"]
    assert [item.row_count for item in result.source_results] == [3, 1]
    assert result.errors == ()
    assert result.warnings == ()
    assert result.data["source_alias"].tolist() == ["alpha", "alpha", "alpha", "beta"]
    assert result.data["source_database"].tolist() == ["alpha", "alpha", "alpha", "beta"]
    assert result.data["id"].tolist() == [1, 2, 3, 10]
    assert len(read_calls) == 5
    assert [item.metadata["chunk_count"] for item in result.source_results] == [2, 1]


def test_fetch_records_chunked_marks_no_rows():
    profile = _profile("alpha")

    def fake_engine_factory(profile: DatabaseProfile, credentials: object | None) -> str:
        return "engine"

    def fake_read_sql(sql: str, engine: str, params: dict[str, object] | None = None) -> pd.DataFrame:
        return pd.DataFrame(columns=["id", "value"])

    result = fetch_records_chunked(
        _request(profile),
        chunk_size=100,
        engine_factory=fake_engine_factory,
        read_sql=fake_read_sql,
    )

    assert result.source_results[0].status is SourceFetchStatus.NO_ROWS
    assert result.source_results[0].row_count == 0
    assert result.warnings == ("Source 'alpha' returned no rows",)
    assert result.errors == ()
    assert result.data.empty


def test_fetch_records_chunked_failure_after_partial_chunks_keeps_partial_data():
    profile = _profile("alpha")
    calls = {"count": 0}

    def fake_engine_factory(profile: DatabaseProfile, credentials: object | None) -> str:
        return "alpha"

    def fake_read_sql(sql: str, engine: str, params: dict[str, object] | None = None) -> pd.DataFrame:
        if calls["count"] == 0:
            calls["count"] += 1
            return pd.DataFrame([{"id": 1, "value": "A1"}, {"id": 2, "value": "A2"}])
        raise RuntimeError("chunk read failed")

    result = fetch_records_chunked(
        _request(profile),
        chunk_size=100,
        engine_factory=fake_engine_factory,
        read_sql=fake_read_sql,
    )

    assert result.source_results[0].status is SourceFetchStatus.FAILED
    assert result.source_results[0].error_code == "query_error"
    assert result.source_results[0].row_count == 2
    assert result.data["id"].tolist() == [1, 2]
    assert result.data["source_alias"].tolist() == ["alpha", "alpha"]
    assert len(result.errors) == 1
    assert result.source_results[0].metadata["chunk_count"] == 1


def test_fetch_records_chunked_fails_when_chunk_lacks_pagination_column():
    profile = _profile("alpha")

    def fake_engine_factory(profile: DatabaseProfile, credentials: object | None) -> str:
        return "alpha"

    def fake_read_sql(sql: str, engine: str, params: dict[str, object] | None = None) -> pd.DataFrame:
        return pd.DataFrame([{"value": "A1"}])

    result = fetch_records_chunked(
        _request(profile, columns=("value",)),
        chunk_size=100,
        engine_factory=fake_engine_factory,
        read_sql=fake_read_sql,
    )

    assert result.source_results[0].status is SourceFetchStatus.FAILED
    assert result.source_results[0].error_code == "query_error"
    assert "missing pagination column 'id'" in result.errors[0]
    assert result.data.empty


def test_fetch_records_chunked_fails_when_chunk_has_non_unique_pagination_values():
    profile = _profile("alpha")

    def fake_engine_factory(profile: DatabaseProfile, credentials: object | None) -> str:
        return "alpha"

    def fake_read_sql(sql: str, engine: str, params: dict[str, object] | None = None) -> pd.DataFrame:
        return pd.DataFrame([{"id": 5, "value": "A1"}, {"id": 5, "value": "A2"}])

    result = fetch_records_chunked(
        _request(profile),
        chunk_size=100,
        engine_factory=fake_engine_factory,
        read_sql=fake_read_sql,
    )

    assert result.source_results[0].status is SourceFetchStatus.FAILED
    assert result.source_results[0].error_code == "query_error"
    assert "non-unique pagination values" in result.errors[0]
    assert result.data.empty


def test_fetch_records_chunked_reports_cancellation_and_progress():
    profile = _profile("alpha")
    token = CancellationToken()
    progress_statuses: list[SourceFetchStatus] = []

    def fake_engine_factory(profile: DatabaseProfile, credentials: object | None) -> str:
        return "alpha"

    def fake_read_sql(sql: str, engine: str, params: dict[str, object] | None = None) -> pd.DataFrame:
        token.cancel()
        return pd.DataFrame([{"id": 1, "value": "A1"}])

    result = fetch_records_chunked(
        _request(profile),
        chunk_size=100,
        engine_factory=fake_engine_factory,
        read_sql=fake_read_sql,
        cancellation_token=token,
        progress_callback=lambda diagnostics: progress_statuses.append(diagnostics.status),
    )

    assert result.source_results[0].status is SourceFetchStatus.CANCELLED
    assert result.source_results[0].error_code == "execute_cancelled"
    assert progress_statuses == [SourceFetchStatus.CANCELLED]
    assert result.data.empty


def test_fetch_records_chunked_reports_timeout(monkeypatch):
    profile = _profile("alpha")
    times = iter([0.0, 2.0, 2.0])

    monkeypatch.setattr("oznak.chunked.perf_counter", lambda: next(times))

    def fake_engine_factory(profile: DatabaseProfile, credentials: object | None) -> str:
        return "alpha"

    def fake_read_sql(sql: str, engine: str, params: dict[str, object] | None = None) -> pd.DataFrame:
        return pd.DataFrame([{"id": 1, "value": "A1"}])

    result = fetch_records_chunked(
        FetchRequest(profiles=(profile,), columns=("id", "value"), timeout_seconds=1.0),
        chunk_size=100,
        engine_factory=fake_engine_factory,
        read_sql=fake_read_sql,
    )

    assert result.source_results[0].status is SourceFetchStatus.TIMEOUT
    assert result.source_results[0].error_code == "execute_timeout"
    assert result.data.empty


@pytest.mark.parametrize("bad_chunk_size", (0, -1, 1.5, "10", True))
def test_fetch_records_chunked_rejects_invalid_chunk_size(bad_chunk_size):
    with pytest.raises(OznakValidationError, match="chunk_size must be a positive integer"):
        fetch_records_chunked(_request(_profile("alpha")), chunk_size=bad_chunk_size)
