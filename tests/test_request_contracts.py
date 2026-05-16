from __future__ import annotations

import pytest

from oznak.errors import OznakValidationError
from oznak.filters import QueryFilter
from oznak.profiles import DatabaseProfile
from oznak.request import QueryRequest


def _profile(alias: str = "db_a") -> DatabaseProfile:
    return DatabaseProfile(
        alias=alias,
        dialect="mysql",
        host="db.example.invalid",
        port=3306,
        database="assembly",
        table="records",
        allowed_columns=("RefName", "Status", "CreatedAt", "id"),
        timestamp_column="CreatedAt",
        pagination_column="id",
    )


def test_query_request_normalizes_legacy_cli_inputs() -> None:
    query_request = QueryRequest.from_inputs(
        databases="db_a, db_b",
        filters=["Status = ACTIVE", QueryFilter.is_null("CreatedAt")],
        select_columns="RefName,Status,RefName",
        last=10,
        date_col="CreatedAt",
        order_by_enabled=False,
        max_workers=2,
    )

    assert query_request.profile_aliases == ("db_a", "db_b")
    assert query_request.columns == ("RefName", "Status")
    assert [item.operator.value for item in query_request.filters] == ["=", "IS NULL"]
    assert query_request.limit == 10
    assert query_request.date_column == "CreatedAt"
    assert query_request.order_by_enabled is False
    assert query_request.max_workers == 2


def test_query_request_resolves_profiles_and_builds_fetch_request() -> None:
    query_request = QueryRequest.from_inputs(
        databases=["db_a"],
        filters=["Status = ACTIVE"],
        select_columns=["RefName"],
    )

    fetch_request = query_request.to_fetch_request({"db_a": _profile("db_a")})

    assert [profile.alias for profile in fetch_request.profiles] == ["db_a"]
    assert fetch_request.columns == ("RefName",)
    assert fetch_request.filters[0].column == "Status"


def test_query_request_rejects_unsafe_or_conflicting_inputs() -> None:
    with pytest.raises(OznakValidationError, match="profile alias"):
        QueryRequest.from_inputs(databases="bad-name")

    with pytest.raises(OznakValidationError, match="only supports NULL"):
        QueryRequest.from_inputs(databases="db_a", filters=["CreatedAt IS TRUE"])

    with pytest.raises(OznakValidationError, match="cannot combine"):
        QueryRequest.from_inputs(databases="db_a", last=10, chunk_size=100)

    with pytest.raises(OznakValidationError, match="max_workers"):
        QueryRequest.from_inputs(databases="db_a", max_workers=0)
