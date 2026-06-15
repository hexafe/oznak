import json

import pandas as pd
import pytest

from oznak.credentials import Credentials, EnvironmentCredentialProvider, MappingCredentialProvider
from oznak.diagnostics import SourceFetchDiagnostics, SourceFetchStatus
from oznak.dialects import DatabaseDialect
from oznak.errors import OznakConfigurationError, OznakValidationError
from oznak.filters import QueryFilter, QueryOperator
from oznak.profiles import DatabaseProfile
from oznak.result import FetchRequest, FetchResult


def _profile() -> DatabaseProfile:
    return DatabaseProfile(
        alias="assembly",
        dialect="mysql",
        host="db.example.invalid",
        port=3306,
        database="process_db",
        table="records",
        allowed_columns=("reference", "status", "updated_at"),
        timestamp_column="updated_at",
        pagination_column="id",
    )


def test_database_profile_normalizes_dialect_and_allows_columns():
    profile = _profile()

    assert profile.dialect is DatabaseDialect.MYSQL
    assert profile.require_allowed_column("reference") == "reference"
    assert "redacted-host" in profile.redacted_location


def test_database_profile_rejects_invalid_identifier_and_disallowed_column():
    with pytest.raises(OznakValidationError):
        DatabaseProfile(
            alias="bad-alias",
            dialect="mysql",
            host="db.example.invalid",
            port=3306,
            database="process_db",
            table="records",
        )

    with pytest.raises(OznakValidationError):
        _profile().require_allowed_column("operator")


@pytest.mark.parametrize("invalid_timeout", ["5", 0, -1, False, True])
def test_database_profile_rejects_invalid_timeout_fields(invalid_timeout):
    with pytest.raises(OznakValidationError, match="connect_timeout_seconds"):
        DatabaseProfile(
            alias="assembly",
            dialect="mysql",
            host="db.example.invalid",
            port=3306,
            database="process_db",
            table="records",
            connect_timeout_seconds=invalid_timeout,
        )

    with pytest.raises(OznakValidationError, match="query_timeout_seconds"):
        DatabaseProfile(
            alias="assembly",
            dialect="mysql",
            host="db.example.invalid",
            port=3306,
            database="process_db",
            table="records",
            query_timeout_seconds=invalid_timeout,
        )


def test_database_profile_accepts_positive_timeout_fields():
    profile = DatabaseProfile(
        alias="assembly",
        dialect="mysql",
        host="db.example.invalid",
        port=3306,
        database="process_db",
        table="records",
        connect_timeout_seconds=2,
        query_timeout_seconds=2.5,
    )

    assert profile.connect_timeout_seconds == 2.0
    assert profile.query_timeout_seconds == 2.5


def test_database_profile_rejects_non_boolean_order_by_enabled():
    with pytest.raises(OznakValidationError, match="order_by_enabled"):
        DatabaseProfile(
            alias="assembly",
            dialect="mysql",
            host="db.example.invalid",
            port=3306,
            database="process_db",
            table="records",
            order_by_enabled="false",
        )


def test_mapping_credential_provider_returns_redactable_credentials():
    provider = MappingCredentialProvider({"assembly": ("svc_user", "secret")})

    credentials = provider.get_credentials("assembly")

    assert credentials == Credentials("svc_user", "secret")
    assert credentials.redacted() == {"username": "svc_user", "password": "<redacted>"}


def test_environment_credential_provider_has_no_dotenv_side_effect(monkeypatch):
    monkeypatch.setenv("ASSEMBLY_USER", "svc_user")
    monkeypatch.setenv("ASSEMBLY_PASSWORD", "secret")

    provider = EnvironmentCredentialProvider()

    assert provider.get_credentials("assembly") == Credentials("svc_user", "secret")


def test_environment_credential_provider_reports_missing_credentials(monkeypatch):
    monkeypatch.delenv("ASSEMBLY_USER", raising=False)
    monkeypatch.delenv("ASSEMBLY_PASSWORD", raising=False)

    with pytest.raises(OznakConfigurationError):
        EnvironmentCredentialProvider().get_credentials("assembly")


def test_query_filter_normalizes_operators_and_null_predicates():
    assert QueryFilter("reference", "==", "R1").operator is QueryOperator.EQ
    assert QueryFilter("status", "not like", "SCRAP%").operator is QueryOperator.NOT_LIKE
    assert QueryFilter.is_null("updated_at").operator is QueryOperator.IS_NULL


def test_query_filter_rejects_unsafe_null_and_empty_in_predicates():
    with pytest.raises(OznakValidationError):
        QueryFilter("updated_at", "IS", "CURRENT_TIMESTAMP")

    with pytest.raises(OznakValidationError):
        QueryFilter("reference", "IN", [])

    with pytest.raises(OznakValidationError):
        QueryFilter("reference", "IN", "R1,R2")


def test_fetch_request_validates_public_boundary():
    request = FetchRequest(
        profiles=(_profile(),),
        filters=(QueryFilter("reference", "=", "R1"),),
        columns=("reference", "status", "reference"),
        limit=100,
        date_column="updated_at",
        timeout_seconds=5.0,
    )

    assert request.columns == ("reference", "status")

    with pytest.raises(ValueError, match="order_by_enabled"):
        FetchRequest(profiles=(_profile(),), order_by_enabled="false")


def test_fetch_result_distinguishes_partial_success():
    result = FetchResult(
        source_results=(
            SourceFetchDiagnostics("assembly_a", SourceFetchStatus.SUCCESS, row_count=3),
            SourceFetchDiagnostics("assembly_b", SourceFetchStatus.FAILED, error_code="connection_error"),
        ),
        errors=("assembly_b failed",),
    )

    assert result.row_count == 3
    assert result.has_errors is True
    assert result.partial_success is True


def test_fetch_result_json_records_normalize_timestamp_scalars_without_mutating_data():
    data = pd.DataFrame(
        [
            {
                "reference": "REF-1",
                "created_at": pd.Timestamp("2026-06-15T12:00:00"),
                "missing": pd.NaT,
                "score": float("nan"),
            }
        ]
    )
    result = FetchResult(
        data=data,
        source_results=(SourceFetchDiagnostics("assembly", SourceFetchStatus.SUCCESS, row_count=1),),
    )

    records = result.to_json_records()

    assert records == [
        {
            "reference": "REF-1",
            "created_at": "2026-06-15T12:00:00.000",
            "missing": None,
            "score": None,
        }
    ]
    assert isinstance(result.data.loc[0, "created_at"], pd.Timestamp)
    json.dumps(records)
