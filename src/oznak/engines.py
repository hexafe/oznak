from __future__ import annotations

import math
from urllib.parse import quote_plus

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from oznak.credentials import Credentials
from oznak.dialects import DatabaseDialect
from oznak.errors import OznakConfigurationError, OznakValidationError
from oznak.profiles import DatabaseProfile

_MSSQL_ODBC_DRIVER = "ODBC Driver 17 for SQL Server"


def create_sqlalchemy_engine(profile: DatabaseProfile, credentials: Credentials | None) -> Engine:
    if credentials is None:
        raise OznakConfigurationError(f"Missing credentials for profile '{profile.alias}'")

    url = _build_sqlalchemy_url(profile, credentials)
    engine_kwargs = _build_engine_kwargs(profile)
    try:
        return create_engine(url, **engine_kwargs)
    except Exception as exc:
        raise OznakConfigurationError(
            f"Failed to create SQLAlchemy engine for profile '{profile.alias}' at {profile.redacted_location}."
        ) from exc


def _build_sqlalchemy_url(profile: DatabaseProfile, credentials: Credentials) -> str:
    encoded_user = quote_plus(credentials.username)
    encoded_password = quote_plus(credentials.password)

    if profile.dialect is DatabaseDialect.MYSQL:
        return (
            f"mysql+pymysql://{encoded_user}:{encoded_password}"
            f"@{profile.host}:{profile.port}/{profile.database}"
        )
    if profile.dialect is DatabaseDialect.MSSQL:
        driver = quote_plus(_MSSQL_ODBC_DRIVER)
        query_params = [f"driver={driver}"]
        if profile.connect_timeout_seconds is not None:
            query_params.append(f"login_timeout={_seconds_to_int(profile.connect_timeout_seconds)}")
        query_string = "&".join(query_params)
        return (
            f"mssql+pyodbc://{encoded_user}:{encoded_password}"
            f"@{profile.host}:{profile.port}/{profile.database}"
            f"?{query_string}"
        )
    raise OznakValidationError(
        f"Unsupported database dialect '{profile.dialect}' for profile '{profile.alias}'."
    )


def _build_engine_kwargs(profile: DatabaseProfile) -> dict[str, object]:
    kwargs: dict[str, object] = {
        "echo": False,
        "pool_pre_ping": True,
    }
    connect_args: dict[str, int] = {}

    if profile.dialect is DatabaseDialect.MYSQL:
        if profile.connect_timeout_seconds is not None:
            connect_args["connect_timeout"] = _seconds_to_int(profile.connect_timeout_seconds)
        if profile.query_timeout_seconds is not None:
            query_timeout = _seconds_to_int(profile.query_timeout_seconds)
            connect_args["read_timeout"] = query_timeout
            connect_args["write_timeout"] = query_timeout
    elif profile.dialect is DatabaseDialect.MSSQL:
        if profile.query_timeout_seconds is not None:
            connect_args["timeout"] = _seconds_to_int(profile.query_timeout_seconds)

    if connect_args:
        kwargs["connect_args"] = connect_args
    if profile.pool_size is not None:
        kwargs["pool_size"] = profile.pool_size
    if profile.max_overflow is not None:
        kwargs["max_overflow"] = profile.max_overflow
    if profile.pool_timeout_seconds is not None:
        kwargs["pool_timeout"] = _seconds_to_int(profile.pool_timeout_seconds)

    return kwargs


def _seconds_to_int(seconds: float) -> int:
    return max(1, int(math.ceil(seconds)))
