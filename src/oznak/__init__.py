"""Public package entrypoint for Oznak."""

from oznak.credentials import (
    CredentialProvider,
    Credentials,
    EnvironmentCredentialProvider,
    MappingCredentialProvider,
)
from oznak.config import load_database_profiles
from oznak.diagnostics import SourceFetchDiagnostics, SourceFetchStatus
from oznak.dialects import DatabaseDialect
from oznak.engines import create_sqlalchemy_engine
from oznak.filters import QueryFilter, QueryOperator, parse_legacy_filter
from oznak.fetcher import fetch_records, fetch_records_chunked
from oznak.profiles import DatabaseProfile
from oznak.query_builder import CompiledQuery, QuerySpec, compile_query
from oznak.result import FetchRequest, FetchResult
from oznak.runtime import CancellationToken

__version__ = "0.1.0"

__all__ = [
    "CompiledQuery",
    "CredentialProvider",
    "Credentials",
    "CancellationToken",
    "DatabaseDialect",
    "DatabaseProfile",
    "EnvironmentCredentialProvider",
    "FetchRequest",
    "FetchResult",
    "MappingCredentialProvider",
    "QueryFilter",
    "QueryOperator",
    "QuerySpec",
    "SourceFetchDiagnostics",
    "SourceFetchStatus",
    "__version__",
    "compile_query",
    "create_sqlalchemy_engine",
    "fetch_records",
    "fetch_records_chunked",
    "load_database_profiles",
    "parse_legacy_filter",
]
