from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml

from oznak.errors import OznakConfigurationError, OznakValidationError
from oznak.profiles import DatabaseProfile

_REQUIRED_PROFILE_KEYS = ("type", "host", "port", "database", "table")


def load_database_profiles(config_path: str | Path) -> dict[str, DatabaseProfile]:
    path = Path(config_path)
    raw_config = _load_yaml(path)
    databases = _require_databases_section(raw_config, path)

    profiles: dict[str, DatabaseProfile] = {}
    for alias, raw_profile in databases.items():
        if not isinstance(raw_profile, Mapping):
            raise OznakConfigurationError(
                f"Profile '{alias}' in '{path}' must be a mapping."
            )
        profiles[str(alias)] = _parse_profile(str(alias), raw_profile, path)

    return profiles


def _load_yaml(path: Path) -> Mapping[str, Any]:
    try:
        raw_config = yaml.safe_load(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise OznakConfigurationError(f"Database config file not found: {path}") from exc
    except yaml.YAMLError as exc:
        raise OznakConfigurationError(f"Invalid YAML in database config '{path}'.") from exc

    if raw_config is None:
        return {}
    if not isinstance(raw_config, Mapping):
        raise OznakConfigurationError(f"Database config '{path}' must be a mapping at the root.")
    return raw_config


def _require_databases_section(raw_config: Mapping[str, Any], path: Path) -> Mapping[str, Any]:
    databases = raw_config.get("databases")
    if not isinstance(databases, Mapping):
        raise OznakConfigurationError(
            f"Database config '{path}' must define a top-level 'databases' mapping."
        )
    return databases


def _parse_profile(alias: str, raw_profile: Mapping[str, Any], path: Path) -> DatabaseProfile:
    missing_keys = [key for key in _REQUIRED_PROFILE_KEYS if key not in raw_profile]
    if missing_keys:
        joined = ", ".join(missing_keys)
        raise OznakConfigurationError(
            f"Profile '{alias}' in '{path}' is missing required keys: {joined}."
        )

    allowed_columns = raw_profile.get("allowed_columns", ())
    if allowed_columns is None:
        allowed_columns = ()
    if not isinstance(allowed_columns, (list, tuple, set, frozenset)):
        raise OznakValidationError(f"Invalid profile '{alias}': allowed_columns must be a sequence of identifiers.")

    metadata = raw_profile.get("metadata", {})
    if metadata is None:
        metadata = {}
    if not isinstance(metadata, Mapping):
        raise OznakValidationError(f"Invalid profile '{alias}': metadata must be a mapping.")

    try:
        return DatabaseProfile(
            alias=alias,
            dialect=raw_profile["type"],
            host=raw_profile["host"],
            port=raw_profile["port"],
            database=raw_profile["database"],
            table=raw_profile["table"],
            allowed_columns=allowed_columns,
            timestamp_column=raw_profile.get("timestamp_column"),
            pagination_column=raw_profile.get("pagination_column"),
            display_name=raw_profile.get("display_name"),
            connect_timeout_seconds=raw_profile.get("connect_timeout_seconds"),
            query_timeout_seconds=raw_profile.get("query_timeout_seconds"),
            order_by_enabled=raw_profile.get("order_by_enabled", True),
            metadata=metadata,
        )
    except OznakValidationError as exc:
        raise OznakValidationError(f"Invalid profile '{alias}': {exc}") from exc
    except ValueError as exc:
        raise OznakValidationError(f"Invalid profile '{alias}': {exc}") from exc
