from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Mapping, Protocol

from oznak.errors import OznakConfigurationError, OznakValidationError
from oznak.profiles import validate_identifier


@dataclass(frozen=True)
class Credentials:
    username: str
    password: str

    def __post_init__(self) -> None:
        if not self.username:
            raise OznakValidationError("Credential username must be non-empty")
        if not self.password:
            raise OznakValidationError("Credential password must be non-empty")

    def redacted(self) -> dict[str, str]:
        return {"username": self.username, "password": "<redacted>"}


class CredentialProvider(Protocol):
    def get_credentials(self, profile_alias: str) -> Credentials:
        """Return credentials for a database profile alias."""


class MappingCredentialProvider:
    def __init__(self, credentials_by_alias: Mapping[str, Credentials | tuple[str, str]]) -> None:
        self._credentials: dict[str, Credentials] = {}
        for alias, credentials in credentials_by_alias.items():
            validate_identifier(alias, field_name="profile alias")
            if isinstance(credentials, Credentials):
                self._credentials[alias] = credentials
            else:
                username, password = credentials
                self._credentials[alias] = Credentials(username=username, password=password)

    def get_credentials(self, profile_alias: str) -> Credentials:
        validate_identifier(profile_alias, field_name="profile alias")
        try:
            return self._credentials[profile_alias]
        except KeyError as exc:
            raise OznakConfigurationError(f"Missing credentials for profile '{profile_alias}'") from exc


class EnvironmentCredentialProvider:
    def __init__(self, *, user_suffix: str = "_USER", password_suffix: str = "_PASSWORD") -> None:
        self.user_suffix = user_suffix
        self.password_suffix = password_suffix

    def get_credentials(self, profile_alias: str) -> Credentials:
        validate_identifier(profile_alias, field_name="profile alias")
        prefix = profile_alias.upper()
        username = os.getenv(f"{prefix}{self.user_suffix}")
        password = os.getenv(f"{prefix}{self.password_suffix}")
        if not username or not password:
            raise OznakConfigurationError(f"Missing environment credentials for profile '{profile_alias}'")
        return Credentials(username=username, password=password)
