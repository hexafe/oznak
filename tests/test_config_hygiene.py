from __future__ import annotations

import ipaddress
from pathlib import Path

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATABASE_CONFIG_PATH = PROJECT_ROOT / "config" / "databases.yaml"
ENV_EXAMPLE_PATH = PROJECT_ROOT / ".env.example"

PRIVATE_OR_LOOPBACK_HOSTS = {
    "localhost",
    "127.0.0.1",
    "::1",
}
PRODUCTION_NAME_TOKENS = {"prod", "production", "live", "plant", "factory"}
CREDENTIAL_KEY_TOKENS = {"password", "passwd", "secret", "token", "apikey", "api_key", "pwd"}


def _iter_strings(value):
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for key, nested in value.items():
            yield str(key)
            yield from _iter_strings(nested)
    elif isinstance(value, (list, tuple, set)):
        for nested in value:
            yield from _iter_strings(nested)


def _looks_like_private_or_loopback_ip(value: str) -> bool:
    try:
        ip_addr = ipaddress.ip_address(value)
    except ValueError:
        return False
    return ip_addr.is_private or ip_addr.is_loopback


def test_public_database_template_contains_only_safe_examples():
    config = yaml.safe_load(DATABASE_CONFIG_PATH.read_text(encoding="utf-8"))
    assert isinstance(config, dict)

    all_strings = [item.lower() for item in _iter_strings(config)]

    assert not any(item in PRIVATE_OR_LOOPBACK_HOSTS for item in all_strings)
    assert not any(_looks_like_private_or_loopback_ip(item) for item in all_strings)

    assert not any(
        token in item.replace("-", "_")
        for item in all_strings
        for token in PRODUCTION_NAME_TOKENS
    )
    assert not any(
        token in item.replace("-", "_")
        for item in all_strings
        for token in CREDENTIAL_KEY_TOKENS
    )


def test_env_example_uses_empty_placeholder_values():
    lines = ENV_EXAMPLE_PATH.read_text(encoding="utf-8").splitlines()
    non_comment_lines = [line.strip() for line in lines if line.strip() and not line.strip().startswith("#")]

    assert non_comment_lines, ".env.example should keep placeholder keys for local setup."

    for line in non_comment_lines:
        assert "=" in line
        key, value = line.split("=", 1)
        assert key.strip()
        assert value == ""
