from __future__ import annotations

import importlib


def test_legacy_load_chunked_symbol_is_package_command() -> None:
    module = importlib.import_module("src.cli.main")

    assert callable(module.load_chunked)
