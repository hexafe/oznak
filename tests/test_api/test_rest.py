from __future__ import annotations

import importlib

import oznak.api


def test_rest_module_delegates_to_package_api():
    module = importlib.import_module("src.api.rest")

    assert module.app is oznak.api.app
    assert module.fetch is oznak.api.fetch
    assert module.health() == {"status": "ok"}
