from __future__ import annotations

from src._legacy import warn_legacy_module
from oznak.api import app, fetch, health

warn_legacy_module("src.api.rest", "oznak.api")

__all__ = ["app", "fetch", "health"]
