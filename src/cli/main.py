from __future__ import annotations

from src._legacy import warn_legacy_module
from oznak.cli import app, benchmark_chunked, load, load_chunked, main, profiles, tui, version

warn_legacy_module("src.cli.main", "oznak.cli")

__all__ = ["app", "benchmark_chunked", "load", "load_chunked", "main", "profiles", "tui", "version"]
