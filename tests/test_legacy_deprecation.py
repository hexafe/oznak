from __future__ import annotations

import pytest

from src._legacy import warn_legacy_module


def test_legacy_warning_points_to_package_replacement() -> None:
    with pytest.warns(DeprecationWarning, match="src.services.multi_database_fetcher"):
        warn_legacy_module("src.services.multi_database_fetcher", "oznak.fetcher")
