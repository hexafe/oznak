from __future__ import annotations

import warnings


def warn_legacy_module(module_name: str, replacement: str) -> None:
    warnings.warn(
        f"{module_name} is deprecated and will be removed in a future release; use {replacement} instead.",
        DeprecationWarning,
        stacklevel=3,
    )


__all__ = ["warn_legacy_module"]
