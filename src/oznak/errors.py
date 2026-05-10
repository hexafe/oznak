from __future__ import annotations


class OznakError(Exception):
    """Base class for public Oznak errors."""


class OznakValidationError(OznakError, ValueError):
    """Raised when a caller provides an invalid public contract."""


class OznakConfigurationError(OznakError):
    """Raised when profiles or credentials cannot be resolved."""


class OznakFetchError(OznakError):
    """Raised when a fetch operation fails before structured results exist."""
