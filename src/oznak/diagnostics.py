from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Mapping


class SourceFetchStatus(str, Enum):
    SUCCESS = "success"
    NO_ROWS = "no_rows"
    FAILED = "failed"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"


@dataclass(frozen=True)
class SourceFetchDiagnostics:
    source_alias: str
    status: SourceFetchStatus
    row_count: int = 0
    elapsed_seconds: float | None = None
    error_code: str | None = None
    message: str | None = None
    query_summary: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    @property
    def succeeded(self) -> bool:
        return self.status in {SourceFetchStatus.SUCCESS, SourceFetchStatus.NO_ROWS}

    @property
    def failed(self) -> bool:
        return not self.succeeded
