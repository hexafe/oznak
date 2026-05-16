from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import pandas as pd

from oznak.diagnostics import SourceFetchDiagnostics
from oznak.errors import OznakConfigurationError, OznakValidationError

ExportFormat = Literal["csv", "xlsx", "parquet"]


@dataclass(frozen=True)
class ExportProfile:
    name: str = "default"
    format: ExportFormat | None = None
    delimiter: str = ","
    compression: str | None = None
    include_metadata: bool = False

    def __post_init__(self) -> None:
        if self.format is not None and self.format not in {"csv", "xlsx", "parquet"}:
            raise OznakValidationError("Export format must be one of: csv, xlsx, parquet")
        if not isinstance(self.delimiter, str) or len(self.delimiter) != 1:
            raise OznakValidationError("Export delimiter must be exactly one character")


def export_output(
    data: pd.DataFrame,
    out_path: str | Path,
    *,
    profile: ExportProfile | None = None,
    diagnostics: tuple[SourceFetchDiagnostics, ...] = (),
) -> None:
    destination = Path(out_path)
    export_profile = profile or ExportProfile()
    export_format = export_profile.format or _format_from_suffix(destination)

    if export_profile.include_metadata:
        data = _with_metadata_columns(data, diagnostics)

    if export_format == "csv":
        data.to_csv(destination, index=False, sep=export_profile.delimiter)
        return
    if export_format == "xlsx":
        data.to_excel(destination, index=False)
        return
    if export_format == "parquet":
        _export_parquet(data, destination, compression=export_profile.compression)
        return

    raise OznakValidationError("Unsupported output format. Use .csv, .xlsx/.xls, or .parquet")


def export_chunks_streaming(
    chunks: Iterable[pd.DataFrame],
    out_path: str | Path,
    *,
    profile: ExportProfile | None = None,
) -> bool:
    destination = Path(out_path)
    export_profile = profile or ExportProfile()
    export_format = export_profile.format or _format_from_suffix(destination)

    if export_format == "csv":
        wrote_any = False
        for chunk in chunks:
            if chunk.empty:
                continue
            chunk.to_csv(
                destination,
                mode="a" if wrote_any else "w",
                header=not wrote_any,
                index=False,
                sep=export_profile.delimiter,
            )
            wrote_any = True
        return wrote_any

    frames = [chunk for chunk in chunks if not chunk.empty]
    if not frames:
        return False
    export_output(pd.concat(frames, ignore_index=True), destination, profile=export_profile)
    return True


def parse_export_profile(name: str, raw_profile: Mapping[str, object]) -> ExportProfile:
    export_format = raw_profile.get("format")
    if export_format is not None:
        export_format = str(export_format).strip().lower()
    delimiter = str(raw_profile.get("delimiter", ","))
    compression = raw_profile.get("compression")
    include_metadata = raw_profile.get("include_metadata", False)
    if type(include_metadata) is not bool:
        raise OznakValidationError(f"Export profile '{name}' include_metadata must be a boolean")
    return ExportProfile(
        name=name,
        format=export_format,  # type: ignore[arg-type]
        delimiter=delimiter,
        compression=str(compression).strip().lower() if compression is not None else None,
        include_metadata=include_metadata,
    )


def _format_from_suffix(path: Path) -> ExportFormat:
    extension = path.suffix.lower()
    if extension == ".csv":
        return "csv"
    if extension in {".xlsx", ".xls"}:
        return "xlsx"
    if extension == ".parquet":
        return "parquet"
    raise OznakValidationError("Unsupported output format. Use .csv, .xlsx/.xls, or .parquet")


def _export_parquet(data: pd.DataFrame, destination: Path, *, compression: str | None) -> None:
    try:
        data.to_parquet(destination, index=False, compression=compression)
    except ImportError as exc:
        raise OznakConfigurationError(
            "Parquet export requires an optional parquet engine such as pyarrow. "
            "Install Oznak with the 'parquet' extra."
        ) from exc


def _with_metadata_columns(
    data: pd.DataFrame,
    diagnostics: tuple[SourceFetchDiagnostics, ...],
) -> pd.DataFrame:
    if not diagnostics:
        return data
    result = data.copy()
    result["oznak_source_count"] = len(diagnostics)
    result["oznak_error_count"] = sum(1 for item in diagnostics if item.failed)
    return result


__all__ = ["ExportProfile", "export_chunks_streaming", "export_output", "parse_export_profile"]
