from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from tempfile import TemporaryDirectory
from time import perf_counter, sleep

import pandas as pd

from oznak.chunked import iter_records_chunked
from oznak.exporter import export_chunks_streaming
from oznak.profiles import DatabaseProfile
from oznak.result import FetchRequest


@dataclass(frozen=True)
class ChunkedBenchmarkConfig:
    source_count: int = 4
    rows_per_source: int = 5000
    chunk_size: int = 1000
    worker_counts: tuple[int, ...] = (1, 2, 4)
    delay_ms: float = 0.0
    export_csv: bool = False
    export_directory: Path | None = None

    def __post_init__(self) -> None:
        _require_positive_int(self.source_count, field_name="source_count")
        _require_positive_int(self.rows_per_source, field_name="rows_per_source")
        _require_positive_int(self.chunk_size, field_name="chunk_size")
        worker_counts = tuple(self.worker_counts)
        if not worker_counts:
            raise ValueError("worker_counts must not be empty")
        for worker_count in worker_counts:
            _require_positive_int(worker_count, field_name="worker_counts")
        if type(self.delay_ms) not in {int, float} or self.delay_ms < 0:
            raise ValueError("delay_ms must be a non-negative number")
        if type(self.export_csv) is not bool:
            raise ValueError("export_csv must be a boolean")
        export_directory = Path(self.export_directory) if self.export_directory is not None else None
        if self.export_csv and export_directory is None:
            raise ValueError("export_directory is required when export_csv is enabled")
        object.__setattr__(self, "worker_counts", worker_counts)
        object.__setattr__(self, "export_directory", export_directory)


@dataclass(frozen=True)
class ChunkedBenchmarkRun:
    max_workers: int
    elapsed_seconds: float
    row_count: int
    chunk_count: int
    query_count: int
    rows_per_second: float
    exported_path: Path | None = None


@dataclass(frozen=True)
class ChunkedBenchmarkResult:
    config: ChunkedBenchmarkConfig
    runs: tuple[ChunkedBenchmarkRun, ...]


def run_synthetic_chunked_benchmark(config: ChunkedBenchmarkConfig | None = None) -> ChunkedBenchmarkResult:
    benchmark_config = config or ChunkedBenchmarkConfig()
    profiles = _synthetic_profiles(benchmark_config.source_count)
    request = FetchRequest(
        profiles=profiles,
        columns=("id", "value"),
    )

    with TemporaryDirectory() as temp_dir_name:
        base_export_dir = benchmark_config.export_directory or Path(temp_dir_name)
        if benchmark_config.export_csv:
            base_export_dir.mkdir(parents=True, exist_ok=True)

        runs = tuple(
            _run_single_benchmark(
                config=benchmark_config,
                request=request,
                max_workers=max_workers,
                export_directory=base_export_dir if benchmark_config.export_csv else None,
            )
            for max_workers in benchmark_config.worker_counts
        )

    return ChunkedBenchmarkResult(config=benchmark_config, runs=runs)


def format_benchmark_result(result: ChunkedBenchmarkResult) -> str:
    lines = [
        "workers\tseconds\trows\tchunks\tqueries\trows_per_second\texport",
    ]
    for run in result.runs:
        export_path = str(run.exported_path) if run.exported_path is not None else "-"
        lines.append(
            "\t".join(
                (
                    str(run.max_workers),
                    f"{run.elapsed_seconds:.4f}",
                    str(run.row_count),
                    str(run.chunk_count),
                    str(run.query_count),
                    f"{run.rows_per_second:.2f}",
                    export_path,
                )
            )
        )
    return "\n".join(lines)


def _run_single_benchmark(
    *,
    config: ChunkedBenchmarkConfig,
    request: FetchRequest,
    max_workers: int,
    export_directory: Path | None,
) -> ChunkedBenchmarkRun:
    query_count = 0
    chunk_count = 0
    row_count = 0
    query_count_lock = Lock()

    def fake_engine_factory(profile: DatabaseProfile, _credentials: object | None) -> str:
        return profile.alias

    def fake_read_sql(_sql: str, engine: str, params: dict[str, object] | None = None) -> pd.DataFrame:
        nonlocal query_count
        with query_count_lock:
            query_count += 1
        if config.delay_ms:
            sleep(config.delay_ms / 1000.0)
        last_seen = int(params.get("pagination_param", 0)) if params else 0
        start = last_seen + 1
        stop = min(last_seen + config.chunk_size, config.rows_per_source)
        if start > config.rows_per_source:
            return pd.DataFrame(columns=["id", "value"])
        return pd.DataFrame(
            {
                "id": range(start, stop + 1),
                "value": [f"{engine}-{row_id}" for row_id in range(start, stop + 1)],
            }
        )

    def frames() -> Iterable[pd.DataFrame]:
        nonlocal chunk_count, row_count
        for event in iter_records_chunked(
            request,
            chunk_size=config.chunk_size,
            engine_factory=fake_engine_factory,
            read_sql=fake_read_sql,
            max_workers=max_workers,
        ):
            if event.frame is None:
                continue
            chunk_count += 1
            row_count += int(len(event.frame.index))
            yield event.frame

    started_at = perf_counter()
    exported_path = None
    if export_directory is None:
        for _frame in frames():
            pass
    else:
        exported_path = export_directory / f"synthetic-chunked-w{max_workers}.csv"
        export_chunks_streaming(frames(), exported_path)
    elapsed_seconds = perf_counter() - started_at

    return ChunkedBenchmarkRun(
        max_workers=max_workers,
        elapsed_seconds=elapsed_seconds,
        row_count=row_count,
        chunk_count=chunk_count,
        query_count=query_count,
        rows_per_second=row_count / elapsed_seconds if elapsed_seconds > 0 else float(row_count),
        exported_path=exported_path,
    )


def _synthetic_profiles(source_count: int) -> tuple[DatabaseProfile, ...]:
    return tuple(
        DatabaseProfile(
            alias=f"source_{index}",
            dialect="mysql",
            host=f"source-{index}.example.invalid",
            port=3306,
            database="synthetic",
            table="records",
            allowed_columns=("id", "value"),
            pagination_column="id",
        )
        for index in range(1, source_count + 1)
    )


def _require_positive_int(value: int, *, field_name: str) -> None:
    if type(value) is not int or value <= 0:
        raise ValueError(f"{field_name} must be a positive integer")


__all__ = [
    "ChunkedBenchmarkConfig",
    "ChunkedBenchmarkResult",
    "ChunkedBenchmarkRun",
    "format_benchmark_result",
    "run_synthetic_chunked_benchmark",
]
