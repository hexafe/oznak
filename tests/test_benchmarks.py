from __future__ import annotations

import pandas as pd
import pytest

from oznak.benchmarks import (
    ChunkedBenchmarkConfig,
    format_benchmark_result,
    run_synthetic_chunked_benchmark,
)


def test_synthetic_chunked_benchmark_counts_rows_chunks_and_queries() -> None:
    result = run_synthetic_chunked_benchmark(
        ChunkedBenchmarkConfig(
            source_count=2,
            rows_per_source=5,
            chunk_size=2,
            worker_counts=(1, 2),
        )
    )

    assert [run.max_workers for run in result.runs] == [1, 2]
    assert [run.row_count for run in result.runs] == [10, 10]
    assert [run.chunk_count for run in result.runs] == [6, 6]
    assert [run.query_count for run in result.runs] == [8, 8]
    assert all(run.rows_per_second > 0 for run in result.runs)


def test_synthetic_chunked_benchmark_can_export_csv(tmp_path) -> None:
    result = run_synthetic_chunked_benchmark(
        ChunkedBenchmarkConfig(
            source_count=1,
            rows_per_source=3,
            chunk_size=2,
            worker_counts=(1,),
            export_csv=True,
            export_directory=tmp_path,
        )
    )

    exported_path = result.runs[0].exported_path
    assert exported_path == tmp_path / "synthetic-chunked-w1.csv"
    assert exported_path is not None
    assert pd.read_csv(exported_path)["id"].tolist() == [1, 2, 3]


def test_format_benchmark_result_is_tabular() -> None:
    result = run_synthetic_chunked_benchmark(
        ChunkedBenchmarkConfig(
            source_count=1,
            rows_per_source=2,
            chunk_size=1,
            worker_counts=(1,),
        )
    )

    text = format_benchmark_result(result)

    assert text.splitlines()[0] == "workers\tseconds\trows\tchunks\tqueries\trows_per_second\texport"
    assert "\t2\t" in text


@pytest.mark.parametrize(
    "kwargs",
    [
        {"source_count": 0},
        {"rows_per_source": 0},
        {"chunk_size": 0},
        {"worker_counts": ()},
        {"worker_counts": (0,)},
        {"delay_ms": -1},
        {"export_csv": "yes"},
        {"export_csv": True},
    ],
)
def test_synthetic_chunked_benchmark_rejects_invalid_config(kwargs) -> None:
    with pytest.raises(ValueError):
        ChunkedBenchmarkConfig(**kwargs)
