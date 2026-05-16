from __future__ import annotations

import pandas as pd
import pytest

from oznak.exporter import ExportProfile, export_chunks_streaming, export_output


def test_export_output_writes_csv_with_profile_delimiter(tmp_path) -> None:
    out_path = tmp_path / "records.csv"

    export_output(
        pd.DataFrame([{"RefName": "A1", "Status": "ACTIVE"}]),
        out_path,
        profile=ExportProfile(format="csv", delimiter=";"),
    )

    assert out_path.read_text(encoding="utf-8").splitlines() == [
        "RefName;Status",
        "A1;ACTIVE",
    ]


def test_export_chunks_streaming_appends_csv_chunks(tmp_path) -> None:
    out_path = tmp_path / "records.csv"

    wrote = export_chunks_streaming(
        [
            pd.DataFrame([{"id": 1, "source_database": "db_a"}]),
            pd.DataFrame([{"id": 2, "source_database": "db_b"}]),
        ],
        out_path,
    )

    assert wrote is True
    assert pd.read_csv(out_path).to_dict(orient="records") == [
        {"id": 1, "source_database": "db_a"},
        {"id": 2, "source_database": "db_b"},
    ]


def test_export_output_writes_parquet_when_engine_is_installed(tmp_path) -> None:
    pytest.importorskip("pyarrow")
    out_path = tmp_path / "records.parquet"

    export_output(pd.DataFrame([{"id": 1}]), out_path)

    assert pd.read_parquet(out_path).to_dict(orient="records") == [{"id": 1}]
