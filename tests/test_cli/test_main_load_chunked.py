import importlib


def test_load_chunked_parses_arguments_and_calls_fetcher(monkeypatch):
    module = importlib.import_module("src.cli.main")

    captured = {}

    class DummyFetcher:
        def fetch_chunked(self, databases, filters, chunk_size, output_path, pagination_column="id", columns=None):
            captured["databases"] = databases
            captured["filters"] = filters
            captured["chunk_size"] = chunk_size
            captured["output_path"] = output_path
            captured["pagination_column"] = pagination_column
            captured["columns"] = columns

    monkeypatch.setattr(module, "MultiDatabaseFetcher", lambda: DummyFetcher())

    module.load_chunked(
        databases="db_a, db_b",
        select_columns="RefName, TimeStamp",
        filters=["RefName = ABC123"],
        chunk_size=200,
        pagination_col="pk_id",
        out="out.csv",
    )

    assert captured == {
        "databases": ["db_a", "db_b"],
        "filters": ["RefName = ABC123"],
        "chunk_size": 200,
        "output_path": "out.csv",
        "pagination_column": "pk_id",
        "columns": ["RefName", "TimeStamp"],
    }
