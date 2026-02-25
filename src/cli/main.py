import typer
from src.services.multi_database_fetcher import MultiDatabaseFetcher
from src.services.filter_parser import normalize_columns, normalize_databases, parse_filters
from src.storage.exporter import export

app = typer.Typer()

@app.command()
def load(
        databases: str = typer.Argument(..., help="Comma-separated list of databases (e.g., database1, database2"),
        select_columns: str = typer.Option(None, "--select-columns", "--c", help="Comma-separated list of columns to select (e.g., 'RefName, Date, FittingForce')"),
        filters: list[str] = typer.Option([], "--filter", "-f", help="Example filter: 'RefName IN V123456, ABC123'"),
        last: int = typer.Option(None, "--last", help="Limit to last N records"),
        date_col: str = typer.Option("TimeStamp", "--date_col", help="Name of the date/timestamp column for ordering (when using --last)"),
        out: str = typer.Option("output.csv", "--out", "-o", help="Output file (CSV or Excel)"),
):
    fetcher = MultiDatabaseFetcher()

    try:
        parsed = parse_filters(filters, last)
        databases_list = normalize_databases(databases)
        columns_list = normalize_columns(select_columns)
    except ValueError as exc:
        print(str(exc))
        raise typer.Exit(code=1)

    if not parsed["filters"] and parsed["limit"] is None:
        print("No filters or limit specified. This will fetch all data from all tables!")
        if not typer.confirm("Are you sure you want to continue?"):
            return

    df = fetcher.fetch(databases_list, parsed["filters"], parsed["limit"], date_col, columns_list)

    if df.empty:
        print("No data to export")
        return

    export(df, out)

@app.command()
def load_chunked(
        databases: str = typer.Argument(..., help="Comma-separated list of databases (e.g., database1, database2"),
        select_columns: str = typer.Option(None, "--select-columns", "--c", help="Comma-separated list of columns to select (e.g., 'RefName, Date, FittingForce')"),
        filters: list[str] = typer.Option([], "--filter", "-f", help="Example filter: 'RefName IN V123456, ABC123'"),
        chunk_size: int = typer.Option(10000, "--chunk-size", "-cs", help="Number of rows per chunk to fetch and export"),
        pagination_col: str = typer.Option("id", "--pagination-column", help="Column name for pagination (should be unique/indexed)"),
        out: str = typer.Option("output.csv", "--out", "-o", help="Output file (CSV or Excel)"),
):
    """
    Load data in chunks from specified databases, applying filters and optional column selection, and export directly to a file
    """
    if chunk_size <= 0:
        print(f"'chunk_size' must be a positive integer")
        raise typer.Exit(code=1)

    fetcher = MultiDatabaseFetcher()

    try:
        parsed = parse_filters(filters, None)
        databases_list = normalize_databases(databases)
        columns_list = normalize_columns(select_columns)
    except ValueError as exc:
        print(str(exc))
        raise typer.Exit(code=1)

    fetcher.fetch_chunked(databases_list, parsed["filters"], chunk_size, out, pagination_col, columns_list)

    print(f"Data from {databases_list} loaded in chunks and exported to {out}")


if __name__ == "__main__":
    app()

