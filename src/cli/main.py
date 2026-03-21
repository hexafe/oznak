import typer

from src.services.multi_database_fetcher import MultiDatabaseFetcher
from src.services.request_preprocessor import preprocess_fetch_request
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
    if last is not None and (not isinstance(last, int) or last <= 0):
        print("'last' must be a positive integer")
        raise typer.Exit(code=1)

    if not date_col.replace("_", "").replace(".", "").isalnum():
        print(f"Invalid date column name: {date_col}")
        raise typer.Exit(code=1)

    try:
        prepared = preprocess_fetch_request(
            databases=databases,
            filters=filters,
            last=last,
            select_columns=select_columns,
        )
    except ValueError as exc:
        print(f"Invalid filters: {exc}")
        raise typer.Exit(code=1)

    fetcher = MultiDatabaseFetcher()

    if not prepared["filters"] and prepared["limit"] is None:
        print("No filters or limit specified. This will fetch all data from all tables!")
        if not typer.confirm("Are you sure you want to continue?"):
            raise typer.Exit(code=0)

    df = fetcher.fetch(
        prepared["databases"],
        prepared["filters"],
        prepared["limit"],
        date_col,
        prepared["columns"],
    )

    if df.empty:
        print("No data to export")
        raise typer.Exit(code=0)

    export(df, out)


@app.command()
def load_chunked(
    databases: str = typer.Argument(..., help="Comma-separated list of databases (e.g., database1, database2"),
    select_columns: str = typer.Option(None, "--select-columns", "--c", help="Comma-separated list of columns to select (e.g., 'RefName, Date, FittingForce')"),
    filters: list[str] = typer.Option([], "--filter", "-f", help="Example filter: 'RefName IN V123456, ABC123'"),
    chunk_size: int = typer.Option(10000, "--chunk-size", "-cs", help="Number of rows per chunk to fetch and export"),
    pagination_col: str = typer.Option("id", "--pagination-column", help="Column name for pagination (should be unique/indexed)"),
    out: str = typer.Option("output.csv", "--out", "-o", help="Output CSV file"),
):
    if chunk_size <= 0:
        print("'chunk_size' must be a positive integer")
        raise typer.Exit(code=1)

    try:
        prepared = preprocess_fetch_request(
            databases=databases,
            filters=filters,
            last=None,
            select_columns=select_columns,
        )
    except ValueError as exc:
        print(f"Invalid filters: {exc}")
        raise typer.Exit(code=1)

    fetcher = MultiDatabaseFetcher()
    exported = fetcher.fetch_chunked(
        prepared["databases"],
        prepared["filters"],
        chunk_size,
        out,
        pagination_col,
        prepared["columns"],
    )

    if exported:
        print(f"Data from {prepared['databases']} loaded in chunks and exported to {out}")
    else:
        print(f"No data exported for {prepared['databases']}")


if __name__ == "__main__":
    app()
