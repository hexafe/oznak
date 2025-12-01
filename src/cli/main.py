import typer
from src.services.multi_database_fetcher import MultiDatabaseFetcher
from src.services.filter_parser import parse_filters
from src.storage.exporter import export

app = typer.Typer()

@app.command()
def load(
        databases: str = typer.Argument(..., help="Comma-separated list of databases (e.g., database1, database2"),
        filters: list[str] = typer.Option([], "--filter", "-f", help="Example filter: 'RefName LIKE V123456'"),
        last: int = typer.Option(None, "--last", help="Limit to last N records"),
        date_col: str = typer.Option("Date", "--date_col", help="Name of the date/timestamp column for ordering (when using --last)"),
        out: str = typer.Option("output.csv", "--out", "-o", help="Output file (CSV or Excel)"),
):
    # Validate inputs
    if last is not None and (not isinstance(last, int) or last <= 0):
        print("'last' must be a positive integer")
        return

    # Validate date column name (basic check, more robust implementation to be done if needed :P)
    if not date_col.replace('_', '').replace('.', '').isalnum():
        print(f"Invalid date column name: {date_col}")
        return

    fetcher = MultiLineFetcher()
    parsed = parse_filters(filters, last)

    if not parsed["filters"] and parsed["limit"] is None:
        print("No filters or limit specified. This will fetch all data from all tables!")
        if not typer.confirm("Are you sure you want to continue?"):
            return

    databases_list = [database.strip() for database in databases.split(",")]

    df = fetcher.fetch(databases_list, parsed["filters"], parsed["limit"], date_col)

    if df.empty:
        print("No data to export")
        return

    export(df, out)


if __name__ == "__main__":
    app()

