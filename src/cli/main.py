import typer
from src.services.multi_line_fetcher import MultiLineFetcher
from src.services.filter_parser import parse_filters
from src.storage.exporter import export

app = typer.Typer()

@app.command()
def load(
        lines: str = typer.Argument(..., help="Comma-separated list of lines (e.g., line1, line2"),
        filters: list[str] = typer.Option([], "--filter", "-f", help="Generic filters like 'RefName LIKE V123456'"),
        last: int = typer.Option(None, "--last", help="Limit to last N records"),
        out: str = typer.Option("output.csv", "--out", "-o", help="Output file (CSV or Excel)"),
):
    # Validate inputs
    if last is not None and (not isinstance(last, int) or last <= 0):
        print("'last' must be a positive integer")
        return

    fetcher = MultiLineFetcher()
    parsed = parse_filters(filters, last)

    if not parsed["filters"] and parsed["limit"] is None:
        print("No filters or limit specified. This will fetch all data from all tables!")
        if not typer.confirm("Are you sure you want to continue?"):
            return

    lines_list = [line.strip() for line in lines.split(",")]

    df = fetcher.fetch(lines_list, parsed["filters"], parsed["limit"])

    if df.empty:
        print("No data to export")
        return

    export(df, out)


if __name__ == "__main__":
    app()

