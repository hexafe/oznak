from __future__ import annotations

import typer

from oznak import __version__
from oznak.config import load_database_profiles, load_export_profiles
from oznak.credentials import EnvironmentCredentialProvider
from oznak.errors import OznakConfigurationError, OznakValidationError
from oznak.exporter import ExportProfile, export_chunks_streaming, export_output
from oznak.chunked import iter_records_chunked
from oznak.fetcher import fetch_records
from oznak.request import QueryRequest
from oznak.tui import run_tui

app = typer.Typer(help="Oznak package CLI")


def _fail(message: str, code: int = 1) -> None:
    typer.echo(message, err=True)
    raise typer.Exit(code=code)


def _resolve_export_profile(
    *,
    config: str,
    profile_name: str | None,
    format_name: str | None,
    delimiter: str,
    compression: str | None,
    include_metadata: bool,
) -> ExportProfile:
    if profile_name:
        profiles = load_export_profiles(config)
        if profile_name not in profiles:
            raise OznakConfigurationError(f"Unknown export profile in config: {profile_name}")
        base_profile = profiles[profile_name]
    else:
        base_profile = ExportProfile()

    return ExportProfile(
        name=base_profile.name,
        format=format_name.lower() if format_name is not None else base_profile.format,  # type: ignore[arg-type]
        delimiter=delimiter if delimiter != "," else base_profile.delimiter,
        compression=compression if compression is not None else base_profile.compression,
        include_metadata=include_metadata or base_profile.include_metadata,
    )


def _fetch_records_with_options(request, *, credential_provider, max_workers: int | None):
    if max_workers is None:
        return fetch_records(request, credential_provider=credential_provider)
    return fetch_records(request, credential_provider=credential_provider, max_workers=max_workers)


@app.command("version")
def version() -> None:
    typer.echo(__version__)


@app.command("profiles")
def profiles(
    config: str = typer.Option("config/databases.yaml", "--config", help="Path to database profiles YAML"),
) -> None:
    try:
        loaded_profiles = load_database_profiles(config)
    except (OznakConfigurationError, OznakValidationError) as exc:
        _fail(f"Configuration error: {exc}")

    for alias in sorted(loaded_profiles):
        profile = loaded_profiles[alias]
        typer.echo(f"{profile.alias}\t{profile.dialect.value}")


@app.command("load")
def load(
    databases: str = typer.Argument(..., help="Comma-separated database aliases"),
    config: str = typer.Option("config/databases.yaml", "--config", help="Path to database profiles YAML"),
    select_columns: str | None = typer.Option(
        None,
        "--select-columns",
        help="Comma-separated list of columns to fetch",
    ),
    filters: list[str] = typer.Option(
        [],
        "--filter",
        help="Filter expression like 'Status = ACTIVE'",
    ),
    last: int | None = typer.Option(None, "--last", help="Limit to last N records"),
    date_col: str = typer.Option(
        "TimeStamp",
        "--date-col",
        "--date_col",
        help="Date/timestamp column used for --last ordering",
    ),
    out: str = typer.Option("output.csv", "--out", help="Output file path (.csv/.xlsx/.xls/.parquet)"),
    max_workers: int | None = typer.Option(None, "--max-workers", help="Maximum concurrent source fetches"),
    export_profile: str | None = typer.Option(None, "--export-profile", help="Named export profile from config"),
    format_name: str | None = typer.Option(None, "--format", help="Output format: csv, xlsx, parquet"),
    delimiter: str = typer.Option(",", "--delimiter", help="CSV delimiter"),
    compression: str | None = typer.Option(None, "--compression", help="Parquet compression"),
    include_metadata: bool = typer.Option(False, "--include-metadata", help="Include Oznak metadata columns"),
    no_order_by: bool = typer.Option(
        False,
        "--no-order-by",
        help="Omit server-side ORDER BY for low-memory SQL servers.",
    ),
) -> None:
    try:
        loaded_profiles = load_database_profiles(config)
        query_request = QueryRequest.from_inputs(
            databases=databases,
            filters=filters,
            select_columns=select_columns,
            last=last,
            date_col=date_col,
            order_by_enabled=not no_order_by,
            max_workers=max_workers,
        )
        request = query_request.to_fetch_request(loaded_profiles)
        resolved_export_profile = _resolve_export_profile(
            config=config,
            profile_name=export_profile,
            format_name=format_name,
            delimiter=delimiter,
            compression=compression,
            include_metadata=include_metadata,
        )
    except (OznakConfigurationError, OznakValidationError, ValueError) as exc:
        _fail(f"Validation error: {exc}")

    provider = EnvironmentCredentialProvider()
    try:
        result = _fetch_records_with_options(
            request,
            credential_provider=provider,
            max_workers=query_request.max_workers,
        )
    except Exception as exc:
        _fail(f"Fetch error: {exc}")

    if result.has_errors:
        combined_errors = "; ".join(result.errors) if result.errors else "Fetch failed"
        _fail(f"Fetch error: {combined_errors}")

    if getattr(result.data, "empty", True):
        typer.echo("No rows returned")
        raise typer.Exit(code=0)

    try:
        export_output(result.data, out, profile=resolved_export_profile, diagnostics=result.source_results)
    except Exception as exc:
        _fail(f"Export error: {exc}")

    typer.echo(f"Exported {result.row_count} rows to {out}")


@app.command("load-chunked")
def load_chunked(
    databases: str = typer.Argument(..., help="Comma-separated database aliases"),
    config: str = typer.Option("config/databases.yaml", "--config", help="Path to database profiles YAML"),
    select_columns: str | None = typer.Option(
        None,
        "--select-columns",
        help="Comma-separated list of columns to fetch",
    ),
    filters: list[str] = typer.Option(
        [],
        "--filter",
        help="Filter expression like 'Status = ACTIVE'",
    ),
    chunk_size: int = typer.Option(10000, "--chunk-size", help="Rows per chunk"),
    pagination_column: str | None = typer.Option(None, "--pagination-column", help="Pagination column"),
    out: str = typer.Option("output.csv", "--out", help="Output file path (.csv/.xlsx/.xls/.parquet)"),
    max_workers: int | None = typer.Option(None, "--max-workers", help="Maximum concurrent source fetches"),
    export_profile: str | None = typer.Option(None, "--export-profile", help="Named export profile from config"),
    format_name: str | None = typer.Option(None, "--format", help="Output format: csv, xlsx, parquet"),
    delimiter: str = typer.Option(",", "--delimiter", help="CSV delimiter"),
    compression: str | None = typer.Option(None, "--compression", help="Parquet compression"),
    include_metadata: bool = typer.Option(False, "--include-metadata", help="Include Oznak metadata columns"),
    no_order_by: bool = typer.Option(
        False,
        "--no-order-by",
        help="Omit server-side ORDER BY. Not valid for chunked fetch.",
    ),
) -> None:
    try:
        loaded_profiles = load_database_profiles(config)
        query_request = QueryRequest.from_inputs(
            databases=databases,
            filters=filters,
            select_columns=select_columns,
            chunk_size=chunk_size,
            pagination_column=pagination_column,
            order_by_enabled=not no_order_by,
            max_workers=max_workers,
        )
        request = query_request.to_fetch_request(loaded_profiles)
        resolved_export_profile = _resolve_export_profile(
            config=config,
            profile_name=export_profile,
            format_name=format_name,
            delimiter=delimiter,
            compression=compression,
            include_metadata=include_metadata,
        )
    except (OznakConfigurationError, OznakValidationError, ValueError) as exc:
        _fail(f"Validation error: {exc}")

    provider = EnvironmentCredentialProvider()
    source_results = []

    def _frames():
        for event in iter_records_chunked(
            request,
            chunk_size=query_request.chunk_size or chunk_size,
            pagination_column=query_request.pagination_column,
            credential_provider=provider,
            max_workers=query_request.max_workers,
        ):
            if event.frame is not None:
                yield event.frame
            if event.diagnostics is not None:
                source_results.append(event.diagnostics)

    try:
        wrote_any = export_chunks_streaming(_frames(), out, profile=resolved_export_profile)
    except Exception as exc:
        _fail(f"Export error: {exc}")

    errors = [item.message for item in source_results if item.failed and item.message]
    if errors:
        _fail(f"Fetch error: {'; '.join(errors)}")
    if not wrote_any:
        typer.echo("No rows returned")
        raise typer.Exit(code=0)

    row_count = sum(item.row_count for item in source_results)
    typer.echo(f"Exported {row_count} rows to {out}")


@app.command("tui")
def tui(
    config: str = typer.Option("config/databases.yaml", "--config", help="Path to database profiles YAML"),
) -> None:
    exit_code = run_tui(config_path=config, output=typer.echo)
    raise typer.Exit(code=exit_code)


def main() -> None:
    app()
