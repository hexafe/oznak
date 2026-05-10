from __future__ import annotations

import typer

from oznak import __version__
from oznak.config import load_database_profiles
from oznak.credentials import EnvironmentCredentialProvider
from oznak.errors import OznakConfigurationError, OznakValidationError
from oznak.fetcher import fetch_records
from oznak.filters import QueryFilter, parse_legacy_filter
from oznak.profiles import DatabaseProfile, validate_identifier
from oznak.result import FetchRequest
from oznak.tui import export_output, run_tui

app = typer.Typer(help="Oznak package CLI")


def _fail(message: str, code: int = 1) -> None:
    typer.echo(message, err=True)
    raise typer.Exit(code=code)


def _parse_aliases(raw_aliases: str) -> tuple[str, ...]:
    aliases = tuple(alias.strip() for alias in raw_aliases.split(",") if alias.strip())
    if not aliases:
        raise OznakValidationError("At least one database alias is required")
    return tuple(validate_identifier(alias, field_name="profile alias") for alias in aliases)


def _parse_select_columns(raw_columns: str | None) -> tuple[str, ...] | None:
    if raw_columns is None:
        return None
    columns = tuple(column.strip() for column in raw_columns.split(",") if column.strip())
    if not columns:
        return None
    return tuple(validate_identifier(column, field_name="selected column") for column in columns)


def _resolve_profiles(loaded_profiles: dict[str, DatabaseProfile], aliases: tuple[str, ...]) -> tuple[DatabaseProfile, ...]:
    profiles: list[DatabaseProfile] = []
    missing: list[str] = []
    for alias in aliases:
        profile = loaded_profiles.get(alias)
        if profile is None:
            missing.append(alias)
            continue
        profiles.append(profile)

    if missing:
        joined = ", ".join(missing)
        raise OznakConfigurationError(f"Unknown database alias(es) in config: {joined}")
    return tuple(profiles)


def _parse_filters(raw_filters: tuple[str, ...]) -> tuple[QueryFilter, ...]:
    return tuple(parse_legacy_filter(value) for value in raw_filters)


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
    out: str = typer.Option("output.csv", "--out", help="Output file path (.csv/.xlsx/.xls)"),
) -> None:
    try:
        aliases = _parse_aliases(databases)
        selected_columns = _parse_select_columns(select_columns)
        parsed_filters = _parse_filters(tuple(filters))
        if last is not None and last <= 0:
            raise OznakValidationError("'last' must be a positive integer")
        if last is not None:
            validate_identifier(date_col, field_name="date column")

        loaded_profiles = load_database_profiles(config)
        request = FetchRequest(
            profiles=_resolve_profiles(loaded_profiles, aliases),
            filters=parsed_filters,
            columns=selected_columns,
            limit=last,
            date_column=date_col if last is not None else None,
        )
    except (OznakConfigurationError, OznakValidationError, ValueError) as exc:
        _fail(f"Validation error: {exc}")

    provider = EnvironmentCredentialProvider()
    try:
        result = fetch_records(request, credential_provider=provider)
    except Exception as exc:
        _fail(f"Fetch error: {exc}")

    if result.has_errors:
        combined_errors = "; ".join(result.errors) if result.errors else "Fetch failed"
        _fail(f"Fetch error: {combined_errors}")

    if getattr(result.data, "empty", True):
        typer.echo("No rows returned")
        raise typer.Exit(code=0)

    try:
        export_output(result.data, out)
    except Exception as exc:
        _fail(f"Export error: {exc}")

    typer.echo(f"Exported {result.row_count} rows to {out}")


@app.command("tui")
def tui(
    config: str = typer.Option("config/databases.yaml", "--config", help="Path to database profiles YAML"),
) -> None:
    exit_code = run_tui(config_path=config, output=typer.echo)
    raise typer.Exit(code=exit_code)


def main() -> None:
    app()
