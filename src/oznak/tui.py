from __future__ import annotations

from pathlib import Path
from typing import Callable

from oznak.config import load_database_profiles
from oznak.credentials import EnvironmentCredentialProvider
from oznak.diagnostics import SourceFetchDiagnostics
from oznak.errors import OznakConfigurationError, OznakValidationError
from oznak.fetcher import fetch_records
from oznak.filters import QueryFilter, parse_legacy_filter
from oznak.profiles import DatabaseProfile, validate_identifier
from oznak.result import FetchRequest
from oznak.runtime import CancellationToken

Prompt = Callable[[str], str]
Output = Callable[[str], None]


def run_tui(
    *,
    config_path: str,
    prompt: Prompt = input,
    output: Output = print,
) -> int:
    try:
        loaded_profiles = load_database_profiles(config_path)
        if not loaded_profiles:
            raise OznakConfigurationError("No database profiles found in config")

        aliases = tuple(sorted(loaded_profiles))
        selected_aliases = _prompt_alias_selection(aliases=aliases, prompt=prompt, output=output)
        selected_profiles = tuple(loaded_profiles[alias] for alias in selected_aliases)

        selected_columns = _prompt_select_columns(selected_profiles, prompt=prompt, output=output)
        parsed_filters = _prompt_filters(prompt=prompt, output=output)
        last, date_column = _prompt_limit(prompt=prompt)
        out_path = _prompt_output_path(prompt=prompt)

        should_run = _prompt_yes_no("Run fetch now? [y/N]: ", default=False, prompt=prompt)
        if not should_run:
            output("Aborted by user")
            return 1

        request = FetchRequest(
            profiles=selected_profiles,
            filters=parsed_filters,
            columns=selected_columns,
            limit=last,
            date_column=date_column if last is not None else None,
        )
    except (OznakConfigurationError, OznakValidationError, ValueError) as exc:
        output(f"Validation error: {exc}")
        return 1
    except KeyboardInterrupt:
        output("Aborted by user")
        return 1

    cancellation_token = CancellationToken()
    provider = EnvironmentCredentialProvider()

    def _progress_callback(diagnostics: SourceFetchDiagnostics) -> None:
        _print_progress(diagnostics, output=output)

    try:
        result = fetch_records(
            request,
            credential_provider=provider,
            cancellation_token=cancellation_token,
            progress_callback=_progress_callback,
        )
    except KeyboardInterrupt:
        cancellation_token.cancel()
        output("Cancelled")
        return 1
    except Exception as exc:
        output(f"Fetch error: {exc}")
        return 1

    if result.has_errors:
        combined_errors = "; ".join(result.errors) if result.errors else "Fetch failed"
        output(f"Fetch error: {combined_errors}")
        return 1

    if getattr(result.data, "empty", True):
        output("No rows returned")
        return 0

    try:
        export_output(result.data, out_path)
    except Exception as exc:
        output(f"Export error: {exc}")
        return 1

    output(f"Exported {result.row_count} rows to {out_path}")
    return 0


def export_output(data, out_path: str) -> None:
    destination = Path(out_path)
    extension = destination.suffix.lower()
    if extension == ".csv":
        data.to_csv(destination, index=False)
        return
    if extension in {".xlsx", ".xls"}:
        data.to_excel(destination, index=False)
        return
    raise OznakValidationError("Unsupported output format. Use .csv, .xlsx, or .xls")


def _prompt_alias_selection(*, aliases: tuple[str, ...], prompt: Prompt, output: Output) -> tuple[str, ...]:
    output("Available database profiles:")
    for index, alias in enumerate(aliases, start=1):
        output(f"  {index}. {alias}")

    raw = prompt("Select profile aliases (comma-separated aliases or numbers): ").strip()
    if not raw:
        raise OznakValidationError("At least one database alias is required")

    selected_aliases: list[str] = []
    seen: set[str] = set()

    for token in (part.strip() for part in raw.split(",")):
        if not token:
            continue
        if token.isdigit():
            index = int(token)
            if index < 1 or index > len(aliases):
                raise OznakValidationError(f"Invalid profile selection index: {token}")
            alias = aliases[index - 1]
        else:
            alias = validate_identifier(token, field_name="profile alias")
            if alias not in aliases:
                raise OznakConfigurationError(f"Unknown database alias in config: {alias}")
        if alias not in seen:
            seen.add(alias)
            selected_aliases.append(alias)

    if not selected_aliases:
        raise OznakValidationError("At least one database alias is required")
    return tuple(selected_aliases)


def _prompt_select_columns(
    profiles: tuple[DatabaseProfile, ...],
    *,
    prompt: Prompt,
    output: Output,
) -> tuple[str, ...] | None:
    allowed_columns: set[str] = set()
    for profile in profiles:
        allowed_columns.update(profile.allowed_columns)

    if allowed_columns:
        output("Allowed columns:")
        output("  " + ", ".join(sorted(allowed_columns)))

    raw = prompt("Select columns (comma-separated, leave blank for all columns): ").strip()
    if not raw:
        return None

    columns = tuple(validate_identifier(item.strip(), field_name="selected column") for item in raw.split(",") if item.strip())
    if not columns:
        return None

    for profile in profiles:
        profile.require_columns(columns)
    return columns


def _prompt_filters(*, prompt: Prompt, output: Output) -> tuple[QueryFilter, ...]:
    filters: list[QueryFilter] = []
    output("Enter filters one per line in format: column operator value")
    output("Leave blank to continue without adding more filters")
    while True:
        raw = prompt("Filter: ").strip()
        if not raw:
            break
        filters.append(parse_legacy_filter(raw))
    return tuple(filters)


def _prompt_limit(*, prompt: Prompt) -> tuple[int | None, str | None]:
    raw_last = prompt("Last N records (leave blank for all records): ").strip()
    if not raw_last:
        return None, None

    try:
        last = int(raw_last)
    except ValueError as exc:
        raise OznakValidationError("'last' must be a positive integer") from exc

    if last <= 0:
        raise OznakValidationError("'last' must be a positive integer")

    date_column = prompt("Date column used for ordering [TimeStamp]: ").strip() or "TimeStamp"
    validate_identifier(date_column, field_name="date column")
    return last, date_column


def _prompt_output_path(*, prompt: Prompt) -> str:
    out_path = prompt("Output file path [.csv/.xlsx/.xls] [output.csv]: ").strip() or "output.csv"
    extension = Path(out_path).suffix.lower()
    if extension not in {".csv", ".xlsx", ".xls"}:
        raise OznakValidationError("Unsupported output format. Use .csv, .xlsx, or .xls")
    return out_path


def _prompt_yes_no(question: str, *, default: bool, prompt: Prompt) -> bool:
    raw = prompt(question).strip().lower()
    if not raw:
        return default
    if raw in {"y", "yes"}:
        return True
    if raw in {"n", "no"}:
        return False
    raise OznakValidationError("Please answer 'y' or 'n'")


def _print_progress(diagnostics: SourceFetchDiagnostics, *, output: Output) -> None:
    elapsed = f"{diagnostics.elapsed_seconds:.2f}s" if diagnostics.elapsed_seconds is not None else "n/a"
    base = f"[{diagnostics.status.value}] {diagnostics.source_alias} rows={diagnostics.row_count} elapsed={elapsed}"
    if diagnostics.message:
        base = f"{base} {diagnostics.message}"
    output(base)
