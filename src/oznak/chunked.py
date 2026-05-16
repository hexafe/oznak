from __future__ import annotations

from collections.abc import Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from queue import Full, Queue
from threading import Event as ThreadEvent
from time import perf_counter
from typing import Any, Callable

import pandas as pd
from sqlalchemy import text

from oznak.credentials import CredentialProvider, Credentials
from oznak.diagnostics import SourceFetchDiagnostics, SourceFetchStatus
from oznak.engines import create_sqlalchemy_engine
from oznak.errors import OznakValidationError
from oznak.profiles import DatabaseProfile
from oznak.query_builder import QuerySpec, compile_query
from oznak.result import FetchRequest, FetchResult
from oznak.runtime import CancellationToken

EngineFactory = Callable[[DatabaseProfile, Credentials | None], Any]
ReadSqlCallable = Callable[..., pd.DataFrame]
ProgressCallback = Callable[[SourceFetchDiagnostics], None]


@dataclass(frozen=True)
class ChunkedFetchEvent:
    source_alias: str
    frame: pd.DataFrame | None = None
    diagnostics: SourceFetchDiagnostics | None = None


def fetch_records_chunked(
    request: FetchRequest,
    chunk_size: int,
    pagination_column: str | None = None,
    credential_provider: CredentialProvider | None = None,
    engine_factory: EngineFactory | None = None,
    read_sql: ReadSqlCallable | None = None,
    cancellation_token: CancellationToken | None = None,
    progress_callback: ProgressCallback | None = None,
    max_workers: int | None = None,
    max_pending_events: int | None = None,
) -> FetchResult:
    if type(chunk_size) is not int or chunk_size <= 0:
        raise OznakValidationError("chunk_size must be a positive integer")

    source_order = {profile.alias: index for index, profile in enumerate(request.profiles)}
    frames: list[tuple[int, pd.DataFrame]] = []
    source_results: list[SourceFetchDiagnostics] = []

    for event in iter_records_chunked(
        request,
        chunk_size=chunk_size,
        pagination_column=pagination_column,
        credential_provider=credential_provider,
        engine_factory=engine_factory,
        read_sql=read_sql,
        cancellation_token=cancellation_token,
        progress_callback=progress_callback,
        max_workers=max_workers,
        max_pending_events=max_pending_events,
    ):
        if event.frame is not None:
            frames.append((source_order.get(event.source_alias, len(source_order)), event.frame))
        if event.diagnostics is None:
            continue
        source_results.append(event.diagnostics)

    ordered_source_results = tuple(
        sorted(source_results, key=lambda item: source_order.get(item.source_alias, len(source_order)))
    )
    warnings = [
        diagnostics.message
        for diagnostics in ordered_source_results
        if diagnostics.status is SourceFetchStatus.NO_ROWS and diagnostics.message
    ]
    errors = [
        diagnostics.message
        for diagnostics in ordered_source_results
        if diagnostics.status in {
            SourceFetchStatus.FAILED,
            SourceFetchStatus.TIMEOUT,
            SourceFetchStatus.CANCELLED,
        }
        and diagnostics.message
    ]

    ordered_frames = [frame for _, frame in sorted(frames, key=lambda item: item[0])]
    combined_data = pd.concat(ordered_frames, ignore_index=True) if ordered_frames else pd.DataFrame()
    return FetchResult(
        data=combined_data,
        source_results=ordered_source_results,
        warnings=tuple(warnings),
        errors=tuple(errors),
    )


def iter_records_chunked(
    request: FetchRequest,
    chunk_size: int,
    pagination_column: str | None = None,
    credential_provider: CredentialProvider | None = None,
    engine_factory: EngineFactory | None = None,
    read_sql: ReadSqlCallable | None = None,
    cancellation_token: CancellationToken | None = None,
    progress_callback: ProgressCallback | None = None,
    max_workers: int | None = None,
    max_pending_events: int | None = None,
) -> Iterator[ChunkedFetchEvent]:
    if type(chunk_size) is not int or chunk_size <= 0:
        raise OznakValidationError("chunk_size must be a positive integer")

    resolved_engine_factory: EngineFactory = engine_factory or _default_engine_factory
    resolved_read_sql: ReadSqlCallable = read_sql or _default_read_sql
    worker_count = _normalize_max_workers(max_workers)
    queue_size = _normalize_max_pending_events(max_pending_events)

    if worker_count is None or worker_count == 1 or len(request.profiles) == 1:
        for profile in request.profiles:
            yield from _iter_single_source_chunks(
                profile=profile,
                request=request,
                chunk_size=chunk_size,
                pagination_column=pagination_column,
                credential_provider=credential_provider,
                engine_factory=resolved_engine_factory,
                read_sql=resolved_read_sql,
                cancellation_token=cancellation_token,
                progress_callback=progress_callback,
            )
        return

    yield from _iter_parallel_source_chunks(
        request=request,
        chunk_size=chunk_size,
        pagination_column=pagination_column,
        credential_provider=credential_provider,
        engine_factory=resolved_engine_factory,
        read_sql=resolved_read_sql,
        cancellation_token=cancellation_token,
        progress_callback=progress_callback,
        max_workers=min(worker_count, len(request.profiles)),
        queue_size=queue_size,
    )


@dataclass(frozen=True)
class _WorkerDone:
    source_alias: str


@dataclass(frozen=True)
class _WorkerFailed:
    source_alias: str
    error: BaseException


_ParallelQueueItem = ChunkedFetchEvent | _WorkerDone | _WorkerFailed


def _iter_parallel_source_chunks(
    *,
    request: FetchRequest,
    chunk_size: int,
    pagination_column: str | None,
    credential_provider: CredentialProvider | None,
    engine_factory: EngineFactory,
    read_sql: ReadSqlCallable,
    cancellation_token: CancellationToken | None,
    progress_callback: ProgressCallback | None,
    max_workers: int,
    queue_size: int,
) -> Iterator[ChunkedFetchEvent]:
    event_queue: Queue[_ParallelQueueItem] = Queue(maxsize=queue_size)
    stop_event = ThreadEvent()
    executor = ThreadPoolExecutor(max_workers=max_workers)
    futures: list[Future[None]] = []

    try:
        for profile in request.profiles:
            futures.append(
                executor.submit(
                    _run_chunk_worker,
                    event_queue=event_queue,
                    stop_event=stop_event,
                    profile=profile,
                    request=request,
                    chunk_size=chunk_size,
                    pagination_column=pagination_column,
                    credential_provider=credential_provider,
                    engine_factory=engine_factory,
                    read_sql=read_sql,
                    cancellation_token=cancellation_token,
                    progress_callback=progress_callback,
                )
            )

        completed_workers = 0
        while completed_workers < len(futures):
            item = event_queue.get()
            if isinstance(item, _WorkerDone):
                completed_workers += 1
                continue
            if isinstance(item, _WorkerFailed):
                stop_event.set()
                raise item.error
            yield item
    finally:
        stop_event.set()
        for future in futures:
            future.cancel()
        executor.shutdown(wait=False, cancel_futures=True)


def _run_chunk_worker(
    *,
    event_queue: Queue[_ParallelQueueItem],
    stop_event: ThreadEvent,
    profile: DatabaseProfile,
    request: FetchRequest,
    chunk_size: int,
    pagination_column: str | None,
    credential_provider: CredentialProvider | None,
    engine_factory: EngineFactory,
    read_sql: ReadSqlCallable,
    cancellation_token: CancellationToken | None,
    progress_callback: ProgressCallback | None,
) -> None:
    try:
        for event in _iter_single_source_chunks(
            profile=profile,
            request=request,
            chunk_size=chunk_size,
            pagination_column=pagination_column,
            credential_provider=credential_provider,
            engine_factory=engine_factory,
            read_sql=read_sql,
            cancellation_token=cancellation_token,
            progress_callback=progress_callback,
        ):
            if stop_event.is_set():
                return
            _put_queue_item(event_queue, event, stop_event)
    except BaseException as exc:  # pragma: no cover - normal source failures become diagnostics.
        _put_queue_item(event_queue, _WorkerFailed(source_alias=profile.alias, error=exc), stop_event)
    finally:
        _put_queue_item(event_queue, _WorkerDone(source_alias=profile.alias), stop_event)


def _put_queue_item(
    event_queue: Queue[_ParallelQueueItem],
    item: _ParallelQueueItem,
    stop_event: ThreadEvent,
) -> None:
    while not stop_event.is_set():
        try:
            event_queue.put(item, timeout=0.1)
            return
        except Full:
            continue


def _iter_single_source_chunks(
    *,
    profile: DatabaseProfile,
    request: FetchRequest,
    chunk_size: int,
    pagination_column: str | None,
    credential_provider: CredentialProvider | None,
    engine_factory: EngineFactory,
    read_sql: ReadSqlCallable,
    cancellation_token: CancellationToken | None,
    progress_callback: ProgressCallback | None,
) -> Iterator[ChunkedFetchEvent]:
    started_at = perf_counter()
    stage = "compile"
    query_summary = _base_query_summary(profile)
    source_row_count = 0
    last_pagination_value: Any = None
    chunk_count = 0
    resolved_pagination_column = pagination_column or profile.pagination_column

    try:
        _raise_if_cancelled(cancellation_token)
        stage = "credentials"
        credentials = credential_provider.get_credentials(profile.alias) if credential_provider is not None else None
        _raise_if_cancelled(cancellation_token)

        stage = "engine"
        engine = engine_factory(profile, credentials)
        _raise_if_cancelled(cancellation_token)

        while True:
            _raise_if_cancelled(cancellation_token)
            stage = "compile"
            query_spec = QuerySpec(
                filters=request.filters,
                columns=request.columns,
                chunk_size=chunk_size,
                pagination_column=pagination_column,
                last_pagination_value=last_pagination_value,
                order_by_enabled=_resolve_order_by_enabled(request, profile),
            )
            compiled = compile_query(profile, query_spec)
            query_summary = _compiled_query_summary(profile, compiled.sql)

            stage = "execute"
            params = dict(compiled.params)
            if params:
                frame = read_sql(compiled.sql, engine, params=params)
            else:
                frame = read_sql(compiled.sql, engine)
            _raise_if_cancelled(cancellation_token)
            _raise_if_timed_out(request, started_at)

            if frame.empty:
                break

            if resolved_pagination_column is None:
                raise OznakValidationError(
                    f"Chunked fetch requires a pagination column for source '{profile.alias}'"
                )

            _validate_chunk_pagination(frame, resolved_pagination_column, profile.alias)

            source_frame = frame.copy()
            source_frame["source_alias"] = profile.alias
            if "source_database" not in source_frame.columns:
                source_frame["source_database"] = profile.alias

            source_row_count += int(len(source_frame.index))
            last_pagination_value = source_frame[resolved_pagination_column].iloc[-1]
            chunk_count += 1
            yield ChunkedFetchEvent(source_alias=profile.alias, frame=source_frame)
    except Exception as exc:  # pragma: no cover - branch behavior is tested via public result surface.
        elapsed_seconds = perf_counter() - started_at
        status, error_code, message = _classify_source_exception(stage=stage, alias=profile.alias, exc=exc)
        diagnostics = SourceFetchDiagnostics(
            source_alias=profile.alias,
            status=status,
            row_count=source_row_count,
            elapsed_seconds=elapsed_seconds,
            error_code=error_code,
            message=message,
            query_summary=query_summary,
            metadata={"chunk_count": chunk_count},
        )
        _emit_progress(progress_callback, diagnostics)
        yield ChunkedFetchEvent(source_alias=profile.alias, diagnostics=diagnostics)
        return

    elapsed_seconds = perf_counter() - started_at
    if source_row_count == 0:
        message = f"Source '{profile.alias}' returned no rows"
        diagnostics = SourceFetchDiagnostics(
            source_alias=profile.alias,
            status=SourceFetchStatus.NO_ROWS,
            row_count=0,
            elapsed_seconds=elapsed_seconds,
            message=message,
            query_summary=query_summary,
            metadata={"chunk_count": 0},
        )
        _emit_progress(progress_callback, diagnostics)
        yield ChunkedFetchEvent(source_alias=profile.alias, diagnostics=diagnostics)
        return

    diagnostics = SourceFetchDiagnostics(
        source_alias=profile.alias,
        status=SourceFetchStatus.SUCCESS,
        row_count=source_row_count,
        elapsed_seconds=elapsed_seconds,
        message=f"Fetched {source_row_count} rows from source '{profile.alias}'",
        query_summary=query_summary,
        metadata={"chunk_count": chunk_count},
    )
    _emit_progress(progress_callback, diagnostics)
    yield ChunkedFetchEvent(source_alias=profile.alias, diagnostics=diagnostics)


def _default_engine_factory(profile: DatabaseProfile, credentials: Credentials | None) -> Any:
    return create_sqlalchemy_engine(profile, credentials)


def _default_read_sql(sql: str, engine: Any, params: dict[str, object] | None = None) -> pd.DataFrame:
    statement = text(sql)
    if params:
        return pd.read_sql(statement, engine, params=params)
    return pd.read_sql(statement, engine)


def _resolve_order_by_enabled(request: FetchRequest, profile: DatabaseProfile) -> bool:
    if request.order_by_enabled is None:
        return profile.order_by_enabled
    return request.order_by_enabled


def _normalize_max_workers(max_workers: int | None) -> int | None:
    if max_workers is None:
        return None
    if type(max_workers) is not int or max_workers <= 0:
        raise OznakValidationError("max_workers must be a positive integer")
    return max_workers


def _normalize_max_pending_events(max_pending_events: int | None) -> int:
    if max_pending_events is None:
        return 16
    if type(max_pending_events) is not int or max_pending_events <= 0:
        raise OznakValidationError("max_pending_events must be a positive integer")
    return max_pending_events


def _raise_if_cancelled(cancellation_token: CancellationToken | None) -> None:
    if cancellation_token is not None:
        cancellation_token.raise_if_cancelled()


def _raise_if_timed_out(request: FetchRequest, started_at: float) -> None:
    if request.timeout_seconds is not None and perf_counter() - started_at > request.timeout_seconds:
        raise TimeoutError(f"exceeded timeout of {request.timeout_seconds} seconds")


def _emit_progress(progress_callback: ProgressCallback | None, diagnostics: SourceFetchDiagnostics) -> None:
    if progress_callback is not None:
        progress_callback(diagnostics)


def _base_query_summary(profile: DatabaseProfile) -> str:
    return f"{profile.alias} {profile.redacted_location}"


def _compiled_query_summary(profile: DatabaseProfile, sql: str) -> str:
    condensed_sql = " ".join(sql.split())
    if len(condensed_sql) > 280:
        condensed_sql = f"{condensed_sql[:277]}..."
    return f"{_base_query_summary(profile)} sql={condensed_sql}"


def _validate_chunk_pagination(frame: pd.DataFrame, pagination_column: str, alias: str) -> None:
    if pagination_column not in frame.columns:
        raise OznakValidationError(
            f"Source '{alias}' chunk is missing pagination column '{pagination_column}' in fetched rows"
        )
    if frame[pagination_column].duplicated().any():
        raise OznakValidationError(
            f"Source '{alias}' chunk has non-unique pagination values in column '{pagination_column}'"
        )


def _classify_source_exception(
    *,
    stage: str,
    alias: str,
    exc: Exception,
) -> tuple[SourceFetchStatus, str, str]:
    if isinstance(exc, TimeoutError):
        return (
            SourceFetchStatus.TIMEOUT,
            f"{stage}_timeout",
            f"Source '{alias}' timed out during {stage}: {exc}",
        )
    if isinstance(exc, InterruptedError):
        return (
            SourceFetchStatus.CANCELLED,
            f"{stage}_cancelled",
            f"Source '{alias}' was cancelled during {stage}: {exc}",
        )
    if stage == "compile":
        if isinstance(exc, OznakValidationError):
            return (
                SourceFetchStatus.FAILED,
                "compile_validation_error",
                f"Source '{alias}' query validation failed: {exc}",
            )
        return (
            SourceFetchStatus.FAILED,
            "compile_error",
            f"Source '{alias}' query compilation failed: {exc}",
        )
    if stage == "credentials":
        return (
            SourceFetchStatus.FAILED,
            "credential_error",
            f"Source '{alias}' credential resolution failed: {exc}",
        )
    if stage == "engine":
        return (
            SourceFetchStatus.FAILED,
            "engine_error",
            f"Source '{alias}' engine initialization failed: {exc}",
        )
    return (
        SourceFetchStatus.FAILED,
        "query_error",
        f"Source '{alias}' query execution failed: {exc}",
    )
