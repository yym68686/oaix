import asyncio
import base64
import codecs
import contextvars
import copy
import hashlib
import inspect
import json
import logging
import math
import mimetypes
import os
import re
import time
import uuid
from collections import deque
from collections.abc import AsyncGenerator, AsyncIterator, Iterable
from contextlib import asynccontextmanager, contextmanager, suppress
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable

import anyio
import httpx
import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict
from sqlalchemy.exc import SQLAlchemyError
from starlette.datastructures import UploadFile
from .chat_image_store import create_chat_image_checkpoint, resolve_chat_image_output_items
from .codex_constants import CODEX_CLI_VERSION, CODEX_USER_AGENT
from .database import (
    close_database,
    init_db,
    reset_db_pool_role,
    reset_db_timing_recorder,
    set_db_pool_role,
    set_db_timing_recorder,
    utcnow,
)
from .oauth import CodexOAuthManager, is_permanently_invalid_refresh_token_error
from .quota import CodexPlanInfo, CodexQuotaService, CodexQuotaSnapshot, extract_codex_plan_info
from .request_store import (
    create_request_log,
    finalize_request_log,
    get_request_log_analytics,
    get_request_costs_by_token,
    get_request_costs_by_account,
    get_request_log_summary,
    list_request_logs,
    upsert_request_logs,
)
from .token_import_jobs import (
    TokenImportBackgroundWorker,
    TokenImportBatchSummary,
    create_token_import_job,
    get_token_import_job,
    job_state_from_lease,
    list_token_import_batch_summaries,
)
from .token_store import (
    DEFAULT_TOKEN_SELECTION_STRATEGY,
    DEFAULT_TOKEN_IMPORT_QUEUE_POSITION,
    DEFAULT_TOKEN_ACTIVE_STREAM_CAP,
    DEFAULT_TOKEN_PLAN_ORDER,
    TOKEN_PLAN_TYPE_FREE,
    TOKEN_SELECTION_STRATEGY_FILL_FIRST,
    TOKEN_LIST_PLAN_ALL,
    TOKEN_LIST_SORT_NEWEST,
    TOKEN_LIST_STATUS_ALL,
    TokenSelectionSettings,
    claim_next_active_token,
    count_token_rows,
    delete_token,
    get_token_row,
    get_token_counts,
    get_token_counts_by_account_ids,
    get_token_plan_counts,
    list_token_pool_rows,
    list_token_pool_scoped_cooldowns,
    get_token_selection_settings,
    get_token_status_counts,
    list_token_rows,
    mark_token_error,
    mark_token_scoped_cooldown,
    mark_token_success,
    normalize_token_active_stream_cap,
    prewarm_fill_first_token_cache,
    repair_duplicate_token_histories,
    set_token_active_state,
    stop_token_status_write_queue,
    token_is_runtime_available,
    parse_token_import_queue_position,
    update_token_active_stream_cap_settings,
    update_token_order_settings,
    update_token_plan_order_settings,
    update_token_selection_settings,
)
from .usage_cost import UsageMetrics, extract_usage_metrics


logger = logging.getLogger("oaix.gateway")
_ORIGINAL_CREATE_REQUEST_LOG = create_request_log
_ORIGINAL_FINALIZE_REQUEST_LOG = finalize_request_log
WEB_DIR = Path(__file__).resolve().parent / "web"
ADMIN_TOKEN_PROBE_INPUT = "say test"
DEFAULT_IMAGES_MAIN_MODEL = "gpt-5.5"
DEFAULT_IMAGES_TOOL_MODEL = "gpt-image-2"
IMAGE_INPUT_RATE_LIMIT_COOLDOWN_SCOPE = f"{DEFAULT_IMAGES_TOOL_MODEL}:input-images"
NON_FREE_ONLY_CODEX_MODELS = frozenset({DEFAULT_IMAGES_TOOL_MODEL})
RESPONSES_IMAGE_COMPAT_MODELS = frozenset({DEFAULT_IMAGES_TOOL_MODEL})
KNOWN_NON_FREE_TOKEN_PLAN_TYPES = tuple(
    plan_type for plan_type in DEFAULT_TOKEN_PLAN_ORDER if plan_type != TOKEN_PLAN_TYPE_FREE
)
RESPONSES_IMAGE_TOOL_TEXT_FIELDS = (
    "size",
    "quality",
    "background",
    "output_format",
    "input_fidelity",
    "moderation",
)
RESPONSES_IMAGE_TOOL_INT_FIELDS = (
    "output_compression",
    "partial_images",
)
CHAT_COMPLETIONS_RESPONSES_PASSTHROUGH_FIELDS = frozenset(
    {
        "model",
        "stream",
        "temperature",
        "top_p",
        "frequency_penalty",
        "presence_penalty",
        "parallel_tool_calls",
        "reasoning",
        "tools",
        "tool_choice",
        "store",
        "metadata",
        "previous_response_id",
        "size",
        "quality",
        "background",
        "output_format",
        "input_fidelity",
        "moderation",
        "output_compression",
        "partial_images",
    }
)
CHAT_COMPLETIONS_MARKDOWN_IMAGE_RE = re.compile(r"!\[[^\]]*\]\(([^)\s]+)\)")
RESPONSES_STREAM_PREFLIGHT_EVENTS = frozenset(
    {
        "response.created",
        "response.in_progress",
        "response.queued",
    }
)
RESPONSES_STREAM_NETWORK_ERRORS = (
    httpx.ReadError,
    httpx.RemoteProtocolError,
    httpx.LocalProtocolError,
    httpx.ReadTimeout,
    httpx.ConnectError,
)
DEFAULT_STREAM_KEEPALIVE_INTERVAL_SECONDS = 30.0
STREAM_KEEPALIVE_PADDING_BYTES = 2048
STREAM_KEEPALIVE_COMMENT = b": keepalive" + (b" " * STREAM_KEEPALIVE_PADDING_BYTES) + b"\n\n"
_SSE_SEPARATOR_TAIL_CHARS = 3
_SSE_EVENT_SEPARATORS = ("\r\n\r\n", "\r\n\n", "\n\r\n", "\n\n")


def _find_sse_event_separator(text: str, start: int = 0) -> tuple[int, int] | None:
    start = max(0, min(start, len(text)))
    best: tuple[int, int] | None = None
    for separator in _SSE_EVENT_SEPARATORS:
        index = text.find(separator, start)
        if index < 0:
            continue
        end = index + len(separator)
        if best is None or index < best[0] or (index == best[0] and end > best[1]):
            best = (index, end)
    return best


def _pop_sse_event_from_buffer(text_buffer: str, scan_start: int = 0) -> tuple[str | None, str, int]:
    separator = _find_sse_event_separator(text_buffer, scan_start)
    if separator is None:
        return None, text_buffer, max(0, len(text_buffer) - _SSE_SEPARATOR_TAIL_CHARS)
    start, end = separator
    return text_buffer[:start], text_buffer[end:], 0


class _RequestTimingRecorder:
    def __init__(self) -> None:
        self._spans: dict[str, int | str] = {}

    def add_elapsed(self, name: str, started_at: float) -> None:
        self.add_ms(name, int((time.perf_counter() - started_at) * 1000))

    def add_ms(self, name: str, value_ms: int | float) -> None:
        key = str(name or "").strip()
        if not key:
            return
        try:
            value = max(0, int(round(float(value_ms))))
        except (TypeError, ValueError):
            return
        self._spans[key] = self._spans.get(key, 0) + value

    def set_ms(self, name: str, value_ms: int | float) -> None:
        key = str(name or "").strip()
        if not key:
            return
        try:
            value = max(0, int(round(float(value_ms))))
        except (TypeError, ValueError):
            return
        self._spans[key] = value

    def set_tag(self, name: str, value: str) -> None:
        key = str(name or "").strip()
        text = str(value or "").strip()
        if key and text:
            self._spans[key] = text[:128]

    @contextmanager
    def measure(self, name: str):
        started_at = time.perf_counter()
        try:
            yield
        finally:
            self.add_elapsed(name, started_at)

    def snapshot(self) -> dict[str, int | str]:
        return dict(self._spans)


def _merge_timing_spans(*spans: dict[str, Any] | None) -> dict[str, int | str] | None:
    merged: dict[str, int | str] = {}
    for span_map in spans:
        if not isinstance(span_map, dict):
            continue
        for key, value in span_map.items():
            name = str(key or "").strip()
            if not name or isinstance(value, bool):
                continue
            if isinstance(value, str):
                text = value.strip()
                if text:
                    merged[name] = text[:128]
                continue
            try:
                resolved = max(0, int(round(float(value))))
            except (TypeError, ValueError):
                continue
            previous = merged.get(name, 0)
            merged[name] = (previous if isinstance(previous, int) else 0) + resolved
    return merged or None


async def _finalize_request_log_with_timing(
    timing: _RequestTimingRecorder | None,
    request_log_ref: Any,
    *,
    success_hook: Callable[[int], Awaitable[None]] | None = None,
    **kwargs,
) -> int | None:
    finalize_method = getattr(request_log_ref, "finalize", None)
    if callable(finalize_method):
        request_log_id, resolved_spans = await finalize_method(timing=timing, success_hook=success_hook, **kwargs)
    else:
        if timing is not None and "timing_spans" not in kwargs:
            kwargs["timing_spans"] = timing.snapshot()
        request_log_id = int(request_log_ref)
        resolved_spans = await finalize_request_log(request_log_id, **kwargs)
        if success_hook is not None:
            await _run_request_log_success_hooks([success_hook], request_log_id)

    if resolved_spans:
        logger.info(
            "Request timing spans: request_log_id=%s request_id=%s spans=%s",
            request_log_id,
            getattr(request_log_ref, "request_id", None),
            json.dumps(resolved_spans, sort_keys=True, separators=(",", ":")),
        )
    return request_log_id


_REQUEST_TIMING: contextvars.ContextVar[_RequestTimingRecorder | None] = contextvars.ContextVar(
    "oaix_request_timing",
    default=None,
)
_UPSTREAM_HTTP_PHASE_TIMING: contextvars.ContextVar[_RequestTimingRecorder | None] = contextvars.ContextVar(
    "oaix_upstream_http_phase_timing",
    default=None,
)
_UPSTREAM_HTTP_PHASE_COMPACT: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "oaix_upstream_http_phase_compact",
    default=False,
)
_TOKEN_ACTIVE_REQUESTS: dict[int, int] = {}
_TOKEN_RECENT_TTFT_MS: dict[int, deque[int]] = {}
_TOKEN_RECENT_TTFT_LIMIT = 200
DEFAULT_FILL_FIRST_TOKEN_ACTIVE_STREAM_CAP = DEFAULT_TOKEN_ACTIVE_STREAM_CAP
DEFAULT_UPSTREAM_HTTP_MAX_CONNECTIONS = 512
DEFAULT_UPSTREAM_HTTP_MAX_KEEPALIVE_CONNECTIONS = 128


def _current_request_timing() -> _RequestTimingRecorder | None:
    return _REQUEST_TIMING.get()


def _percentile(values: list[int], percentile: float) -> int | None:
    if not values:
        return None
    if len(values) == 1:
        return values[0]
    ordered = sorted(values)
    index = int(math.ceil((percentile / 100.0) * len(ordered))) - 1
    return ordered[max(0, min(index, len(ordered) - 1))]


def _token_active_stream_count(token_id: int) -> int:
    try:
        resolved_token_id = int(token_id)
    except (TypeError, ValueError):
        return 0
    return max(0, int(_TOKEN_ACTIVE_REQUESTS.get(resolved_token_id, 0) or 0))


def _record_selected_token_observation_start(token_id: int, timing: _RequestTimingRecorder | None) -> None:
    active_count = _token_active_stream_count(token_id)
    if timing is not None:
        timing.set_ms("selected_token_active_streams", active_count)
        recent = list(_TOKEN_RECENT_TTFT_MS.get(token_id) or ())
        recent_p50 = _percentile(recent, 50)
        recent_p90 = _percentile(recent, 90)
        if recent_p50 is not None:
            timing.set_ms("selected_token_recent_ttft_p50", recent_p50)
        if recent_p90 is not None:
            timing.set_ms("selected_token_recent_ttft_p90", recent_p90)
    _TOKEN_ACTIVE_REQUESTS[token_id] = active_count + 1


def _record_selected_token_observation_finish(
    token_id: int,
    *,
    started_at: datetime,
    first_token_at: datetime | None,
) -> None:
    current = _TOKEN_ACTIVE_REQUESTS.get(token_id, 0)
    if current <= 1:
        _TOKEN_ACTIVE_REQUESTS.pop(token_id, None)
    else:
        _TOKEN_ACTIVE_REQUESTS[token_id] = current - 1
    ttft_ms = int((first_token_at - started_at).total_seconds() * 1000) if first_token_at is not None else None
    if ttft_ms is None or ttft_ms < 0:
        return
    bucket = _TOKEN_RECENT_TTFT_MS.get(token_id)
    if bucket is None:
        bucket = deque(maxlen=_TOKEN_RECENT_TTFT_LIMIT)
        _TOKEN_RECENT_TTFT_MS[token_id] = bucket
    bucket.append(ttft_ms)


class _HTTPPhaseTraceRecorder:
    def __init__(self, timing: _RequestTimingRecorder) -> None:
        self._timing = timing
        self._started_at = time.perf_counter()
        self._first_trace_seen = False
        self._operation_started_at: dict[str, float] = {}

    async def record(self, name: str, info: dict[str, Any]) -> None:
        del info
        parts = name.split(".")
        if len(parts) < 2:
            return

        phase = parts[-1]
        operation = parts[-2]
        now = time.perf_counter()
        if phase == "started":
            if not self._first_trace_seen:
                self._first_trace_seen = True
                self._timing.add_ms("http_pool_wait_ms", (now - self._started_at) * 1000)
            self._operation_started_at[operation] = now
            return

        if phase not in {"complete", "failed"}:
            return

        started_at = self._operation_started_at.pop(operation, None)
        if started_at is None:
            return

        elapsed_ms = (now - started_at) * 1000
        if operation in {"connect_tcp", "connect_unix_socket"}:
            self._timing.add_ms("http_connect_ms", elapsed_ms)
        elif operation == "start_tls":
            self._timing.add_ms("http_tls_ms", elapsed_ms)
        elif operation in {"send_request_headers", "send_request_body"}:
            self._timing.add_ms("http_request_write_ms", elapsed_ms)
            self._timing.add_ms(f"http_{operation}_ms", elapsed_ms)
        elif operation == "receive_response_headers":
            if _UPSTREAM_HTTP_PHASE_COMPACT.get():
                self._timing.add_ms("upstream_compact_headers_ms", elapsed_ms)
            else:
                self._timing.add_ms("upstream_headers_ms", elapsed_ms)
        elif operation == "retry":
            self._timing.add_ms("http_connect_retry_sleep_ms", elapsed_ms)


class _TimingAsyncHTTPTransport(httpx.AsyncBaseTransport):
    def __init__(self, **kwargs: Any) -> None:
        self._transport = httpx.AsyncHTTPTransport(**kwargs)

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        timing = _UPSTREAM_HTTP_PHASE_TIMING.get()
        if timing is None:
            return await self._transport.handle_async_request(request)

        recorder = _HTTPPhaseTraceRecorder(timing)
        previous_trace = request.extensions.get("trace")

        async def trace(name: str, info: dict[str, Any]) -> None:
            await recorder.record(name, info)
            if previous_trace is None:
                return
            result = previous_trace(name, info)
            if inspect.isawaitable(result):
                await result

        request.extensions["trace"] = trace
        return await self._transport.handle_async_request(request)

    async def aclose(self) -> None:
        await self._transport.aclose()


async def _record_event_loop_lag(timing: _RequestTimingRecorder | None) -> None:
    if timing is None:
        await asyncio.sleep(0)
        return
    started_at = time.perf_counter()
    await asyncio.sleep(0)
    timing.add_elapsed("event_loop_lag_ms", started_at)


async def _await_timed(
    awaitable: Awaitable[Any],
    *,
    timing: _RequestTimingRecorder | None,
    span_name: str | None = None,
    db_wait: bool = False,
    output_queue: asyncio.Queue[bytes | object] | None = None,
    heartbeat_interval_seconds: float | None = None,
) -> Any:
    started_at = time.perf_counter()
    try:
        with timing.measure(span_name) if timing is not None and span_name else suppress():
            if output_queue is None:
                return await awaitable
            return await _await_with_keepalive_queue(
                awaitable,
                output_queue=output_queue,
                heartbeat_interval_seconds=(
                    heartbeat_interval_seconds
                    if heartbeat_interval_seconds is not None
                    else _stream_keepalive_interval_seconds()
                ),
            )
    finally:
        if timing is not None and db_wait:
            timing.add_elapsed("db_query_ms", started_at)


def _token_row_success_mark_is_noop(token_row: Any | None) -> bool:
    if token_row is None:
        return False
    return (
        getattr(token_row, "merged_into_token_id", None) is None
        and getattr(token_row, "is_active", None) is True
        and getattr(token_row, "cooldown_until", None) is None
        and not getattr(token_row, "last_error", None)
    )


async def _mark_token_success_with_timing(
    timing: _RequestTimingRecorder | None,
    token_id: int,
    *,
    token_row: Any | None = None,
) -> None:
    if _token_row_success_mark_is_noop(token_row):
        return
    await _await_timed(
        mark_token_success(token_id),
        timing=timing,
        span_name="mark_success_ms",
        db_wait=False,
    )


@dataclass
class _TokenPoolSnapshot:
    tokens: tuple[Any, ...]
    scoped_cooldowns: dict[tuple[int, str], datetime]
    loaded_at: float


def _token_plan_sort_value(token_row: Any, settings: TokenSelectionSettings) -> int:
    if not settings.plan_order_enabled:
        return 0
    plan_order = list(settings.plan_order)
    plan_map = {plan_type: index for index, plan_type in enumerate(plan_order)}
    plan_type = str(getattr(token_row, "plan_type", "") or "").strip().lower()
    if plan_type.startswith("chatgpt_"):
        plan_type = plan_type[len("chatgpt_") :]
    return plan_map.get(plan_type, len(plan_map))


def _normalize_token_plan_value(value: Any) -> str | None:
    normalized = str(value or "").strip().lower()
    if normalized.startswith("chatgpt_"):
        normalized = normalized[len("chatgpt_") :]
    return normalized or None


def _normalize_token_plan_filter(values: Iterable[str] | None) -> set[str]:
    if values is None:
        return set()
    return {
        plan_type
        for value in values
        if (plan_type := _normalize_token_plan_value(value)) is not None
    }


def _token_row_prefilter_plan_type(token_row: Any) -> str | None:
    stored_plan_type = _normalize_token_plan_value(getattr(token_row, "plan_type", None))
    if stored_plan_type is not None:
        return stored_plan_type
    return _declared_token_plan_info(token_row).plan_type


def _token_row_matches_plan_filters(
    token_row: Any,
    *,
    include_plan_types: Iterable[str] | None,
    exclude_plan_types: Iterable[str] | None,
) -> bool:
    include = _normalize_token_plan_filter(include_plan_types)
    exclude = _normalize_token_plan_filter(exclude_plan_types)
    if not include and not exclude:
        return True
    plan_type = _token_row_prefilter_plan_type(token_row)
    if include and plan_type not in include:
        return False
    if plan_type is not None and plan_type in exclude:
        return False
    return True


def _token_order_sort_value(token_row: Any, settings: TokenSelectionSettings) -> int:
    order = list(settings.token_order)
    if not order:
        return int(getattr(token_row, "id", 0) or 0)
    order_map = {int(token_id): index for index, token_id in enumerate(order)}
    return order_map.get(int(getattr(token_row, "id", 0) or 0), len(order_map))


def _datetime_sort_value(value: Any) -> tuple[int, float]:
    if value is None:
        return (0, 0.0)
    if isinstance(value, datetime):
        return (1, value.timestamp())
    return (1, 0.0)


def _token_snapshot_ordered_tokens(snapshot: _TokenPoolSnapshot, settings: TokenSelectionSettings) -> list[Any]:
    tokens = list(snapshot.tokens)
    if settings.strategy == TOKEN_SELECTION_STRATEGY_FILL_FIRST:
        return sorted(
            tokens,
            key=lambda token_row: (
                _token_plan_sort_value(token_row, settings),
                _token_order_sort_value(token_row, settings),
                int(getattr(token_row, "id", 0) or 0),
            ),
        )
    return sorted(
        tokens,
        key=lambda token_row: (
            _token_plan_sort_value(token_row, settings),
            _datetime_sort_value(getattr(token_row, "last_used_at", None)),
            _datetime_sort_value(getattr(token_row, "updated_at", None)),
            int(getattr(token_row, "id", 0) or 0),
        ),
    )


def _snapshot_scoped_cooldown_active(
    snapshot: _TokenPoolSnapshot,
    token_id: int,
    *,
    scoped_cooldown_scope: str | None,
    now: datetime,
) -> bool:
    if not scoped_cooldown_scope:
        return False
    cooldown_until = snapshot.scoped_cooldowns.get((int(token_id), scoped_cooldown_scope))
    return cooldown_until is not None and cooldown_until > now


def _token_snapshot_row_available(
    token_row: Any,
    snapshot: _TokenPoolSnapshot,
    *,
    scoped_cooldown_scope: str | None,
    now: datetime,
) -> bool:
    if getattr(token_row, "merged_into_token_id", None) is not None:
        return False
    if not bool(getattr(token_row, "is_active", False)):
        return False
    if not getattr(token_row, "refresh_token", None):
        return False
    if str(getattr(token_row, "token_type", "codex") or "codex") != "codex":
        return False
    cooldown_until = getattr(token_row, "cooldown_until", None)
    if cooldown_until is not None and cooldown_until > now:
        return False
    token_id = int(getattr(token_row, "id", 0) or 0)
    if _snapshot_scoped_cooldown_active(
        snapshot,
        token_id,
        scoped_cooldown_scope=scoped_cooldown_scope,
        now=now,
    ):
        return False
    return token_is_runtime_available(
        token_row,
        now=now,
        scoped_cooldown_scope=scoped_cooldown_scope,
    )


async def _load_token_pool_snapshot() -> _TokenPoolSnapshot:
    token_rows, cooldown_rows = await asyncio.gather(
        list_token_pool_rows(),
        list_token_pool_scoped_cooldowns(),
    )
    scoped_cooldowns: dict[tuple[int, str], datetime] = {}
    for cooldown in cooldown_rows:
        scope = str(getattr(cooldown, "scope", "") or "").strip()
        cooldown_until = getattr(cooldown, "cooldown_until", None)
        if not scope or cooldown_until is None:
            continue
        scoped_cooldowns[(int(cooldown.token_id), scope)] = cooldown_until
    return _TokenPoolSnapshot(
        tokens=tuple(token_rows),
        scoped_cooldowns=scoped_cooldowns,
        loaded_at=time.monotonic(),
    )


async def _refresh_token_pool_snapshot(app: FastAPI) -> _TokenPoolSnapshot:
    snapshot = await _load_token_pool_snapshot()
    app.state.token_pool_snapshot = snapshot
    return snapshot


async def _refresh_token_pool_snapshot_best_effort(app: FastAPI, *, reason: str) -> None:
    try:
        await _refresh_token_pool_snapshot(app)
    except Exception:
        logger.exception("Failed to refresh token pool snapshot: reason=%s", reason)


def _current_token_pool_snapshot(app: FastAPI) -> _TokenPoolSnapshot | None:
    snapshot = getattr(app.state, "token_pool_snapshot", None)
    return snapshot if isinstance(snapshot, _TokenPoolSnapshot) else None


def _claim_next_active_token_from_snapshot(
    snapshot: _TokenPoolSnapshot,
    selection_settings: TokenSelectionSettings,
    *,
    exclude_token_ids: set[int],
    scoped_cooldown_scope: str | None,
    timing: _RequestTimingRecorder | None,
    include_plan_types: Iterable[str] | None = None,
    exclude_plan_types: Iterable[str] | None = None,
) -> Any | None:
    now = utcnow()
    active_stream_cap = normalize_token_active_stream_cap(selection_settings.active_stream_cap)
    effective_excluded_ids = set(exclude_token_ids)
    effective_excluded_ids.update(
        token_id
        for token_id, active_count in _TOKEN_ACTIVE_REQUESTS.items()
        if int(active_count or 0) >= active_stream_cap
    )

    if timing is not None:
        timing.set_ms("token_pool_snapshot_age_ms", (time.monotonic() - snapshot.loaded_at) * 1000)

    for token_row in _token_snapshot_ordered_tokens(snapshot, selection_settings):
        token_id = int(getattr(token_row, "id", 0) or 0)
        if token_id <= 0 or token_id in effective_excluded_ids:
            continue
        if not _token_row_matches_plan_filters(
            token_row,
            include_plan_types=include_plan_types,
            exclude_plan_types=exclude_plan_types,
        ):
            continue
        if not _token_snapshot_row_available(
            token_row,
            snapshot,
            scoped_cooldown_scope=scoped_cooldown_scope,
            now=now,
        ):
            continue
        if _token_active_stream_count(token_id) >= active_stream_cap:
            continue
        token_row.last_used_at = now
        token_row.updated_at = now
        if timing is not None:
            timing.add_ms("claim_token_memory_count", 1)
            timing.set_tag("claim_token_source", "memory")
        _record_selected_token_observation_start(token_id, timing)
        return token_row
    return None


async def _claim_next_active_token_for_request(
    selection_settings: TokenSelectionSettings,
    *,
    exclude_token_ids: set[int],
    scoped_cooldown_scope: str | None = None,
    timing: _RequestTimingRecorder | None = None,
    token_pool_snapshot: _TokenPoolSnapshot | None = None,
    include_plan_types: Iterable[str] | None = None,
    exclude_plan_types: Iterable[str] | None = None,
) -> Any:
    if token_pool_snapshot is not None:
        token_row = _claim_next_active_token_from_snapshot(
            token_pool_snapshot,
            selection_settings,
            exclude_token_ids=exclude_token_ids,
            scoped_cooldown_scope=scoped_cooldown_scope,
            timing=timing,
            include_plan_types=include_plan_types,
            exclude_plan_types=exclude_plan_types,
        )
        if token_row is not None:
            return token_row
        if timing is not None:
            timing.add_ms("claim_token_memory_miss_count", 1)
            timing.set_tag("claim_token_source", "memory")
        return None

    local_excluded_ids: set[int] = set()
    fill_first = selection_settings.strategy == TOKEN_SELECTION_STRATEGY_FILL_FIRST
    active_stream_cap = normalize_token_active_stream_cap(selection_settings.active_stream_cap)
    claim_signature = inspect.signature(claim_next_active_token)
    while True:
        effective_excluded_ids = set(exclude_token_ids)
        effective_excluded_ids.update(local_excluded_ids)
        effective_excluded_ids.update(
            token_id
            for token_id, active_count in _TOKEN_ACTIVE_REQUESTS.items()
            if int(active_count or 0) >= active_stream_cap
        )

        kwargs: dict[str, Any] = {
            "selection_strategy": selection_settings.strategy,
            "exclude_token_ids": effective_excluded_ids,
        }
        if fill_first:
            kwargs["token_order"] = selection_settings.token_order
        if "plan_order_enabled" in claim_signature.parameters:
            kwargs["plan_order_enabled"] = selection_settings.plan_order_enabled
        if "plan_order" in claim_signature.parameters:
            kwargs["plan_order"] = selection_settings.plan_order
        if scoped_cooldown_scope is not None and "scoped_cooldown_scope" in claim_signature.parameters:
            kwargs["scoped_cooldown_scope"] = scoped_cooldown_scope
        if include_plan_types is not None and "include_plan_types" in claim_signature.parameters:
            kwargs["include_plan_types"] = include_plan_types
        if exclude_plan_types is not None and "exclude_plan_types" in claim_signature.parameters:
            kwargs["exclude_plan_types"] = exclude_plan_types

        token_row = await claim_next_active_token(**kwargs)
        if token_row is None:
            return None

        token_id = int(token_row.id)
        if _token_active_stream_count(token_id) < active_stream_cap:
            if timing is not None:
                timing.add_ms("claim_token_db_fallback_count", 1)
                timing.set_tag("claim_token_source", "db")
            _record_selected_token_observation_start(token_id, timing)
            return token_row

        local_excluded_ids.add(token_id)


async def _claim_next_token_for_model_request(
    selection_settings: TokenSelectionSettings,
    *,
    exclude_token_ids: set[int],
    scoped_cooldown_scope: str | None,
    timing: _RequestTimingRecorder | None,
    token_pool_snapshot: _TokenPoolSnapshot | None,
    require_non_free_token: bool,
) -> Any:
    if not require_non_free_token:
        return await _claim_next_active_token_for_request(
            selection_settings,
            exclude_token_ids=exclude_token_ids,
            scoped_cooldown_scope=scoped_cooldown_scope,
            timing=timing,
            token_pool_snapshot=token_pool_snapshot,
        )

    if token_pool_snapshot is None:
        claim_signature = inspect.signature(claim_next_active_token)
        if (
            "include_plan_types" not in claim_signature.parameters
            or "exclude_plan_types" not in claim_signature.parameters
        ):
            return await _claim_next_active_token_for_request(
                selection_settings,
                exclude_token_ids=exclude_token_ids,
                scoped_cooldown_scope=scoped_cooldown_scope,
                timing=timing,
                token_pool_snapshot=token_pool_snapshot,
            )

    token_row = await _claim_next_active_token_for_request(
        selection_settings,
        exclude_token_ids=exclude_token_ids,
        scoped_cooldown_scope=scoped_cooldown_scope,
        timing=timing,
        token_pool_snapshot=token_pool_snapshot,
        include_plan_types=KNOWN_NON_FREE_TOKEN_PLAN_TYPES,
    )
    if token_row is not None:
        return token_row

    return await _claim_next_active_token_for_request(
        selection_settings,
        exclude_token_ids=exclude_token_ids,
        scoped_cooldown_scope=scoped_cooldown_scope,
        timing=timing,
        token_pool_snapshot=token_pool_snapshot,
        exclude_plan_types=(TOKEN_PLAN_TYPE_FREE,),
    )


@dataclass
class _RequestLogWriteJob:
    payload: dict[str, Any]
    queued_at: float
    owner: "_RequestLogHandle | None" = None
    success_hooks: list[Callable[[int], Awaitable[None]]] = field(default_factory=list)


_REQUEST_LOG_WRITE_QUEUE: asyncio.Queue[_RequestLogWriteJob] | None = None
_REQUEST_LOG_WRITE_QUEUE_LOOP: asyncio.AbstractEventLoop | None = None
_REQUEST_LOG_WRITE_DRAINERS: set[asyncio.Task[None]] = set()


def _request_log_write_concurrency() -> int:
    return _int_env("REQUEST_LOG_WRITE_CONCURRENCY", 2, minimum=1)


def _request_log_write_batch_size() -> int:
    return _int_env("REQUEST_LOG_WRITE_BATCH_SIZE", 50, minimum=1)


def _request_log_write_queue() -> asyncio.Queue[_RequestLogWriteJob]:
    global _REQUEST_LOG_WRITE_QUEUE, _REQUEST_LOG_WRITE_QUEUE_LOOP, _REQUEST_LOG_WRITE_DRAINERS
    loop = asyncio.get_running_loop()
    if _REQUEST_LOG_WRITE_QUEUE is None or _REQUEST_LOG_WRITE_QUEUE_LOOP is not loop:
        _REQUEST_LOG_WRITE_QUEUE = asyncio.Queue()
        _REQUEST_LOG_WRITE_QUEUE_LOOP = loop
        _REQUEST_LOG_WRITE_DRAINERS = set()
    return _REQUEST_LOG_WRITE_QUEUE


def _merge_request_log_payload(base: dict[str, Any], update: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in update.items():
        if key == "timing_spans":
            merged[key] = _merge_timing_spans(merged.get(key), value)
        elif value is not None:
            merged[key] = value
    return merged


def _request_log_job_payload_with_queue_wait(job: _RequestLogWriteJob) -> dict[str, Any]:
    payload = dict(job.payload)
    queue_wait_ms = max(0, int((time.perf_counter() - job.queued_at) * 1000))
    payload["timing_spans"] = _merge_timing_spans(payload.get("timing_spans"), {"request_log_queue_wait_ms": queue_wait_ms})
    return payload


async def _run_request_log_success_hooks(hooks: list[Callable[[int], Awaitable[None]]], request_log_id: int) -> None:
    for hook in hooks:
        try:
            await hook(request_log_id)
        except Exception:
            logger.exception("Failed to persist proxy success side effects for request log id=%s", request_log_id)


def _request_log_store_is_monkeypatched() -> bool:
    return create_request_log is not _ORIGINAL_CREATE_REQUEST_LOG or finalize_request_log is not _ORIGINAL_FINALIZE_REQUEST_LOG


async def _write_request_log_batch_legacy(jobs: list[_RequestLogWriteJob]) -> None:
    for job in jobs:
        payload = _request_log_job_payload_with_queue_wait(job)
        owner = job.owner
        if owner is None:
            continue
        if owner._request_log_id is None:
            item = await create_request_log(
                request_id=owner.request_id,
                endpoint=owner.endpoint,
                model=owner.model,
                is_stream=owner.is_stream,
                started_at=owner.started_at,
                client_ip=owner.client_ip,
                user_agent=owner.user_agent,
            )
            owner._request_log_id = int(item.id)

        if payload.get("status_code") is None:
            continue

        request_log_id = owner._request_log_id
        if request_log_id is None:
            continue
        finalize_keys = {
            "status_code",
            "success",
            "attempt_count",
            "finished_at",
            "first_token_at",
            "token_id",
            "account_id",
            "model_name",
            "input_tokens",
            "output_tokens",
            "total_tokens",
            "estimated_cost_usd",
            "timing_spans",
            "error_message",
        }
        finalize_kwargs = {key: payload.get(key) for key in finalize_keys}
        await finalize_request_log(request_log_id, **finalize_kwargs)
        if job.success_hooks:
            await _run_request_log_success_hooks(job.success_hooks, request_log_id)


async def _write_request_log_batch(jobs: list[_RequestLogWriteJob]) -> None:
    if not jobs:
        return
    if _request_log_store_is_monkeypatched():
        await _write_request_log_batch_legacy(jobs)
        return

    coalesced_payloads: dict[str, dict[str, Any]] = {}
    owners: dict[str, _RequestLogHandle] = {}
    success_hooks: dict[str, list[Callable[[int], Awaitable[None]]]] = {}
    for job in jobs:
        request_id = str(job.payload.get("request_id") or "").strip()
        if not request_id:
            continue
        payload = _request_log_job_payload_with_queue_wait(job)
        current = coalesced_payloads.get(request_id)
        coalesced_payloads[request_id] = payload if current is None else _merge_request_log_payload(current, payload)
        if job.owner is not None:
            owners[request_id] = job.owner
        if job.success_hooks:
            success_hooks.setdefault(request_id, []).extend(job.success_hooks)

    if not coalesced_payloads:
        return

    log_timing = _RequestTimingRecorder()
    db_timing_token = set_db_timing_recorder(log_timing)
    try:
        results = await upsert_request_logs(
            list(coalesced_payloads.values()),
            timing_recorder=log_timing,
        )
    finally:
        reset_db_timing_recorder(db_timing_token)

    for result in results:
        request_id = str(result.get("request_id") or "").strip()
        if not request_id:
            continue
        request_log_id = result.get("id")
        try:
            resolved_request_log_id = int(request_log_id)
        except (TypeError, ValueError):
            continue
        owner = owners.get(request_id)
        if owner is not None:
            owner._request_log_id = resolved_request_log_id
        hooks = success_hooks.get(request_id) or []
        if hooks:
            await _run_request_log_success_hooks(hooks, resolved_request_log_id)


async def _drain_request_log_write_queue(queue: asyncio.Queue[_RequestLogWriteJob]) -> None:
    while True:
        jobs: list[_RequestLogWriteJob] = []
        try:
            jobs.append(queue.get_nowait())
        except asyncio.QueueEmpty:
            return

        batch_size = _request_log_write_batch_size()
        while len(jobs) < batch_size:
            try:
                jobs.append(queue.get_nowait())
            except asyncio.QueueEmpty:
                break

        try:
            await asyncio.sleep(0)
            await _write_request_log_batch(jobs)
        except Exception as exc:
            logger.exception("Request log write batch failed: %s", exc)
        finally:
            for _ in jobs:
                queue.task_done()


def _discard_request_log_drainer(task: asyncio.Task[None]) -> None:
    _REQUEST_LOG_WRITE_DRAINERS.discard(task)
    try:
        task.result()
    except asyncio.CancelledError:
        pass
    except Exception:
        logger.exception("Request log write worker failed")
    if _REQUEST_LOG_WRITE_QUEUE is not None and not _REQUEST_LOG_WRITE_QUEUE.empty():
        _ensure_request_log_write_drainers()


def _ensure_request_log_write_drainers() -> None:
    global _REQUEST_LOG_WRITE_DRAINERS
    queue = _request_log_write_queue()
    _REQUEST_LOG_WRITE_DRAINERS = {task for task in _REQUEST_LOG_WRITE_DRAINERS if not task.done()}
    while not queue.empty() and len(_REQUEST_LOG_WRITE_DRAINERS) < _request_log_write_concurrency():
        task = asyncio.create_task(
            _drain_request_log_write_queue(queue),
            name="oaix-request-log-write-worker",
        )
        _REQUEST_LOG_WRITE_DRAINERS.add(task)
        task.add_done_callback(_discard_request_log_drainer)


def _submit_request_log_write(job: _RequestLogWriteJob) -> None:
    queue = _request_log_write_queue()
    queue.put_nowait(job)
    _ensure_request_log_write_drainers()


async def _flush_request_log_write_queue() -> None:
    queue = _request_log_write_queue()
    _ensure_request_log_write_drainers()
    await queue.join()
    if _REQUEST_LOG_WRITE_DRAINERS:
        await asyncio.gather(*list(_REQUEST_LOG_WRITE_DRAINERS), return_exceptions=True)


@dataclass
class _RequestLogHandle:
    request_id: str
    endpoint: str
    model: str | None
    is_stream: bool
    started_at: datetime
    client_ip: str | None
    user_agent: str | None
    timing: _RequestTimingRecorder
    _request_log_id: int | None = None
    _finalize_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    _finalized: bool = False
    _finalized_spans: dict[str, Any] | None = None

    @classmethod
    def start(
        cls,
        *,
        endpoint: str,
        model: str | None,
        is_stream: bool,
        started_at: datetime,
        client_ip: str | None,
        user_agent: str | None,
        timing: _RequestTimingRecorder,
    ) -> "_RequestLogHandle":
        handle = cls(
            request_id=str(uuid.uuid4()),
            endpoint=endpoint,
            model=model,
            is_stream=is_stream,
            started_at=started_at,
            client_ip=client_ip,
            user_agent=user_agent,
            timing=timing,
        )
        handle._enqueue_start()
        return handle

    def _base_payload(self) -> dict[str, Any]:
        return {
            "request_id": self.request_id,
            "endpoint": self.endpoint,
            "model": self.model,
            "model_name": self.model,
            "is_stream": self.is_stream,
            "started_at": self.started_at,
            "client_ip": self.client_ip,
            "user_agent": self.user_agent,
        }

    def _enqueue_start(self) -> None:
        _submit_request_log_write(
            _RequestLogWriteJob(
                payload=self._base_payload(),
                queued_at=time.perf_counter(),
                owner=self,
            )
        )

    async def finalize(
        self,
        *,
        timing: _RequestTimingRecorder | None,
        success_hook: Callable[[int], Awaitable[None]] | None = None,
        **kwargs,
    ) -> tuple[int | None, dict[str, Any] | None]:
        async with self._finalize_lock:
            if self._finalized:
                if success_hook is not None and self._request_log_id is not None:
                    await _run_request_log_success_hooks([success_hook], self._request_log_id)
                return self._request_log_id, self._finalized_spans

            if timing is not None and "timing_spans" not in kwargs:
                kwargs["timing_spans"] = timing.snapshot()
            resolved_spans = _merge_timing_spans(kwargs.get("timing_spans"))
            kwargs["timing_spans"] = resolved_spans
            payload = _merge_request_log_payload(self._base_payload(), kwargs)
            job = _RequestLogWriteJob(
                payload=payload,
                queued_at=time.perf_counter(),
                owner=self,
                success_hooks=[success_hook] if success_hook is not None else [],
            )
            if _request_log_store_is_monkeypatched():
                await _write_request_log_batch_legacy([job])
            else:
                _submit_request_log_write(job)
            self._finalized = True
            self._finalized_spans = resolved_spans
            return self._request_log_id, resolved_spans


class _AsyncioSocketSendWarningFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return not (
            record.name == "asyncio"
            and record.levelno == logging.WARNING
            and record.getMessage() == "socket.send() raised exception."
        )


_ASYNCIO_SOCKET_SEND_WARNING_FILTER = _AsyncioSocketSendWarningFilter()


def _configure_runtime_logging() -> None:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper())
    asyncio_logger = logging.getLogger("asyncio")
    if not any(isinstance(item, _AsyncioSocketSendWarningFilter) for item in asyncio_logger.filters):
        asyncio_logger.addFilter(_ASYNCIO_SOCKET_SEND_WARNING_FILTER)

    http_client_log_level = os.getenv("HTTP_CLIENT_LOG_LEVEL", "WARNING").upper()
    logging.getLogger("httpx").setLevel(http_client_log_level)
    logging.getLogger("httpcore").setLevel(http_client_log_level)


RESPONSES_FAILURE_STATUS_BY_CODE = {
    "account_deactivated": 403,
    "account_disabled": 403,
    "account_suspended": 403,
    "authentication_error": 401,
    "billing_hard_limit_reached": 429,
    "context_length_exceeded": 400,
    "deactivated_workspace": 403,
    "incorrect_api_key_provided": 401,
    "insufficient_quota": 429,
    "invalid_api_key": 401,
    "invalid_request_error": 400,
    "invalid_type": 400,
    "model_not_found": 404,
    "moderation_blocked": 400,
    "not_found_error": 404,
    "permission_denied": 403,
    "rate_limit_exceeded": 429,
    "unsupported_parameter": 400,
    "user_deactivated": 403,
    "user_suspended": 403,
}


def _web_asset_version(path: Path) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()[:12]
    except OSError:
        return "dev"


RESPONSES_FAILURE_STATUS_BY_TYPE = {
    "authentication_error": 401,
    "image_generation_user_error": 400,
    "invalid_request_error": 400,
    "not_found_error": 404,
    "permission_error": 403,
    "rate_limit_error": 429,
    "tokens": 429,
}


class ResponsesRequest(BaseModel):
    model: str
    input: Any
    stream: bool | None = None

    model_config = ConfigDict(extra="allow")


class ChatCompletionsRequest(BaseModel):
    model: str
    messages: list[Any]
    stream: bool | None = None

    model_config = ConfigDict(extra="allow")


class TokenSelectionUpdateRequest(BaseModel):
    strategy: str


class TokenSelectionOrderUpdateRequest(BaseModel):
    token_ids: list[int]


class TokenSelectionPlanOrderUpdateRequest(BaseModel):
    enabled: bool
    plan_order: list[str] = []


class TokenSelectionConcurrencyUpdateRequest(BaseModel):
    active_stream_cap: int


class TokenActivationUpdateRequest(BaseModel):
    active: bool
    clear_cooldown: bool = False


class TokenProbeRequest(BaseModel):
    model: str | None = None


@dataclass(frozen=True)
class ProxyRequestResult:
    response: Response
    status_code: int
    model_name: str | None = None
    first_token_at: datetime | None = None
    usage_metrics: UsageMetrics | None = None
    on_success: Callable[[int], Awaitable[None]] | None = None
    stream_capture: "_ProxyStreamCapture | None" = None


@dataclass(frozen=True)
class _PrimedResponsesStream:
    buffered_chunks: list[bytes]
    stream_committed: bool
    model_name: str | None
    first_token_observed: bool
    emit_initial_keepalive: bool = False
    pending_chunk_task: asyncio.Task[bytes] | None = None


@dataclass(frozen=True)
class ImageProxyRequest:
    model_name: str
    response_format: str
    stream: bool
    stream_prefix: str
    responses_payload: dict[str, Any]


@dataclass(frozen=True)
class ImageCallResult:
    result_b64: str
    revised_prompt: str | None = None
    output_format: str | None = None
    size: str | None = None
    background: str | None = None
    quality: str | None = None


@dataclass(frozen=True)
class _ChatContentToken:
    kind: str
    text: str | None = None
    image_url: str | None = None
    raw_item: dict[str, Any] | None = None


class _ProxyStreamCapture:
    def __init__(
        self,
        *,
        initial_model_name: str | None = None,
        initial_first_token_at: datetime | None = None,
    ) -> None:
        self.model_name = initial_model_name
        self.first_token_at = initial_first_token_at
        self.usage_metrics: UsageMetrics | None = None
        self.completed = False
        self.error_status_code: int | None = None
        self.error_message: str | None = None
        self._response_snapshot: dict[str, Any] = {}
        self._output_items_by_index: dict[int, dict[str, Any]] = {}
        self._output_items_fallback: list[dict[str, Any]] = []
        self._decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
        self._text_buffer = ""
        self._sse_scan_start = 0

    @property
    def response_payload(self) -> dict[str, Any] | None:
        if not self._response_snapshot:
            return None

        patched = _patch_completed_output_from_output_items(
            {
                "type": "response.completed",
                "response": copy.deepcopy(self._response_snapshot),
            },
            output_items_by_index=self._output_items_by_index,
            output_items_fallback=self._output_items_fallback,
        )
        response_payload = patched.get("response")
        return response_payload if isinstance(response_payload, dict) else None

    def feed(self, chunk: bytes) -> None:
        if not chunk:
            return
        try:
            self._text_buffer += self._decoder.decode(chunk)

            while True:
                raw_event, self._text_buffer, self._sse_scan_start = _pop_sse_event_from_buffer(
                    self._text_buffer,
                    self._sse_scan_start,
                )
                if raw_event is None:
                    break

                if not raw_event.strip():
                    continue

                event_type, event_payload = _extract_responses_stream_event(raw_event)
                extracted_model_name = _extract_response_model_name(event_payload)
                if extracted_model_name:
                    self.model_name = extracted_model_name

                if self.first_token_at is None and event_type and event_type not in RESPONSES_STREAM_PREFLIGHT_EVENTS:
                    self.first_token_at = utcnow()

                if not isinstance(event_payload, dict):
                    continue

                if event_type == "error":
                    error_obj = event_payload.get("error")
                    if isinstance(error_obj, dict):
                        self.error_status_code = _responses_error_status_code(error_obj)
                        error_message = _normalize_optional_text(error_obj.get("message"))
                        if error_message is not None:
                            self.error_message = error_message
                    continue

                if event_type == "response.output_item.done":
                    _collect_responses_output_item_done(
                        event_payload,
                        output_items_by_index=self._output_items_by_index,
                        output_items_fallback=self._output_items_fallback,
                    )
                    continue

                response_obj = event_payload.get("response")
                if isinstance(response_obj, dict):
                    if event_type == "response.completed":
                        self.completed = True
                        event_payload = _patch_completed_output_from_output_items(
                            event_payload,
                            output_items_by_index=self._output_items_by_index,
                            output_items_fallback=self._output_items_fallback,
                        )
                        response_obj = event_payload.get("response")
                    _merge_mapping(self._response_snapshot, response_obj)

                usage_payload = self.response_payload or event_payload
                usage_metrics = extract_usage_metrics(usage_payload, model_name=self.model_name)
                if usage_metrics is not None:
                    self.usage_metrics = usage_metrics
        except Exception:
            logger.exception("Failed to observe streamed response usage")


class _GatewayProxyHTTPException(HTTPException):
    def __init__(
        self,
        *,
        status_code: int,
        detail: str,
        retryable: bool | None = None,
        record_token_error: bool = False,
    ) -> None:
        super().__init__(status_code=status_code, detail=detail)
        self.retryable = retryable
        self.record_token_error = record_token_error


class _SSEProxyStreamingResponse(Response):
    def __init__(
        self,
        content: AsyncIterator[bytes],
        *,
        status_code: int = 200,
        headers: dict[str, str] | None = None,
        media_type: str | None = None,
    ) -> None:
        super().__init__(content=None, status_code=status_code, headers=headers, media_type=media_type)
        self.body_iterator = content
        if "content-length" in self.headers:
            del self.headers["content-length"]

    async def __call__(self, scope, receive, send) -> None:
        response_started = False
        send_failed = False
        try:
            async for chunk in self.body_iterator:
                if isinstance(chunk, str):
                    chunk = chunk.encode("utf-8")
                try:
                    if not response_started:
                        await send(
                            {
                                "type": "http.response.start",
                                "status": self.status_code,
                                "headers": self.raw_headers,
                            }
                        )
                        response_started = True
                    await send(
                        {
                            "type": "http.response.body",
                            "body": chunk,
                            "more_body": True,
                        }
                    )
                except (OSError, RuntimeError):
                    send_failed = True
                    break
        except asyncio.CancelledError:
            send_failed = True
            raise
        finally:
            try:
                if not send_failed:
                    if not response_started:
                        await send(
                            {
                                "type": "http.response.start",
                                "status": self.status_code,
                                "headers": self.raw_headers,
                            }
                        )
                    await send(
                        {
                            "type": "http.response.body",
                            "body": b"",
                            "more_body": False,
                        }
                    )
            except (OSError, RuntimeError):
                pass
            finally:
                close_iterator = getattr(self.body_iterator, "aclose", None)
                if callable(close_iterator):
                    with suppress(Exception):
                        await close_iterator()


class _IdempotentAsyncCallback:
    def __init__(self, callback: Callable[[], Awaitable[None]]) -> None:
        self._callback = callback
        self._lock = asyncio.Lock()
        self._called = False

    async def __call__(self) -> None:
        if self._called:
            return
        async with self._lock:
            if self._called:
                return
            self._called = True
            await self._callback()


class _FinalizingStreamingResponse(StreamingResponse):
    def __init__(
        self,
        content: AsyncIterator[bytes],
        *,
        on_close: Callable[[], Awaitable[None]],
        status_code: int = 200,
        headers: dict[str, str] | None = None,
        media_type: str | None = None,
    ) -> None:
        super().__init__(content, status_code=status_code, headers=headers, media_type=media_type)
        self._on_close_once = _IdempotentAsyncCallback(on_close)

    async def __call__(self, scope, receive, send) -> None:
        try:
            await super().__call__(scope, receive, send)
        finally:
            async def close_with_cancel_shield() -> None:
                with anyio.CancelScope(shield=True):
                    await self._on_close_once()

            cleanup_task = asyncio.create_task(close_with_cancel_shield(), name="oaix-streamed-response-finalizer")
            try:
                await asyncio.shield(cleanup_task)
            except asyncio.CancelledError:
                try:
                    with anyio.CancelScope(shield=True):
                        await asyncio.shield(cleanup_task)
                except Exception:
                    logger.exception("Failed to finalize streaming response after ASGI cancellation")
                raise
            except Exception:
                logger.exception("Failed to finalize streaming response")


class ResponseTrafficLease:
    def __init__(self, controller: "ResponseTrafficController") -> None:
        self._controller = controller
        self._released = False

    async def release(self) -> None:
        if self._released:
            return
        self._released = True
        self._controller.release_response()


class ResponseTrafficController:
    def __init__(self) -> None:
        self._active_responses = 0
        self._responses_idle = asyncio.Event()
        self._responses_idle.set()

    @property
    def active_responses(self) -> int:
        return self._active_responses

    def start_response(self) -> ResponseTrafficLease:
        self._active_responses += 1
        self._responses_idle.clear()
        return ResponseTrafficLease(self)

    def release_response(self) -> None:
        if self._active_responses <= 0:
            self._active_responses = 0
            self._responses_idle.set()
            return
        self._active_responses -= 1
        if self._active_responses == 0:
            self._responses_idle.set()

    async def wait_for_import_turn(self, *, timeout_seconds: float | None = None) -> bool:
        waited = False
        resolved_timeout = _import_wait_timeout_seconds() if timeout_seconds is None else float(timeout_seconds)
        deadline: float | None = None
        loop = asyncio.get_running_loop()
        if resolved_timeout > 0:
            deadline = loop.time() + resolved_timeout

        while True:
            while self._active_responses > 0:
                waited = True
                if deadline is None:
                    await self._responses_idle.wait()
                    continue

                remaining = deadline - loop.time()
                if remaining <= 0:
                    raise TimeoutError("Timed out waiting for active /v1/responses traffic to drain before import")

                try:
                    await asyncio.wait_for(self._responses_idle.wait(), timeout=remaining)
                except asyncio.TimeoutError as exc:
                    raise TimeoutError(
                        "Timed out waiting for active /v1/responses traffic to drain before import"
                    ) from exc

            if not waited:
                return False

            grace_seconds = _import_response_idle_grace_seconds()
            if grace_seconds <= 0:
                return True

            if deadline is not None:
                remaining = deadline - loop.time()
                if remaining <= 0 or remaining < grace_seconds:
                    raise TimeoutError("Timed out waiting for active /v1/responses traffic to drain before import")

            await asyncio.sleep(grace_seconds)
            if self._active_responses == 0:
                return True


def _int_env(name: str, default: int, *, minimum: int = 0) -> int:
    raw = str(os.getenv(name, "") or "").strip()
    try:
        value = int(raw) if raw else int(default)
    except ValueError:
        value = int(default)
    return max(minimum, value)


def _float_env(name: str, default: float, *, minimum: float = 0.0) -> float:
    raw = str(os.getenv(name, "") or "").strip()
    try:
        value = float(raw) if raw else float(default)
    except ValueError:
        value = float(default)
    return max(minimum, value)


def _csv_env(name: str) -> list[str]:
    raw = str(os.getenv(name, "") or "").strip()
    return [item.strip() for item in raw.split(",") if item.strip()]


def _max_request_account_retries() -> int:
    return _int_env("MAX_REQUEST_ACCOUNT_RETRIES", 100, minimum=1)


def _fill_first_token_active_stream_cap() -> int:
    return normalize_token_active_stream_cap(
        _int_env(
            "FILL_FIRST_TOKEN_ACTIVE_STREAM_CAP",
            DEFAULT_FILL_FIRST_TOKEN_ACTIVE_STREAM_CAP,
            minimum=1,
        )
    )


def _upstream_http_max_connections() -> int:
    return _int_env(
        "UPSTREAM_HTTP_MAX_CONNECTIONS",
        DEFAULT_UPSTREAM_HTTP_MAX_CONNECTIONS,
        minimum=1,
    )


def _upstream_http_max_keepalive_connections() -> int:
    max_connections = _upstream_http_max_connections()
    keepalive_connections = _int_env(
        "UPSTREAM_HTTP_MAX_KEEPALIVE_CONNECTIONS",
        DEFAULT_UPSTREAM_HTTP_MAX_KEEPALIVE_CONNECTIONS,
        minimum=0,
    )
    return min(keepalive_connections, max_connections)


def _import_http_max_connections() -> int:
    return _int_env("IMPORT_HTTP_MAX_CONNECTIONS", 64, minimum=1)


def _import_http_max_keepalive_connections() -> int:
    return min(
        _int_env("IMPORT_HTTP_MAX_KEEPALIVE_CONNECTIONS", 16, minimum=0),
        _import_http_max_connections(),
    )


def _admin_http_max_connections() -> int:
    return _int_env("ADMIN_HTTP_MAX_CONNECTIONS", 32, minimum=1)


def _admin_http_max_keepalive_connections() -> int:
    return min(
        _int_env("ADMIN_HTTP_MAX_KEEPALIVE_CONNECTIONS", 8, minimum=0),
        _admin_http_max_connections(),
    )


def _state_http_client(app: FastAPI, name: str) -> httpx.AsyncClient:
    client = getattr(app.state, name, None)
    if isinstance(client, httpx.AsyncClient):
        return client
    return app.state.http_client


def _image_request_max_account_retries() -> int:
    return _int_env("IMAGE_REQUEST_MAX_ACCOUNT_RETRIES", 8, minimum=1)


def _image_input_max_per_request() -> int:
    return _int_env("IMAGE_INPUT_MAX_PER_REQUEST", 249, minimum=1)


def _image_rate_limit_default_cooldown_seconds() -> int:
    return _int_env("IMAGE_RATE_LIMIT_DEFAULT_COOLDOWN_SECONDS", 5, minimum=1)


def _image_rate_limit_min_cooldown_seconds() -> int:
    return _int_env("IMAGE_RATE_LIMIT_MIN_COOLDOWN_SECONDS", 1, minimum=1)


def _default_usage_limit_cooldown_seconds() -> int:
    return _int_env("DEFAULT_USAGE_LIMIT_COOLDOWN_SECONDS", 300, minimum=1)


def _import_response_idle_grace_seconds() -> float:
    return _float_env("IMPORT_RESPONSE_IDLE_GRACE_SECONDS", 0.25, minimum=0.0)


def _import_wait_timeout_seconds() -> float:
    return _float_env("IMPORT_WAIT_TIMEOUT_SECONDS", 30.0, minimum=0.0)


def _compact_server_error_cooldown_seconds() -> int:
    return _int_env("COMPACT_SERVER_ERROR_COOLDOWN_SECONDS", 60, minimum=0)


def _compact_upstream_headers_timeout_seconds() -> float:
    return _float_env("COMPACT_UPSTREAM_HEADERS_TIMEOUT_SECONDS", 45.0, minimum=1.0)


def _compact_timeout_fallback_enabled() -> bool:
    return str(os.getenv("COMPACT_TIMEOUT_FALLBACK_ENABLED", "1") or "").strip().lower() not in {
        "0",
        "false",
        "no",
        "off",
    }


def _image_non_stream_total_timeout_seconds() -> float:
    return _float_env("IMAGE_NON_STREAM_TOTAL_TIMEOUT_SECONDS", 600.0, minimum=1.0)


def _image_non_stream_read_timeout_seconds() -> float:
    return min(
        _image_non_stream_total_timeout_seconds(),
        _float_env("IMAGE_NON_STREAM_READ_TIMEOUT_SECONDS", 600.0, minimum=1.0),
    )


def _image_non_stream_disconnect_poll_seconds() -> float:
    return _float_env("IMAGE_NON_STREAM_DISCONNECT_POLL_SECONDS", 1.0, minimum=0.1)


def _image_stream_first_output_timeout_seconds() -> float:
    return _float_env("IMAGE_STREAM_FIRST_OUTPUT_TIMEOUT_SECONDS", 600.0, minimum=1.0)


def _admin_quota_cache_ttl_seconds() -> int:
    return _int_env("ADMIN_QUOTA_CACHE_TTL_SECONDS", 60, minimum=5)


def _admin_quota_max_concurrency() -> int:
    return _int_env("ADMIN_QUOTA_MAX_CONCURRENCY", 2, minimum=1)


def _get_service_api_keys() -> set[str]:
    raw = os.getenv("SERVICE_API_KEYS") or os.getenv("API_KEY") or ""
    return {item.strip() for item in raw.split(",") if item.strip()}


def _cors_allow_origins() -> list[str]:
    origins = _csv_env("CORS_ALLOW_ORIGINS")
    return origins or ["*"]


def _cors_allow_origin_regex() -> str | None:
    return _normalize_optional_text(os.getenv("CORS_ALLOW_ORIGIN_REGEX"))


def _cors_allow_credentials() -> bool:
    return _coerce_bool(os.getenv("CORS_ALLOW_CREDENTIALS"), False)


async def verify_service_api_key(http_request: Request) -> None:
    valid_keys = _get_service_api_keys()
    if not valid_keys:
        return

    header = (http_request.headers.get("Authorization") or "").strip()
    scheme, _, token = header.partition(" ")
    if scheme.lower() != "bearer" or token.strip() not in valid_keys:
        raise HTTPException(status_code=401, detail="Invalid or missing bearer token")


def _normalize_responses_upstream_url(base_url: str) -> str:
    base = (base_url or "").strip()
    if not base:
        return "https://chatgpt.com/backend-api/codex/responses"

    base = base.rstrip("/")
    if base.endswith("/v1/responses") or base.endswith("/responses"):
        return base
    return f"{base}/responses"


def _normalize_responses_compact_upstream_url(base_url: str) -> str:
    base = (base_url or "").strip()
    if not base:
        return "https://chatgpt.com/backend-api/codex/responses/compact"

    base = base.rstrip("/")
    if base.endswith("/v1/responses/compact") or base.endswith("/responses/compact"):
        return base

    base = _normalize_responses_upstream_url(base)
    if base.endswith("/v1/responses") or base.endswith("/responses"):
        return f"{base}/compact"
    if base.endswith("/compact"):
        return base
    return f"{base}/compact"


def _codex_responses_url(*, compact: bool = False) -> str:
    base = os.getenv("CODEX_BASE_URL") or ""
    if compact:
        return _normalize_responses_compact_upstream_url(base)
    return _normalize_responses_upstream_url(base)


def _responses_endpoint_path(*, compact: bool = False) -> str:
    if compact:
        return "/v1/responses/compact"
    return "/v1/responses"


def _build_upstream_headers(http_request: Request, access_token: str, account_id: str | None, stream: bool) -> dict[str, str]:
    session_id = (http_request.headers.get("Session_id") or "").strip() or str(uuid.uuid4())
    conversation_id = (http_request.headers.get("Conversation_id") or "").strip() or session_id

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Openai-Beta": (http_request.headers.get("Openai-Beta") or "responses=experimental").strip(),
        "Originator": (http_request.headers.get("Originator") or "codex_cli_rs").strip(),
        "Version": CODEX_CLI_VERSION,
        "Session_id": session_id,
        "Conversation_id": conversation_id,
        "User-Agent": (http_request.headers.get("User-Agent") or CODEX_USER_AGENT).strip(),
        "Connection": "Keep-Alive",
        "Accept": "text/event-stream" if stream else "application/json",
    }
    if account_id:
        headers["Chatgpt-Account-Id"] = account_id
    return headers


def _sanitize_codex_payload(
    payload: dict[str, Any],
    *,
    compact: bool = False,
    preserve_previous_response_id: bool = False,
) -> dict[str, Any]:
    payload.pop("max_output_tokens", None)
    payload.pop("response_format", None)
    if not preserve_previous_response_id:
        payload.pop("previous_response_id", None)
    payload.pop("prompt_cache_retention", None)
    payload.pop("safety_identifier", None)
    if compact:
        payload.pop("store", None)
    else:
        payload["store"] = False
    payload.setdefault("instructions", "")
    return payload


def _admin_token_probe_model() -> str:
    return _normalize_optional_text(os.getenv("ADMIN_TOKEN_PROBE_MODEL")) or "gpt-5.5"


def _decode_error_body(raw: bytes) -> str:
    try:
        return raw.decode("utf-8", errors="replace").strip()
    except Exception:
        return repr(raw)


def _mapping_get(payload: Any, *keys: str) -> Any | None:
    current: Any = payload
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _normalize_optional_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _normalize_image_response_format(value: Any) -> str:
    return (_normalize_optional_text(value) or "b64_json").lower()


def _mime_type_from_output_format(output_format: str | None) -> str:
    normalized = _normalize_optional_text(output_format)
    if normalized is None:
        return "image/png"
    if "/" in normalized:
        return normalized
    return {
        "png": "image/png",
        "jpg": "image/jpeg",
        "jpeg": "image/jpeg",
        "webp": "image/webp",
    }.get(normalized.lower(), "image/png")


def _coerce_int(value: Any, default: int | None = None) -> int | None:
    if value is None or value == "":
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _coerce_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    normalized = str(value).strip().lower()
    if not normalized:
        return default
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return default


def _normalize_chat_message_role(value: Any) -> str | None:
    role = _normalize_optional_text(value)
    if role is None:
        return None
    normalized = role.lower()
    if normalized in {"system", "developer", "user", "assistant", "tool"}:
        return normalized
    return None


def _chat_text_part_type_for_role(role: str) -> str:
    return "output_text" if role == "assistant" else "input_text"


def _extract_chat_image_url(item: Any) -> str | None:
    if isinstance(item, str):
        return _normalize_optional_text(item)
    if not isinstance(item, dict):
        return None

    image_url = item.get("image_url")
    if isinstance(image_url, dict):
        candidate = _normalize_optional_text(image_url.get("url"))
        if candidate is not None:
            return candidate
    candidate = _normalize_optional_text(image_url)
    if candidate is not None:
        return candidate
    return _normalize_optional_text(item.get("url"))


def _image_rate_limit_scoped_cooldown_scope(request_model: str | None) -> str | None:
    if _is_responses_image_compat_model(request_model):
        return IMAGE_INPUT_RATE_LIMIT_COOLDOWN_SCOPE
    return None


def _count_responses_input_images(value: Any) -> int:
    if isinstance(value, list):
        return sum(_count_responses_input_images(item) for item in value)

    if not isinstance(value, dict):
        return 0

    item_type = _normalize_optional_text(value.get("type"))
    if item_type in {"image_url", "input_image"}:
        return 1 if _extract_chat_image_url(value) is not None else 0

    count = 0
    mask_value = value.get("input_image_mask")
    if mask_value is not None:
        mask_candidate = _extract_image_url_candidate(mask_value)
        if mask_candidate is None and isinstance(mask_value, dict):
            mask_candidate = _extract_image_url_candidate(mask_value.get("image_url"))
        if mask_candidate is not None:
            count += 1

    for key, child in value.items():
        if key == "input_image_mask":
            continue
        count += _count_responses_input_images(child)
    return count


def _enforce_image_input_count_limit(image_count: int) -> None:
    max_images = _image_input_max_per_request()
    if image_count <= max_images:
        return
    raise HTTPException(
        status_code=400,
        detail=f"Too many input images: {image_count}. At most {max_images} input images are allowed per request.",
    )


def _append_chat_content_tokens_from_text(tokens: list[_ChatContentToken], text: str) -> None:
    cursor = 0
    matched_any = False

    for match in CHAT_COMPLETIONS_MARKDOWN_IMAGE_RE.finditer(text):
        image_url = _normalize_optional_text(match.group(1))
        if image_url is None:
            continue

        matched_any = True
        if match.start() > cursor:
            prefix = text[cursor:match.start()]
            if prefix:
                tokens.append(_ChatContentToken(kind="text", text=prefix))
        tokens.append(_ChatContentToken(kind="image", image_url=image_url))
        cursor = match.end()

    if cursor < len(text):
        suffix = text[cursor:]
        if suffix:
            tokens.append(_ChatContentToken(kind="text", text=suffix))
    elif not matched_any and text:
        tokens.append(_ChatContentToken(kind="text", text=text))


def _extract_chat_content_tokens(content_value: Any, *, role: str) -> list[_ChatContentToken]:
    tokens: list[_ChatContentToken] = []

    if isinstance(content_value, str):
        _append_chat_content_tokens_from_text(tokens, content_value)
        return tokens

    if not isinstance(content_value, list):
        return tokens

    for item in content_value:
        if isinstance(item, str):
            _append_chat_content_tokens_from_text(tokens, item)
            continue
        if not isinstance(item, dict):
            continue

        item_type = _normalize_optional_text(item.get("type"))
        if item_type in {"text", "input_text", "output_text"}:
            text_value = str(item.get("text") or "")
            if text_value:
                _append_chat_content_tokens_from_text(tokens, text_value)
            continue

        if item_type in {"image_url", "input_image"}:
            image_url = _extract_chat_image_url(item)
            if image_url is not None:
                tokens.append(_ChatContentToken(kind="image", image_url=image_url))
            continue

        if item_type == "input_audio" and role == "user":
            tokens.append(_ChatContentToken(kind="audio", raw_item=copy.deepcopy(item)))

    return tokens


def _chat_content_tokens_to_parts(tokens: list[_ChatContentToken], *, role: str) -> list[dict[str, Any]]:
    text_part_type = _chat_text_part_type_for_role(role)
    content_parts: list[dict[str, Any]] = []

    for token in tokens:
        if token.kind == "text" and token.text:
            content_parts.append({"type": text_part_type, "text": token.text})
            continue
        if token.kind == "image" and token.image_url:
            content_parts.append({"type": "input_image", "image_url": token.image_url})
            continue
        if token.kind == "audio" and role == "user" and isinstance(token.raw_item, dict):
            content_parts.append(copy.deepcopy(token.raw_item))

    return content_parts


def _append_chat_text_and_markdown_images(
    content_parts: list[dict[str, Any]],
    text: str,
    *,
    text_part_type: str,
) -> None:
    cursor = 0
    matched_any = False

    for match in CHAT_COMPLETIONS_MARKDOWN_IMAGE_RE.finditer(text):
        image_url = _normalize_optional_text(match.group(1))
        if image_url is None:
            continue

        matched_any = True
        if match.start() > cursor:
            prefix = text[cursor:match.start()]
            if prefix:
                content_parts.append({"type": text_part_type, "text": prefix})
        content_parts.append({"type": "input_image", "image_url": image_url})
        cursor = match.end()

    if cursor < len(text):
        suffix = text[cursor:]
        if suffix:
            content_parts.append({"type": text_part_type, "text": suffix})
    elif not matched_any and text:
        content_parts.append({"type": text_part_type, "text": text})


def _append_chat_content_parts(
    content_parts: list[dict[str, Any]],
    content_value: Any,
    *,
    role: str,
) -> None:
    text_part_type = _chat_text_part_type_for_role(role)

    if isinstance(content_value, str):
        _append_chat_text_and_markdown_images(content_parts, content_value, text_part_type=text_part_type)
        return

    if not isinstance(content_value, list):
        return

    for item in content_value:
        if isinstance(item, str):
            _append_chat_text_and_markdown_images(content_parts, item, text_part_type=text_part_type)
            continue
        if not isinstance(item, dict):
            continue

        item_type = _normalize_optional_text(item.get("type"))
        if item_type in {"text", "input_text", "output_text"}:
            text_value = str(item.get("text") or "")
            if text_value:
                _append_chat_text_and_markdown_images(content_parts, text_value, text_part_type=text_part_type)
            continue

        if item_type in {"image_url", "input_image"}:
            image_url = _extract_chat_image_url(item)
            if image_url is not None:
                content_parts.append({"type": "input_image", "image_url": image_url})
            continue

        if item_type == "input_audio" and role == "user":
            content_parts.append(copy.deepcopy(item))


def _stringify_chat_tool_output(content_value: Any) -> str:
    if isinstance(content_value, str):
        return content_value

    if isinstance(content_value, list):
        text_parts: list[str] = []
        for item in content_value:
            if isinstance(item, str):
                text_parts.append(item)
                continue
            if not isinstance(item, dict):
                continue
            item_type = _normalize_optional_text(item.get("type"))
            if item_type in {"text", "input_text", "output_text"} and item.get("text") is not None:
                text_parts.append(str(item.get("text")))
        if text_parts:
            return "\n".join(text_parts)

    try:
        return json.dumps(content_value, ensure_ascii=False)
    except Exception:
        return str(content_value or "")


def _chat_messages_to_responses_input(messages: list[Any]) -> list[dict[str, Any]]:
    input_items: list[dict[str, Any]] = []

    for message in messages:
        if not isinstance(message, dict):
            continue

        role = _normalize_chat_message_role(message.get("role"))
        if role is None:
            continue

        if role == "tool":
            tool_call_id = _normalize_optional_text(message.get("tool_call_id"))
            if tool_call_id is None:
                continue
            input_items.append(
                {
                    "type": "function_call_output",
                    "call_id": tool_call_id,
                    "output": _stringify_chat_tool_output(message.get("content")),
                }
            )
            continue

        responses_role = "developer" if role == "system" else role
        content_parts: list[dict[str, Any]] = []
        _append_chat_content_parts(content_parts, message.get("content"), role=role)
        input_items.append(
            {
                "type": "message",
                "role": responses_role,
                "content": content_parts,
            }
        )

        if role != "assistant":
            continue

        tool_calls = message.get("tool_calls")
        if not isinstance(tool_calls, list):
            continue

        for tool_call in tool_calls:
            if not isinstance(tool_call, dict):
                continue
            if _normalize_optional_text(tool_call.get("type")) != "function":
                continue
            function_obj = tool_call.get("function")
            if not isinstance(function_obj, dict):
                continue
            name = _normalize_optional_text(function_obj.get("name"))
            if name is None:
                continue
            input_items.append(
                {
                    "type": "function_call",
                    "call_id": _normalize_optional_text(tool_call.get("id")) or f"call_{uuid.uuid4().hex}",
                    "name": name,
                    "arguments": function_obj.get("arguments") or "",
                }
            )

    return input_items


def _chat_image_checkpoint_scope_hash(http_request: Request | None) -> str:
    if http_request is None:
        return hashlib.sha256(b"oaix-chat-image-scope:anonymous").hexdigest()

    authorization = _normalize_optional_text(http_request.headers.get("authorization"))
    bearer_token: str | None = None
    if authorization is not None and authorization.lower().startswith("bearer "):
        bearer_token = _normalize_optional_text(authorization[7:])

    scope_payload = {
        "authorization": bearer_token or "",
        "origin": _normalize_optional_text(http_request.headers.get("origin")) or "",
        "client_ip": "" if bearer_token is not None else (http_request.client.host if http_request.client is not None else ""),
        "user_agent": "" if bearer_token is not None else (_normalize_optional_text(http_request.headers.get("user-agent")) or ""),
    }
    encoded = json.dumps(scope_payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _append_assistant_tool_calls(
    input_items: list[dict[str, Any]],
    message: dict[str, Any],
) -> None:
    tool_calls = message.get("tool_calls")
    if not isinstance(tool_calls, list):
        return

    for tool_call in tool_calls:
        if not isinstance(tool_call, dict):
            continue
        if _normalize_optional_text(tool_call.get("type")) != "function":
            continue
        function_obj = tool_call.get("function")
        if not isinstance(function_obj, dict):
            continue
        name = _normalize_optional_text(function_obj.get("name"))
        if name is None:
            continue
        input_items.append(
            {
                "type": "function_call",
                "call_id": _normalize_optional_text(tool_call.get("id")) or f"call_{uuid.uuid4().hex}",
                "name": name,
                "arguments": function_obj.get("arguments") or "",
            }
        )


def _resolved_chat_image_to_responses_input_item(resolved_image: Any) -> dict[str, Any] | None:
    # Replaying the full upstream output item back into `input` is invalid.
    # Stateless follow-up edits need the schema-legal image generation call
    # payload, not the full tool output object.
    image_call_id = _normalize_optional_text(getattr(resolved_image, "image_call_id", None))
    output_item = getattr(resolved_image, "output_item", None)
    if image_call_id is None and isinstance(output_item, dict):
        if _normalize_optional_text(output_item.get("type")) == "image_generation_call":
            image_call_id = _normalize_optional_text(output_item.get("id"))
    result_b64 = None
    status = None
    if isinstance(output_item, dict):
        result_b64 = _normalize_optional_text(output_item.get("result"))
        status = _normalize_optional_text(output_item.get("status"))
    if image_call_id is None or result_b64 is None:
        return None
    return {
        "type": "image_generation_call",
        "id": image_call_id,
        "result": result_b64,
        "status": status or "completed",
    }


async def _chat_messages_to_responses_input_with_image_checkpoints(
    messages: list[Any],
    *,
    scope_hash: str,
    client_model: str,
) -> list[dict[str, Any]]:
    parsed_messages: list[tuple[dict[str, Any], str, str, list[_ChatContentToken]]] = []
    assistant_image_urls: list[str] = []

    for message in messages:
        if not isinstance(message, dict):
            continue

        role = _normalize_chat_message_role(message.get("role"))
        if role is None:
            continue

        if role == "tool":
            parsed_messages.append((message, role, role, []))
            continue

        tokens = _extract_chat_content_tokens(message.get("content"), role=role)
        parsed_messages.append((message, role, "developer" if role == "system" else role, tokens))
        if role == "assistant":
            assistant_image_urls.extend(
                token.image_url
                for token in tokens
                if token.kind == "image" and token.image_url is not None
            )

    resolved_images = await resolve_chat_image_output_items(
        scope_hash=scope_hash,
        client_model=client_model,
        image_urls=assistant_image_urls,
    )
    resolved_image_iter = iter(resolved_images)
    unresolved_image_urls: list[str] = []
    input_items: list[dict[str, Any]] = []

    for message, role, responses_role, tokens in parsed_messages:
        if role == "tool":
            tool_call_id = _normalize_optional_text(message.get("tool_call_id"))
            if tool_call_id is None:
                continue
            input_items.append(
                {
                    "type": "function_call_output",
                    "call_id": tool_call_id,
                    "output": _stringify_chat_tool_output(message.get("content")),
                }
            )
            continue

        if role != "assistant":
            input_items.append(
                {
                    "type": "message",
                    "role": responses_role,
                    "content": _chat_content_tokens_to_parts(tokens, role=role),
                }
            )
            continue

        buffered_parts: list[dict[str, Any]] = []

        def flush_assistant_parts() -> None:
            nonlocal buffered_parts
            if not buffered_parts:
                return
            input_items.append(
                {
                    "type": "message",
                    "role": responses_role,
                    "content": buffered_parts,
                }
            )
            buffered_parts = []

        for token in tokens:
            if token.kind == "text" and token.text:
                buffered_parts.append({"type": "output_text", "text": token.text})
                continue

            if token.kind != "image" or token.image_url is None:
                continue

            flush_assistant_parts()
            resolved_image = next(resolved_image_iter, None)
            if resolved_image is None:
                unresolved_image_urls.append(token.image_url)
                continue
            input_item = _resolved_chat_image_to_responses_input_item(resolved_image)
            if input_item is None:
                unresolved_image_urls.append(token.image_url)
                continue
            input_items.append(input_item)

        flush_assistant_parts()
        _append_assistant_tool_calls(input_items, message)

    if unresolved_image_urls:
        raise HTTPException(
            status_code=400,
            detail=(
                "Assistant image history could not be resolved. "
                "The image was not generated by oaix or its checkpoint expired."
            ),
        )

    return input_items


async def _chat_completions_request_to_responses_request(
    request_data: ChatCompletionsRequest,
    *,
    http_request: Request | None = None,
) -> ResponsesRequest:
    raw_payload = request_data.model_dump(exclude_unset=True)
    messages = raw_payload.pop("messages", [])
    payload = {
        key: copy.deepcopy(value)
        for key, value in raw_payload.items()
        if key in CHAT_COMPLETIONS_RESPONSES_PASSTHROUGH_FIELDS
    }
    payload["model"] = request_data.model
    message_list = messages if isinstance(messages, list) else []
    if _is_responses_image_compat_model(request_data.model):
        payload["input"] = await _chat_messages_to_responses_input_with_image_checkpoints(
            message_list,
            scope_hash=_chat_image_checkpoint_scope_hash(http_request),
            client_model=request_data.model,
        )
        _enforce_image_input_count_limit(_count_responses_input_images(payload))
    else:
        payload["input"] = _chat_messages_to_responses_input(message_list)
    if request_data.stream is not None:
        payload["stream"] = bool(request_data.stream)
    return ResponsesRequest.model_validate(payload)


def _is_responses_image_compat_model(model_name: Any) -> bool:
    normalized = _normalize_optional_text(model_name)
    if normalized is None:
        return False
    return normalized in RESPONSES_IMAGE_COMPAT_MODELS


def _non_free_only_codex_model_name(request_model: str | None) -> str | None:
    normalized = _normalize_optional_text(request_model)
    if normalized is None:
        return None
    return normalized if normalized in NON_FREE_ONLY_CODEX_MODELS else None


def _request_requires_non_free_codex_token(request_model: str | None) -> bool:
    return _non_free_only_codex_model_name(request_model) is not None


def _non_free_codex_token_unavailable_detail(model_name: str) -> str:
    return f"No non-free Codex token available for {model_name} requests"


def _declared_token_plan_info(token_row: Any) -> CodexPlanInfo:
    return extract_codex_plan_info(
        getattr(token_row, "id_token", None),
        account_id=getattr(token_row, "account_id", None),
        raw_payload=getattr(token_row, "raw_payload", None),
    )


async def _effective_token_plan_type(http_request: Request, token_row: Any) -> str | None:
    stored_plan_type = _normalize_optional_text(getattr(token_row, "plan_type", None))
    if stored_plan_type is not None:
        return stored_plan_type.lower()

    declared_plan_info = _declared_token_plan_info(token_row)
    if declared_plan_info.plan_type is not None:
        return declared_plan_info.plan_type

    quota_service: CodexQuotaService | None = getattr(http_request.app.state, "quota_service", None)
    if quota_service is not None:
        cached_snapshot = quota_service.get_cached_snapshot(getattr(token_row, "id", 0))
        if cached_snapshot is not None and cached_snapshot.plan_type:
            return cached_snapshot.plan_type

    return None


async def _is_free_plan_token(http_request: Request, token_row: Any) -> bool:
    return await _effective_token_plan_type(http_request, token_row) == "free"


def _ensure_responses_image_generation_tool(payload: dict[str, Any]) -> dict[str, Any]:
    tools_payload = payload.get("tools")
    tools: list[Any] = []
    image_tool: dict[str, Any] | None = None

    if isinstance(tools_payload, list):
        for item in tools_payload:
            if isinstance(item, dict):
                copied_item = copy.deepcopy(item)
                if image_tool is None and _normalize_optional_text(copied_item.get("type")) == "image_generation":
                    image_tool = copied_item
                tools.append(copied_item)
            else:
                tools.append(copy.deepcopy(item))

    if image_tool is None:
        image_tool = {"type": "image_generation"}
        tools.append(image_tool)

    payload["tools"] = tools
    return image_tool


def _apply_images_responses_defaults(payload: dict[str, Any]) -> None:
    payload.setdefault("instructions", "")
    payload.setdefault("parallel_tool_calls", True)
    if not isinstance(payload.get("reasoning"), dict):
        payload["reasoning"] = {"effort": "medium", "summary": "auto"}

    include_payload = payload.get("include")
    if isinstance(include_payload, (list, tuple)):
        include_items = [copy.deepcopy(item) for item in include_payload]
    else:
        include_items = []
    if "reasoning.encrypted_content" not in include_items:
        include_items.append("reasoning.encrypted_content")
    payload["include"] = include_items
    payload["store"] = False


def _translate_responses_image_compat_payload(
    payload: dict[str, Any],
    *,
    compact: bool,
) -> tuple[dict[str, Any], str | None]:
    requested_model = _normalize_optional_text(payload.get("model"))
    if requested_model is None or not _is_responses_image_compat_model(requested_model):
        return payload, None

    if compact:
        raise HTTPException(status_code=400, detail="gpt-image-2 is only supported on /v1/responses")

    _enforce_image_input_count_limit(_count_responses_input_images(payload))

    translated = copy.deepcopy(payload)
    translated["model"] = DEFAULT_IMAGES_MAIN_MODEL

    image_tool = _ensure_responses_image_generation_tool(translated)
    image_tool["type"] = "image_generation"
    image_tool["model"] = requested_model
    image_tool.setdefault("action", "auto")

    for field in RESPONSES_IMAGE_TOOL_TEXT_FIELDS:
        value = _normalize_optional_text(translated.pop(field, None))
        if value is not None:
            image_tool[field] = value

    for field in RESPONSES_IMAGE_TOOL_INT_FIELDS:
        raw_value = translated.pop(field, None)
        coerced = _coerce_int(raw_value)
        if raw_value is not None and coerced is not None:
            image_tool[field] = coerced

    _apply_images_responses_defaults(translated)
    translated.pop("tool_choice", None)
    return translated, requested_model


def _apply_response_model_alias(payload: Any, model_alias: str | None) -> Any:
    normalized_alias = _normalize_optional_text(model_alias)
    if normalized_alias is None or not isinstance(payload, dict):
        return payload

    patched = copy.deepcopy(payload)
    if "model" in patched or "model_name" in patched:
        patched["model"] = normalized_alias
        if "model_name" in patched:
            patched["model_name"] = normalized_alias

    response_payload = patched.get("response")
    if isinstance(response_payload, dict):
        response_payload["model"] = normalized_alias
        if "model_name" in response_payload:
            response_payload["model_name"] = normalized_alias

    return patched


async def _upload_file_to_data_url(upload: UploadFile) -> str:
    try:
        payload = await upload.read()
    finally:
        try:
            await upload.close()
        except Exception:
            logger.exception("Failed to close uploaded image file")

    media_type = _normalize_optional_text(getattr(upload, "content_type", None))
    if media_type is None:
        media_type = mimetypes.guess_type(getattr(upload, "filename", "") or "")[0] or "application/octet-stream"
    encoded = base64.b64encode(payload).decode("ascii")
    return f"data:{media_type};base64,{encoded}"


def _extract_image_url_candidate(value: Any) -> str | None:
    if isinstance(value, str):
        return _normalize_optional_text(value)
    if isinstance(value, dict):
        return _normalize_optional_text(value.get("url"))
    return None


def _extract_json_image_reference(item: Any) -> str | None:
    if isinstance(item, str):
        return _normalize_optional_text(item)
    if not isinstance(item, dict):
        return None

    image_url = _extract_image_url_candidate(item.get("image_url"))
    if image_url is not None:
        return image_url
    return _extract_image_url_candidate(item.get("url"))


def _extract_json_mask_reference(item: Any) -> str | None:
    if isinstance(item, str):
        return _normalize_optional_text(item)
    if not isinstance(item, dict):
        return None

    if _normalize_optional_text(item.get("file_id")) is not None:
        raise HTTPException(status_code=400, detail="mask.file_id is not supported (use mask.image_url instead)")

    image_url = _extract_image_url_candidate(item.get("image_url"))
    if image_url is not None:
        return image_url
    return _extract_image_url_candidate(item.get("url"))


def _build_images_responses_payload(prompt: str, images: list[str], tool: dict[str, Any]) -> dict[str, Any]:
    content: list[dict[str, Any]] = [{"type": "input_text", "text": prompt}]
    for image_url in images:
        normalized = _normalize_optional_text(image_url)
        if normalized is None:
            continue
        content.append(
            {
                "type": "input_image",
                "image_url": normalized,
            }
        )

    payload = {
        "stream": True,
        "model": DEFAULT_IMAGES_MAIN_MODEL,
        "input": [{"type": "message", "role": "user", "content": content}],
        "tools": [tool],
    }
    _apply_images_responses_defaults(payload)
    return payload


async def _parse_images_generations_request(http_request: Request) -> ImageProxyRequest:
    try:
        body = await http_request.json()
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail="Request body must be valid JSON") from exc

    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="Request body must be a JSON object")

    prompt = _normalize_optional_text(body.get("prompt"))
    if prompt is None:
        raise HTTPException(status_code=400, detail="prompt is required")

    image_model = _normalize_optional_text(body.get("model")) or DEFAULT_IMAGES_TOOL_MODEL
    response_format = _normalize_image_response_format(body.get("response_format"))
    stream = _coerce_bool(body.get("stream"), False)

    tool: dict[str, Any] = {
        "type": "image_generation",
        "action": "generate",
        "model": image_model,
    }
    for field in ("size", "quality", "background", "output_format", "moderation"):
        value = _normalize_optional_text(body.get(field))
        if value is not None:
            tool[field] = value
    for field in ("output_compression", "partial_images"):
        raw_value = body.get(field)
        coerced = _coerce_int(raw_value)
        if raw_value is not None and coerced is not None:
            tool[field] = coerced

    return ImageProxyRequest(
        model_name=image_model,
        response_format=response_format,
        stream=stream,
        stream_prefix="image_generation",
        responses_payload=_build_images_responses_payload(prompt, [], tool),
    )


async def _parse_images_edits_json_request(http_request: Request) -> ImageProxyRequest:
    try:
        body = await http_request.json()
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail="Request body must be valid JSON") from exc

    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="Request body must be a JSON object")

    prompt = _normalize_optional_text(body.get("prompt"))
    if prompt is None:
        raise HTTPException(status_code=400, detail="prompt is required")

    images: list[str] = []
    if isinstance(body.get("images"), list):
        for item in body["images"]:
            image_url = _extract_json_image_reference(item)
            if image_url is not None:
                images.append(image_url)
    if not images:
        raise HTTPException(status_code=400, detail="images[].image_url is required (file_id is not supported)")

    mask_image_url = _extract_json_mask_reference(body.get("mask"))
    _enforce_image_input_count_limit(len(images) + (1 if mask_image_url is not None else 0))
    image_model = _normalize_optional_text(body.get("model")) or DEFAULT_IMAGES_TOOL_MODEL
    response_format = _normalize_image_response_format(body.get("response_format"))
    stream = _coerce_bool(body.get("stream"), False)

    tool: dict[str, Any] = {
        "type": "image_generation",
        "action": "edit",
        "model": image_model,
    }
    for field in ("size", "quality", "background", "output_format", "input_fidelity", "moderation"):
        value = _normalize_optional_text(body.get(field))
        if value is not None:
            tool[field] = value
    for field in ("output_compression", "partial_images"):
        raw_value = body.get(field)
        coerced = _coerce_int(raw_value)
        if raw_value is not None and coerced is not None:
            tool[field] = coerced

    if mask_image_url is not None:
        tool["input_image_mask"] = {"image_url": mask_image_url}

    return ImageProxyRequest(
        model_name=image_model,
        response_format=response_format,
        stream=stream,
        stream_prefix="image_edit",
        responses_payload=_build_images_responses_payload(prompt, images, tool),
    )


async def _parse_images_edits_multipart_request(http_request: Request) -> ImageProxyRequest:
    try:
        form = await http_request.form()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Invalid multipart form: {exc}") from exc

    prompt = _normalize_optional_text(form.get("prompt"))
    if prompt is None:
        raise HTTPException(status_code=400, detail="prompt is required")

    image_entries = form.getlist("image[]") or form.getlist("image")
    mask_value = form.get("mask")
    image_entry_count = sum(
        1
        for entry in image_entries
        if isinstance(entry, UploadFile) or _normalize_optional_text(entry) is not None
    )
    mask_entry_count = (
        1
        if isinstance(mask_value, UploadFile) or _normalize_optional_text(mask_value) is not None
        else 0
    )
    _enforce_image_input_count_limit(image_entry_count + mask_entry_count)

    images: list[str] = []
    for entry in image_entries:
        if isinstance(entry, UploadFile):
            images.append(await _upload_file_to_data_url(entry))
            continue
        image_url = _normalize_optional_text(entry)
        if image_url is not None:
            images.append(image_url)

    if not images:
        raise HTTPException(status_code=400, detail="image is required")

    mask_image_url: str | None = None
    if isinstance(mask_value, UploadFile):
        mask_image_url = await _upload_file_to_data_url(mask_value)
    else:
        mask_image_url = _normalize_optional_text(mask_value)

    image_model = _normalize_optional_text(form.get("model")) or DEFAULT_IMAGES_TOOL_MODEL
    response_format = _normalize_image_response_format(form.get("response_format"))
    stream = _coerce_bool(form.get("stream"), False)

    tool: dict[str, Any] = {
        "type": "image_generation",
        "action": "edit",
        "model": image_model,
    }
    for field in ("size", "quality", "background", "output_format", "input_fidelity", "moderation"):
        value = _normalize_optional_text(form.get(field))
        if value is not None:
            tool[field] = value
    for field in ("output_compression", "partial_images"):
        raw_value = form.get(field)
        coerced = _coerce_int(raw_value)
        if raw_value not in (None, "") and coerced is not None:
            tool[field] = coerced

    if mask_image_url is not None:
        tool["input_image_mask"] = {"image_url": mask_image_url}

    return ImageProxyRequest(
        model_name=image_model,
        response_format=response_format,
        stream=stream,
        stream_prefix="image_edit",
        responses_payload=_build_images_responses_payload(prompt, images, tool),
    )


async def _parse_images_edits_request(http_request: Request) -> ImageProxyRequest:
    content_type = (http_request.headers.get("content-type") or "").strip().lower()
    if content_type.startswith("application/json"):
        return await _parse_images_edits_json_request(http_request)
    if not content_type or content_type.startswith("multipart/form-data"):
        return await _parse_images_edits_multipart_request(http_request)
    raise HTTPException(status_code=400, detail=f"Unsupported Content-Type {content_type!r}")


def _extract_response_model_name(payload: Any) -> str | None:
    if not isinstance(payload, dict):
        return None

    for candidate in (
        payload.get("model_name"),
        payload.get("model"),
        _mapping_get(payload, "response", "model_name"),
        _mapping_get(payload, "response", "model"),
    ):
        model_name = _normalize_optional_text(candidate)
        if model_name:
            return model_name
    return None


def _extract_responses_stream_event(raw_event: str) -> tuple[str, Any]:
    event_name = ""
    data_lines: list[str] = []
    for line in raw_event.splitlines():
        if line.startswith("event:"):
            event_name = line[6:].strip()
        elif line.startswith("data:"):
            data_lines.append(line[5:].strip())

    data_str = "\n".join(data_lines).strip()
    if data_str == "[DONE]":
        return "[DONE]", "[DONE]"

    parsed_payload: Any = data_str
    if data_str:
        try:
            parsed_payload = json.loads(data_str)
        except Exception:
            parsed_payload = data_str

    if not event_name and isinstance(parsed_payload, dict):
        event_name = str(parsed_payload.get("type") or "").strip()

    return event_name, parsed_payload


def _stream_keepalive_interval_seconds() -> float:
    raw = str(os.getenv("STREAM_KEEPALIVE_INTERVAL_SECONDS", "") or "").strip()
    try:
        value = float(raw) if raw else DEFAULT_STREAM_KEEPALIVE_INTERVAL_SECONDS
    except ValueError:
        value = DEFAULT_STREAM_KEEPALIVE_INTERVAL_SECONDS
    return max(1.0, value)


def _sse_response_headers() -> dict[str, str]:
    return {
        "Cache-Control": "no-cache, no-transform",
        "X-Accel-Buffering": "no",
    }


def _is_sse_comment_frame(raw_event: str) -> bool:
    has_line = False
    for line in raw_event.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        has_line = True
        if not stripped.startswith(":"):
            return False
    return has_line


async def _iterate_with_sse_keepalive(
    source_iter: AsyncIterator[bytes],
    *,
    heartbeat_interval_seconds: float | None,
    heartbeat_bytes: bytes = STREAM_KEEPALIVE_COMMENT,
    pending_chunk_task: asyncio.Task[bytes] | None = None,
) -> AsyncGenerator[bytes, None]:
    if heartbeat_interval_seconds is None or heartbeat_interval_seconds <= 0:
        next_chunk_task = pending_chunk_task
        if next_chunk_task is None:
            async for chunk in source_iter:
                yield chunk
            return
        try:
            while True:
                try:
                    chunk = await next_chunk_task
                except StopAsyncIteration:
                    return
                yield chunk
                next_chunk_task = asyncio.create_task(source_iter.__anext__())
        finally:
            if next_chunk_task is not None and not next_chunk_task.done():
                next_chunk_task.cancel()
                with suppress(asyncio.CancelledError):
                    await next_chunk_task
        return

    next_chunk_task = pending_chunk_task or asyncio.create_task(source_iter.__anext__())
    try:
        while True:
            done, _ = await asyncio.wait({next_chunk_task}, timeout=heartbeat_interval_seconds)
            if next_chunk_task in done:
                try:
                    chunk = next_chunk_task.result()
                except StopAsyncIteration:
                    return
                yield chunk
                next_chunk_task = asyncio.create_task(source_iter.__anext__())
                continue

            yield heartbeat_bytes
    finally:
        if not next_chunk_task.done():
            next_chunk_task.cancel()
            with suppress(asyncio.CancelledError):
                await next_chunk_task


async def _wrap_sse_stream_with_initial_keepalive(
    source_iter: AsyncIterator[bytes],
    *,
    heartbeat_interval_seconds: float | None,
    heartbeat_bytes: bytes = STREAM_KEEPALIVE_COMMENT,
) -> AsyncGenerator[bytes, None]:
    if heartbeat_interval_seconds is None or heartbeat_interval_seconds <= 0:
        async for chunk in source_iter:
            yield chunk
        return

    first_chunk_task = asyncio.create_task(source_iter.__anext__())
    handoff_to_keepalive = False
    try:
        try:
            done, _ = await asyncio.wait({first_chunk_task}, timeout=heartbeat_interval_seconds)
            if first_chunk_task in done:
                try:
                    first_chunk = first_chunk_task.result()
                except StopAsyncIteration:
                    return
                yield first_chunk
                async for chunk in _iterate_with_sse_keepalive(
                    source_iter,
                    heartbeat_interval_seconds=heartbeat_interval_seconds,
                    heartbeat_bytes=heartbeat_bytes,
                ):
                    yield chunk
                return

            handoff_to_keepalive = True
            yield heartbeat_bytes
            async for chunk in _iterate_with_sse_keepalive(
                source_iter,
                heartbeat_interval_seconds=heartbeat_interval_seconds,
                heartbeat_bytes=heartbeat_bytes,
                pending_chunk_task=first_chunk_task,
            ):
                yield chunk
        except StopAsyncIteration:
            return
        except HTTPException as exc:
            yield _build_stream_error_event(exc.status_code, str(getattr(exc, "detail", "") or exc))
            yield b"data: [DONE]\n\n"
        except Exception as exc:
            yield _build_stream_error_event(502, str(exc) or type(exc).__name__)
            yield b"data: [DONE]\n\n"
    finally:
        if not handoff_to_keepalive and not first_chunk_task.done():
            first_chunk_task.cancel()
            with suppress(asyncio.CancelledError):
                await first_chunk_task


def _responses_error_status_code(error_obj: Any) -> int:
    if _is_terminal_image_generation_error(error_obj):
        return 400

    if isinstance(error_obj, dict):
        raw_status = error_obj.get("status_code") or error_obj.get("status")
        try:
            status_code = int(raw_status)
        except (TypeError, ValueError):
            status_code = None
        if status_code is not None and 100 <= status_code <= 599:
            return status_code

        error_code = str(error_obj.get("code") or "").strip().lower()
        if error_code in RESPONSES_FAILURE_STATUS_BY_CODE:
            return RESPONSES_FAILURE_STATUS_BY_CODE[error_code]

        error_type = str(error_obj.get("type") or "").strip().lower()
        if error_type in RESPONSES_FAILURE_STATUS_BY_TYPE:
            return RESPONSES_FAILURE_STATUS_BY_TYPE[error_type]

        message = str(error_obj.get("message") or "").lower()
        if "rate limit" in message or "too many requests" in message:
            return 429
        if "invalid" in message or "unsupported" in message:
            return 400
        if "not found" in message:
            return 404
        if "permission" in message or "forbidden" in message:
            return 403
        if "auth" in message or "api key" in message or "unauthorized" in message:
            return 401

    return 500


def _is_terminal_image_generation_error_text(value: Any) -> bool:
    text = str(value or "").strip().lower()
    return (
        "image_generation_user_error" in text
        or "moderation_blocked" in text
        or "rejected by the safety system" in text
        or "safety_violations" in text
    )


def _is_terminal_image_generation_error(error_obj: Any) -> bool:
    if not isinstance(error_obj, dict):
        return _is_terminal_image_generation_error_text(error_obj)

    error_type = str(error_obj.get("type") or "").strip().lower()
    error_code = str(error_obj.get("code") or "").strip().lower()
    return (
        error_type == "image_generation_user_error"
        or error_code == "moderation_blocked"
        or _is_terminal_image_generation_error_text(error_obj.get("message"))
    )


def _responses_failure_http_exception(payload: Any) -> HTTPException | None:
    if not isinstance(payload, dict):
        return None

    error_obj = None
    response_status = str(_mapping_get(payload, "response", "status") or "").strip().lower()
    payload_status = str(payload.get("status") or "").strip().lower()
    payload_type = str(payload.get("type") or "").strip().lower()

    if payload_type == "error" and payload.get("error") is not None:
        error_obj = payload.get("error")
    elif payload_type == "response.failed":
        error_obj = _mapping_get(payload, "response", "error")
    elif payload_status == "failed":
        error_obj = payload.get("error")
    elif response_status == "failed":
        error_obj = _mapping_get(payload, "response", "error")
    elif isinstance(payload.get("error"), dict):
        error_obj = payload.get("error")

    if error_obj is None and (payload_status == "failed" or response_status == "failed"):
        error_obj = {"message": "Responses upstream returned status=failed"}

    if error_obj is None:
        return None

    detail = json.dumps({"error": error_obj}, ensure_ascii=False)
    if _is_terminal_image_generation_error(error_obj):
        return _non_retryable_gateway_http_exception(
            status_code=_responses_error_status_code(error_obj),
            detail=detail,
            record_token_error=False,
        )

    return HTTPException(status_code=_responses_error_status_code(error_obj), detail=detail)


def _merge_mapping(target: dict[str, Any], patch: dict[str, Any]) -> dict[str, Any]:
    for key, value in patch.items():
        if isinstance(value, dict) and isinstance(target.get(key), dict):
            _merge_mapping(target[key], value)
            continue
        target[key] = value
    return target


def _append_output_text_delta(
    output_text_parts: dict[tuple[int, int], list[str]],
    event_payload: dict[str, Any],
) -> None:
    delta = event_payload.get("delta")
    if delta is None:
        return

    try:
        output_index = int(event_payload.get("output_index", 0) or 0)
    except (TypeError, ValueError):
        output_index = 0
    try:
        content_index = int(event_payload.get("content_index", 0) or 0)
    except (TypeError, ValueError):
        content_index = 0

    output_text_parts.setdefault((output_index, content_index), []).append(str(delta))


def _build_responses_output_from_text_parts(
    output_text_parts: dict[tuple[int, int], list[str]],
) -> list[dict[str, Any]]:
    grouped: dict[int, dict[int, str]] = {}
    for (output_index, content_index), parts in sorted(output_text_parts.items()):
        grouped.setdefault(output_index, {})[content_index] = "".join(parts)

    output: list[dict[str, Any]] = []
    for output_index in sorted(grouped):
        content: list[dict[str, Any]] = []
        for content_index in sorted(grouped[output_index]):
            text = grouped[output_index][content_index]
            content.append(
                {
                    "type": "output_text",
                    "text": text,
                }
            )
        output.append(
            {
                "type": "message",
                "content": content,
            }
        )
    return output


def _finalize_collected_response(
    response_snapshot: dict[str, Any] | None,
    *,
    model: str,
    output_text_parts: dict[tuple[int, int], list[str]],
) -> dict[str, Any]:
    response_data = copy.deepcopy(response_snapshot or {})

    if not response_data.get("id"):
        response_data["id"] = f"resp_{uuid.uuid4().hex}"
    if not response_data.get("object"):
        response_data["object"] = "response"
    if response_data.get("created_at") is None:
        response_data["created_at"] = int(utcnow().timestamp())
    if not response_data.get("model"):
        response_data["model"] = model
    response_data["status"] = "completed"

    if output_text_parts and not response_data.get("output"):
        response_data["output"] = _build_responses_output_from_text_parts(output_text_parts)

    return response_data


def _collect_responses_output_item_done(
    event_payload: Any,
    *,
    output_items_by_index: dict[int, dict[str, Any]],
    output_items_fallback: list[dict[str, Any]],
) -> None:
    if not isinstance(event_payload, dict):
        return

    item = event_payload.get("item")
    if not isinstance(item, dict):
        return

    output_index = _coerce_int(event_payload.get("output_index"))
    item_copy = copy.deepcopy(item)
    if output_index is None:
        output_items_fallback.append(item_copy)
        return
    output_items_by_index[output_index] = item_copy


def _patch_completed_output_from_output_items(
    payload: Any,
    *,
    output_items_by_index: dict[int, dict[str, Any]],
    output_items_fallback: list[dict[str, Any]],
) -> Any:
    if not isinstance(payload, dict):
        return payload

    response_payload = payload.get("response")
    if not isinstance(response_payload, dict):
        return payload

    output_items = response_payload.get("output")
    should_patch_output = (
        (not isinstance(output_items, list) or not output_items)
        and (output_items_by_index or output_items_fallback)
    )
    if not should_patch_output:
        return payload

    patched_payload = copy.deepcopy(payload)
    patched_response = patched_payload.get("response")
    if not isinstance(patched_response, dict):
        return patched_payload

    patched_output: list[dict[str, Any]] = [
        copy.deepcopy(output_items_by_index[index]) for index in sorted(output_items_by_index)
    ]
    patched_output.extend(copy.deepcopy(item) for item in output_items_fallback)
    patched_response["output"] = patched_output
    return patched_payload


def _build_synthetic_completed_event_from_output_items(
    *,
    response_id: str | None = None,
    model_name: str | None = None,
    created_at: int | None = None,
    output_items_by_index: dict[int, dict[str, Any]],
    output_items_fallback: list[dict[str, Any]],
) -> dict[str, Any] | None:
    if not output_items_by_index and not output_items_fallback:
        return None

    response_payload: dict[str, Any] = {"status": "completed"}
    if response_id:
        response_payload["id"] = response_id
    if model_name:
        response_payload["model"] = model_name
    if created_at is not None and created_at > 0:
        response_payload["created_at"] = created_at

    patched_payload = _patch_completed_output_from_output_items(
        {
            "type": "response.completed",
            "response": response_payload,
        },
        output_items_by_index=output_items_by_index,
        output_items_fallback=output_items_fallback,
    )
    patched_response = patched_payload.get("response")
    if not isinstance(patched_response, dict):
        return None

    output_items = patched_response.get("output")
    if not isinstance(output_items, list) or not output_items:
        return None
    return patched_payload


def _non_retryable_gateway_http_exception(
    *,
    status_code: int,
    detail: str,
    record_token_error: bool = True,
) -> _GatewayProxyHTTPException:
    return _GatewayProxyHTTPException(
        status_code=status_code,
        detail=detail,
        retryable=False,
        record_token_error=record_token_error,
    )


def _retryable_gateway_http_exception(
    *,
    status_code: int,
    detail: str,
    record_token_error: bool = True,
) -> _GatewayProxyHTTPException:
    return _GatewayProxyHTTPException(
        status_code=status_code,
        detail=detail,
        retryable=True,
        record_token_error=record_token_error,
    )


def _upstream_error_http_exception(status_code: int, detail: str) -> HTTPException:
    error_obj = _extract_error_object(detail)
    if _is_terminal_image_generation_error(error_obj) or _is_terminal_image_generation_error_text(detail):
        return _non_retryable_gateway_http_exception(
            status_code=400,
            detail=detail,
            record_token_error=False,
        )
    return HTTPException(status_code=status_code, detail=detail)


async def _collect_responses_json_from_sse(
    upstream_iter: AsyncIterator[bytes],
    *,
    model: str,
) -> tuple[dict[str, Any], datetime | None]:
    response_snapshot: dict[str, Any] | None = None
    output_text_parts: dict[tuple[int, int], list[str]] = {}
    output_items_by_index: dict[int, dict[str, Any]] = {}
    output_items_fallback: list[dict[str, Any]] = []
    first_token_at: datetime | None = None

    decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
    text_buffer = ""
    sse_scan_start = 0

    while True:
        try:
            chunk = await upstream_iter.__anext__()
        except StopAsyncIteration:
            if text_buffer.strip():
                raise HTTPException(status_code=502, detail="Upstream closed stream with an incomplete SSE event")
            break

        text_buffer += decoder.decode(chunk)

        while True:
            raw_event, text_buffer, sse_scan_start = _pop_sse_event_from_buffer(text_buffer, sse_scan_start)
            if raw_event is None:
                break

            if not raw_event.strip():
                continue

            event_type, event_payload = _extract_responses_stream_event(raw_event)

            if event_type == "[DONE]":
                return _finalize_collected_response(
                    response_snapshot,
                    model=model,
                    output_text_parts=output_text_parts,
                ), first_token_at

            semantic_failure = _responses_failure_http_exception(event_payload)
            if semantic_failure is not None:
                raise semantic_failure

            if first_token_at is None and event_type and event_type not in RESPONSES_STREAM_PREFLIGHT_EVENTS:
                first_token_at = utcnow()

            if not isinstance(event_payload, dict):
                continue

            if event_type == "response.output_item.done":
                _collect_responses_output_item_done(
                    event_payload,
                    output_items_by_index=output_items_by_index,
                    output_items_fallback=output_items_fallback,
                )
                continue

            response_obj = event_payload.get("response")
            if isinstance(response_obj, dict):
                if event_type == "response.completed":
                    event_payload = _patch_completed_output_from_output_items(
                        event_payload,
                        output_items_by_index=output_items_by_index,
                        output_items_fallback=output_items_fallback,
                    )
                    response_obj = event_payload.get("response")
                if response_snapshot is None:
                    response_snapshot = {}
                _merge_mapping(response_snapshot, response_obj)

            if event_type == "response.output_text.delta":
                _append_output_text_delta(output_text_parts, event_payload)

            if event_type == "response.completed":
                return _finalize_collected_response(
                    response_snapshot,
                    model=model,
                    output_text_parts=output_text_parts,
                ), first_token_at

    if response_snapshot is None and not output_text_parts:
        raise HTTPException(status_code=502, detail="Upstream closed stream without data")

    if response_snapshot is not None:
        response_snapshot = _patch_completed_output_from_output_items(
            {
                "type": "response.completed",
                "response": response_snapshot,
            },
            output_items_by_index=output_items_by_index,
            output_items_fallback=output_items_fallback,
        )["response"]

    return _finalize_collected_response(
        response_snapshot,
        model=model,
        output_text_parts=output_text_parts,
    ), first_token_at


async def _read_response_body_with_first_chunk_time(
    upstream_iter: AsyncIterator[bytes],
) -> tuple[bytes, datetime | None]:
    chunks: list[bytes] = []
    first_chunk_at: datetime | None = None

    async for chunk in upstream_iter:
        if chunk and first_chunk_at is None:
            first_chunk_at = utcnow()
        chunks.append(chunk)

    return b"".join(chunks), first_chunk_at


def _decode_responses_json_body(raw: bytes) -> dict[str, Any]:
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=502, detail="Upstream returned invalid JSON") from exc

    semantic_failure = _responses_failure_http_exception(payload)
    if semantic_failure is not None:
        raise semantic_failure
    if not isinstance(payload, dict):
        raise HTTPException(status_code=502, detail="Upstream returned a non-object JSON response")
    return payload


async def _prime_responses_upstream_stream(
    upstream_iter: AsyncIterator[bytes],
    *,
    heartbeat_interval_seconds: float | None = None,
) -> _PrimedResponsesStream:
    """
    Buffer only initial status events so semantic stream failures can still
    trigger key failover before the downstream response is committed.
    """
    buffered_chunks: list[bytes] = []
    decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
    text_buffer = ""
    sse_scan_start = 0
    model_name: str | None = None
    keepalive_deadline: float | None = None
    pending_chunk_task: asyncio.Task[bytes] | None = None

    if heartbeat_interval_seconds is not None and heartbeat_interval_seconds > 0:
        keepalive_deadline = asyncio.get_running_loop().time() + heartbeat_interval_seconds

    pending_chunk_task = asyncio.create_task(upstream_iter.__anext__())

    try:
        while True:
            timeout: float | None = None
            if keepalive_deadline is not None:
                timeout = max(0.0, keepalive_deadline - asyncio.get_running_loop().time())
                if timeout == 0.0:
                    task_to_return = pending_chunk_task
                    pending_chunk_task = None
                    return _PrimedResponsesStream(
                        buffered_chunks=buffered_chunks,
                        stream_committed=True,
                        model_name=model_name,
                        first_token_observed=False,
                        emit_initial_keepalive=True,
                        pending_chunk_task=task_to_return,
                    )

            done, _ = await asyncio.wait({pending_chunk_task}, timeout=timeout)
            if pending_chunk_task not in done:
                task_to_return = pending_chunk_task
                pending_chunk_task = None
                return _PrimedResponsesStream(
                    buffered_chunks=buffered_chunks,
                    stream_committed=True,
                    model_name=model_name,
                    first_token_observed=False,
                    emit_initial_keepalive=True,
                    pending_chunk_task=task_to_return,
                )

            try:
                chunk = pending_chunk_task.result()
            except StopAsyncIteration as exc:
                if not buffered_chunks:
                    raise HTTPException(status_code=502, detail="Upstream closed stream without data") from exc
                if text_buffer.strip():
                    raise HTTPException(status_code=502, detail="Upstream closed stream with an incomplete SSE event")
                return _PrimedResponsesStream(
                    buffered_chunks=buffered_chunks,
                    stream_committed=False,
                    model_name=model_name,
                    first_token_observed=False,
                )

            pending_chunk_task = None
            buffered_chunks.append(chunk)
            text_buffer += decoder.decode(chunk)

            while True:
                raw_event, text_buffer, sse_scan_start = _pop_sse_event_from_buffer(text_buffer, sse_scan_start)
                if raw_event is None:
                    break

                if not raw_event.strip():
                    continue

                event_type, event_payload = _extract_responses_stream_event(raw_event)
                extracted_model_name = _extract_response_model_name(event_payload)
                if extracted_model_name:
                    model_name = extracted_model_name
                if event_type == "[DONE]":
                    return _PrimedResponsesStream(
                        buffered_chunks=buffered_chunks,
                        stream_committed=False,
                        model_name=model_name,
                        first_token_observed=False,
                    )

                semantic_failure = _responses_failure_http_exception(event_payload)
                if semantic_failure is not None:
                    raise semantic_failure

                if not event_type or event_type not in RESPONSES_STREAM_PREFLIGHT_EVENTS:
                    return _PrimedResponsesStream(
                        buffered_chunks=buffered_chunks,
                        stream_committed=True,
                        model_name=model_name,
                        first_token_observed=True,
                    )

            pending_chunk_task = asyncio.create_task(upstream_iter.__anext__())
    finally:
        if pending_chunk_task is not None and not pending_chunk_task.done():
            pending_chunk_task.cancel()
            with suppress(asyncio.CancelledError):
                await pending_chunk_task


async def _enter_upstream_stream_with_timing(stream_cm, *, compact: bool = False):
    timing = _current_request_timing()
    started_at = time.perf_counter()
    timing_context_token = _UPSTREAM_HTTP_PHASE_TIMING.set(timing) if timing is not None else None
    compact_context_token = _UPSTREAM_HTTP_PHASE_COMPACT.set(bool(compact))
    try:
        return await stream_cm.__aenter__()
    finally:
        _UPSTREAM_HTTP_PHASE_COMPACT.reset(compact_context_token)
        if timing_context_token is not None:
            _UPSTREAM_HTTP_PHASE_TIMING.reset(timing_context_token)
        if timing is not None:
            timing.add_elapsed(
                "upstream_compact_stream_enter_ms" if compact else "upstream_stream_enter_ms",
                started_at,
            )


async def _prime_responses_upstream_stream_with_timing(
    upstream_iter: AsyncIterator[bytes],
    *,
    heartbeat_interval_seconds: float | None = None,
) -> _PrimedResponsesStream:
    timing = _current_request_timing()
    with timing.measure("upstream_first_semantic_sse_ms") if timing is not None else suppress():
        return await _prime_responses_upstream_stream(
            upstream_iter,
            heartbeat_interval_seconds=heartbeat_interval_seconds,
        )


def _extract_images_from_completed_response(
    payload: Any,
) -> tuple[list[ImageCallResult], int, dict[str, Any] | None]:
    if not isinstance(payload, dict) or str(payload.get("type") or "").strip() != "response.completed":
        raise ValueError("Unexpected image response event type")

    response_payload = payload.get("response")
    if not isinstance(response_payload, dict):
        raise ValueError("Image response.completed event is missing response payload")

    created_at = _coerce_int(response_payload.get("created_at"))
    if created_at is None or created_at <= 0:
        created_at = int(utcnow().timestamp())

    results: list[ImageCallResult] = []
    output_items = response_payload.get("output")
    if isinstance(output_items, list):
        for item in output_items:
            if not isinstance(item, dict) or str(item.get("type") or "").strip() != "image_generation_call":
                continue
            result_b64 = _normalize_optional_text(item.get("result"))
            if result_b64 is None:
                continue
            results.append(
                ImageCallResult(
                    result_b64=result_b64,
                    revised_prompt=_normalize_optional_text(item.get("revised_prompt")),
                    output_format=_normalize_optional_text(item.get("output_format")),
                    size=_normalize_optional_text(item.get("size")),
                    background=_normalize_optional_text(item.get("background")),
                    quality=_normalize_optional_text(item.get("quality")),
                )
            )

    if not results:
        raise ValueError("Upstream did not return image output")

    usage_payload = _mapping_get(payload, "response", "tool_usage", "image_gen")
    usage = copy.deepcopy(usage_payload) if isinstance(usage_payload, dict) else None
    return results, created_at, usage


def _build_images_api_response(
    results: list[ImageCallResult],
    *,
    created_at: int,
    usage_payload: dict[str, Any] | None,
    response_format: str,
) -> dict[str, Any]:
    normalized_response_format = _normalize_image_response_format(response_format)
    data: list[dict[str, Any]] = []
    for item in results:
        result_item: dict[str, Any] = {}
        if normalized_response_format == "url":
            mime_type = _mime_type_from_output_format(item.output_format)
            result_item["url"] = f"data:{mime_type};base64,{item.result_b64}"
        else:
            result_item["b64_json"] = item.result_b64
        if item.revised_prompt is not None:
            result_item["revised_prompt"] = item.revised_prompt
        data.append(result_item)

    response: dict[str, Any] = {
        "created": created_at,
        "data": data,
    }
    first_item = results[0]
    if first_item.background is not None:
        response["background"] = first_item.background
    if first_item.output_format is not None:
        response["output_format"] = first_item.output_format
    if first_item.quality is not None:
        response["quality"] = first_item.quality
    if first_item.size is not None:
        response["size"] = first_item.size
    if usage_payload is not None:
        response["usage"] = copy.deepcopy(usage_payload)
    return response


def _chat_completion_created_at(payload: Any) -> int:
    created_at = _coerce_int(
        _mapping_get(payload, "created")
        or _mapping_get(payload, "created_at")
        or _mapping_get(payload, "response", "created_at")
        or _mapping_get(payload, "response", "created")
    )
    if created_at is None or created_at <= 0:
        return int(utcnow().timestamp())
    return created_at


def _chat_completion_response_id(payload: Any) -> str:
    for candidate in (
        _mapping_get(payload, "id"),
        _mapping_get(payload, "response", "id"),
    ):
        normalized = _normalize_optional_text(candidate)
        if normalized is not None:
            return normalized
    return f"chatcmpl_{uuid.uuid4().hex}"


def _chat_completion_tool_calls_from_output(output_items: Any) -> list[dict[str, Any]]:
    if not isinstance(output_items, list):
        return []

    tool_calls: list[dict[str, Any]] = []
    for item in output_items:
        if not isinstance(item, dict):
            continue
        if _normalize_optional_text(item.get("type")) != "function_call":
            continue

        name = _normalize_optional_text(item.get("name"))
        if name is None:
            continue
        tool_calls.append(
            {
                "id": _normalize_optional_text(item.get("call_id")) or f"call_{uuid.uuid4().hex}",
                "type": "function",
                "function": {
                    "name": name,
                    "arguments": str(item.get("arguments") or ""),
                },
            }
        )
    return tool_calls


def _chat_completion_message_from_response_payload(
    payload: dict[str, Any],
) -> tuple[dict[str, Any], str]:
    output_items = payload.get("output")
    message_parts: list[str] = []

    if isinstance(output_items, list):
        for item in output_items:
            if not isinstance(item, dict):
                continue

            item_type = _normalize_optional_text(item.get("type"))
            if item_type == "message":
                content_items = item.get("content")
                if not isinstance(content_items, list):
                    continue
                text_parts: list[str] = []
                for content_item in content_items:
                    if not isinstance(content_item, dict):
                        continue
                    content_type = _normalize_optional_text(content_item.get("type"))
                    if content_type in {"output_text", "input_text", "text"} and content_item.get("text") is not None:
                        text_parts.append(str(content_item.get("text")))
                if text_parts:
                    message_parts.append("".join(text_parts))
                continue

            if item_type == "image_generation_call":
                result_b64 = _normalize_optional_text(item.get("result"))
                if result_b64 is None:
                    continue
                mime_type = _mime_type_from_output_format(_normalize_optional_text(item.get("output_format")))
                message_parts.append(f"![image](data:{mime_type};base64,{result_b64})")

    content = "\n\n".join(part for part in message_parts if part)
    tool_calls = _chat_completion_tool_calls_from_output(output_items)

    message: dict[str, Any] = {
        "role": "assistant",
        "content": content or None,
    }
    if tool_calls:
        message["tool_calls"] = tool_calls
        if not content:
            message["content"] = None
        return message, "tool_calls"
    return message, "stop"


def _chat_completion_usage_payload(payload: dict[str, Any], *, model_name: str | None) -> dict[str, Any] | None:
    usage_metrics = extract_usage_metrics(payload, model_name=model_name)
    if usage_metrics is None:
        return None
    return {
        "prompt_tokens": usage_metrics.input_tokens,
        "completion_tokens": usage_metrics.output_tokens,
        "total_tokens": usage_metrics.total_tokens,
    }


def _chat_completion_stream_include_usage(request_data: ChatCompletionsRequest) -> bool:
    stream_options = request_data.model_dump(exclude_unset=True).get("stream_options")
    if not isinstance(stream_options, dict):
        return True
    return _coerce_bool(stream_options.get("include_usage"), True)


def _build_chat_completions_response(
    payload: dict[str, Any],
    *,
    request_model: str,
) -> dict[str, Any]:
    model_name = _extract_response_model_name(payload) or request_model
    message, finish_reason = _chat_completion_message_from_response_payload(payload)
    response = {
        "id": _chat_completion_response_id(payload),
        "object": "chat.completion",
        "created": _chat_completion_created_at(payload),
        "model": model_name,
        "choices": [
            {
                "index": 0,
                "message": message,
                "finish_reason": finish_reason,
            }
        ],
    }
    usage_payload = _chat_completion_usage_payload(payload, model_name=model_name)
    if usage_payload is not None:
        response["usage"] = usage_payload
    return response


def _chat_completion_assistant_message(chat_payload: dict[str, Any]) -> dict[str, Any] | None:
    choices = chat_payload.get("choices")
    if not isinstance(choices, list) or not choices:
        return None
    choice = choices[0]
    if not isinstance(choice, dict):
        return None
    message = choice.get("message")
    return message if isinstance(message, dict) else None


def _chat_completion_assistant_content_from_response_payload(
    responses_payload: dict[str, Any] | None,
    *,
    request_model: str,
) -> str | None:
    if not isinstance(responses_payload, dict):
        return None
    message = _chat_completion_assistant_message(
        _build_chat_completions_response(
            responses_payload,
            request_model=request_model,
        )
    )
    if not isinstance(message, dict):
        return None
    content = message.get("content")
    return str(content) if content is not None else None


def _build_chat_image_checkpoint_callback(
    *,
    request_model: str,
    scope_hash: str,
    responses_payload_getter: Callable[[], dict[str, Any] | None],
    assistant_content_getter: Callable[[], str | None],
) -> Callable[[int], Awaitable[None]] | None:
    if not _is_responses_image_compat_model(request_model):
        return None

    async def on_success(request_log_id: int) -> None:
        responses_payload = responses_payload_getter()
        if not isinstance(responses_payload, dict):
            return
        await create_chat_image_checkpoint(
            scope_hash=scope_hash,
            client_model=request_model,
            responses_payload=responses_payload,
            assistant_content=assistant_content_getter(),
            request_log_id=request_log_id,
        )

    return on_success


def _build_chat_completion_chunk_bytes(
    *,
    response_id: str,
    created_at: int,
    model_name: str,
    delta: dict[str, Any],
    finish_reason: str | None = None,
    usage_payload: dict[str, Any] | None = None,
) -> bytes:
    payload = {
        "id": response_id,
        "object": "chat.completion.chunk",
        "created": created_at,
        "model": model_name,
        "choices": [
            {
                "index": 0,
                "delta": delta,
                "finish_reason": finish_reason,
            }
        ],
    }
    if usage_payload is not None:
        payload["usage"] = copy.deepcopy(usage_payload)
    return b"data: " + json.dumps(payload, ensure_ascii=False).encode("utf-8") + b"\n\n"


async def _stream_responses_to_chat_completions(
    body_iterator: AsyncIterator[bytes],
    *,
    request_model: str,
    include_usage: bool = True,
) -> AsyncGenerator[bytes, None]:
    decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
    text_buffer = ""
    sse_scan_start = 0
    emitted_content = ""
    role_sent = False
    response_id = f"chatcmpl_{uuid.uuid4().hex}"
    created_at = int(utcnow().timestamp())
    model_name = request_model
    completed_response_seen = False
    error_seen = False
    output_items_by_index: dict[int, dict[str, Any]] = {}
    output_items_fallback: list[dict[str, Any]] = []

    async def emit_content_delta(content: str) -> AsyncGenerator[bytes, None]:
        nonlocal role_sent, emitted_content
        if not content:
            return
        delta: dict[str, Any] = {"content": content}
        if not role_sent:
            delta["role"] = "assistant"
            role_sent = True
        emitted_content += content
        yield _build_chat_completion_chunk_bytes(
            response_id=response_id,
            created_at=created_at,
            model_name=model_name,
            delta=delta,
        )

    async def emit_completed_payload(event_payload: dict[str, Any]) -> AsyncGenerator[bytes, None]:
        nonlocal completed_response_seen, role_sent
        completed_response_seen = True
        completed_response = event_payload.get("response")
        if not isinstance(completed_response, dict):
            completed_response = {}
        message, finish_reason = _chat_completion_message_from_response_payload(completed_response)
        final_content = str(message.get("content") or "")
        usage_payload = _chat_completion_usage_payload(event_payload, model_name=model_name) if include_usage else None

        if final_content:
            suffix = final_content
            if emitted_content and final_content.startswith(emitted_content):
                suffix = final_content[len(emitted_content):]
            async for content_chunk in emit_content_delta(suffix):
                yield content_chunk

        if message.get("tool_calls"):
            tool_call_deltas = []
            for index, tool_call in enumerate(message["tool_calls"]):
                tool_call_delta = copy.deepcopy(tool_call)
                tool_call_delta["index"] = index
                tool_call_deltas.append(tool_call_delta)
            delta: dict[str, Any] = {"tool_calls": tool_call_deltas}
            if not role_sent:
                delta["role"] = "assistant"
                role_sent = True
            yield _build_chat_completion_chunk_bytes(
                response_id=response_id,
                created_at=created_at,
                model_name=model_name,
                delta=delta,
            )

        if not role_sent:
            yield _build_chat_completion_chunk_bytes(
                response_id=response_id,
                created_at=created_at,
                model_name=model_name,
                delta={"role": "assistant"},
            )
            role_sent = True

        yield _build_chat_completion_chunk_bytes(
            response_id=response_id,
            created_at=created_at,
            model_name=model_name,
            delta={},
            finish_reason=finish_reason,
            usage_payload=usage_payload,
        )
        yield b"data: [DONE]\n\n"

    try:
        async for chunk in body_iterator:
            text_buffer += decoder.decode(chunk)
            while True:
                raw_event, text_buffer, sse_scan_start = _pop_sse_event_from_buffer(text_buffer, sse_scan_start)
                if raw_event is None:
                    break

                if not raw_event.strip():
                    continue

                if _is_sse_comment_frame(raw_event):
                    yield raw_event.encode("utf-8") + b"\n\n"
                    continue

                event_type, event_payload = _extract_responses_stream_event(raw_event)
                if event_type == "[DONE]":
                    synthetic_completed_payload = None
                    if not completed_response_seen and not error_seen:
                        synthetic_completed_payload = _build_synthetic_completed_event_from_output_items(
                            response_id=response_id,
                            model_name=model_name,
                            created_at=created_at,
                            output_items_by_index=output_items_by_index,
                            output_items_fallback=output_items_fallback,
                        )
                    if synthetic_completed_payload is not None:
                        async for completed_chunk in emit_completed_payload(synthetic_completed_payload):
                            yield completed_chunk
                        return
                    yield b"data: [DONE]\n\n"
                    return

                if event_type == "error":
                    error_seen = True
                    if isinstance(event_payload, dict):
                        yield b"data: " + json.dumps(event_payload, ensure_ascii=False).encode("utf-8") + b"\n\n"
                    else:
                        yield raw_event.encode("utf-8") + b"\n\n"
                    continue

                if isinstance(event_payload, dict):
                    response_id = _chat_completion_response_id(event_payload)
                    created_at = _chat_completion_created_at(event_payload)
                    model_name = _extract_response_model_name(event_payload) or model_name

                if event_type == "response.output_item.done":
                    _collect_responses_output_item_done(
                        event_payload,
                        output_items_by_index=output_items_by_index,
                        output_items_fallback=output_items_fallback,
                    )
                    continue

                if event_type == "response.output_text.delta" and isinstance(event_payload, dict):
                    delta_text = str(event_payload.get("delta") or "")
                    async for content_chunk in emit_content_delta(delta_text):
                        yield content_chunk
                    continue

                if event_type != "response.completed" or not isinstance(event_payload, dict):
                    continue

                patched_payload = _patch_completed_output_from_output_items(
                    event_payload,
                    output_items_by_index=output_items_by_index,
                    output_items_fallback=output_items_fallback,
                )
                async for completed_chunk in emit_completed_payload(patched_payload):
                    yield completed_chunk
                return

        synthetic_completed_payload = None
        if not completed_response_seen and not error_seen:
            synthetic_completed_payload = _build_synthetic_completed_event_from_output_items(
                response_id=response_id,
                model_name=model_name,
                created_at=created_at,
                output_items_by_index=output_items_by_index,
                output_items_fallback=output_items_fallback,
            )
        if synthetic_completed_payload is not None:
            async for completed_chunk in emit_completed_payload(synthetic_completed_payload):
                yield completed_chunk
            return

        yield b"data: [DONE]\n\n"
    finally:
        close_iterator = getattr(body_iterator, "aclose", None)
        if callable(close_iterator):
            with suppress(Exception):
                await close_iterator()


async def _await_image_non_stream_chunk(
    upstream_iter: AsyncIterator[bytes],
    *,
    http_request: Request | None,
    started_at_monotonic: float,
    total_timeout_seconds: float | None,
    read_timeout_seconds: float | None,
    disconnect_poll_seconds: float,
) -> bytes:
    loop = asyncio.get_running_loop()
    read_started_at = loop.time()
    next_chunk_task = asyncio.create_task(upstream_iter.__anext__())

    try:
        while True:
            now = loop.time()
            timeouts: list[float] = [disconnect_poll_seconds]

            if total_timeout_seconds is not None:
                remaining_total = total_timeout_seconds - (now - started_at_monotonic)
                if remaining_total <= 0:
                    raise _retryable_gateway_http_exception(
                        status_code=504,
                        detail=(
                            "Upstream image request exceeded total timeout "
                            f"of {int(total_timeout_seconds)} seconds"
                        ),
                    )
                timeouts.append(remaining_total)

            if read_timeout_seconds is not None:
                remaining_read = read_timeout_seconds - (now - read_started_at)
                if remaining_read <= 0:
                    raise _retryable_gateway_http_exception(
                        status_code=504,
                        detail=(
                            "Upstream image stream exceeded read timeout "
                            f"of {int(read_timeout_seconds)} seconds"
                        ),
                    )
                timeouts.append(remaining_read)

            done, _ = await asyncio.wait({next_chunk_task}, timeout=max(0.01, min(timeouts)))
            if next_chunk_task in done:
                return next_chunk_task.result()

            if http_request is not None and await http_request.is_disconnected():
                raise _non_retryable_gateway_http_exception(
                    status_code=499,
                    detail="Client disconnected during image generation",
                    record_token_error=False,
                )
    finally:
        if not next_chunk_task.done():
            next_chunk_task.cancel()
            with suppress(asyncio.CancelledError):
                await next_chunk_task


async def _collect_images_api_response_from_sse(
    upstream_iter: AsyncIterator[bytes],
    *,
    response_format: str,
    http_request: Request | None = None,
    total_timeout_seconds: float | None = None,
    read_timeout_seconds: float | None = None,
    disconnect_poll_seconds: float = 1.0,
) -> tuple[dict[str, Any], datetime | None]:
    decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
    text_buffer = ""
    sse_scan_start = 0
    first_token_at: datetime | None = None
    output_items_by_index: dict[int, dict[str, Any]] = {}
    output_items_fallback: list[dict[str, Any]] = []
    started_at_monotonic = asyncio.get_running_loop().time()

    while True:
        try:
            chunk = await _await_image_non_stream_chunk(
                upstream_iter,
                http_request=http_request,
                started_at_monotonic=started_at_monotonic,
                total_timeout_seconds=total_timeout_seconds,
                read_timeout_seconds=read_timeout_seconds,
                disconnect_poll_seconds=disconnect_poll_seconds,
            )
        except StopAsyncIteration:
            if text_buffer.strip():
                raise _retryable_gateway_http_exception(
                    status_code=502,
                    detail="Upstream closed stream with an incomplete SSE event",
                )
            break

        text_buffer += decoder.decode(chunk)
        while True:
            raw_event, text_buffer, sse_scan_start = _pop_sse_event_from_buffer(text_buffer, sse_scan_start)
            if raw_event is None:
                break

            if not raw_event.strip():
                continue

            event_type, event_payload = _extract_responses_stream_event(raw_event)
            if event_type == "[DONE]":
                raise _retryable_gateway_http_exception(
                    status_code=502,
                    detail="Upstream closed image stream before completion",
                )

            semantic_failure = _responses_failure_http_exception(event_payload)
            if semantic_failure is not None:
                raise semantic_failure

            if first_token_at is None and event_type and event_type not in RESPONSES_STREAM_PREFLIGHT_EVENTS:
                first_token_at = utcnow()

            if event_type == "response.output_item.done":
                _collect_responses_output_item_done(
                    event_payload,
                    output_items_by_index=output_items_by_index,
                    output_items_fallback=output_items_fallback,
                )
                continue

            if event_type != "response.completed":
                continue

            patched_event_payload = _patch_completed_output_from_output_items(
                event_payload,
                output_items_by_index=output_items_by_index,
                output_items_fallback=output_items_fallback,
            )
            try:
                results, created_at, usage_payload = _extract_images_from_completed_response(patched_event_payload)
            except ValueError as exc:
                raise _retryable_gateway_http_exception(
                    status_code=502,
                    detail=str(exc),
                ) from exc

            return _build_images_api_response(
                results,
                created_at=created_at,
                usage_payload=usage_payload,
                response_format=response_format,
            ), first_token_at

    raise _retryable_gateway_http_exception(
        status_code=502,
        detail="Upstream closed image stream before completion",
    )


def _encode_sse_event(event_name: str, payload: dict[str, Any]) -> bytes:
    prefix = f"event: {event_name}\n".encode("utf-8") if event_name else b""
    return prefix + b"data: " + json.dumps(payload, ensure_ascii=False).encode("utf-8") + b"\n\n"


def _build_stream_error_event(status_code: int, message: str) -> bytes:
    error_type = "invalid_request_error" if 400 <= status_code < 500 else "server_error"
    return _encode_sse_event(
        "error",
        {
            "error": {
                "message": message,
                "type": error_type,
                "status_code": status_code,
            }
        },
    )


def _build_chat_completions_stream_error_event(status_code: int, message: str) -> bytes:
    error_type = "invalid_request_error" if 400 <= status_code < 500 else "server_error"
    return b"data: " + json.dumps(
        {
            "error": {
                "message": message,
                "type": error_type,
                "status_code": status_code,
            }
        },
        ensure_ascii=False,
    ).encode("utf-8") + b"\n\n"


def _build_downstream_stream_error_event(*, endpoint: str, status_code: int, message: str) -> bytes:
    if endpoint == "/v1/chat/completions":
        return _build_chat_completions_stream_error_event(status_code, message)
    return _build_stream_error_event(status_code, message)


def _is_sse_done_frame(chunk: bytes) -> bool:
    return b"data: [DONE]" in chunk


def _is_sse_comment_chunk(chunk: bytes) -> bool:
    try:
        return _is_sse_comment_frame(chunk.decode("utf-8"))
    except Exception:
        return False


def _extract_stream_error_from_chunk(chunk: bytes) -> HTTPException | None:
    try:
        _, payload = _extract_responses_stream_event(chunk.decode("utf-8"))
    except Exception:
        return None

    if not isinstance(payload, dict):
        return None

    error_obj = payload.get("error")
    if not isinstance(error_obj, dict):
        return None

    status_code = _responses_error_status_code(error_obj)
    message = _normalize_optional_text(error_obj.get("message"))
    if message is None:
        return None
    return _upstream_error_http_exception(status_code, message)


def _transform_image_stream_event(
    event_type: str,
    event_payload: Any,
    *,
    response_format: str,
    stream_prefix: str,
) -> tuple[list[bytes], bool]:
    if not isinstance(event_payload, dict):
        return [], False

    normalized_response_format = _normalize_image_response_format(response_format)
    if event_type == "response.image_generation_call.partial_image":
        partial_b64 = _normalize_optional_text(event_payload.get("partial_image_b64"))
        if partial_b64 is None:
            return [], False

        partial_index = _coerce_int(event_payload.get("partial_image_index"), 0) or 0
        output_format = _normalize_optional_text(event_payload.get("output_format"))
        event_name = f"{stream_prefix}.partial_image"
        response_payload: dict[str, Any] = {
            "type": event_name,
            "partial_image_index": partial_index,
        }
        if normalized_response_format == "url":
            mime_type = _mime_type_from_output_format(output_format)
            response_payload["url"] = f"data:{mime_type};base64,{partial_b64}"
        else:
            response_payload["b64_json"] = partial_b64
        return [_encode_sse_event(event_name, response_payload)], False

    if event_type != "response.completed":
        return [], False

    results, _, usage_payload = _extract_images_from_completed_response(event_payload)
    event_name = f"{stream_prefix}.completed"
    events: list[bytes] = []
    for item in results:
        response_payload = {"type": event_name}
        if normalized_response_format == "url":
            mime_type = _mime_type_from_output_format(item.output_format)
            response_payload["url"] = f"data:{mime_type};base64,{item.result_b64}"
        else:
            response_payload["b64_json"] = item.result_b64
        if item.revised_prompt is not None:
            response_payload["revised_prompt"] = item.revised_prompt
        if usage_payload is not None:
            response_payload["usage"] = copy.deepcopy(usage_payload)
        events.append(_encode_sse_event(event_name, response_payload))
    return events, True


async def _stream_upstream_image_response(
    stream_cm,
    upstream_iter: AsyncIterator[bytes],
    buffered_chunks: list[bytes],
    *,
    stream_committed: bool,
    response_format: str,
    stream_prefix: str,
    heartbeat_interval_seconds: float | None = None,
    emit_initial_keepalive: bool = False,
    pending_chunk_task: asyncio.Task[bytes] | None = None,
    stream_capture: _ProxyStreamCapture | None = None,
) -> AsyncGenerator[bytes, None]:
    decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
    text_buffer = ""
    sse_scan_start = 0
    response_id: str | None = None
    response_model_name: str | None = None
    response_created_at: int | None = None
    completed_response_seen = False
    output_items_by_index: dict[int, dict[str, Any]] = {}
    output_items_fallback: list[dict[str, Any]] = []

    def drain_buffer(*, final: bool = False) -> tuple[list[bytes], bool]:
        nonlocal text_buffer, sse_scan_start, response_id, response_model_name, response_created_at, completed_response_seen
        events: list[bytes] = []
        while True:
            raw_event, text_buffer, sse_scan_start = _pop_sse_event_from_buffer(text_buffer, sse_scan_start)
            if raw_event is None:
                break

            if not raw_event.strip():
                continue

            if _is_sse_comment_frame(raw_event):
                events.append(raw_event.encode("utf-8") + b"\n\n")
                continue

            event_type, event_payload = _extract_responses_stream_event(raw_event)
            if event_type == "[DONE]":
                if not completed_response_seen:
                    synthetic_completed_payload = _build_synthetic_completed_event_from_output_items(
                        response_id=response_id,
                        model_name=response_model_name,
                        created_at=response_created_at,
                        output_items_by_index=output_items_by_index,
                        output_items_fallback=output_items_fallback,
                    )
                    if synthetic_completed_payload is not None:
                        try:
                            transformed_events, done = _transform_image_stream_event(
                                "response.completed",
                                synthetic_completed_payload,
                                response_format=response_format,
                                stream_prefix=stream_prefix,
                            )
                        except ValueError as exc:
                            events.append(_build_stream_error_event(502, str(exc)))
                            return events, True
                        events.extend(transformed_events)
                        return events, done
                return events, True

            semantic_failure = _responses_failure_http_exception(event_payload)
            if semantic_failure is not None:
                detail = str(getattr(semantic_failure, "detail", "") or semantic_failure)
                events.append(_build_stream_error_event(semantic_failure.status_code, detail))
                return events, True

            if isinstance(event_payload, dict):
                response_id = _normalize_optional_text(
                    _mapping_get(event_payload, "response", "id") or response_id
                ) or response_id
                response_model_name = _normalize_optional_text(
                    _mapping_get(event_payload, "response", "model") or response_model_name
                ) or response_model_name
                response_created_at = _coerce_int(
                    _mapping_get(event_payload, "response", "created_at")
                    or _mapping_get(event_payload, "response", "created")
                    or response_created_at
                )

            if event_type == "response.output_item.done":
                _collect_responses_output_item_done(
                    event_payload,
                    output_items_by_index=output_items_by_index,
                    output_items_fallback=output_items_fallback,
                )
                continue

            if event_type == "response.completed":
                completed_response_seen = True
                event_payload = _patch_completed_output_from_output_items(
                    event_payload,
                    output_items_by_index=output_items_by_index,
                    output_items_fallback=output_items_fallback,
                )

            try:
                transformed_events, done = _transform_image_stream_event(
                    event_type,
                    event_payload,
                    response_format=response_format,
                    stream_prefix=stream_prefix,
                )
            except ValueError as exc:
                events.append(_build_stream_error_event(502, str(exc)))
                return events, True

            events.extend(transformed_events)
            if done:
                return events, True

        if final and not completed_response_seen:
            synthetic_completed_payload = _build_synthetic_completed_event_from_output_items(
                response_id=response_id,
                model_name=response_model_name,
                created_at=response_created_at,
                output_items_by_index=output_items_by_index,
                output_items_fallback=output_items_fallback,
            )
            if synthetic_completed_payload is not None:
                try:
                    transformed_events, done = _transform_image_stream_event(
                        "response.completed",
                        synthetic_completed_payload,
                        response_format=response_format,
                        stream_prefix=stream_prefix,
                    )
                except ValueError as exc:
                    events.append(_build_stream_error_event(502, str(exc)))
                    return events, True
                events.extend(transformed_events)
                return events, done

        if final and text_buffer.strip():
            events.append(_build_stream_error_event(502, "Upstream closed stream with an incomplete SSE event"))
            text_buffer = ""
            sse_scan_start = 0
            return events, True

        return events, False

    try:
        if emit_initial_keepalive:
            yield STREAM_KEEPALIVE_COMMENT
        for chunk in buffered_chunks:
            if stream_capture is not None:
                stream_capture.feed(chunk)
            text_buffer += decoder.decode(chunk)
            events, done = drain_buffer()
            for event in events:
                yield event
            if done:
                return

        async for chunk in _iterate_with_sse_keepalive(
            upstream_iter,
            heartbeat_interval_seconds=heartbeat_interval_seconds,
            pending_chunk_task=pending_chunk_task,
        ):
            if stream_capture is not None and chunk != STREAM_KEEPALIVE_COMMENT:
                stream_capture.feed(chunk)
            text_buffer += decoder.decode(chunk)
            events, done = drain_buffer()
            for event in events:
                yield event
            if done:
                return

        events, _ = drain_buffer(final=True)
        for event in events:
            yield event
    except RESPONSES_STREAM_NETWORK_ERRORS as exc:
        logger.info(
            "Codex upstream image stream aborted after response commit: error_type=%s detail=%s",
            type(exc).__name__,
            str(exc) or type(exc).__name__,
        )
        if stream_committed:
            error_event = _build_stream_error_event(
                502,
                f"Upstream image stream aborted before completion: {type(exc).__name__}: {str(exc) or type(exc).__name__}",
            )
            if stream_capture is not None:
                stream_capture.feed(error_event)
            yield error_event
            yield b"data: [DONE]\n\n"
    finally:
        await stream_cm.__aexit__(None, None, None)


async def _proxy_image_request_with_token(
    client: httpx.AsyncClient,
    http_request: Request,
    image_request: ImageProxyRequest,
    *,
    access_token: str,
    account_id: str | None,
) -> ProxyRequestResult:
    headers = _build_upstream_headers(
        http_request,
        access_token=access_token,
        account_id=account_id,
        stream=True,
    )
    upstream_url = _codex_responses_url(compact=False)
    json_payload = json.dumps(image_request.responses_payload, ensure_ascii=False)
    timeout = httpx.Timeout(connect=30.0, read=None, write=30.0, pool=30.0)
    non_stream_total_timeout_seconds = _image_non_stream_total_timeout_seconds()
    non_stream_read_timeout_seconds = _image_non_stream_read_timeout_seconds()
    non_stream_disconnect_poll_seconds = _image_non_stream_disconnect_poll_seconds()
    stream_keepalive_interval_seconds = (
        _stream_keepalive_interval_seconds() if _is_responses_image_compat_model(image_request.model_name) else None
    )

    if image_request.stream:
        stream_capture = _ProxyStreamCapture(
            initial_model_name=image_request.model_name,
            initial_first_token_at=None,
        )

        async def body_iterator() -> AsyncGenerator[bytes, None]:
            stream_cm = client.stream(
                "POST",
                upstream_url,
                headers=headers,
                content=json_payload,
                timeout=timeout,
            )
            upstream_response = await _enter_upstream_stream_with_timing(stream_cm, compact=False)
            if upstream_response.status_code < 200 or upstream_response.status_code >= 300:
                try:
                    raw = await upstream_response.aread()
                finally:
                    await stream_cm.__aexit__(None, None, None)
                raise _upstream_error_http_exception(upstream_response.status_code, _decode_error_body(raw))

            upstream_iter = upstream_response.aiter_raw()
            try:
                primed_stream = await _prime_responses_upstream_stream_with_timing(
                    upstream_iter,
                    heartbeat_interval_seconds=stream_keepalive_interval_seconds,
                )
            except Exception:
                await stream_cm.__aexit__(None, None, None)
                raise

            async for chunk in _stream_upstream_image_response(
                stream_cm,
                upstream_iter,
                primed_stream.buffered_chunks,
                stream_committed=primed_stream.stream_committed,
                response_format=image_request.response_format,
                stream_prefix=image_request.stream_prefix,
                heartbeat_interval_seconds=stream_keepalive_interval_seconds,
                emit_initial_keepalive=primed_stream.emit_initial_keepalive,
                pending_chunk_task=primed_stream.pending_chunk_task,
                stream_capture=stream_capture,
            ):
                yield chunk

        return ProxyRequestResult(
            response=StreamingResponse(
                _wrap_sse_stream_with_initial_keepalive(
                    body_iterator(),
                    heartbeat_interval_seconds=stream_keepalive_interval_seconds,
                ),
                media_type="text/event-stream",
                headers=_sse_response_headers(),
            ),
            status_code=200,
            model_name=image_request.model_name,
            first_token_at=None,
            stream_capture=stream_capture,
        )

    stream_cm = client.stream(
        "POST",
        upstream_url,
        headers=headers,
        content=json_payload,
        timeout=timeout,
    )
    upstream_response = await _enter_upstream_stream_with_timing(stream_cm)
    try:
        if upstream_response.status_code < 200 or upstream_response.status_code >= 300:
            raw = await upstream_response.aread()
            raise _upstream_error_http_exception(upstream_response.status_code, _decode_error_body(raw))

        response_body, first_token_at = await _collect_images_api_response_from_sse(
            upstream_response.aiter_raw(),
            response_format=image_request.response_format,
            http_request=http_request,
            total_timeout_seconds=non_stream_total_timeout_seconds,
            read_timeout_seconds=non_stream_read_timeout_seconds,
            disconnect_poll_seconds=non_stream_disconnect_poll_seconds,
        )
        return ProxyRequestResult(
            response=JSONResponse(status_code=upstream_response.status_code, content=response_body),
            status_code=upstream_response.status_code,
            model_name=image_request.model_name,
            first_token_at=first_token_at,
        )
    finally:
        await stream_cm.__aexit__(None, None, None)


def _load_error_mapping(error_text: str) -> dict[str, Any] | None:
    try:
        payload = json.loads(error_text)
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None

    detail = payload.get("detail")
    if isinstance(detail, str):
        try:
            detail_payload = json.loads(detail)
        except Exception:
            detail_payload = None
        if isinstance(detail_payload, dict):
            return detail_payload

    return payload


def _extract_error_object(error_text: str) -> dict[str, Any] | None:
    payload = _load_error_mapping(error_text)
    if not isinstance(payload, dict):
        return None

    error = payload.get("error")
    if isinstance(error, dict):
        return error

    detail = payload.get("detail")
    if isinstance(detail, dict):
        detail_error = detail.get("error")
        if isinstance(detail_error, dict):
            return detail_error
        return detail

    return None


def _extract_retry_after_cooldown_seconds(message: str) -> int | None:
    match = re.search(
        r"try\s+again\s+in\s+([0-9]+(?:\.[0-9]+)?)\s*(ms|milliseconds?|s|sec|secs|seconds?)\b",
        message,
        flags=re.IGNORECASE,
    )
    if match is None:
        return None

    value = float(match.group(1))
    unit = match.group(2).lower()
    seconds = value / 1000.0 if unit.startswith("ms") or unit.startswith("millisecond") else value
    return max(_image_rate_limit_min_cooldown_seconds(), int(math.ceil(seconds)))


def _extract_image_rate_limit_cooldown_seconds(
    status_code: int,
    error_text: str,
    *,
    request_model: str | None,
) -> int | None:
    if status_code != 429 or _image_rate_limit_scoped_cooldown_scope(request_model) is None:
        return None

    error = _extract_error_object(error_text)
    if error is None:
        return None

    error_type = str(error.get("type") or "").strip().lower()
    error_code = str(error.get("code") or "").strip().lower()
    error_message = str(error.get("message") or "").strip()
    error_message_lower = error_message.lower()
    is_rate_limit = error_code == "rate_limit_exceeded" or "rate limit" in error_message_lower
    is_image_bucket = error_type == "input-images" or "input-images" in error_message_lower
    if not is_rate_limit or not is_image_bucket:
        return None

    return _extract_retry_after_cooldown_seconds(error_message) or _image_rate_limit_default_cooldown_seconds()


def _extract_usage_limit_cooldown_seconds(status_code: int, error_text: str) -> int | None:
    if status_code != 429:
        return None

    try:
        payload = json.loads(error_text)
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None

    error = payload.get("error")
    if not isinstance(error, dict):
        return None

    error_type = str(error.get("type") or "").strip()
    error_message = str(error.get("message") or "").strip().lower()
    if error_type != "usage_limit_reached" and "usage limit" not in error_message:
        return None

    resets_in_seconds = error.get("resets_in_seconds")
    if resets_in_seconds is not None:
        try:
            return max(0, int(float(resets_in_seconds)))
        except Exception:
            pass

    resets_at = error.get("resets_at")
    if resets_at is not None:
        try:
            reset_at_epoch = int(float(resets_at))
            now_epoch = int(datetime.now(timezone.utc).timestamp())
            return max(0, reset_at_epoch - now_epoch)
        except Exception:
            pass

    return _default_usage_limit_cooldown_seconds()


def _is_permanent_account_disable_error(status_code: int, error_text: str) -> bool:
    if status_code not in (401, 402):
        return False

    try:
        payload = json.loads(error_text)
    except Exception:
        return False
    if not isinstance(payload, dict):
        return False

    if status_code == 402:
        detail = payload.get("detail")
        if isinstance(detail, dict):
            detail_code = str(detail.get("code") or "").strip()
            if detail_code == "deactivated_workspace":
                return True

    if status_code == 401:
        error = payload.get("error")
        if isinstance(error, dict):
            error_code = str(error.get("code") or "").strip()
            if error_code == "account_deactivated":
                return True

    return False


def _get_compact_codex_server_error_cooling_time(
    *,
    compact: bool,
    status_code: int,
    error_text: str,
) -> int:
    if not compact:
        return 0
    if status_code < 500:
        return 0
    if _extract_usage_limit_cooldown_seconds(status_code, error_text) is not None:
        return 0
    return _compact_server_error_cooldown_seconds()


def _should_retry_upstream_server_error(status_code: int) -> bool:
    return 500 <= status_code <= 599


def _should_retry_http_exception(exc: HTTPException) -> bool:
    retryable = getattr(exc, "retryable", None)
    if retryable is not None:
        return bool(retryable)
    return _should_retry_upstream_server_error(getattr(exc, "status_code", 500))


def _should_record_http_exception_token_error(exc: HTTPException) -> bool:
    return bool(getattr(exc, "record_token_error", False))


def _is_retryable_upstream_transport_exception(exc: Exception) -> bool:
    if isinstance(exc, httpx.HTTPError):
        return True
    if isinstance(exc, AttributeError):
        return "'NoneType' object has no attribute 'call_soon'" in str(exc)
    if isinstance(exc, RuntimeError):
        return "Event loop is closed" in str(exc)
    return False


def _is_images_endpoint(endpoint: str) -> bool:
    return endpoint in {"/v1/images/generations", "/v1/images/edits"}


def _effective_proxy_max_attempts(*, endpoint: str) -> int:
    max_attempts = _max_request_account_retries()
    if _is_images_endpoint(endpoint):
        max_attempts = min(max_attempts, _image_request_max_account_retries())
    return max_attempts


def _usage_finalize_kwargs(usage_metrics: UsageMetrics | None) -> dict[str, Any]:
    if usage_metrics is None:
        return {}
    return {
        "input_tokens": usage_metrics.input_tokens,
        "output_tokens": usage_metrics.output_tokens,
        "total_tokens": usage_metrics.total_tokens,
        "estimated_cost_usd": usage_metrics.estimated_cost_usd,
    }


async def _finalize_cancelled_request_log(
    *args,
    timing: _RequestTimingRecorder | None = None,
    **kwargs,
) -> None:
    async def finalize_with_cancel_shield() -> None:
        with anyio.CancelScope(shield=True):
            if not args:
                return
            await _finalize_request_log_with_timing(timing, args[0], **kwargs)

    finalize_task = asyncio.create_task(finalize_with_cancel_shield(), name="oaix-cancelled-request-log-finalize")
    try:
        await asyncio.shield(finalize_task)
    except asyncio.CancelledError:
        try:
            with anyio.CancelScope(shield=True):
                await asyncio.shield(finalize_task)
        except Exception:
            logger.exception("Failed to finalize cancelled request log")
        raise
    except Exception:
        logger.exception("Failed to finalize cancelled request log")


async def _probe_token_with_latest_access_token(
    app: FastAPI,
    *,
    http_request: Request,
    token_row: Any,
    probe_model: str | None = None,
) -> dict[str, Any]:
    client: httpx.AsyncClient = _state_http_client(app, "response_http_client")
    oauth_manager: CodexOAuthManager = app.state.oauth_manager
    resolved_probe_model = _normalize_optional_text(probe_model) or _admin_token_probe_model()
    probe_request = ResponsesRequest(
        model=resolved_probe_model,
        input=[
            {
                "type": "message",
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": ADMIN_TOKEN_PROBE_INPUT,
                    }
                ],
            }
        ],
        stream=False,
        store=False,
    )

    try:
        access_token, _ = await oauth_manager.get_access_token(
            token_row,
            client,
            force_refresh=True,
            reactivate_on_refresh=False,
        )
    except HTTPException as exc:
        oauth_manager.invalidate(token_row.id)
        detail = str(getattr(exc, "detail", "") or exc)
        if is_permanently_invalid_refresh_token_error(exc):
            await mark_token_error(
                token_row.id,
                detail,
                deactivate=True,
                clear_access_token=True,
            )
            return {
                "id": token_row.id,
                "outcome": "disabled",
                "status_code": getattr(exc, "status_code", 500),
                "message": "测试失败：refresh token 已失效，当前已标记为禁用。",
                "detail": detail,
                "probe_model": probe_request.model,
                "probe_input": ADMIN_TOKEN_PROBE_INPUT,
            }
        if getattr(exc, "status_code", None) in (401, 403):
            await mark_token_error(
                token_row.id,
                detail,
                clear_access_token=True,
            )
            return {
                "id": token_row.id,
                "outcome": "inconclusive",
                "status_code": getattr(exc, "status_code", 500),
                "message": f"测试未得出结论：刷新最新 access token 失败（{getattr(exc, 'status_code', 500)}），当前状态未改变。",
                "detail": detail,
                "probe_model": probe_request.model,
                "probe_input": ADMIN_TOKEN_PROBE_INPUT,
            }
        return {
            "id": token_row.id,
            "outcome": "inconclusive",
            "status_code": getattr(exc, "status_code", 500),
            "message": f"测试未得出结论：刷新最新 access token 时返回 {getattr(exc, 'status_code', 500)}，当前状态未改变。",
            "detail": detail,
            "probe_model": probe_request.model,
            "probe_input": ADMIN_TOKEN_PROBE_INPUT,
        }
    except httpx.HTTPError as exc:
        return {
            "id": token_row.id,
            "outcome": "inconclusive",
            "status_code": 502,
            "message": f"测试未得出结论：刷新最新 access token 时发生 {type(exc).__name__}，当前状态未改变。",
            "detail": str(exc) or type(exc).__name__,
            "probe_model": probe_request.model,
            "probe_input": ADMIN_TOKEN_PROBE_INPUT,
        }

    try:
        proxy_result = await _proxy_request_with_token(
            client,
            http_request,
            probe_request,
            access_token=access_token,
            account_id=token_row.account_id,
            compact=False,
        )
        await mark_token_success(token_row.id)
        return {
            "id": token_row.id,
            "outcome": "reactivated",
            "status_code": proxy_result.status_code,
            "message": "测试成功：使用最新 access token 请求上游成功，当前已标记为可用。",
            "detail": None,
            "probe_model": probe_request.model,
            "probe_input": ADMIN_TOKEN_PROBE_INPUT,
            "response_model": proxy_result.model_name,
        }
    except HTTPException as exc:
        status_code = getattr(exc, "status_code", 500)
        detail = str(getattr(exc, "detail", "") or exc)
        cooldown_seconds = _extract_usage_limit_cooldown_seconds(status_code, detail)
        if cooldown_seconds is not None:
            await mark_token_error(
                token_row.id,
                detail,
                cooldown_seconds=cooldown_seconds,
            )
            return {
                "id": token_row.id,
                "outcome": "cooling",
                "status_code": status_code,
                "message": f"测试结果：上游返回额度限制，当前已转为冷却 {cooldown_seconds} 秒。",
                "detail": detail,
                "cooldown_seconds": cooldown_seconds,
                "probe_model": probe_request.model,
                "probe_input": ADMIN_TOKEN_PROBE_INPUT,
            }

        if _is_permanent_account_disable_error(status_code, detail) or status_code in (401, 403):
            oauth_manager.invalidate(token_row.id)
            await mark_token_error(
                token_row.id,
                detail,
                deactivate=True,
                clear_access_token=True,
            )
            return {
                "id": token_row.id,
                "outcome": "disabled",
                "status_code": status_code,
                "message": f"测试失败：使用最新 access token 请求上游后仍返回鉴权/停用错误（{status_code}），当前已标记为禁用。",
                "detail": detail,
                "probe_model": probe_request.model,
                "probe_input": ADMIN_TOKEN_PROBE_INPUT,
            }

        return {
            "id": token_row.id,
            "outcome": "inconclusive",
            "status_code": status_code,
            "message": f"测试未得出结论：上游返回 {status_code}，当前状态未改变。",
            "detail": detail,
            "probe_model": probe_request.model,
            "probe_input": ADMIN_TOKEN_PROBE_INPUT,
        }
    except httpx.HTTPError as exc:
        return {
            "id": token_row.id,
            "outcome": "inconclusive",
            "status_code": 502,
            "message": f"测试未得出结论：上游请求发生 {type(exc).__name__}，当前状态未改变。",
            "detail": str(exc) or type(exc).__name__,
            "probe_model": probe_request.model,
            "probe_input": ADMIN_TOKEN_PROBE_INPUT,
        }


async def _finalize_stream_request_log(
    *,
    request_log_ref: Any,
    status_code: int,
    attempt_count: int,
    token_id: int | None,
    account_id: str | None,
    fallback_model_name: str | None,
    fallback_first_token_at: datetime | None,
    stream_capture: _ProxyStreamCapture,
    timing: _RequestTimingRecorder | None = None,
    success_hook: Callable[[int], Awaitable[None]] | None = None,
) -> int | None:
    try:
        failed = stream_capture.error_message is not None and not stream_capture.completed
        final_status_code = (stream_capture.error_status_code or status_code) if failed else status_code
        return await _finalize_request_log_with_timing(
            timing,
            request_log_ref,
            success_hook=success_hook if not failed else None,
            status_code=final_status_code,
            success=not failed,
            attempt_count=attempt_count,
            finished_at=utcnow(),
            first_token_at=stream_capture.first_token_at or fallback_first_token_at,
            token_id=token_id,
            account_id=account_id,
            model_name=stream_capture.model_name or fallback_model_name,
            error_message=stream_capture.error_message if failed else None,
            **_usage_finalize_kwargs(stream_capture.usage_metrics),
        )
    except Exception:
        logger.exception("Failed to finalize streamed request log")
        return None


async def _run_proxy_result_success_hook(proxy_result: ProxyRequestResult, *, request_log_id: int) -> None:
    if proxy_result.on_success is None:
        return
    try:
        await proxy_result.on_success(request_log_id)
    except Exception:
        logger.exception("Failed to persist proxy success side effects for request log id=%s", request_log_id)


async def _await_with_keepalive_queue(
    awaitable: Awaitable[Any],
    *,
    output_queue: asyncio.Queue[bytes | object],
    heartbeat_interval_seconds: float | None,
) -> Any:
    task = asyncio.create_task(awaitable)
    try:
        if heartbeat_interval_seconds is None or heartbeat_interval_seconds <= 0:
            return await task

        while True:
            done, _ = await asyncio.wait({task}, timeout=heartbeat_interval_seconds)
            if task in done:
                return task.result()
            await output_queue.put(STREAM_KEEPALIVE_COMMENT)
    finally:
        if not task.done():
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task


def _gpt_image_stream_keepalive_response(
    http_request: Request,
    *,
    endpoint: str,
    request_model: str | None,
    compact: bool,
    proxy_call: Callable[[httpx.AsyncClient, str, str | None], Awaitable[ProxyRequestResult]],
    request_log: _RequestLogHandle,
    response_lease: Any,
    timing: _RequestTimingRecorder | None,
) -> StreamingResponse:
    output_queue: asyncio.Queue[bytes | object] = asyncio.Queue()
    queue_done_sentinel = object()
    heartbeat_interval_seconds = _stream_keepalive_interval_seconds()
    first_output_timeout_seconds = _image_stream_first_output_timeout_seconds()
    response_body_started = False

    async def worker() -> None:
        timing_context_token = _REQUEST_TIMING.set(timing) if timing is not None else None
        db_timing_context_token = set_db_timing_recorder(timing) if timing is not None else None
        client: httpx.AsyncClient = _state_http_client(http_request.app, "response_http_client")
        oauth_manager: CodexOAuthManager = http_request.app.state.oauth_manager
        selection_settings = _current_token_selection_settings(http_request.app)

        attempt_count = 0
        last_token_id: int | None = None
        last_account_id: str | None = None
        stream_finalized = False
        restricted_model_name = _non_free_only_codex_model_name(request_model)
        require_non_free_token = restricted_model_name is not None
        restricted_model_label = restricted_model_name or "requested"
        scoped_cooldown_scope = _image_rate_limit_scoped_cooldown_scope(request_model)
        skipped_free_token_ids: set[int] = set()
        excluded_token_ids: set[int] = set()
        postponed_unknown_plan_tokens: list[Any] = []

        try:
            await _record_event_loop_lag(timing)
            max_attempts = _effective_proxy_max_attempts(endpoint=endpoint)
            max_claims = _max_request_account_retries()
            claim_count = 0
            last_error: HTTPException | None = None

            while attempt_count < max_attempts and claim_count < max_claims:
                claim_count += 1
                token_row = await _await_timed(
                    _claim_next_token_for_model_request(
                        selection_settings,
                        exclude_token_ids=excluded_token_ids,
                        scoped_cooldown_scope=scoped_cooldown_scope,
                        timing=timing,
                        token_pool_snapshot=_current_token_pool_snapshot(http_request.app),
                        require_non_free_token=require_non_free_token,
                    ),
                    timing=timing,
                    span_name="claim_token_ms",
                    db_wait=True,
                    output_queue=output_queue,
                    heartbeat_interval_seconds=heartbeat_interval_seconds,
                )
                using_postponed_unknown_plan = False
                if token_row is None:
                    if require_non_free_token and postponed_unknown_plan_tokens:
                        token_row = postponed_unknown_plan_tokens.pop(0)
                        using_postponed_unknown_plan = True
                        _record_selected_token_observation_start(token_row.id, timing)
                    else:
                        break

                if require_non_free_token:
                    effective_plan_type = await _effective_token_plan_type(http_request, token_row)
                    if effective_plan_type == "free":
                        excluded_token_ids.add(token_row.id)
                        skipped_free_token_ids.add(token_row.id)
                        logger.info(
                            "Skipping free-plan Codex token for %s request: token_id=%s account_id=%s",
                            restricted_model_label,
                            token_row.id,
                            token_row.account_id,
                        )
                        _record_selected_token_observation_finish(
                            token_row.id,
                            started_at=request_log.started_at,
                            first_token_at=None,
                        )
                        continue
                    if effective_plan_type is None and not using_postponed_unknown_plan:
                        excluded_token_ids.add(token_row.id)
                        postponed_unknown_plan_tokens.append(token_row)
                        logger.info(
                            "Postponing unknown-plan Codex token while searching for a known non-free %s token: "
                            "token_id=%s account_id=%s",
                            restricted_model_label,
                            token_row.id,
                            token_row.account_id,
                        )
                        _record_selected_token_observation_finish(
                            token_row.id,
                            started_at=request_log.started_at,
                            first_token_at=None,
                        )
                        continue

                attempt_count += 1
                attempt = attempt_count
                last_token_id = token_row.id
                last_account_id = token_row.account_id

                try:
                    with timing.measure("oauth_ms") if timing is not None else suppress():
                        access_token, access_token_refreshed = await _await_with_keepalive_queue(
                            oauth_manager.get_access_token(token_row, client),
                            output_queue=output_queue,
                            heartbeat_interval_seconds=heartbeat_interval_seconds,
                        )
                except HTTPException as exc:
                    oauth_manager.invalidate(token_row.id)
                    permanent_refresh_failure = is_permanently_invalid_refresh_token_error(exc)
                    await mark_token_error(
                        token_row.id,
                        str(exc.detail),
                        deactivate=permanent_refresh_failure,
                        clear_access_token=True,
                    )
                    if exc.status_code in (401, 403):
                        last_error = exc
                        _record_selected_token_observation_finish(
                            token_row.id,
                            started_at=request_log.started_at,
                            first_token_at=None,
                        )
                        continue
                    _record_selected_token_observation_finish(
                        token_row.id,
                        started_at=request_log.started_at,
                        first_token_at=None,
                    )
                    raise
                except BaseException:
                    _record_selected_token_observation_finish(
                        token_row.id,
                        started_at=request_log.started_at,
                        first_token_at=None,
                    )
                    raise

                token_observation_first_token_at: datetime | None = None
                try:
                    proxy_result = await _await_with_keepalive_queue(
                        proxy_call(client, access_token, token_row.account_id),
                        output_queue=output_queue,
                        heartbeat_interval_seconds=heartbeat_interval_seconds,
                    )
                    if not isinstance(proxy_result.response, StreamingResponse):
                        raise HTTPException(
                            status_code=502,
                            detail="Proxy call returned a non-streaming response for a streaming request",
                        )

                    body_iterator = proxy_result.response.body_iterator
                    body_iterator = _wrap_sse_stream_with_initial_keepalive(
                        body_iterator,
                        heartbeat_interval_seconds=heartbeat_interval_seconds,
                    )
                    saw_semantic_chunk = False
                    stream_started_monotonic = asyncio.get_running_loop().time()
                    try:
                        async for chunk in body_iterator:
                            if not saw_semantic_chunk:
                                stream_error = _extract_stream_error_from_chunk(chunk)
                                if stream_error is not None:
                                    raise stream_error

                                if _is_sse_done_frame(chunk) and (
                                    proxy_result.stream_capture is None or not proxy_result.stream_capture.completed
                                ):
                                    raise HTTPException(
                                        status_code=502,
                                        detail="Upstream image stream ended before producing a response",
                                    )

                                if _is_sse_comment_chunk(chunk):
                                    if (
                                        asyncio.get_running_loop().time() - stream_started_monotonic
                                        >= first_output_timeout_seconds
                                    ):
                                        raise HTTPException(
                                            status_code=504,
                                            detail=(
                                                "Upstream image stream timed out before producing a response "
                                                f"({first_output_timeout_seconds:g}s without image output)"
                                            ),
                                        )
                                    await output_queue.put(chunk)
                                    continue

                                saw_semantic_chunk = True
                            await output_queue.put(chunk)
                    finally:
                        close_iterator = getattr(body_iterator, "aclose", None)
                        if callable(close_iterator):
                            with suppress(Exception):
                                await close_iterator()

                    if (
                        not saw_semantic_chunk
                        and (
                            proxy_result.stream_capture is None
                            or (
                                proxy_result.stream_capture.error_message is None
                                and not proxy_result.stream_capture.completed
                            )
                        )
                    ):
                        raise HTTPException(
                            status_code=502,
                            detail="Upstream image stream ended before producing a response",
                        )

                    if proxy_result.stream_capture is not None:
                        finalized_request_log_id = await _finalize_stream_request_log(
                            request_log_ref=request_log,
                            status_code=proxy_result.status_code,
                            attempt_count=attempt,
                            token_id=token_row.id,
                            account_id=token_row.account_id,
                            fallback_model_name=proxy_result.model_name,
                            fallback_first_token_at=proxy_result.first_token_at,
                            stream_capture=proxy_result.stream_capture,
                            timing=timing,
                            success_hook=proxy_result.on_success,
                        )
                        if proxy_result.stream_capture.error_message is not None and not proxy_result.stream_capture.completed:
                            await mark_token_error(
                                token_row.id,
                                proxy_result.stream_capture.error_message,
                            )
                            stream_finalized = True
                            return
                    else:
                        finalized_request_log_id = await _finalize_request_log_with_timing(
                            timing,
                            request_log,
                            status_code=proxy_result.status_code,
                            success=True,
                            attempt_count=attempt,
                            finished_at=utcnow(),
                            first_token_at=proxy_result.first_token_at,
                            token_id=token_row.id,
                            account_id=token_row.account_id,
                            model_name=proxy_result.model_name,
                            success_hook=proxy_result.on_success,
                        )
                    await _mark_token_success_with_timing(timing, token_row.id, token_row=token_row)
                    token_observation_first_token_at = (
                        (
                            proxy_result.stream_capture.first_token_at
                            if proxy_result.stream_capture is not None
                            else None
                        )
                        or proxy_result.first_token_at
                    )
                    stream_finalized = True
                    return
                except HTTPException as exc:
                    status_code = getattr(exc, "status_code", 500)
                    detail = str(getattr(exc, "detail", "") or exc)
                    cooldown_seconds = _extract_usage_limit_cooldown_seconds(status_code, detail)

                    if cooldown_seconds is not None:
                        await mark_token_error(
                            token_row.id,
                            detail,
                            cooldown_seconds=cooldown_seconds,
                        )
                        logger.info(
                            "Codex account cooled down: token_id=%s account_id=%s cooldown_seconds=%s attempt=%s/%s",
                            token_row.id,
                            token_row.account_id,
                            cooldown_seconds,
                            attempt,
                            max_attempts,
                        )
                        last_error = exc
                        continue

                    image_cooldown_seconds = _extract_image_rate_limit_cooldown_seconds(
                        status_code,
                        detail,
                        request_model=request_model,
                    )
                    if image_cooldown_seconds is not None and scoped_cooldown_scope is not None:
                        await mark_token_scoped_cooldown(
                            token_row.id,
                            scoped_cooldown_scope,
                            detail,
                            cooldown_seconds=image_cooldown_seconds,
                        )
                        logger.info(
                            "Codex image bucket cooled down: token_id=%s account_id=%s scope=%s cooldown_seconds=%s attempt=%s/%s",
                            token_row.id,
                            token_row.account_id,
                            scoped_cooldown_scope,
                            image_cooldown_seconds,
                            attempt,
                            max_attempts,
                        )
                        last_error = exc
                        continue

                    if _should_record_http_exception_token_error(exc):
                        await mark_token_error(token_row.id, detail)

                    permanently_disabled = _is_permanent_account_disable_error(status_code, detail)
                    auth_failed_after_refresh = status_code in (401, 403) and access_token_refreshed

                    if permanently_disabled or auth_failed_after_refresh:
                        oauth_manager.invalidate(token_row.id)
                        await mark_token_error(
                            token_row.id,
                            detail,
                            deactivate=True,
                            clear_access_token=True,
                        )
                        logger.warning(
                            "Codex account disabled after upstream auth failure: token_id=%s account_id=%s status=%s refreshed=%s attempt=%s/%s",
                            token_row.id,
                            token_row.account_id,
                            status_code,
                            access_token_refreshed,
                            attempt,
                            max_attempts,
                        )
                        last_error = exc
                        continue

                    if status_code in (401, 403):
                        oauth_manager.invalidate(token_row.id)
                        await mark_token_error(token_row.id, detail, clear_access_token=True)
                        logger.warning(
                            "Codex upstream auth failed; invalidated access token for retry: token_id=%s account_id=%s status=%s attempt=%s/%s",
                            token_row.id,
                            token_row.account_id,
                            status_code,
                            attempt,
                            max_attempts,
                        )
                        last_error = exc
                        continue

                    compact_server_error_cooling_time = _get_compact_codex_server_error_cooling_time(
                        compact=compact,
                        status_code=status_code,
                        error_text=detail,
                    )
                    if _should_retry_http_exception(exc) and attempt < max_attempts:
                        excluded_token_ids.add(token_row.id)
                        mark_kwargs: dict[str, Any] = {}
                        if compact_server_error_cooling_time > 0:
                            mark_kwargs["cooldown_seconds"] = compact_server_error_cooling_time
                        await mark_token_error(token_row.id, detail, **mark_kwargs)
                        logger.info(
                            "Codex upstream server error before response commit: token_id=%s account_id=%s status=%s compact=%s cooldown_seconds=%s attempt=%s/%s",
                            token_row.id,
                            token_row.account_id,
                            status_code,
                            compact,
                            compact_server_error_cooling_time,
                            attempt,
                            max_attempts,
                        )
                        last_error = exc
                        continue

                    if _should_retry_http_exception(exc):
                        await mark_token_error(token_row.id, detail)

                    raise
                except httpx.HTTPError as exc:
                    message = f"Upstream request failed: {type(exc).__name__}: {exc}"
                    compact_server_error_cooling_time = _get_compact_codex_server_error_cooling_time(
                        compact=compact,
                        status_code=500,
                        error_text=message,
                    )
                    if attempt < max_attempts:
                        excluded_token_ids.add(token_row.id)
                        mark_kwargs: dict[str, Any] = {}
                        if compact_server_error_cooling_time > 0:
                            mark_kwargs["cooldown_seconds"] = compact_server_error_cooling_time
                        await mark_token_error(token_row.id, message, **mark_kwargs)
                        logger.info(
                            "Codex upstream transport error before response commit: token_id=%s account_id=%s error_type=%s compact=%s cooldown_seconds=%s attempt=%s/%s",
                            token_row.id,
                            token_row.account_id,
                            type(exc).__name__,
                            compact,
                            compact_server_error_cooling_time,
                            attempt,
                            max_attempts,
                        )
                        last_error = HTTPException(status_code=502, detail=message)
                        continue
                    await mark_token_error(token_row.id, message)
                    raise HTTPException(status_code=502, detail=message) from exc
                finally:
                    _record_selected_token_observation_finish(
                        token_row.id,
                        started_at=request_log.started_at,
                        first_token_at=token_observation_first_token_at,
                    )

            if last_error is not None:
                if _should_retry_http_exception(last_error):
                    raise last_error
                raise HTTPException(
                    status_code=503,
                    detail=f"All available Codex accounts are exhausted or cooling down. Last error: {last_error.detail}",
                )
            if require_non_free_token and skipped_free_token_ids:
                raise HTTPException(
                    status_code=503,
                    detail=_non_free_codex_token_unavailable_detail(restricted_model_label),
                )
            raise HTTPException(status_code=503, detail="No available Codex token could satisfy this request")
        except asyncio.CancelledError:
            if not stream_finalized:
                await _finalize_cancelled_request_log(
                    request_log,
                    status_code=499,
                    success=False,
                    attempt_count=attempt_count,
                    finished_at=utcnow(),
                    token_id=last_token_id,
                    account_id=last_account_id,
                    error_message="Client disconnected",
                    timing=timing,
                )
            raise
        except HTTPException as exc:
            if not stream_finalized:
                await _finalize_request_log_with_timing(
                    timing,
                    request_log,
                    status_code=getattr(exc, "status_code", 500),
                    success=False,
                    attempt_count=attempt_count,
                    finished_at=utcnow(),
                    token_id=last_token_id,
                    account_id=last_account_id,
                    error_message=str(getattr(exc, "detail", "") or exc),
                )
            await output_queue.put(_build_stream_error_event(getattr(exc, "status_code", 500), str(getattr(exc, "detail", "") or exc)))
            await output_queue.put(b"data: [DONE]\n\n")
        except Exception as exc:
            if not stream_finalized:
                await _finalize_request_log_with_timing(
                    timing,
                    request_log,
                    status_code=500,
                    success=False,
                    attempt_count=attempt_count,
                    finished_at=utcnow(),
                    token_id=last_token_id,
                    account_id=last_account_id,
                    error_message=str(exc),
                )
            await output_queue.put(_build_stream_error_event(500, str(exc) or type(exc).__name__))
            await output_queue.put(b"data: [DONE]\n\n")
        finally:
            try:
                await output_queue.put(queue_done_sentinel)
                await response_lease.release()
            finally:
                if timing_context_token is not None:
                    _REQUEST_TIMING.reset(timing_context_token)
                if db_timing_context_token is not None:
                    reset_db_timing_recorder(db_timing_context_token)

    async def response_body() -> AsyncGenerator[bytes, None]:
        nonlocal response_body_started
        response_body_started = True
        worker_task = asyncio.create_task(worker())
        try:
            # Commit an initial SSE comment immediately so downstream proxies/CDNs
            # don't wait for the first upstream model event before forwarding body bytes.
            yield STREAM_KEEPALIVE_COMMENT
            while True:
                item = await output_queue.get()
                if item is queue_done_sentinel:
                    break
                if isinstance(item, bytes):
                    yield item
        finally:
            if not worker_task.done():
                worker_task.cancel()
                with suppress(asyncio.CancelledError):
                    await worker_task

    async def on_response_close_before_body_start() -> None:
        if response_body_started:
            return
        try:
            await _finalize_cancelled_request_log(
                request_log,
                status_code=499,
                success=False,
                attempt_count=0,
                finished_at=utcnow(),
                token_id=None,
                account_id=None,
                error_message="Client disconnected",
                timing=timing,
            )
        finally:
            await response_lease.release()

    return _FinalizingStreamingResponse(
        response_body(),
        on_close=on_response_close_before_body_start,
        media_type="text/event-stream",
        headers=_sse_response_headers(),
    )


async def _execute_proxy_request_with_failover(
    http_request: Request,
    *,
    endpoint: str,
    request_model: str | None,
    is_stream: bool,
    proxy_call: Callable[[httpx.AsyncClient, str, str | None], Awaitable[ProxyRequestResult]],
    compact: bool = False,
) -> Response:
    response_traffic: ResponseTrafficController = http_request.app.state.response_traffic
    response_lease = response_traffic.start_response()
    release_response_traffic = True
    timing = _RequestTimingRecorder()
    request_started_at = utcnow()
    request_log = _RequestLogHandle.start(
        endpoint=endpoint,
        model=request_model,
        is_stream=is_stream,
        started_at=request_started_at,
        client_ip=http_request.client.host if http_request.client is not None else None,
        user_agent=(http_request.headers.get("User-Agent") or "").strip() or None,
        timing=timing,
    )
    attempt_count = 0
    last_token_id: int | None = None
    last_account_id: str | None = None

    timing_context_token = _REQUEST_TIMING.set(timing)
    db_timing_context_token = set_db_timing_recorder(timing)
    try:
        await _record_event_loop_lag(timing)
        if is_stream and _is_responses_image_compat_model(request_model):
            release_response_traffic = False
            return _gpt_image_stream_keepalive_response(
                http_request,
                endpoint=endpoint,
                request_model=request_model,
                compact=compact,
                proxy_call=proxy_call,
                request_log=request_log,
                response_lease=response_lease,
                timing=timing,
            )

        max_attempts = _effective_proxy_max_attempts(endpoint=endpoint)
        max_claims = _max_request_account_retries()
        claim_count = 0
        client: httpx.AsyncClient = _state_http_client(http_request.app, "response_http_client")
        oauth_manager: CodexOAuthManager = http_request.app.state.oauth_manager
        selection_settings = _current_token_selection_settings(http_request.app)
        last_error: HTTPException | None = None
        restricted_model_name = _non_free_only_codex_model_name(request_model)
        require_non_free_token = restricted_model_name is not None
        restricted_model_label = restricted_model_name or "requested"
        scoped_cooldown_scope = _image_rate_limit_scoped_cooldown_scope(request_model)
        excluded_token_ids: set[int] = set()
        skipped_free_token_ids: set[int] = set()
        postponed_unknown_plan_tokens: list[Any] = []

        while attempt_count < max_attempts and claim_count < max_claims:
            claim_count += 1
            token_row = await _await_timed(
                _claim_next_token_for_model_request(
                    selection_settings,
                    exclude_token_ids=excluded_token_ids,
                    scoped_cooldown_scope=scoped_cooldown_scope,
                    timing=timing,
                    token_pool_snapshot=_current_token_pool_snapshot(http_request.app),
                    require_non_free_token=require_non_free_token,
                ),
                timing=timing,
                span_name="claim_token_ms",
                db_wait=True,
            )
            using_postponed_unknown_plan = False
            if token_row is None:
                if require_non_free_token and postponed_unknown_plan_tokens:
                    token_row = postponed_unknown_plan_tokens.pop(0)
                    using_postponed_unknown_plan = True
                    _record_selected_token_observation_start(token_row.id, timing)
                else:
                    break

            if require_non_free_token:
                effective_plan_type = await _effective_token_plan_type(http_request, token_row)
                if effective_plan_type == "free":
                    excluded_token_ids.add(token_row.id)
                    skipped_free_token_ids.add(token_row.id)
                    logger.info(
                        "Skipping free-plan Codex token for %s request: token_id=%s account_id=%s",
                        restricted_model_label,
                        token_row.id,
                        token_row.account_id,
                    )
                    _record_selected_token_observation_finish(
                        token_row.id,
                        started_at=request_started_at,
                        first_token_at=None,
                    )
                    continue
                if effective_plan_type is None and not using_postponed_unknown_plan:
                    excluded_token_ids.add(token_row.id)
                    postponed_unknown_plan_tokens.append(token_row)
                    logger.info(
                        "Postponing unknown-plan Codex token while searching for a known non-free %s token: "
                        "token_id=%s account_id=%s",
                        restricted_model_label,
                        token_row.id,
                        token_row.account_id,
                    )
                    _record_selected_token_observation_finish(
                        token_row.id,
                        started_at=request_started_at,
                        first_token_at=None,
                    )
                    continue

            attempt_count += 1
            attempt = attempt_count
            last_token_id = token_row.id
            last_account_id = token_row.account_id

            try:
                with timing.measure("oauth_ms"):
                    access_token, access_token_refreshed = await oauth_manager.get_access_token(token_row, client)
            except HTTPException as exc:
                oauth_manager.invalidate(token_row.id)
                permanent_refresh_failure = is_permanently_invalid_refresh_token_error(exc)
                await mark_token_error(
                    token_row.id,
                    str(exc.detail),
                    deactivate=permanent_refresh_failure,
                    clear_access_token=True,
                )
                if exc.status_code in (401, 403):
                    last_error = exc
                    _record_selected_token_observation_finish(
                        token_row.id,
                        started_at=request_started_at,
                        first_token_at=None,
                    )
                    continue
                _record_selected_token_observation_finish(
                    token_row.id,
                    started_at=request_started_at,
                    first_token_at=None,
                )
                raise
            except BaseException:
                _record_selected_token_observation_finish(
                    token_row.id,
                    started_at=request_started_at,
                    first_token_at=None,
                )
                raise

            token_observation_transferred = False
            token_observation_first_token_at: datetime | None = None
            try:
                proxy_result = await proxy_call(client, access_token, token_row.account_id)
                if isinstance(proxy_result.response, StreamingResponse):

                    async def on_stream_close(
                        *,
                        attempt_index: int = attempt,
                        token_id: int = token_row.id,
                        proxied_account_id: str | None = token_row.account_id,
                        proxied_result: ProxyRequestResult = proxy_result,
                        selected_token_row: Any = token_row,
                    ) -> None:
                        db_timing_context_token = set_db_timing_recorder(timing)
                        try:
                            disconnected_before_completion = (
                                proxied_result.stream_capture is not None
                                and proxied_result.stream_capture.error_message is None
                                and not proxied_result.stream_capture.completed
                            )
                            if disconnected_before_completion:
                                finalized_request_log_id = await _finalize_request_log_with_timing(
                                    timing,
                                    request_log,
                                    status_code=499,
                                    success=False,
                                    attempt_count=attempt_index,
                                    finished_at=utcnow(),
                                    first_token_at=(
                                        proxied_result.stream_capture.first_token_at
                                        or proxied_result.first_token_at
                                    ),
                                    token_id=token_id,
                                    account_id=proxied_account_id,
                                    model_name=(
                                        proxied_result.stream_capture.model_name
                                        or proxied_result.model_name
                                    ),
                                    error_message="Client disconnected",
                                )
                            elif proxied_result.stream_capture is not None:
                                finalized_request_log_id = await _finalize_stream_request_log(
                                    request_log_ref=request_log,
                                    status_code=proxied_result.status_code,
                                    attempt_count=attempt_index,
                                    token_id=token_id,
                                    account_id=proxied_account_id,
                                    fallback_model_name=proxied_result.model_name,
                                    fallback_first_token_at=proxied_result.first_token_at,
                                    stream_capture=proxied_result.stream_capture,
                                    timing=timing,
                                    success_hook=proxied_result.on_success,
                                )
                            else:
                                finalized_request_log_id = await _finalize_request_log_with_timing(
                                    timing,
                                    request_log,
                                    success_hook=proxied_result.on_success,
                                    status_code=proxied_result.status_code,
                                    success=True,
                                    attempt_count=attempt_index,
                                    finished_at=utcnow(),
                                    first_token_at=proxied_result.first_token_at,
                                    token_id=token_id,
                                    account_id=proxied_account_id,
                                    model_name=proxied_result.model_name,
                                )
                            if (
                                not disconnected_before_completion
                                and (
                                    proxied_result.stream_capture is None
                                    or proxied_result.stream_capture.error_message is None
                                    or proxied_result.stream_capture.completed
                                )
                            ):
                                await _mark_token_success_with_timing(
                                    timing,
                                    token_id,
                                    token_row=selected_token_row,
                                )
                        finally:
                            _record_selected_token_observation_finish(
                                token_id,
                                started_at=request_started_at,
                                first_token_at=(
                                    (
                                        proxied_result.stream_capture.first_token_at
                                        if proxied_result.stream_capture is not None
                                        else None
                                    )
                                    or proxied_result.first_token_at
                                ),
                            )
                            reset_db_timing_recorder(db_timing_context_token)
                            await response_lease.release()

                    body_iterator = proxy_result.response.body_iterator
                    if _is_responses_image_compat_model(request_model):
                        body_iterator = _wrap_sse_stream_with_initial_keepalive(
                            body_iterator,
                            heartbeat_interval_seconds=_stream_keepalive_interval_seconds(),
                        )
                    on_stream_close_once = _IdempotentAsyncCallback(on_stream_close)
                    body_iterator = _wrap_streaming_body_iterator(
                        body_iterator,
                        on_close=on_stream_close_once,
                    )
                    release_response_traffic = False
                    token_observation_transferred = True
                    return _FinalizingStreamingResponse(
                        body_iterator,
                        on_close=on_stream_close_once,
                        status_code=proxy_result.response.status_code,
                        headers=dict(proxy_result.response.headers),
                        media_type=proxy_result.response.media_type,
                    )

                await _mark_token_success_with_timing(timing, token_row.id, token_row=token_row)
                token_observation_first_token_at = proxy_result.first_token_at
                finalized_request_log_id = await _finalize_request_log_with_timing(
                    timing,
                    request_log,
                    success_hook=proxy_result.on_success,
                    status_code=proxy_result.status_code,
                    success=True,
                    attempt_count=attempt,
                    finished_at=utcnow(),
                    first_token_at=proxy_result.first_token_at,
                    token_id=token_row.id,
                    account_id=token_row.account_id,
                    model_name=proxy_result.model_name,
                    **_usage_finalize_kwargs(proxy_result.usage_metrics),
                )
                return proxy_result.response
            except HTTPException as exc:
                status_code = getattr(exc, "status_code", 500)
                detail = str(getattr(exc, "detail", "") or exc)
                cooldown_seconds = _extract_usage_limit_cooldown_seconds(status_code, detail)

                if cooldown_seconds is not None:
                    await mark_token_error(
                        token_row.id,
                        detail,
                        cooldown_seconds=cooldown_seconds,
                    )
                    logger.info(
                        "Codex account cooled down: token_id=%s account_id=%s cooldown_seconds=%s attempt=%s/%s",
                        token_row.id,
                        token_row.account_id,
                        cooldown_seconds,
                        attempt,
                        max_attempts,
                    )
                    last_error = exc
                    continue

                image_cooldown_seconds = _extract_image_rate_limit_cooldown_seconds(
                    status_code,
                    detail,
                    request_model=request_model,
                )
                if image_cooldown_seconds is not None and scoped_cooldown_scope is not None:
                    await mark_token_scoped_cooldown(
                        token_row.id,
                        scoped_cooldown_scope,
                        detail,
                        cooldown_seconds=image_cooldown_seconds,
                    )
                    logger.info(
                        "Codex image bucket cooled down: token_id=%s account_id=%s scope=%s cooldown_seconds=%s attempt=%s/%s",
                        token_row.id,
                        token_row.account_id,
                        scoped_cooldown_scope,
                        image_cooldown_seconds,
                        attempt,
                        max_attempts,
                    )
                    last_error = exc
                    continue

                if _should_record_http_exception_token_error(exc):
                    await mark_token_error(token_row.id, detail)

                permanently_disabled = _is_permanent_account_disable_error(status_code, detail)
                auth_failed_after_refresh = status_code in (401, 403) and access_token_refreshed

                if permanently_disabled or auth_failed_after_refresh:
                    oauth_manager.invalidate(token_row.id)
                    await mark_token_error(
                        token_row.id,
                        detail,
                        deactivate=True,
                        clear_access_token=True,
                    )
                    logger.warning(
                        "Codex account disabled after upstream auth failure: token_id=%s account_id=%s status=%s refreshed=%s attempt=%s/%s",
                        token_row.id,
                        token_row.account_id,
                        status_code,
                        access_token_refreshed,
                        attempt,
                        max_attempts,
                    )
                    last_error = exc
                    continue

                if status_code in (401, 403):
                    oauth_manager.invalidate(token_row.id)
                    await mark_token_error(token_row.id, detail, clear_access_token=True)
                    logger.warning(
                        "Codex upstream auth failed; invalidated access token for retry: token_id=%s account_id=%s status=%s attempt=%s/%s",
                        token_row.id,
                        token_row.account_id,
                        status_code,
                        attempt,
                        max_attempts,
                    )
                    last_error = exc
                    continue

                compact_server_error_cooling_time = _get_compact_codex_server_error_cooling_time(
                    compact=compact,
                    status_code=status_code,
                    error_text=detail,
                )
                if _should_retry_http_exception(exc) and attempt < max_attempts:
                    excluded_token_ids.add(token_row.id)
                    mark_kwargs: dict[str, Any] = {}
                    if compact_server_error_cooling_time > 0:
                        mark_kwargs["cooldown_seconds"] = compact_server_error_cooling_time
                    await mark_token_error(token_row.id, detail, **mark_kwargs)
                    logger.info(
                        "Codex upstream server error before response commit: token_id=%s account_id=%s status=%s compact=%s cooldown_seconds=%s attempt=%s/%s",
                        token_row.id,
                        token_row.account_id,
                        status_code,
                        compact,
                        compact_server_error_cooling_time,
                        attempt,
                        max_attempts,
                    )
                    last_error = exc
                    continue

                raise
            except httpx.HTTPError as exc:
                message = f"Upstream request failed: {type(exc).__name__}: {exc}"
                compact_server_error_cooling_time = _get_compact_codex_server_error_cooling_time(
                    compact=compact,
                    status_code=500,
                    error_text=message,
                )
                if attempt < max_attempts:
                    excluded_token_ids.add(token_row.id)
                    mark_kwargs: dict[str, Any] = {}
                    if compact_server_error_cooling_time > 0:
                        mark_kwargs["cooldown_seconds"] = compact_server_error_cooling_time
                    await mark_token_error(token_row.id, message, **mark_kwargs)
                    logger.info(
                        "Codex upstream transport error before response commit: token_id=%s account_id=%s error_type=%s compact=%s cooldown_seconds=%s attempt=%s/%s",
                        token_row.id,
                        token_row.account_id,
                        type(exc).__name__,
                        compact,
                        compact_server_error_cooling_time,
                        attempt,
                        max_attempts,
                    )
                    last_error = HTTPException(status_code=502, detail=message)
                    continue
                await mark_token_error(token_row.id, message)
                raise HTTPException(status_code=502, detail=message) from exc
            except Exception as exc:
                if not _is_retryable_upstream_transport_exception(exc):
                    raise
                message = f"Upstream request failed: {type(exc).__name__}: {exc}"
                compact_server_error_cooling_time = _get_compact_codex_server_error_cooling_time(
                    compact=compact,
                    status_code=500,
                    error_text=message,
                )
                if attempt < max_attempts:
                    excluded_token_ids.add(token_row.id)
                    mark_kwargs: dict[str, Any] = {}
                    if compact_server_error_cooling_time > 0:
                        mark_kwargs["cooldown_seconds"] = compact_server_error_cooling_time
                    await mark_token_error(token_row.id, message, **mark_kwargs)
                    logger.info(
                        "Codex upstream transport error before response commit: token_id=%s account_id=%s error_type=%s compact=%s cooldown_seconds=%s attempt=%s/%s",
                        token_row.id,
                        token_row.account_id,
                        type(exc).__name__,
                        compact,
                        compact_server_error_cooling_time,
                        attempt,
                        max_attempts,
                    )
                    last_error = HTTPException(status_code=502, detail=message)
                    continue
                await mark_token_error(token_row.id, message)
                raise HTTPException(status_code=502, detail=message) from exc
            finally:
                if not token_observation_transferred:
                    _record_selected_token_observation_finish(
                        token_row.id,
                        started_at=request_started_at,
                        first_token_at=token_observation_first_token_at,
                    )

        if last_error is not None:
            if _should_retry_http_exception(last_error):
                raise last_error
            raise HTTPException(
                status_code=503,
                detail=f"All available Codex accounts are exhausted or cooling down. Last error: {last_error.detail}",
            )
        if require_non_free_token and skipped_free_token_ids:
            raise HTTPException(
                status_code=503,
                detail=_non_free_codex_token_unavailable_detail(restricted_model_label),
            )
        raise HTTPException(status_code=503, detail="No available Codex token could satisfy this request")
    except asyncio.CancelledError:
        await _finalize_cancelled_request_log(
            request_log,
            status_code=499,
            success=False,
            attempt_count=attempt_count,
            finished_at=utcnow(),
            token_id=last_token_id,
            account_id=last_account_id,
            error_message="Client disconnected",
            timing=timing,
        )
        raise
    except HTTPException as exc:
        await _finalize_request_log_with_timing(
            timing,
            request_log,
            status_code=getattr(exc, "status_code", 500),
            success=False,
            attempt_count=attempt_count,
            finished_at=utcnow(),
            token_id=last_token_id,
            account_id=last_account_id,
            error_message=str(getattr(exc, "detail", "") or exc),
        )
        raise
    except Exception as exc:
        await _finalize_request_log_with_timing(
            timing,
            request_log,
            status_code=500,
            success=False,
            attempt_count=attempt_count,
            finished_at=utcnow(),
            token_id=last_token_id,
            account_id=last_account_id,
            error_message=str(exc),
        )
        raise
    finally:
        if release_response_traffic:
            await response_lease.release()
        reset_db_timing_recorder(db_timing_context_token)
        _REQUEST_TIMING.reset(timing_context_token)


async def _wrap_streaming_body_iterator(
    body_iterator: AsyncIterator[bytes],
    *,
    on_close: Callable[[], Awaitable[None]],
) -> AsyncGenerator[bytes, None]:
    try:
        async for chunk in body_iterator:
            yield chunk
    finally:
        close_iterator = getattr(body_iterator, "aclose", None)
        if callable(close_iterator):
            try:
                with anyio.CancelScope(shield=True):
                    await close_iterator()
            except Exception:
                logger.exception("Failed to close streamed response body iterator")

        async def on_close_with_cancel_shield() -> None:
            with anyio.CancelScope(shield=True):
                await on_close()

        cleanup_task = asyncio.create_task(on_close_with_cancel_shield(), name="oaix-streamed-response-cleanup")
        try:
            await asyncio.shield(cleanup_task)
        except asyncio.CancelledError:
            try:
                with anyio.CancelScope(shield=True):
                    await asyncio.shield(cleanup_task)
            except Exception:
                logger.exception("Failed to run streamed response cleanup")
            raise
        except Exception:
            logger.exception("Failed to run streamed response cleanup")


def _selection_strategy_label(strategy: str) -> str:
    if strategy == TOKEN_SELECTION_STRATEGY_FILL_FIRST:
        return "Fill-first"
    return "最旧未使用优先"


def _selection_strategy_description(strategy: str) -> str:
    if strategy == TOKEN_SELECTION_STRATEGY_FILL_FIRST:
        return "持续使用第一个可用 key；当前并发达到上限后才填下一个 key。"
    return "优先分配最久未使用的 key，把请求更均匀地摊在账号窗口上。"


def _serialize_token_selection_settings(settings: TokenSelectionSettings) -> dict[str, Any]:
    return {
        "strategy": settings.strategy,
        "label": _selection_strategy_label(settings.strategy),
        "description": _selection_strategy_description(settings.strategy),
        "token_order": list(settings.token_order),
        "plan_order_enabled": settings.plan_order_enabled,
        "plan_order": list(settings.plan_order),
        "active_stream_cap": normalize_token_active_stream_cap(settings.active_stream_cap),
        "updated_at": settings.updated_at,
        "options": [
            {
                "id": DEFAULT_TOKEN_SELECTION_STRATEGY,
                "label": _selection_strategy_label(DEFAULT_TOKEN_SELECTION_STRATEGY),
                "description": _selection_strategy_description(DEFAULT_TOKEN_SELECTION_STRATEGY),
            },
            {
                "id": TOKEN_SELECTION_STRATEGY_FILL_FIRST,
                "label": _selection_strategy_label(TOKEN_SELECTION_STRATEGY_FILL_FIRST),
                "description": _selection_strategy_description(TOKEN_SELECTION_STRATEGY_FILL_FIRST),
            },
        ],
    }


def _serialize_token_import_batch_summary(batch: TokenImportBatchSummary) -> dict[str, Any]:
    return asdict(batch)


def _current_token_selection_settings(app: FastAPI) -> TokenSelectionSettings:
    selection = getattr(app.state, "token_selection_settings", None)
    if isinstance(selection, TokenSelectionSettings):
        return selection
    return TokenSelectionSettings(strategy=DEFAULT_TOKEN_SELECTION_STRATEGY)


def _set_current_token_selection_settings(app: FastAPI, selection: TokenSelectionSettings) -> None:
    app.state.token_selection_settings = selection


def _serialize_admin_token_item(
    token_row,
    *,
    plan_info: CodexPlanInfo,
    quota_snapshot: CodexQuotaSnapshot | None,
    observed_cost_usd: float | None,
    active_stream_cap: int | None = None,
) -> dict[str, Any]:
    resolved_active_stream_cap = normalize_token_active_stream_cap(
        active_stream_cap if active_stream_cap is not None else _fill_first_token_active_stream_cap()
    )
    item = {
        "id": token_row.id,
        "email": token_row.email,
        "account_id": token_row.account_id,
        "chatgpt_account_id": plan_info.chatgpt_account_id or token_row.account_id,
        "is_active": token_row.is_active,
        "cooldown_until": token_row.cooldown_until,
        "last_refresh_at": token_row.last_refresh_at,
        "expires_at": token_row.expires_at,
        "last_used_at": token_row.last_used_at,
        "last_error": token_row.last_error,
        "source_file": token_row.source_file,
        "created_at": token_row.created_at,
        "updated_at": token_row.updated_at,
        "plan_type": (
            quota_snapshot.plan_type
            if quota_snapshot and quota_snapshot.plan_type
            else _normalize_optional_text(getattr(token_row, "plan_type", None)) or plan_info.plan_type
        ),
        "subscription_active_start": plan_info.subscription_active_start,
        "subscription_active_until": plan_info.subscription_active_until,
        "observed_cost_usd": observed_cost_usd,
        "active_streams": _token_active_stream_count(token_row.id),
        "active_stream_cap": resolved_active_stream_cap,
        "quota": asdict(quota_snapshot) if quota_snapshot is not None else None,
    }
    return item


def _observed_cost_fallback_account_id(token_row) -> str | None:
    account_id = str(token_row.account_id or "").strip()
    return account_id or None


def _resolve_token_observed_cost_usd(
    observed_costs_by_token: dict[int, float],
    observed_costs_by_account: dict[str, float],
    token_counts_by_account: dict[str, int],
    *,
    token_row,
) -> float | None:
    direct_cost = observed_costs_by_token.get(token_row.id)
    if direct_cost is not None:
        return round(float(direct_cost), 6)

    account_id = _observed_cost_fallback_account_id(token_row)
    if not account_id or token_counts_by_account.get(account_id) != 1:
        return None

    fallback_cost = observed_costs_by_account.get(account_id)
    if fallback_cost is None:
        return None
    return round(float(fallback_cost), 6)


def _parse_admin_token_ids(value: str, *, limit: int = 100) -> tuple[int, ...]:
    token_ids: list[int] = []
    seen: set[int] = set()
    for raw_part in re.split(r"[\s,]+", str(value or "").strip()):
        if not raw_part:
            continue
        try:
            token_id = int(raw_part)
        except ValueError as exc:
            raise ValueError("ids must be a comma-separated list of positive integers") from exc
        if token_id <= 0:
            raise ValueError("ids must be a comma-separated list of positive integers")
        if token_id in seen:
            continue
        seen.add(token_id)
        token_ids.append(token_id)
        if len(token_ids) >= max(1, int(limit)):
            break
    return tuple(token_ids)


async def _build_admin_token_items(
    app: FastAPI,
    *,
    token_rows: list[Any],
    include_quota: bool,
) -> list[dict[str, Any]]:
    active_stream_cap = normalize_token_active_stream_cap(
        _current_token_selection_settings(app).active_stream_cap
    )
    plan_info_by_id = {
        token_row.id: extract_codex_plan_info(
            token_row.id_token,
            account_id=token_row.account_id,
            raw_payload=token_row.raw_payload,
        )
        for token_row in token_rows
    }

    quota_by_id: dict[int, CodexQuotaSnapshot] = {}
    observed_costs_by_token: dict[int, float] = {}
    observed_costs_by_account: dict[str, float] = {}
    token_counts_by_account: dict[str, int] = {}
    observed_token_ids = {token_row.id for token_row in token_rows}
    if observed_token_ids:
        try:
            observed_costs_by_token = await get_request_costs_by_token(observed_token_ids)
        except Exception:
            logger.exception("Failed to collect admin token observed costs by token")

    fallback_account_ids = {
        account_id
        for token_row in token_rows
        if token_row.id not in observed_costs_by_token
        for account_id in [_observed_cost_fallback_account_id(token_row)]
        if account_id is not None
    }
    if fallback_account_ids:
        try:
            observed_costs_by_account, token_counts_by_account = await asyncio.gather(
                get_request_costs_by_account(fallback_account_ids),
                get_token_counts_by_account_ids(fallback_account_ids),
            )
        except Exception:
            logger.exception("Failed to collect admin token observed cost fallbacks")
    if include_quota and token_rows:
        quota_service: CodexQuotaService = app.state.quota_service
        client: httpx.AsyncClient = _state_http_client(app, "admin_http_client")
        oauth_manager: CodexOAuthManager = app.state.oauth_manager
        effective_account_ids = {
            token_row.id: plan_info_by_id[token_row.id].chatgpt_account_id or token_row.account_id
            for token_row in token_rows
        }
        quota_token_rows = [
            token_row
            for token_row in token_rows
            if bool(getattr(token_row, "is_active", False))
            and _normalize_optional_text(getattr(token_row, "refresh_token", None)) is not None
        ]
        try:
            quota_by_id = await quota_service.get_many(
                quota_token_rows,
                client=client,
                oauth_manager=oauth_manager,
                account_ids=effective_account_ids,
            )
        except Exception:
            logger.exception("Failed to collect admin token quota snapshots")

    return [
        _serialize_admin_token_item(
            token_row,
            plan_info=plan_info_by_id[token_row.id],
            quota_snapshot=quota_by_id.get(token_row.id),
            observed_cost_usd=_resolve_token_observed_cost_usd(
                observed_costs_by_token,
                observed_costs_by_account,
                token_counts_by_account,
                token_row=token_row,
            ),
            active_stream_cap=active_stream_cap,
        )
        for token_row in token_rows
    ]


async def _build_admin_token_quota_items(
    app: FastAPI,
    *,
    token_rows: list[Any],
) -> list[dict[str, Any]]:
    if not token_rows:
        return []

    active_stream_cap = normalize_token_active_stream_cap(
        _current_token_selection_settings(app).active_stream_cap
    )
    plan_info_by_id = {
        token_row.id: extract_codex_plan_info(
            token_row.id_token,
            account_id=token_row.account_id,
            raw_payload=token_row.raw_payload,
        )
        for token_row in token_rows
    }
    quota_by_id: dict[int, CodexQuotaSnapshot] = {}
    quota_token_rows = [
        token_row
        for token_row in token_rows
        if bool(getattr(token_row, "is_active", False))
        and _normalize_optional_text(getattr(token_row, "refresh_token", None)) is not None
    ]
    if quota_token_rows:
        quota_service: CodexQuotaService = app.state.quota_service
        client: httpx.AsyncClient = _state_http_client(app, "admin_http_client")
        oauth_manager: CodexOAuthManager = app.state.oauth_manager
        effective_account_ids = {
            token_row.id: plan_info_by_id[token_row.id].chatgpt_account_id or token_row.account_id
            for token_row in token_rows
        }
        try:
            quota_by_id = await quota_service.get_many(
                quota_token_rows,
                client=client,
                oauth_manager=oauth_manager,
                account_ids=effective_account_ids,
            )
        except Exception:
            logger.exception("Failed to collect admin token quota snapshots")

    return [
        {
            "id": token_row.id,
            "plan_type": (
                quota_by_id[token_row.id].plan_type
                if token_row.id in quota_by_id and quota_by_id[token_row.id].plan_type
                else _normalize_optional_text(getattr(token_row, "plan_type", None)) or plan_info_by_id[token_row.id].plan_type
            ),
            "active_streams": _token_active_stream_count(token_row.id),
            "active_stream_cap": active_stream_cap,
            "quota": asdict(quota_by_id[token_row.id]) if token_row.id in quota_by_id else None,
        }
        for token_row in token_rows
    ]


@asynccontextmanager
async def lifespan(app: FastAPI):
    _configure_runtime_logging()
    await init_db()
    repair_summary = await repair_duplicate_token_histories()
    if repair_summary.merged_row_count:
        logger.info(
            "Merged duplicate token histories on startup: groups=%s merged_rows=%s canonical_ids=%s shadow_ids=%s",
            repair_summary.duplicate_group_count,
            repair_summary.merged_row_count,
            list(repair_summary.canonical_ids),
            list(repair_summary.shadow_ids),
        )
    app.state.response_http_client = httpx.AsyncClient(
        follow_redirects=True,
        transport=_TimingAsyncHTTPTransport(
            http2=False,
            limits=httpx.Limits(
                max_connections=_upstream_http_max_connections(),
                max_keepalive_connections=_upstream_http_max_keepalive_connections(),
            ),
        ),
    )
    app.state.http_client = app.state.response_http_client
    app.state.import_http_client = httpx.AsyncClient(
        follow_redirects=True,
        limits=httpx.Limits(
            max_connections=_import_http_max_connections(),
            max_keepalive_connections=_import_http_max_keepalive_connections(),
        ),
    )
    app.state.admin_http_client = httpx.AsyncClient(
        follow_redirects=True,
        limits=httpx.Limits(
            max_connections=_admin_http_max_connections(),
            max_keepalive_connections=_admin_http_max_keepalive_connections(),
        ),
    )
    app.state.oauth_manager = CodexOAuthManager()
    app.state.quota_service = CodexQuotaService(
        ttl_seconds=_admin_quota_cache_ttl_seconds(),
        concurrency=_admin_quota_max_concurrency(),
    )
    app.state.token_selection_settings = await get_token_selection_settings()
    try:
        await prewarm_fill_first_token_cache(app.state.token_selection_settings)
    except Exception:
        logger.exception("Failed to prewarm fill_first token cache")
    try:
        await _refresh_token_pool_snapshot(app)
    except Exception:
        logger.exception("Failed to load token pool snapshot")

    async def publish_token_selection_settings(selection: TokenSelectionSettings) -> None:
        _set_current_token_selection_settings(app, selection)
        try:
            await _refresh_token_pool_snapshot(app)
        except Exception:
            logger.exception("Failed to refresh token pool snapshot after import publish")

    app.state.token_import_worker = TokenImportBackgroundWorker(
        response_traffic=app.state.response_traffic,
        http_client=app.state.import_http_client,
        on_token_selection_settings_updated=publish_token_selection_settings,
    )
    await app.state.token_import_worker.start()
    try:
        yield
    finally:
        await app.state.token_import_worker.stop()
        await _flush_request_log_write_queue()
        await stop_token_status_write_queue()
        await app.state.import_http_client.aclose()
        await app.state.admin_http_client.aclose()
        await app.state.response_http_client.aclose()
        await close_database()


def create_app() -> FastAPI:
    app = FastAPI(title="oaix Gateway", version="0.1.0", lifespan=lifespan)
    app.state.response_traffic = ResponseTrafficController()
    app.state.token_import_worker = None
    app.add_middleware(
        CORSMiddleware,
        allow_origins=_cors_allow_origins(),
        allow_origin_regex=_cors_allow_origin_regex(),
        allow_credentials=_cors_allow_credentials(),
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.mount("/assets", StaticFiles(directory=str(WEB_DIR)), name="assets")

    @app.middleware("http")
    async def db_pool_role_middleware(request: Request, call_next):
        role_token = set_db_pool_role("admin" if request.url.path.startswith("/admin/") else "request")
        try:
            return await call_next(request)
        finally:
            reset_db_pool_role(role_token)

    @app.get("/")
    async def index() -> HTMLResponse:
        html = (WEB_DIR / "index.html").read_text(encoding="utf-8")
        css_version = _web_asset_version(WEB_DIR / "styles.css")
        js_version = _web_asset_version(WEB_DIR / "app.js")
        html = html.replace("/assets/styles.css", f"/assets/styles.css?v={css_version}")
        html = html.replace("/assets/app.js", f"/assets/app.js?v={js_version}")
        return HTMLResponse(
            content=html,
            headers={"Cache-Control": "no-store, max-age=0"},
        )

    @app.get("/favicon.ico", include_in_schema=False)
    @app.get("/apple-touch-icon.png", include_in_schema=False)
    @app.get("/apple-touch-icon-precomposed.png", include_in_schema=False)
    async def empty_icon() -> Response:
        return Response(status_code=204, headers={"Cache-Control": "public, max-age=86400"})

    @app.get("/healthz")
    async def healthz() -> JSONResponse:
        counts = await get_token_counts()
        ok = counts.available > 0
        return JSONResponse(
            status_code=200 if ok else 503,
            content={
                "ok": ok,
                "upstream": _codex_responses_url(),
                "counts": asdict(counts),
                "service_key_protected": bool(_get_service_api_keys()),
            },
        )

    @app.get("/admin/token-selection")
    async def get_token_selection_route(
        _: None = Depends(verify_service_api_key),
    ) -> dict[str, Any]:
        return _serialize_token_selection_settings(_current_token_selection_settings(app))

    @app.post("/admin/token-selection")
    async def update_token_selection_route(
        http_request: Request,
        payload: TokenSelectionUpdateRequest,
        _: None = Depends(verify_service_api_key),
    ) -> dict[str, Any]:
        try:
            selection = await update_token_selection_settings(strategy=payload.strategy)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        app.state.token_selection_settings = selection
        await _refresh_token_pool_snapshot_best_effort(http_request.app, reason="token_selection_update")
        return _serialize_token_selection_settings(selection)

    @app.post("/admin/token-selection/order")
    async def update_token_selection_order_route(
        http_request: Request,
        payload: TokenSelectionOrderUpdateRequest,
        _: None = Depends(verify_service_api_key),
    ) -> dict[str, Any]:
        selection = await update_token_order_settings(token_ids=payload.token_ids)
        app.state.token_selection_settings = selection
        await _refresh_token_pool_snapshot_best_effort(http_request.app, reason="token_selection_order_update")
        return _serialize_token_selection_settings(selection)

    @app.post("/admin/token-selection/plan-order")
    async def update_token_selection_plan_order_route(
        http_request: Request,
        payload: TokenSelectionPlanOrderUpdateRequest,
        _: None = Depends(verify_service_api_key),
    ) -> dict[str, Any]:
        selection = await update_token_plan_order_settings(
            enabled=payload.enabled,
            plan_order=payload.plan_order,
        )
        app.state.token_selection_settings = selection
        await _refresh_token_pool_snapshot_best_effort(http_request.app, reason="token_selection_plan_order_update")
        return _serialize_token_selection_settings(selection)

    @app.post("/admin/token-selection/concurrency")
    async def update_token_selection_concurrency_route(
        http_request: Request,
        payload: TokenSelectionConcurrencyUpdateRequest,
        _: None = Depends(verify_service_api_key),
    ) -> dict[str, Any]:
        try:
            selection = await update_token_active_stream_cap_settings(active_stream_cap=payload.active_stream_cap)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        app.state.token_selection_settings = selection
        await _refresh_token_pool_snapshot_best_effort(http_request.app, reason="token_selection_concurrency_update")
        return _serialize_token_selection_settings(selection)

    @app.get("/admin/tokens")
    async def list_tokens_route(
        http_request: Request,
        limit: int = Query(100, ge=1, le=500),
        offset: int = Query(0, ge=0),
        include_quota: bool = Query(False),
        q: str = Query("", max_length=512),
        status: str = Query(TOKEN_LIST_STATUS_ALL, max_length=32),
        plan_type: str = Query(TOKEN_LIST_PLAN_ALL, max_length=32),
        sort: str = Query(TOKEN_LIST_SORT_NEWEST, max_length=64),
        import_batch_id: int | None = Query(None, ge=1),
        _: None = Depends(verify_service_api_key),
    ) -> dict[str, Any]:
        import_batches = await list_token_import_batch_summaries(limit=30)
        batch_token_ids: tuple[int, ...] | None = None
        selected_import_batch: TokenImportBatchSummary | None = None
        if import_batch_id is not None:
            selected_import_batch = next(
                (batch for batch in import_batches if int(batch.id) == int(import_batch_id)),
                None,
            )
            batch_token_ids = selected_import_batch.token_ids if selected_import_batch is not None else ()

        counts, filtered_counts, plan_counts, filtered_total, token_rows = await asyncio.gather(
            get_token_counts(),
            get_token_status_counts(search=q, plan_type=plan_type, token_ids=batch_token_ids),
            get_token_plan_counts(search=q, status=status, token_ids=batch_token_ids),
            count_token_rows(search=q, status=status, plan_type=plan_type, token_ids=batch_token_ids),
            list_token_rows(
                limit=limit,
                offset=offset,
                search=q,
                status=status,
                plan_type=plan_type,
                sort=sort,
                token_ids=batch_token_ids,
            ),
        )
        items = await _build_admin_token_items(
            http_request.app,
            token_rows=token_rows,
            include_quota=include_quota,
        )
        total = max(0, int(filtered_total))
        total_pages = max(1, int(math.ceil(total / limit))) if limit else 1
        return {
            "counts": asdict(counts),
            "filtered_counts": asdict(filtered_counts),
            "plan_counts": asdict(plan_counts),
            "selection": _serialize_token_selection_settings(_current_token_selection_settings(http_request.app)),
            "import_batches": [_serialize_token_import_batch_summary(batch) for batch in import_batches],
            "selected_import_batch": (
                _serialize_token_import_batch_summary(selected_import_batch)
                if selected_import_batch is not None
                else None
            ),
            "query": {
                "q": q,
                "status": status,
                "plan_type": plan_type,
                "sort": sort,
                "import_batch_id": import_batch_id,
            },
            "pagination": {
                "limit": limit,
                "offset": offset,
                "returned": len(items),
                "total": total,
                "page": int(offset // limit) + 1,
                "total_pages": total_pages,
                "has_previous": offset > 0,
                "has_next": offset + len(items) < total,
            },
            "items": items,
        }

    @app.get("/admin/tokens/quota")
    async def list_token_quota_route(
        http_request: Request,
        ids: str = Query("", max_length=2048),
        _: None = Depends(verify_service_api_key),
    ) -> dict[str, Any]:
        try:
            token_ids = _parse_admin_token_ids(ids, limit=100)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        if not token_ids:
            return {"items": []}

        token_rows = await list_token_rows(
            limit=len(token_ids),
            offset=0,
            token_ids=token_ids,
        )
        items = await _build_admin_token_quota_items(http_request.app, token_rows=token_rows)
        return {"items": items}

    @app.post("/admin/tokens/{token_id}/activation")
    async def update_token_activation_route(
        http_request: Request,
        token_id: int,
        payload: TokenActivationUpdateRequest,
        _: None = Depends(verify_service_api_key),
    ) -> dict[str, Any]:
        token = await set_token_active_state(
            token_id,
            active=payload.active,
            clear_cooldown=payload.clear_cooldown,
        )
        if token is None:
            raise HTTPException(status_code=404, detail="Token not found")
        await _refresh_token_pool_snapshot_best_effort(http_request.app, reason="token_activation_update")
        counts = await get_token_counts()
        return {
            "id": token.id,
            "is_active": token.is_active,
            "cooldown_until": token.cooldown_until,
            "counts": asdict(counts),
        }

    @app.post("/admin/tokens/{token_id}/probe")
    async def probe_token_route(
        http_request: Request,
        token_id: int,
        payload: TokenProbeRequest | None = None,
        _: None = Depends(verify_service_api_key),
    ) -> dict[str, Any]:
        token_row = await get_token_row(token_id)
        if token_row is None:
            raise HTTPException(status_code=404, detail="Token not found")
        return await _probe_token_with_latest_access_token(
            http_request.app,
            http_request=http_request,
            token_row=token_row,
            probe_model=payload.model if payload is not None else None,
        )

    @app.delete("/admin/tokens/{token_id}")
    async def delete_token_route(
        http_request: Request,
        token_id: int,
        _: None = Depends(verify_service_api_key),
    ) -> dict[str, Any]:
        deleted = await delete_token(token_id)
        if deleted is None:
            raise HTTPException(status_code=404, detail="Token not found")
        await _refresh_token_pool_snapshot_best_effort(http_request.app, reason="token_delete")
        counts = await get_token_counts()
        return {
            "id": deleted.canonical_id,
            "deleted_ids": list(deleted.deleted_ids),
            "counts": asdict(counts),
        }

    @app.get("/admin/requests")
    async def list_requests_route(
        limit: int = Query(100, ge=1, le=500),
        _: None = Depends(verify_service_api_key),
    ) -> dict[str, Any]:
        try:
            summary, analytics, raw_items = await asyncio.gather(
                get_request_log_summary(),
                get_request_log_analytics(hours=24, bucket_minutes=60, top_models=6),
                list_request_logs(limit=limit),
            )
            items = [asdict(item) for item in raw_items]
        except SQLAlchemyError as exc:
            raise HTTPException(status_code=503, detail="Request logs temporarily unavailable") from exc
        return {
            "summary": asdict(summary),
            "analytics": asdict(analytics),
            "items": items,
        }

    @app.post("/admin/tokens/import", status_code=202)
    async def import_tokens_route(
        http_request: Request,
        _: None = Depends(verify_service_api_key),
    ) -> dict[str, Any]:
        try:
            body = await http_request.json()
        except json.JSONDecodeError as exc:
            raise HTTPException(status_code=400, detail="Request body must be valid JSON") from exc

        if isinstance(body, dict) and isinstance(body.get("tokens"), list):
            payloads = body["tokens"]
            try:
                import_queue_position = parse_token_import_queue_position(
                    body.get("import_queue_position", DEFAULT_TOKEN_IMPORT_QUEUE_POSITION)
                )
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
        elif isinstance(body, list):
            payloads = body
            import_queue_position = DEFAULT_TOKEN_IMPORT_QUEUE_POSITION
        elif isinstance(body, dict):
            payloads = [body]
            import_queue_position = DEFAULT_TOKEN_IMPORT_QUEUE_POSITION
        else:
            raise HTTPException(
                status_code=400,
                detail="Body must be a token object, an array of token objects, or {'tokens': [...]}",
            )
        worker = getattr(http_request.app.state, "token_import_worker", None)
        if worker is not None:
            await worker.start()
        job = await create_token_import_job(payloads, import_queue_position=import_queue_position)
        if worker is not None:
            worker.submit(job)
        return {"job": asdict(job_state_from_lease(job))}

    @app.get("/admin/tokens/import-jobs/{job_id}")
    async def get_import_job_route(
        job_id: int,
        _: None = Depends(verify_service_api_key),
    ) -> dict[str, Any]:
        worker = getattr(app.state, "token_import_worker", None)
        if worker is not None:
            await worker.start()
        job = await get_token_import_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="Import job not found")
        return {"job": asdict(job)}

    @app.post("/v1/chat/completions")
    async def chat_completions_route(
        http_request: Request,
        request_data: ChatCompletionsRequest,
        _: None = Depends(verify_service_api_key),
    ) -> Response:
        async def proxy_call(
            client: httpx.AsyncClient,
            access_token: str,
            account_id: str | None,
        ) -> ProxyRequestResult:
            return await _proxy_chat_completions_with_token(
                client,
                http_request,
                request_data,
                access_token=access_token,
                account_id=account_id,
            )

        return await _execute_proxy_request_with_failover(
            http_request,
            endpoint="/v1/chat/completions",
            request_model=request_data.model,
            is_stream=bool(request_data.stream),
            proxy_call=proxy_call,
        )

    @app.post("/v1/responses")
    async def responses_route(
        http_request: Request,
        request_data: ResponsesRequest,
        _: None = Depends(verify_service_api_key),
    ) -> Response:
        return await _responses_route_common(http_request, request_data, compact=False)

    @app.post("/v1/responses/compact")
    async def responses_compact_route(
        http_request: Request,
        request_data: ResponsesRequest,
        _: None = Depends(verify_service_api_key),
    ) -> Response:
        return await _responses_route_common(http_request, request_data, compact=True)

    async def _responses_route_common(
        http_request: Request,
        request_data: ResponsesRequest,
        *,
        compact: bool,
    ) -> Response:
        endpoint = _responses_endpoint_path(compact=compact)
        async def proxy_call(
            client: httpx.AsyncClient,
            access_token: str,
            account_id: str | None,
        ) -> ProxyRequestResult:
            return await _proxy_request_with_token(
                client,
                http_request,
                request_data,
                access_token=access_token,
                account_id=account_id,
                compact=compact,
            )

        return await _execute_proxy_request_with_failover(
            http_request,
            endpoint=endpoint,
            request_model=request_data.model,
            is_stream=bool(request_data.stream),
            proxy_call=proxy_call,
            compact=compact,
        )

    @app.post("/v1/images/generations")
    async def images_generations_route(
        http_request: Request,
        _: None = Depends(verify_service_api_key),
    ) -> Response:
        image_request = await _parse_images_generations_request(http_request)

        async def proxy_call(
            client: httpx.AsyncClient,
            access_token: str,
            account_id: str | None,
        ) -> ProxyRequestResult:
            return await _proxy_image_request_with_token(
                client,
                http_request,
                image_request,
                access_token=access_token,
                account_id=account_id,
            )

        return await _execute_proxy_request_with_failover(
            http_request,
            endpoint="/v1/images/generations",
            request_model=image_request.model_name,
            is_stream=image_request.stream,
            proxy_call=proxy_call,
        )

    @app.post("/v1/images/edits")
    async def images_edits_route(
        http_request: Request,
        _: None = Depends(verify_service_api_key),
    ) -> Response:
        image_request = await _parse_images_edits_request(http_request)

        async def proxy_call(
            client: httpx.AsyncClient,
            access_token: str,
            account_id: str | None,
        ) -> ProxyRequestResult:
            return await _proxy_image_request_with_token(
                client,
                http_request,
                image_request,
                access_token=access_token,
                account_id=account_id,
            )

        return await _execute_proxy_request_with_failover(
            http_request,
            endpoint="/v1/images/edits",
            request_model=image_request.model_name,
            is_stream=image_request.stream,
            proxy_call=proxy_call,
        )

    return app


async def _stream_upstream_response(
    stream_cm,
    upstream_iter: AsyncIterator[bytes],
    buffered_chunks: list[bytes],
    *,
    stream_committed: bool,
    stream_capture: _ProxyStreamCapture | None = None,
    response_model_alias: str | None = None,
    emit_initial_keepalive: bool = False,
    pending_chunk_task: asyncio.Task[bytes] | None = None,
) -> AsyncGenerator[bytes, None]:
    if response_model_alias is None:
        try:
            if emit_initial_keepalive:
                yield STREAM_KEEPALIVE_COMMENT
            for chunk in buffered_chunks:
                if stream_capture is not None:
                    stream_capture.feed(chunk)
                yield chunk
            async for chunk in upstream_iter:
                if stream_capture is not None:
                    stream_capture.feed(chunk)
                yield chunk
        except RESPONSES_STREAM_NETWORK_ERRORS as exc:
            logger.info(
                "Codex upstream stream aborted after response commit: error_type=%s detail=%s",
                type(exc).__name__,
                str(exc) or type(exc).__name__,
            )
            if stream_committed:
                error_event = _build_stream_error_event(
                    502,
                    f"Upstream stream aborted before completion: {type(exc).__name__}: {str(exc) or type(exc).__name__}",
                )
                if stream_capture is not None:
                    stream_capture.feed(error_event)
                yield error_event
                yield b"data: [DONE]\n\n"
        finally:
            await stream_cm.__aexit__(None, None, None)
        return

    decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
    text_buffer = ""
    sse_scan_start = 0
    heartbeat_interval_seconds = (
        _stream_keepalive_interval_seconds() if _is_responses_image_compat_model(response_model_alias) else None
    )

    def drain_buffer(*, final: bool = False) -> tuple[list[bytes], bool]:
        nonlocal text_buffer, sse_scan_start
        events: list[bytes] = []

        while True:
            raw_event, text_buffer, sse_scan_start = _pop_sse_event_from_buffer(text_buffer, sse_scan_start)
            if raw_event is None:
                break

            if not raw_event.strip():
                continue

            event_type, event_payload = _extract_responses_stream_event(raw_event)
            if event_type == "[DONE]":
                events.append(b"data: [DONE]\n\n")
                return events, True

            if isinstance(event_payload, dict):
                patched_payload = _apply_response_model_alias(event_payload, response_model_alias)
                encoded_event = _encode_sse_event(event_type, patched_payload)
            else:
                encoded_event = raw_event.encode("utf-8") + b"\n\n"

            events.append(encoded_event)

        if final and text_buffer.strip():
            events.append(text_buffer.encode("utf-8"))
            text_buffer = ""
            sse_scan_start = 0
            return events, True

        return events, False

    try:
        if emit_initial_keepalive:
            yield STREAM_KEEPALIVE_COMMENT
        for chunk in buffered_chunks:
            text_buffer += decoder.decode(chunk)
            events, done = drain_buffer()
            for event in events:
                if stream_capture is not None and event != STREAM_KEEPALIVE_COMMENT:
                    stream_capture.feed(event)
                yield event
            if done:
                return

        async for chunk in _iterate_with_sse_keepalive(
            upstream_iter,
            heartbeat_interval_seconds=heartbeat_interval_seconds,
            pending_chunk_task=pending_chunk_task,
        ):
            text_buffer += decoder.decode(chunk)
            events, done = drain_buffer()
            for event in events:
                if stream_capture is not None and event != STREAM_KEEPALIVE_COMMENT:
                    stream_capture.feed(event)
                yield event
            if done:
                return

        events, _ = drain_buffer(final=True)
        for event in events:
            if stream_capture is not None and event != STREAM_KEEPALIVE_COMMENT:
                stream_capture.feed(event)
            yield event
    except RESPONSES_STREAM_NETWORK_ERRORS as exc:
        logger.info(
            "Codex upstream stream aborted after response commit: error_type=%s detail=%s",
            type(exc).__name__,
            str(exc) or type(exc).__name__,
        )
        if stream_committed and response_model_alias is not None:
            error_event = _build_stream_error_event(
                502,
                f"Upstream image stream aborted before completion: {type(exc).__name__}: {str(exc) or type(exc).__name__}",
            )
            if stream_capture is not None:
                stream_capture.feed(error_event)
            yield error_event
            yield b"data: [DONE]\n\n"
        elif stream_committed:
            yield b"data: [DONE]\n\n"
    finally:
        await stream_cm.__aexit__(None, None, None)


async def _proxy_request_with_token(
    client: httpx.AsyncClient,
    http_request: Request,
    request_data: ResponsesRequest,
    *,
    access_token: str,
    account_id: str | None,
    compact: bool = False,
) -> ProxyRequestResult:
    downstream_stream = bool(request_data.stream)
    upstream_stream = downstream_stream or not compact
    raw_payload = request_data.model_dump(exclude_unset=True)
    payload, response_model_alias = _translate_responses_image_compat_payload(
        raw_payload,
        compact=compact,
    )
    effective_requested_model = response_model_alias or request_data.model
    payload = _sanitize_codex_payload(
        payload,
        compact=compact,
        preserve_previous_response_id=response_model_alias is not None,
    )
    if compact and not downstream_stream:
        payload.pop("stream", None)
    elif not downstream_stream:
        payload["stream"] = True

    headers = _build_upstream_headers(
        http_request,
        access_token=access_token,
        account_id=account_id,
        stream=upstream_stream,
    )
    upstream_url = _codex_responses_url(compact=compact)
    json_payload = json.dumps(payload, ensure_ascii=False)

    if downstream_stream and response_model_alias is not None:
        heartbeat_interval_seconds = _stream_keepalive_interval_seconds()
        stream_capture = _ProxyStreamCapture(
            initial_model_name=effective_requested_model,
            initial_first_token_at=None,
        )

        async def body_iterator() -> AsyncGenerator[bytes, None]:
            stream_cm = client.stream(
                "POST",
                upstream_url,
                headers=headers,
                content=json_payload,
                timeout=httpx.Timeout(connect=30.0, read=None, write=30.0, pool=30.0),
            )
            upstream_response = await _enter_upstream_stream_with_timing(stream_cm)
            if upstream_response.status_code < 200 or upstream_response.status_code >= 300:
                try:
                    raw = await upstream_response.aread()
                finally:
                    await stream_cm.__aexit__(None, None, None)
                raise _upstream_error_http_exception(upstream_response.status_code, _decode_error_body(raw))

            upstream_iter = upstream_response.aiter_raw()
            try:
                primed_stream = await _prime_responses_upstream_stream_with_timing(
                    upstream_iter,
                    heartbeat_interval_seconds=heartbeat_interval_seconds,
                )
            except Exception:
                await stream_cm.__aexit__(None, None, None)
                raise

            async for chunk in _stream_upstream_response(
                stream_cm,
                upstream_iter,
                primed_stream.buffered_chunks,
                stream_committed=primed_stream.stream_committed,
                stream_capture=stream_capture,
                response_model_alias=response_model_alias,
                emit_initial_keepalive=primed_stream.emit_initial_keepalive,
                pending_chunk_task=primed_stream.pending_chunk_task,
            ):
                yield chunk

        return ProxyRequestResult(
            response=StreamingResponse(
                _wrap_sse_stream_with_initial_keepalive(
                    body_iterator(),
                    heartbeat_interval_seconds=heartbeat_interval_seconds,
                ),
                media_type="text/event-stream",
                headers=_sse_response_headers(),
            ),
            status_code=200,
            model_name=effective_requested_model,
            first_token_at=None,
            stream_capture=stream_capture,
        )

    if downstream_stream:
        stream_cm = client.stream(
            "POST",
            upstream_url,
            headers=headers,
            content=json_payload,
            timeout=httpx.Timeout(connect=30.0, read=None, write=30.0, pool=30.0),
        )
        upstream_response = await _enter_upstream_stream_with_timing(stream_cm, compact=compact)
        if upstream_response.status_code < 200 or upstream_response.status_code >= 300:
            raw = await upstream_response.aread()
            await stream_cm.__aexit__(None, None, None)
            raise _upstream_error_http_exception(upstream_response.status_code, _decode_error_body(raw))

        upstream_iter = upstream_response.aiter_raw()
        try:
            primed_stream = await _prime_responses_upstream_stream_with_timing(upstream_iter)
        except HTTPException:
            await stream_cm.__aexit__(None, None, None)
            raise
        except RESPONSES_STREAM_NETWORK_ERRORS:
            await stream_cm.__aexit__(None, None, None)
            raise

        effective_model_name = primed_stream.model_name or request_data.model
        initial_first_token_at = utcnow() if primed_stream.first_token_observed else None
        stream_capture = _ProxyStreamCapture(
            initial_model_name=effective_model_name,
            initial_first_token_at=initial_first_token_at,
        )

        return ProxyRequestResult(
            response=StreamingResponse(
                _stream_upstream_response(
                    stream_cm,
                    upstream_iter,
                    primed_stream.buffered_chunks,
                    stream_committed=primed_stream.stream_committed,
                    stream_capture=stream_capture,
                ),
                media_type="text/event-stream",
                headers=_sse_response_headers(),
            ),
            status_code=upstream_response.status_code,
            model_name=effective_model_name,
            first_token_at=initial_first_token_at,
            stream_capture=stream_capture,
        )

    if upstream_stream:
        stream_cm = client.stream(
            "POST",
            upstream_url,
            headers=headers,
            content=json_payload,
            timeout=httpx.Timeout(connect=30.0, read=None, write=30.0, pool=30.0),
        )
        upstream_response = await _enter_upstream_stream_with_timing(stream_cm, compact=compact)
        try:
            if upstream_response.status_code < 200 or upstream_response.status_code >= 300:
                raw = await upstream_response.aread()
                raise _upstream_error_http_exception(upstream_response.status_code, _decode_error_body(raw))

            data, first_token_at = await _collect_responses_json_from_sse(
                upstream_response.aiter_raw(),
                model=effective_requested_model,
            )
            if response_model_alias is not None:
                data = _apply_response_model_alias(data, response_model_alias)
                effective_model_name = response_model_alias
            else:
                effective_model_name = _extract_response_model_name(data) or request_data.model
            return ProxyRequestResult(
                response=JSONResponse(status_code=upstream_response.status_code, content=data),
                status_code=upstream_response.status_code,
                model_name=effective_model_name,
                first_token_at=first_token_at,
                usage_metrics=extract_usage_metrics(data, model_name=effective_model_name),
            )
        finally:
            await stream_cm.__aexit__(None, None, None)

    compact_headers_timeout = _compact_upstream_headers_timeout_seconds()
    stream_cm = client.stream(
        "POST",
        upstream_url,
        headers=headers,
        content=json_payload,
        timeout=httpx.Timeout(connect=30.0, read=compact_headers_timeout, write=30.0, pool=30.0),
    )
    try:
        upstream_response = await _enter_upstream_stream_with_timing(stream_cm, compact=compact)
    except httpx.ReadTimeout as exc:
        with suppress(Exception):
            await stream_cm.__aexit__(type(exc), exc, exc.__traceback__)
        timing = _current_request_timing()
        if timing is not None:
            timing.add_ms("upstream_compact_timeout_ms", compact_headers_timeout * 1000)
        if not _compact_timeout_fallback_enabled():
            raise HTTPException(
                status_code=504,
                detail=f"Upstream compact request timed out waiting for response headers after {compact_headers_timeout:g}s",
            ) from exc
        if timing is not None:
            timing.add_ms("upstream_compact_fallback_count", 1)
        fallback_payload = dict(payload)
        fallback_payload["store"] = False
        fallback_payload["stream"] = True
        fallback_headers = _build_upstream_headers(
            http_request,
            access_token=access_token,
            account_id=account_id,
            stream=True,
        )
        fallback_stream_cm = client.stream(
            "POST",
            _codex_responses_url(compact=False),
            headers=fallback_headers,
            content=json.dumps(fallback_payload, ensure_ascii=False),
            timeout=httpx.Timeout(connect=30.0, read=None, write=30.0, pool=30.0),
        )
        fallback_response = await _enter_upstream_stream_with_timing(fallback_stream_cm, compact=False)
        try:
            if fallback_response.status_code < 200 or fallback_response.status_code >= 300:
                raw = await fallback_response.aread()
                raise _upstream_error_http_exception(fallback_response.status_code, _decode_error_body(raw))

            data, first_token_at = await _collect_responses_json_from_sse(
                fallback_response.aiter_raw(),
                model=effective_requested_model,
            )
            if response_model_alias is not None:
                data = _apply_response_model_alias(data, response_model_alias)
                effective_model_name = response_model_alias
            else:
                effective_model_name = _extract_response_model_name(data) or request_data.model
            return ProxyRequestResult(
                response=JSONResponse(status_code=fallback_response.status_code, content=data),
                status_code=fallback_response.status_code,
                model_name=effective_model_name,
                first_token_at=first_token_at,
                usage_metrics=extract_usage_metrics(data, model_name=effective_model_name),
            )
        finally:
            await fallback_stream_cm.__aexit__(None, None, None)
    try:
        if upstream_response.status_code < 200 or upstream_response.status_code >= 300:
            raw = await upstream_response.aread()
            raise _upstream_error_http_exception(upstream_response.status_code, _decode_error_body(raw))

        raw, first_token_at = await _read_response_body_with_first_chunk_time(upstream_response.aiter_raw())
        data = _decode_responses_json_body(raw)
        if response_model_alias is not None:
            data = _apply_response_model_alias(data, response_model_alias)
            effective_model_name = response_model_alias
        else:
            effective_model_name = _extract_response_model_name(data) or request_data.model
        return ProxyRequestResult(
            response=JSONResponse(status_code=upstream_response.status_code, content=data),
            status_code=upstream_response.status_code,
            model_name=effective_model_name,
            first_token_at=first_token_at,
            usage_metrics=extract_usage_metrics(data, model_name=effective_model_name),
        )
    finally:
        await stream_cm.__aexit__(None, None, None)


async def _proxy_chat_completions_with_token(
    client: httpx.AsyncClient,
    http_request: Request,
    request_data: ChatCompletionsRequest,
    *,
    access_token: str,
    account_id: str | None,
) -> ProxyRequestResult:
    include_usage = _chat_completion_stream_include_usage(request_data)
    responses_request = await _chat_completions_request_to_responses_request(
        request_data,
        http_request=http_request,
    )
    scope_hash = _chat_image_checkpoint_scope_hash(http_request)
    proxy_result = await _proxy_request_with_token(
        client,
        http_request,
        responses_request,
        access_token=access_token,
        account_id=account_id,
        compact=False,
    )

    if isinstance(proxy_result.response, StreamingResponse):
        checkpoint_callback = _build_chat_image_checkpoint_callback(
            request_model=request_data.model,
            scope_hash=scope_hash,
            responses_payload_getter=lambda: proxy_result.stream_capture.response_payload
            if proxy_result.stream_capture is not None
            else None,
            assistant_content_getter=lambda: _chat_completion_assistant_content_from_response_payload(
                proxy_result.stream_capture.response_payload if proxy_result.stream_capture is not None else None,
                request_model=request_data.model,
            ),
        )
        return ProxyRequestResult(
            response=StreamingResponse(
                _stream_responses_to_chat_completions(
                    proxy_result.response.body_iterator,
                    request_model=request_data.model,
                    include_usage=include_usage,
                ),
                media_type="text/event-stream",
                headers=_sse_response_headers(),
            ),
            status_code=proxy_result.status_code,
            model_name=request_data.model,
            first_token_at=proxy_result.first_token_at,
            usage_metrics=proxy_result.usage_metrics,
            on_success=checkpoint_callback,
            stream_capture=proxy_result.stream_capture,
        )

    body = getattr(proxy_result.response, "body", b"{}")
    try:
        responses_payload = json.loads(body)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=502, detail="Responses proxy returned invalid JSON") from exc

    if not isinstance(responses_payload, dict):
        raise HTTPException(status_code=502, detail="Responses proxy returned a non-object JSON response")

    chat_payload = _build_chat_completions_response(
        responses_payload,
        request_model=request_data.model,
    )
    assistant_message = _chat_completion_assistant_message(chat_payload)
    checkpoint_callback = _build_chat_image_checkpoint_callback(
        request_model=request_data.model,
        scope_hash=scope_hash,
        responses_payload_getter=lambda payload=copy.deepcopy(responses_payload): payload,
        assistant_content_getter=lambda message=copy.deepcopy(assistant_message): (
            str(message.get("content") or "") if isinstance(message, dict) else None
        ),
    )
    return ProxyRequestResult(
        response=JSONResponse(status_code=proxy_result.status_code, content=chat_payload),
        status_code=proxy_result.status_code,
        model_name=request_data.model,
        first_token_at=proxy_result.first_token_at,
        usage_metrics=proxy_result.usage_metrics,
        on_success=checkpoint_callback,
    )


app = create_app()


def main() -> None:
    uvicorn.run(
        "oaix_gateway.api_server:app",
        host=os.getenv("HOST", "0.0.0.0"),
        port=int(os.getenv("PORT", "8000")),
    )
