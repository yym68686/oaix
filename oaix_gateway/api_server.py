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
import random
import re
import time
import uuid
from collections import Counter, OrderedDict, deque
from collections.abc import AsyncGenerator, AsyncIterator, Iterable
from contextlib import asynccontextmanager, contextmanager, nullcontext, suppress
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable

import anyio
import httpx
import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict
from sqlalchemy.exc import SQLAlchemyError
from types import SimpleNamespace
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
from .prompt_observability import build_prompt_cache_trace
from .prompt_cache import (
    client_scope_hash,
    derive_chat_prompt_cache_key,
    derive_responses_prompt_cache_key,
    deterministic_uuid,
    extract_previous_response_id,
    extract_prompt_cache_key,
    normalize_seed_json,
    prompt_cache_key_hash,
    short_hash,
)
from .quota import CodexPlanInfo, CodexQuotaService, CodexQuotaSnapshot, extract_codex_plan_info
from .request_store import (
    create_request_log,
    delete_request_logs_older_than,
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
    list_token_import_batch_failed_items,
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
    is_access_token_only_refresh_token,
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
WEB_VERSION_TZ = timezone(timedelta(hours=8), name="UTC+8")
ADMIN_TOKEN_PROBE_INPUT = "say test"
DEFAULT_IMAGES_MAIN_MODEL = "gpt-5.5"
DEFAULT_IMAGES_TOOL_MODEL = "gpt-image-2"
IMAGE_INPUT_RATE_LIMIT_COOLDOWN_SCOPE = f"{DEFAULT_IMAGES_TOOL_MODEL}:input-images"
NON_FREE_ONLY_CODEX_MODELS = frozenset({DEFAULT_IMAGES_TOOL_MODEL, "gpt-5.4", "gpt-5.3-codex", "gpt-5.2"})
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
        "prompt_cache_key",
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
        "keepalive",
    }
)
RESPONSES_STREAM_NETWORK_ERRORS = (
    httpx.ReadError,
    httpx.RemoteProtocolError,
    httpx.LocalProtocolError,
    httpx.ReadTimeout,
    httpx.ConnectTimeout,
    httpx.ConnectError,
    httpx.PoolTimeout,
)
DEFAULT_STREAM_KEEPALIVE_INTERVAL_SECONDS = 30.0
STREAM_KEEPALIVE_PADDING_BYTES = 2048
STREAM_KEEPALIVE_COMMENT = b": keepalive" + (b" " * STREAM_KEEPALIVE_PADDING_BYTES) + b"\n\n"
_SSE_SEPARATOR_TAIL_CHARS = 3
_SSE_EVENT_SEPARATORS = ("\r\n\r\n", "\r\n\n", "\n\r\n", "\n\n")
_STREAM_OWNER_ACTIVE: dict[str, tuple[str, str]] = {}
_STREAM_OWNER_COUNTERS: Counter[str] = Counter()
_GATEWAY_GUARD_EVENTS: deque[tuple[float, str | None, int, str | None]] = deque(maxlen=50000)
_GATEWAY_GUARD_WINDOW_SECONDS = 300.0
_RUNTIME_GUARD_SAMPLES: deque[tuple[float, int | None, int, int, int]] = deque(maxlen=5000)


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


def _record_gateway_guard_event(
    *,
    endpoint: str | None,
    status_code: int,
    error_message: str | None = None,
) -> None:
    if status_code < 400 and "PoolTimeout" not in (error_message or ""):
        return
    _GATEWAY_GUARD_EVENTS.append((time.monotonic(), endpoint, status_code, error_message))


def _gateway_guard_window_metrics(*, window_seconds: float = _GATEWAY_GUARD_WINDOW_SECONDS) -> dict[str, Any]:
    now = time.monotonic()
    cutoff = now - max(1.0, float(window_seconds))
    while _GATEWAY_GUARD_EVENTS and _GATEWAY_GUARD_EVENTS[0][0] < cutoff:
        _GATEWAY_GUARD_EVENTS.popleft()

    responses_events = [
        event
        for event in _GATEWAY_GUARD_EVENTS
        if event[1] in (None, "/v1/responses")
    ]
    pooltimeout_count = sum(
        1
        for _, _, _, error_message in responses_events
        if "PoolTimeout" in (error_message or "")
    )
    status_499_count = sum(1 for _, _, status_code, _ in responses_events if status_code == 499)
    status_5xx_count = sum(1 for _, _, status_code, _ in responses_events if status_code >= 500)
    return {
        "window_seconds": window_seconds,
        "responses_499": status_499_count,
        "responses_5xx": status_5xx_count,
        "responses_pooltimeout": pooltimeout_count,
    }


def _record_runtime_guard_sample(
    *,
    open_fds: int | None,
    tcp_close_wait_443: int,
    active_responses: int,
    responses_499: int,
) -> None:
    _RUNTIME_GUARD_SAMPLES.append(
        (
            time.monotonic(),
            open_fds,
            max(0, int(tcp_close_wait_443 or 0)),
            max(0, int(active_responses or 0)),
            max(0, int(responses_499 or 0)),
        )
    )


def _runtime_guard_window_metrics(*, window_seconds: float = 900.0) -> dict[str, Any]:
    now = time.monotonic()
    cutoff = now - max(1.0, float(window_seconds))
    while _RUNTIME_GUARD_SAMPLES and _RUNTIME_GUARD_SAMPLES[0][0] < cutoff:
        _RUNTIME_GUARD_SAMPLES.popleft()

    samples = list(_RUNTIME_GUARD_SAMPLES)
    open_fd_values = [sample[1] for sample in samples if sample[1] is not None]
    close_wait_values = [sample[2] for sample in samples]
    active_values = [sample[3] for sample in samples]
    fd_delta = None
    fd_slope_per_min = None
    fd_elapsed_seconds = None
    if len(open_fd_values) >= 2 and len(samples) >= 2:
        first_fd_sample = next((sample for sample in samples if sample[1] is not None), None)
        last_fd_sample = next((sample for sample in reversed(samples) if sample[1] is not None), None)
        first_fd = first_fd_sample[1] if first_fd_sample is not None else None
        last_fd = last_fd_sample[1] if last_fd_sample is not None else None
        if first_fd is not None and last_fd is not None:
            fd_delta = int(last_fd) - int(first_fd)
            fd_elapsed_seconds = max(1.0, float(last_fd_sample[0] - first_fd_sample[0]))
            fd_slope_per_min = fd_delta / (fd_elapsed_seconds / 60.0)

    recent_499 = samples[-1][4] if samples else 0
    last_open_fds = open_fd_values[-1] if open_fd_values else None
    last_active = active_values[-1] if active_values else 0
    return {
        "window_seconds": window_seconds,
        "sample_count": len(samples),
        "open_fds_first": open_fd_values[0] if open_fd_values else None,
        "open_fds_last": last_open_fds,
        "open_fds_delta": fd_delta,
        "open_fds_slope_per_min": fd_slope_per_min,
        "open_fds_elapsed_seconds": fd_elapsed_seconds,
        "tcp_close_wait_443_max": max(close_wait_values) if close_wait_values else 0,
        "active_responses_last": last_active,
        "responses_499_last": recent_499,
        "open_fds_rising_with_499": bool(
            fd_delta is not None
            and fd_slope_per_min is not None
            and fd_elapsed_seconds is not None
            and len(open_fd_values) >= 4
            and fd_elapsed_seconds >= 900.0
            and fd_delta > 0
            and fd_slope_per_min > 1.0
            and recent_499 > 0
        ),
        "active_low_open_fds_high": bool(
            last_open_fds is not None
            and last_active <= 5
            and last_open_fds >= 512
        ),
    }


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

    status_code = _coerce_int(kwargs.get("status_code"), 0) or 0
    _record_gateway_guard_event(
        endpoint=getattr(request_log_ref, "endpoint", None),
        status_code=status_code,
        error_message=_normalize_optional_text(kwargs.get("error_message")),
    )
    should_log_timing = bool(status_code >= 400)
    if not should_log_timing:
        sample_rate = _request_timing_log_sample_rate()
        should_log_timing = sample_rate >= 1.0 or (sample_rate > 0.0 and random.random() < sample_rate)
    if resolved_spans and should_log_timing:
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
DEFAULT_UPSTREAM_HTTP_SHARD_COUNT = 8
DEFAULT_UPSTREAM_HTTP_MAX_CONNECTIONS = 2048
DEFAULT_UPSTREAM_HTTP_MAX_KEEPALIVE_CONNECTIONS = 512
DEFAULT_UPSTREAM_HTTP_KEEPALIVE_EXPIRY_SECONDS = 5.0
DEFAULT_UPSTREAM_HTTP_POOL_SWEEP_INTERVAL_SECONDS = 10.0
DEFAULT_PROXY_MAX_ACTIVE_RESPONSES = 1024
DEFAULT_PROXY_QUEUE_MAX_SIZE = 4096
DEFAULT_REQUEST_LOG_WRITE_CONCURRENCY = 4
DEFAULT_REQUEST_LOG_WRITE_BATCH_SIZE = 250
DEFAULT_REQUEST_LOG_WRITE_QUEUE_MAX_SIZE = 20000


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
    ordered_tokens_by_key: dict[tuple[str, tuple[int, ...], bool, tuple[str, ...]], list[Any]] = field(
        default_factory=dict
    )
    token_by_id: dict[int, Any] = field(default_factory=dict)
    ready_token_ids_by_key: dict[tuple[str, tuple[int, ...], bool, tuple[str, ...]], deque[int]] = field(
        default_factory=dict
    )


def _token_snapshot_order_key(
    settings: TokenSelectionSettings,
) -> tuple[str, tuple[int, ...], bool, tuple[str, ...]]:
    plan_order_enabled = bool(settings.plan_order_enabled)
    return (
        settings.strategy,
        tuple(int(token_id) for token_id in settings.token_order),
        plan_order_enabled,
        tuple(settings.plan_order) if plan_order_enabled else (),
    )


def _token_snapshot_order_cache(snapshot: Any) -> dict[tuple[str, tuple[int, ...], bool, tuple[str, ...]], list[Any]]:
    cache = getattr(snapshot, "ordered_tokens_by_key", None)
    if isinstance(cache, dict):
        return cache
    cache = {}
    with suppress(Exception):
        setattr(snapshot, "ordered_tokens_by_key", cache)
    return cache


def _token_snapshot_token_by_id(snapshot: _TokenPoolSnapshot) -> dict[int, Any]:
    if snapshot.token_by_id:
        return snapshot.token_by_id
    snapshot.token_by_id = {
        int(getattr(token_row, "id", 0) or 0): token_row
        for token_row in snapshot.tokens
        if int(getattr(token_row, "id", 0) or 0) > 0
    }
    return snapshot.token_by_id


def _token_snapshot_ready_queue(
    snapshot: _TokenPoolSnapshot,
    settings: TokenSelectionSettings,
) -> deque[int]:
    cache_key = _token_snapshot_order_key(settings)
    queue = snapshot.ready_token_ids_by_key.get(cache_key)
    if queue is not None:
        return queue
    queue = deque(
        int(getattr(token_row, "id", 0) or 0)
        for token_row in _token_snapshot_ordered_tokens(snapshot, settings)
        if int(getattr(token_row, "id", 0) or 0) > 0
    )
    snapshot.ready_token_ids_by_key[cache_key] = queue
    return queue


def _token_plan_sort_value(token_row: Any, settings: TokenSelectionSettings) -> int:
    if not settings.plan_order_enabled:
        return 0
    plan_map = {plan_type: index for index, plan_type in enumerate(settings.plan_order)}
    return _token_plan_sort_value_from_map(token_row, plan_map)


def _token_plan_sort_value_from_map(token_row: Any, plan_map: dict[str, int]) -> int:
    if not plan_map:
        return 0
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
    order_map = {int(token_id): index for index, token_id in enumerate(settings.token_order)}
    return _token_order_sort_value_from_map(token_row, order_map)


def _token_order_sort_value_from_map(token_row: Any, order_map: dict[int, int]) -> int:
    if not order_map:
        return int(getattr(token_row, "id", 0) or 0)
    return order_map.get(int(getattr(token_row, "id", 0) or 0), len(order_map))


def _datetime_sort_value(value: Any) -> tuple[int, float]:
    if value is None:
        return (0, 0.0)
    if isinstance(value, datetime):
        return (1, value.timestamp())
    return (1, 0.0)


def _token_snapshot_ordered_tokens(snapshot: _TokenPoolSnapshot, settings: TokenSelectionSettings) -> list[Any]:
    cache_key = _token_snapshot_order_key(settings)
    order_cache = _token_snapshot_order_cache(snapshot)
    cached = order_cache.get(cache_key)
    if cached is not None:
        return cached

    tokens = list(snapshot.tokens)
    plan_map = (
        {plan_type: index for index, plan_type in enumerate(settings.plan_order)}
        if settings.plan_order_enabled
        else {}
    )
    if settings.strategy == TOKEN_SELECTION_STRATEGY_FILL_FIRST:
        order_map = {int(token_id): index for index, token_id in enumerate(settings.token_order)}
        ordered = sorted(
            tokens,
            key=lambda token_row: (
                _token_plan_sort_value_from_map(token_row, plan_map),
                _token_order_sort_value_from_map(token_row, order_map),
                int(getattr(token_row, "id", 0) or 0),
            ),
        )
        order_cache[cache_key] = ordered
        return ordered

    ordered = sorted(
        tokens,
        key=lambda token_row: (
            _token_plan_sort_value_from_map(token_row, plan_map),
            _datetime_sort_value(getattr(token_row, "last_used_at", None)),
            _datetime_sort_value(getattr(token_row, "updated_at", None)),
            int(getattr(token_row, "id", 0) or 0),
        ),
    )
    order_cache[cache_key] = ordered
    return ordered


def _token_snapshot_mark_token_claimed(
    snapshot: _TokenPoolSnapshot,
    settings: TokenSelectionSettings,
    token_row: Any,
) -> None:
    if settings.strategy == TOKEN_SELECTION_STRATEGY_FILL_FIRST:
        return
    cache_key = _token_snapshot_order_key(settings)
    order_cache = _token_snapshot_order_cache(snapshot)
    ordered = order_cache.get(cache_key)
    if not ordered:
        return

    token_id = int(getattr(token_row, "id", 0) or 0)
    if token_id <= 0:
        return

    selected_index: int | None = None
    for index, candidate in enumerate(ordered):
        if int(getattr(candidate, "id", 0) or 0) == token_id:
            selected_index = index
            break
    if selected_index is None:
        return

    selected = ordered.pop(selected_index)
    plan_map = (
        {plan_type: index for index, plan_type in enumerate(settings.plan_order)}
        if settings.plan_order_enabled
        else {}
    )
    selected_plan_rank = _token_plan_sort_value_from_map(selected, plan_map)
    insert_at = len(ordered)
    for index, candidate in enumerate(ordered):
        if _token_plan_sort_value_from_map(candidate, plan_map) > selected_plan_rank:
            insert_at = index
            break
    ordered.insert(insert_at, selected)


def _claim_next_lru_token_from_snapshot(
    snapshot: _TokenPoolSnapshot,
    selection_settings: TokenSelectionSettings,
    *,
    effective_excluded_ids: set[int],
    scoped_cooldown_scope: str | None,
    now: datetime,
    include_plan_types: Iterable[str] | None,
    exclude_plan_types: Iterable[str] | None,
) -> Any | None:
    queue = _token_snapshot_ready_queue(snapshot, selection_settings)
    if not queue:
        return None
    token_by_id = _token_snapshot_token_by_id(snapshot)
    active_stream_cap = normalize_token_active_stream_cap(selection_settings.active_stream_cap)
    for _ in range(len(queue)):
        token_id = queue.popleft()
        token_row = token_by_id.get(token_id)
        if token_row is None:
            continue
        queue.append(token_id)
        if token_id in effective_excluded_ids:
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
        return token_row
    return None


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
        token_by_id={
            int(getattr(token_row, "id", 0) or 0): token_row
            for token_row in token_rows
            if int(getattr(token_row, "id", 0) or 0) > 0
        },
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


def _token_pool_snapshot_max_age_seconds() -> float:
    return _float_env("TOKEN_POOL_SNAPSHOT_MAX_AGE_SECONDS", 10.0, minimum=0.1)


def _token_pool_snapshot_age_seconds(snapshot: _TokenPoolSnapshot) -> float:
    return max(0.0, time.monotonic() - snapshot.loaded_at)


def _token_pool_snapshot_is_fresh(snapshot: _TokenPoolSnapshot) -> bool:
    return _token_pool_snapshot_age_seconds(snapshot) <= _token_pool_snapshot_max_age_seconds()


def _schedule_token_pool_snapshot_refresh(app: FastAPI, *, reason: str) -> None:
    existing = getattr(app.state, "token_pool_snapshot_refresh_task", None)
    if isinstance(existing, asyncio.Task) and not existing.done():
        return
    try:
        task = asyncio.create_task(
            _refresh_token_pool_snapshot_best_effort(app, reason=reason),
            name="oaix-token-pool-snapshot-refresh",
        )
    except RuntimeError:
        return
    app.state.token_pool_snapshot_refresh_task = task


def _fresh_token_pool_snapshot(app: FastAPI, *, timing: _RequestTimingRecorder | None = None) -> _TokenPoolSnapshot | None:
    snapshot = _current_token_pool_snapshot(app)
    if snapshot is None:
        return None
    age_seconds = _token_pool_snapshot_age_seconds(snapshot)
    if timing is not None:
        timing.set_ms("token_pool_snapshot_age_ms", age_seconds * 1000)
    if age_seconds <= _token_pool_snapshot_max_age_seconds():
        return snapshot
    if timing is not None:
        timing.add_ms("token_pool_snapshot_stale_count", 1)
    _schedule_token_pool_snapshot_refresh(app, reason="stale_request_snapshot")
    return None


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

    if selection_settings.strategy == TOKEN_SELECTION_STRATEGY_FILL_FIRST:
        candidates: Iterable[Any] = _token_snapshot_ordered_tokens(snapshot, selection_settings)
    else:
        claimed = _claim_next_lru_token_from_snapshot(
            snapshot,
            selection_settings,
            effective_excluded_ids=effective_excluded_ids,
            scoped_cooldown_scope=scoped_cooldown_scope,
            now=now,
            include_plan_types=include_plan_types,
            exclude_plan_types=exclude_plan_types,
        )
        candidates = (claimed,) if claimed is not None else ()

    for token_row in candidates:
        token_id = int(getattr(token_row, "id", 0) or 0)
        if token_id <= 0 or token_id in effective_excluded_ids:
            continue
        if selection_settings.strategy == TOKEN_SELECTION_STRATEGY_FILL_FIRST:
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
        _token_snapshot_mark_token_claimed(snapshot, selection_settings, token_row)
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


def _prompt_cache_lane_store(app: FastAPI) -> OrderedDict[str, "_PromptCacheLaneState"]:
    store = getattr(app.state, "prompt_cache_affinity_lanes", None)
    if not isinstance(store, OrderedDict):
        store = OrderedDict()
        app.state.prompt_cache_affinity_lanes = store
    return store


def _prompt_cache_response_store(app: FastAPI) -> OrderedDict[str, "_PromptCacheResponseBinding"]:
    store = getattr(app.state, "prompt_cache_response_bindings", None)
    if not isinstance(store, OrderedDict):
        store = OrderedDict()
        app.state.prompt_cache_response_bindings = store
    return store


def _prompt_cache_prune_store(app: FastAPI) -> None:
    now = time.monotonic()
    last_pruned_at = float(getattr(app.state, "prompt_cache_last_pruned_at", 0.0) or 0.0)
    if now - last_pruned_at < _prompt_cache_prune_interval_seconds():
        return
    app.state.prompt_cache_last_pruned_at = now
    lane_store = _prompt_cache_lane_store(app)
    lane_ttl = _prompt_cache_lane_ttl_seconds()
    for key in list(lane_store.keys()):
        state = lane_store[key]
        if now - state.last_seen_at > lane_ttl:
            lane_store.pop(key, None)
    while len(lane_store) > _prompt_cache_max_entries():
        lane_store.popitem(last=False)

    response_store = _prompt_cache_response_store(app)
    for response_id in list(response_store.keys()):
        binding = response_store[response_id]
        if binding.expires_at <= now:
            response_store.pop(response_id, None)
    while len(response_store) > _prompt_cache_response_max_entries():
        response_store.popitem(last=False)


def _prompt_cache_bind_lane(
    app: FastAPI,
    context: "_PromptCacheRequestContext | None",
    token_id: int,
    *,
    prefer_primary: bool = False,
) -> int | None:
    if context is None or not context.affinity_key or token_id <= 0 or not _prompt_cache_affinity_enabled():
        return None
    _prompt_cache_prune_store(app)
    store = _prompt_cache_lane_store(app)
    state = store.get(context.affinity_key)
    if state is None:
        state = _PromptCacheLaneState(primary_token_id=token_id, lanes=[token_id])
        store[context.affinity_key] = state
        return 0

    state.last_seen_at = time.monotonic()
    store.move_to_end(context.affinity_key)
    if state.primary_token_id is None or (prefer_primary and _prompt_cache_rebind_primary_enabled()):
        state.primary_token_id = token_id
    if token_id not in state.lanes:
        if len(state.lanes) < _prompt_cache_max_lanes_per_key():
            state.lanes.append(token_id)
        else:
            return None
    try:
        return state.lanes.index(token_id)
    except ValueError:
        return None


def _prompt_cache_remove_token(app: FastAPI, token_id: int) -> None:
    try:
        resolved_token_id = int(token_id)
    except (TypeError, ValueError):
        return
    if resolved_token_id <= 0:
        return
    for key, state in list(_prompt_cache_lane_store(app).items()):
        if resolved_token_id not in state.lanes and state.primary_token_id != resolved_token_id:
            continue
        state.lanes = [lane_id for lane_id in state.lanes if lane_id != resolved_token_id]
        if state.primary_token_id == resolved_token_id:
            state.primary_token_id = state.lanes[0] if state.lanes else None
        state.last_seen_at = time.monotonic()
        if not state.lanes:
            _prompt_cache_lane_store(app).pop(key, None)


def _clear_token_row_access_token_for_retry(token_row: Any) -> None:
    for attribute_name in ("access_token", "expires_at"):
        with suppress(Exception):
            setattr(token_row, attribute_name, None)


def _exclude_auth_failed_token_for_request(
    excluded_token_ids: set[int],
    token_row: Any,
    *,
    app: FastAPI | None = None,
) -> None:
    try:
        token_id = int(token_row.id)
    except (AttributeError, TypeError, ValueError):
        return
    excluded_token_ids.add(token_id)
    _clear_token_row_access_token_for_retry(token_row)
    if app is not None:
        _prompt_cache_remove_token(app, token_id)


def _prompt_cache_bind_response(
    app: FastAPI,
    response_id: str | None,
    token_id: int,
) -> None:
    normalized = _normalize_optional_text(response_id)
    if normalized is None:
        return
    try:
        resolved_token_id = int(token_id)
    except (TypeError, ValueError):
        return
    if resolved_token_id <= 0:
        return
    _prompt_cache_prune_store(app)
    store = _prompt_cache_response_store(app)
    store[normalized] = _PromptCacheResponseBinding(
        token_id=resolved_token_id,
        expires_at=time.monotonic() + _prompt_cache_response_ttl_seconds(),
    )
    store.move_to_end(normalized)


def _prompt_cache_response_owner(app: FastAPI, response_id: str | None) -> int | None:
    normalized = _normalize_optional_text(response_id)
    if normalized is None:
        return None
    _prompt_cache_prune_store(app)
    binding = _prompt_cache_response_store(app).get(normalized)
    if binding is None:
        return None
    if binding.expires_at <= time.monotonic():
        _prompt_cache_response_store(app).pop(normalized, None)
        return None
    return binding.token_id


def _prompt_cache_snapshot_token(snapshot: _TokenPoolSnapshot | None, token_id: int) -> Any | None:
    if snapshot is None:
        return None
    for token_row in snapshot.tokens:
        if int(getattr(token_row, "id", 0) or 0) == int(token_id):
            return token_row
    return None


def _prompt_cache_token_available(
    token_row: Any,
    snapshot: _TokenPoolSnapshot,
    *,
    scoped_cooldown_scope: str | None,
    include_plan_types: Iterable[str] | None,
    exclude_plan_types: Iterable[str] | None,
) -> bool:
    if not _token_row_matches_plan_filters(
        token_row,
        include_plan_types=include_plan_types,
        exclude_plan_types=exclude_plan_types,
    ):
        return False
    return _token_snapshot_row_available(
        token_row,
        snapshot,
        scoped_cooldown_scope=scoped_cooldown_scope,
        now=utcnow(),
    )


async def _prompt_cache_wait_for_token_slot(token_id: int, active_stream_cap: int, wait_ms: int) -> bool:
    if _token_active_stream_count(token_id) < active_stream_cap:
        return True
    if wait_ms <= 0:
        return False
    deadline = time.monotonic() + (wait_ms / 1000.0)
    while time.monotonic() < deadline:
        await asyncio.sleep(min(0.05, max(0.001, deadline - time.monotonic())))
        if _token_active_stream_count(token_id) < active_stream_cap:
            return True
    return _token_active_stream_count(token_id) < active_stream_cap


def _prompt_cache_rendezvous_score(affinity_key: str, token_id: int) -> int:
    digest = hashlib.blake2b(f"{affinity_key}:{token_id}".encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, "big", signed=False)


def _prompt_cache_recent_ttft_p90(token_id: int) -> int:
    recent = list(_TOKEN_RECENT_TTFT_MS.get(token_id) or ())
    return _percentile(recent, 90) if recent else 0


def _prompt_cache_candidate_sort_key(
    *,
    affinity_key: str,
    token_row: Any,
) -> tuple[int, int, int]:
    token_id = int(getattr(token_row, "id", 0) or 0)
    return (
        _token_active_stream_count(token_id),
        _prompt_cache_recent_ttft_p90(token_id),
        -_prompt_cache_rendezvous_score(affinity_key, token_id),
    )


async def _prompt_cache_claim_specific_token(
    snapshot: _TokenPoolSnapshot,
    selection_settings: TokenSelectionSettings,
    *,
    token_id: int,
    exclude_token_ids: set[int],
    scoped_cooldown_scope: str | None,
    timing: _RequestTimingRecorder | None,
    wait_ms: int,
    include_plan_types: Iterable[str] | None = None,
    exclude_plan_types: Iterable[str] | None = None,
) -> Any | None:
    if token_id <= 0 or token_id in exclude_token_ids:
        return None
    token_row = _prompt_cache_snapshot_token(snapshot, token_id)
    if token_row is None:
        return None
    if not _prompt_cache_token_available(
        token_row,
        snapshot,
        scoped_cooldown_scope=scoped_cooldown_scope,
        include_plan_types=include_plan_types,
        exclude_plan_types=exclude_plan_types,
    ):
        return None
    active_stream_cap = normalize_token_active_stream_cap(selection_settings.active_stream_cap)
    if not await _prompt_cache_wait_for_token_slot(token_id, active_stream_cap, wait_ms):
        return None
    if not _prompt_cache_token_available(
        token_row,
        snapshot,
        scoped_cooldown_scope=scoped_cooldown_scope,
        include_plan_types=include_plan_types,
        exclude_plan_types=exclude_plan_types,
    ):
        return None
    if _token_active_stream_count(token_id) >= active_stream_cap:
        return None
    now = utcnow()
    token_row.last_used_at = now
    token_row.updated_at = now
    _token_snapshot_mark_token_claimed(snapshot, selection_settings, token_row)
    _record_selected_token_observation_start(token_id, timing)
    return token_row


def _prompt_cache_lane_candidates(
    snapshot: _TokenPoolSnapshot,
    selection_settings: TokenSelectionSettings,
    *,
    affinity_key: str,
    token_ids: Iterable[int],
    exclude_token_ids: set[int],
    scoped_cooldown_scope: str | None,
    include_plan_types: Iterable[str] | None = None,
    exclude_plan_types: Iterable[str] | None = None,
) -> list[Any]:
    active_stream_cap = normalize_token_active_stream_cap(selection_settings.active_stream_cap)
    candidates: list[Any] = []
    seen: set[int] = set()
    for raw_token_id in token_ids:
        try:
            token_id = int(raw_token_id)
        except (TypeError, ValueError):
            continue
        if token_id <= 0 or token_id in seen or token_id in exclude_token_ids:
            continue
        seen.add(token_id)
        if _token_active_stream_count(token_id) >= active_stream_cap:
            continue
        token_row = _prompt_cache_snapshot_token(snapshot, token_id)
        if token_row is None:
            continue
        if not _prompt_cache_token_available(
            token_row,
            snapshot,
            scoped_cooldown_scope=scoped_cooldown_scope,
            include_plan_types=include_plan_types,
            exclude_plan_types=exclude_plan_types,
        ):
            continue
        candidates.append(token_row)
    return sorted(
        candidates,
        key=lambda token_row: _prompt_cache_candidate_sort_key(
            affinity_key=affinity_key,
            token_row=token_row,
        ),
    )


def _prompt_cache_new_lane_candidates(
    snapshot: _TokenPoolSnapshot,
    selection_settings: TokenSelectionSettings,
    *,
    affinity_key: str,
    existing_lanes: Iterable[int],
    exclude_token_ids: set[int],
    scoped_cooldown_scope: str | None,
    include_plan_types: Iterable[str] | None = None,
    exclude_plan_types: Iterable[str] | None = None,
) -> list[Any]:
    existing = {int(token_id) for token_id in existing_lanes if int(token_id or 0) > 0}
    active_stream_cap = normalize_token_active_stream_cap(selection_settings.active_stream_cap)
    candidates: list[Any] = []
    for token_row in snapshot.tokens:
        token_id = int(getattr(token_row, "id", 0) or 0)
        if token_id <= 0 or token_id in existing or token_id in exclude_token_ids:
            continue
        if _token_active_stream_count(token_id) >= active_stream_cap:
            continue
        if not _prompt_cache_token_available(
            token_row,
            snapshot,
            scoped_cooldown_scope=scoped_cooldown_scope,
            include_plan_types=include_plan_types,
            exclude_plan_types=exclude_plan_types,
        ):
            continue
        candidates.append(token_row)
    return sorted(
        candidates,
        key=lambda token_row: _prompt_cache_candidate_sort_key(
            affinity_key=affinity_key,
            token_row=token_row,
        ),
    )


async def _claim_prompt_cache_affinity_token(
    app: FastAPI,
    selection_settings: TokenSelectionSettings,
    *,
    prompt_cache_context: "_PromptCacheRequestContext | None",
    exclude_token_ids: set[int],
    scoped_cooldown_scope: str | None,
    timing: _RequestTimingRecorder | None,
    token_pool_snapshot: _TokenPoolSnapshot | None,
    require_non_free_token: bool,
) -> "_PromptCacheClaim | None":
    if prompt_cache_context is None or not _prompt_cache_affinity_enabled():
        return None
    snapshot = token_pool_snapshot or _current_token_pool_snapshot(app)
    if snapshot is None:
        return None

    include_plan_types = KNOWN_NON_FREE_TOKEN_PLAN_TYPES if require_non_free_token else None
    exclude_plan_types = None
    if timing is not None:
        if prompt_cache_context.prompt_cache_key_hash:
            timing.set_tag("prompt_cache_key_hash", prompt_cache_context.prompt_cache_key_hash)
        timing.set_tag("prompt_cache_source", prompt_cache_context.source)

    previous_owner_id = _prompt_cache_response_owner(app, prompt_cache_context.previous_response_id)
    if previous_owner_id is not None:
        token_row = await _prompt_cache_claim_specific_token(
            snapshot,
            selection_settings,
            token_id=previous_owner_id,
            exclude_token_ids=exclude_token_ids,
            scoped_cooldown_scope=scoped_cooldown_scope,
            timing=timing,
            wait_ms=_prompt_cache_previous_owner_wait_ms(),
            include_plan_types=include_plan_types,
            exclude_plan_types=exclude_plan_types,
        )
        if token_row is not None:
            lane_index = _prompt_cache_bind_lane(app, prompt_cache_context, previous_owner_id, prefer_primary=True)
            return _PromptCacheClaim(token_row=token_row, result="previous_owner_hit", lane_index=lane_index)
        if _prompt_cache_previous_strict_enabled():
            if timing is not None:
                timing.set_tag("cache_affinity_result", "previous_owner_busy")
            return None

    affinity_key = prompt_cache_context.affinity_key
    if not affinity_key:
        return None

    _prompt_cache_prune_store(app)
    store = _prompt_cache_lane_store(app)
    state = store.get(affinity_key)
    if state is None:
        state = _PromptCacheLaneState()
        store[affinity_key] = state
    state.last_seen_at = time.monotonic()
    store.move_to_end(affinity_key)

    primary_id = state.primary_token_id or (state.lanes[0] if state.lanes else None)
    if primary_id is not None:
        token_row = await _prompt_cache_claim_specific_token(
            snapshot,
            selection_settings,
            token_id=primary_id,
            exclude_token_ids=exclude_token_ids,
            scoped_cooldown_scope=scoped_cooldown_scope,
            timing=timing,
            wait_ms=_prompt_cache_primary_wait_ms(),
            include_plan_types=include_plan_types,
            exclude_plan_types=exclude_plan_types,
        )
        if token_row is not None:
            lane_index = _prompt_cache_bind_lane(app, prompt_cache_context, int(primary_id), prefer_primary=True)
            return _PromptCacheClaim(token_row=token_row, result="primary_hit", lane_index=lane_index)

    secondary_ids = [token_id for token_id in state.lanes if token_id != primary_id]
    for token_row in _prompt_cache_lane_candidates(
        snapshot,
        selection_settings,
        affinity_key=affinity_key,
        token_ids=secondary_ids,
        exclude_token_ids=exclude_token_ids,
        scoped_cooldown_scope=scoped_cooldown_scope,
        include_plan_types=include_plan_types,
        exclude_plan_types=exclude_plan_types,
    ):
        token_id = int(getattr(token_row, "id", 0) or 0)
        claimed = await _prompt_cache_claim_specific_token(
            snapshot,
            selection_settings,
            token_id=token_id,
            exclude_token_ids=exclude_token_ids,
            scoped_cooldown_scope=scoped_cooldown_scope,
            timing=timing,
            wait_ms=0,
            include_plan_types=include_plan_types,
            exclude_plan_types=exclude_plan_types,
        )
        if claimed is not None:
            lane_index = _prompt_cache_bind_lane(app, prompt_cache_context, token_id)
            return _PromptCacheClaim(token_row=claimed, result="lane_hit", lane_index=lane_index)

    if len(state.lanes) < _prompt_cache_max_lanes_per_key():
        for token_row in _prompt_cache_new_lane_candidates(
            snapshot,
            selection_settings,
            affinity_key=affinity_key,
            existing_lanes=state.lanes,
            exclude_token_ids=exclude_token_ids,
            scoped_cooldown_scope=scoped_cooldown_scope,
            include_plan_types=include_plan_types,
            exclude_plan_types=exclude_plan_types,
        ):
            token_id = int(getattr(token_row, "id", 0) or 0)
            claimed = await _prompt_cache_claim_specific_token(
                snapshot,
                selection_settings,
                token_id=token_id,
                exclude_token_ids=exclude_token_ids,
                scoped_cooldown_scope=scoped_cooldown_scope,
                timing=timing,
                wait_ms=0,
                include_plan_types=include_plan_types,
                exclude_plan_types=exclude_plan_types,
            )
            if claimed is not None:
                lane_index = _prompt_cache_bind_lane(app, prompt_cache_context, token_id)
                return _PromptCacheClaim(token_row=claimed, result="lane_created", lane_index=lane_index)

    if _prompt_cache_lane_wait_ms() > 0:
        await asyncio.sleep(_prompt_cache_lane_wait_ms() / 1000.0)
        for token_row in _prompt_cache_lane_candidates(
            snapshot,
            selection_settings,
            affinity_key=affinity_key,
            token_ids=state.lanes,
            exclude_token_ids=exclude_token_ids,
            scoped_cooldown_scope=scoped_cooldown_scope,
            include_plan_types=include_plan_types,
            exclude_plan_types=exclude_plan_types,
        ):
            token_id = int(getattr(token_row, "id", 0) or 0)
            claimed = await _prompt_cache_claim_specific_token(
                snapshot,
                selection_settings,
                token_id=token_id,
                exclude_token_ids=exclude_token_ids,
                scoped_cooldown_scope=scoped_cooldown_scope,
                timing=timing,
                wait_ms=0,
                include_plan_types=include_plan_types,
                exclude_plan_types=exclude_plan_types,
            )
            if claimed is not None:
                lane_index = _prompt_cache_bind_lane(app, prompt_cache_context, token_id)
                return _PromptCacheClaim(token_row=claimed, result="lane_hit_after_wait", lane_index=lane_index)

    if timing is not None:
        timing.set_tag("cache_affinity_result", "global_fallback")
    return None


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
    return _int_env("REQUEST_LOG_WRITE_CONCURRENCY", DEFAULT_REQUEST_LOG_WRITE_CONCURRENCY, minimum=1)


def _request_log_write_batch_size() -> int:
    return _int_env("REQUEST_LOG_WRITE_BATCH_SIZE", DEFAULT_REQUEST_LOG_WRITE_BATCH_SIZE, minimum=1)


def _request_log_write_queue_max_size() -> int:
    return _int_env("REQUEST_LOG_WRITE_QUEUE_MAX_SIZE", DEFAULT_REQUEST_LOG_WRITE_QUEUE_MAX_SIZE, minimum=1)


def _request_log_write_timeout_seconds() -> float:
    return _float_env("REQUEST_LOG_WRITE_TIMEOUT_SECONDS", 5.0, minimum=0.1)


def _request_log_start_write_enabled() -> bool:
    return _bool_env("REQUEST_LOG_WRITE_START_ENABLED", False)


def _request_timing_log_sample_rate() -> float:
    return min(1.0, _float_env("REQUEST_TIMING_LOG_SAMPLE_RATE", 0.0, minimum=0.0))


def _request_log_write_queue() -> asyncio.Queue[_RequestLogWriteJob]:
    global _REQUEST_LOG_WRITE_QUEUE, _REQUEST_LOG_WRITE_QUEUE_LOOP, _REQUEST_LOG_WRITE_DRAINERS
    loop = asyncio.get_running_loop()
    if _REQUEST_LOG_WRITE_QUEUE is None or _REQUEST_LOG_WRITE_QUEUE_LOOP is not loop:
        _REQUEST_LOG_WRITE_QUEUE = asyncio.Queue(maxsize=_request_log_write_queue_max_size())
        _REQUEST_LOG_WRITE_QUEUE_LOOP = loop
        _REQUEST_LOG_WRITE_DRAINERS = set()
    return _REQUEST_LOG_WRITE_QUEUE


def _request_log_write_queue_stats() -> dict[str, Any]:
    queue = _REQUEST_LOG_WRITE_QUEUE
    return {
        "queue_depth": queue.qsize() if queue is not None else 0,
        "queue_max_size": queue.maxsize if queue is not None else _request_log_write_queue_max_size(),
        "dropped": _REQUEST_LOG_WRITE_DROPPED,
        "active_workers": len([task for task in _REQUEST_LOG_WRITE_DRAINERS if not task.done()]),
        "worker_limit": _request_log_write_concurrency(),
        "batch_size": _request_log_write_batch_size(),
        "start_logs_enabled": _request_log_start_write_enabled(),
    }


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
            start_keys = {
                "request_payload_hash",
                "upstream_payload_hash",
                "prompt_template_hash",
                "prompt_dynamic_hash",
                "prompt_cache_source",
                "prompt_cache_key_hash",
                "prompt_cache_retention_requested",
                "prompt_cache_retention_sent",
                "session_id_hash",
                "session_id_source",
                "previous_response_id_hash",
                "upstream_response_id",
                "cache_hit_ratio",
                "cache_affinity_result",
                "cache_affinity_lane_index",
                "prompt_cache_trace",
            }
            item = await create_request_log(
                request_id=owner.request_id,
                endpoint=owner.endpoint,
                model=owner.model,
                is_stream=owner.is_stream,
                started_at=owner.started_at,
                client_ip=owner.client_ip,
                user_agent=owner.user_agent,
                **{key: payload.get(key) for key in start_keys},
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
            "cached_input_tokens",
            "output_tokens",
            "total_tokens",
            "estimated_cost_usd",
            "request_payload_hash",
            "upstream_payload_hash",
            "prompt_template_hash",
            "prompt_dynamic_hash",
            "prompt_cache_source",
            "prompt_cache_key_hash",
            "prompt_cache_retention_requested",
            "prompt_cache_retention_sent",
            "session_id_hash",
            "session_id_source",
            "previous_response_id_hash",
            "upstream_response_id",
            "cache_hit_ratio",
            "cache_affinity_result",
            "cache_affinity_lane_index",
            "prompt_cache_trace",
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
            await asyncio.wait_for(
                _write_request_log_batch(jobs),
                timeout=_request_log_write_timeout_seconds(),
            )
        except asyncio.TimeoutError:
            logger.warning("Request log write batch timed out; dropping %s queued log job(s)", len(jobs))
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


_REQUEST_LOG_WRITE_DROPPED = 0


def _request_log_job_is_final(job: _RequestLogWriteJob) -> bool:
    return job.payload.get("status_code") is not None or bool(job.success_hooks)


def _drop_queued_request_log_job(queue: asyncio.Queue[_RequestLogWriteJob], *, prefer_start_only: bool) -> bool:
    if queue.empty():
        return False
    held_jobs: list[_RequestLogWriteJob] = []
    dropped = False
    try:
        while True:
            job = queue.get_nowait()
            if prefer_start_only and not dropped and _request_log_job_is_final(job):
                queue.task_done()
                held_jobs.append(job)
                continue
            queue.task_done()
            dropped = True
            break
    except asyncio.QueueEmpty:
        pass
    finally:
        for held_job in held_jobs:
            try:
                queue.put_nowait(held_job)
            except asyncio.QueueFull:
                pass
        if not dropped and not prefer_start_only:
            return False
    return dropped


def _submit_request_log_write(job: _RequestLogWriteJob) -> None:
    global _REQUEST_LOG_WRITE_DROPPED
    queue = _request_log_write_queue()
    if queue.full():
        if not _request_log_job_is_final(job):
            _REQUEST_LOG_WRITE_DROPPED += 1
            if _REQUEST_LOG_WRITE_DROPPED == 1 or _REQUEST_LOG_WRITE_DROPPED % 100 == 0:
                logger.warning(
                    "Request log write queue full; dropped %s start log job(s)",
                    _REQUEST_LOG_WRITE_DROPPED,
                )
            return
        if not _drop_queued_request_log_job(queue, prefer_start_only=True):
            _drop_queued_request_log_job(queue, prefer_start_only=False)

    try:
        queue.put_nowait(job)
    except asyncio.QueueFull:
        _REQUEST_LOG_WRITE_DROPPED += 1
        if _REQUEST_LOG_WRITE_DROPPED == 1 or _REQUEST_LOG_WRITE_DROPPED % 100 == 0:
            logger.warning("Request log write queue full; dropped %s log job(s)", _REQUEST_LOG_WRITE_DROPPED)
        return
    _ensure_request_log_write_drainers()


async def _flush_request_log_write_queue() -> None:
    queue = _request_log_write_queue()
    _ensure_request_log_write_drainers()
    await queue.join()
    if _REQUEST_LOG_WRITE_DRAINERS:
        await asyncio.gather(*list(_REQUEST_LOG_WRITE_DRAINERS), return_exceptions=True)


async def _run_request_log_retention_once() -> None:
    retention_days = _request_log_retention_days()
    if retention_days <= 0:
        return
    cutoff = utcnow() - timedelta(days=retention_days)
    deleted_count = await delete_request_logs_older_than(cutoff)
    if deleted_count:
        logger.info("Deleted old request logs: retention_days=%s deleted=%s", retention_days, deleted_count)


async def _request_log_retention_worker() -> None:
    while True:
        try:
            await _run_request_log_retention_once()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Request log retention cleanup failed")
        await asyncio.sleep(_request_log_cleanup_interval_seconds())


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
    prompt_cache_trace: dict[str, Any] | None = None
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
        prompt_cache_trace: dict[str, Any] | None = None,
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
            prompt_cache_trace=prompt_cache_trace if isinstance(prompt_cache_trace, dict) else None,
        )
        if _request_log_start_write_enabled():
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
            "prompt_cache_trace": copy.deepcopy(self.prompt_cache_trace) if self.prompt_cache_trace is not None else None,
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


def _web_bundle_version_hash() -> str:
    digest = hashlib.sha256()
    for path in (WEB_DIR / "index.html", WEB_DIR / "styles.css", WEB_DIR / "app.js"):
        try:
            digest.update(path.read_bytes())
        except OSError:
            continue
    return digest.hexdigest()[:12]


def _web_bundle_version_time() -> str:
    mtimes: list[float] = []
    for path in (WEB_DIR / "index.html", WEB_DIR / "styles.css", WEB_DIR / "app.js"):
        try:
            mtimes.append(path.stat().st_mtime)
        except OSError:
            continue
    if not mtimes:
        return "dev"
    version_time = datetime.fromtimestamp(max(mtimes), tz=timezone.utc).astimezone(WEB_VERSION_TZ)
    return version_time.strftime("%Y-%m-%d %H:%M:%S UTC+8")


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
    on_close: Callable[[], Awaitable[None]] | None = None
    stream_capture: "_ProxyStreamCapture | None" = None
    response_id: str | None = None


@dataclass(frozen=True)
class _PromptCacheRequestContext:
    endpoint: str
    model: str | None
    compact: bool
    previous_response_id: str | None
    prompt_cache_key: str | None
    prompt_cache_key_hash: str | None
    affinity_key: str | None
    client_scope: str
    source: str
    raw_payload: dict[str, Any] | None = None


@dataclass
class _PromptCacheLaneState:
    primary_token_id: int | None = None
    lanes: list[int] = field(default_factory=list)
    created_at: float = field(default_factory=time.monotonic)
    last_seen_at: float = field(default_factory=time.monotonic)


@dataclass
class _PromptCacheResponseBinding:
    token_id: int
    expires_at: float


@dataclass(frozen=True)
class _PromptCacheClaim:
    token_row: Any
    result: str
    lane_index: int | None = None


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
        self._response_payload_cache: dict[str, Any] | None = None
        self._output_items_by_index: dict[int, dict[str, Any]] = {}
        self._output_items_fallback: list[dict[str, Any]] = []
        self._decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
        self._text_buffer = ""
        self._sse_scan_start = 0
        self._captured_bytes = 0
        self._capture_disabled = False

    @property
    def response_payload(self) -> dict[str, Any] | None:
        if self._response_payload_cache is not None:
            return self._response_payload_cache
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
        if not isinstance(response_payload, dict):
            return None
        if self.completed:
            self._response_payload_cache = response_payload
        return response_payload

    def finalize_usage_metrics(self) -> None:
        response_payload = self.response_payload
        if response_payload is not None:
            self._update_usage_metrics({"response": response_payload})

    def _invalidate_response_payload_cache(self) -> None:
        self._response_payload_cache = None

    def _update_usage_metrics(self, payload: Any) -> None:
        usage_metrics = extract_usage_metrics(payload, model_name=self.model_name)
        if usage_metrics is not None:
            self.usage_metrics = usage_metrics

    def feed(self, chunk: bytes) -> None:
        if not chunk or self._capture_disabled:
            return
        self._captured_bytes += len(chunk)
        if self._captured_bytes > _stream_capture_max_bytes():
            self._capture_disabled = True
            self._text_buffer = ""
            self._response_snapshot.clear()
            self._response_payload_cache = None
            self._output_items_by_index.clear()
            self._output_items_fallback.clear()
            logger.info("Stream capture disabled after exceeding %s bytes", _stream_capture_max_bytes())
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
                    self._invalidate_response_payload_cache()
                    continue

                response_obj = event_payload.get("response")
                if isinstance(response_obj, dict):
                    self._invalidate_response_payload_cache()
                    if event_type == "response.completed":
                        self.completed = True
                        event_payload = _patch_completed_output_from_output_items(
                            event_payload,
                            output_items_by_index=self._output_items_by_index,
                            output_items_fallback=self._output_items_fallback,
                        )
                        response_obj = event_payload.get("response")
                    _merge_mapping(self._response_snapshot, response_obj)

                if event_type == "response.completed":
                    self.finalize_usage_metrics()
                else:
                    self._update_usage_metrics(event_payload)
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
                await _close_async_iterator_safely(
                    self.body_iterator,
                    label="SSE proxy response body iterator",
                )


class _IdempotentAsyncCallback:
    def __init__(self, callback: Callable[[], Awaitable[Any]]) -> None:
        self._callback = callback
        self._lock = asyncio.Lock()
        self._called = False

    async def __call__(self) -> None:
        if self._called:
            return
        async with self._lock:
            if self._called:
                return
            result = await self._callback()
            if result is False:
                return
            self._called = True


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
                    await _close_async_iterator_safely(
                        self.body_iterator,
                        label="Finalizing streaming response body iterator",
                    )
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


class ProxyConcurrencyLease:
    def __init__(self, limiter: "ProxyConcurrencyLimiter") -> None:
        self._limiter = limiter
        self._released = False

    async def release(self) -> None:
        if self._released:
            return
        self._released = True
        await self._limiter.release()


class ProxyConcurrencyLimiter:
    def __init__(self, max_active: int, *, max_queue: int | None = None) -> None:
        self.max_active = max(1, int(max_active))
        self.max_queue = max(0, int(max_queue if max_queue is not None else DEFAULT_PROXY_QUEUE_MAX_SIZE))
        self._active = 0
        self._waiting = 0
        self._rejected_total = 0
        self._condition = asyncio.Condition()

    @property
    def active(self) -> int:
        return self._active

    @property
    def queued(self) -> int:
        return self._waiting

    @property
    def rejected_total(self) -> int:
        return self._rejected_total

    async def acquire(self, *, timeout_seconds: float) -> ProxyConcurrencyLease:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + max(0.0, float(timeout_seconds))
        async with self._condition:
            if self._active >= self.max_active:
                if self.max_queue <= 0 or self._waiting >= self.max_queue:
                    self._rejected_total += 1
                    raise TimeoutError("Proxy concurrency queue is full")
                self._waiting += 1
                joined_queue = True
            else:
                joined_queue = False
            try:
                while self._active >= self.max_active:
                    remaining = deadline - loop.time()
                    if remaining <= 0:
                        self._rejected_total += 1
                        raise TimeoutError("Timed out waiting for proxy concurrency slot")
                    try:
                        await asyncio.wait_for(self._condition.wait(), timeout=remaining)
                    except asyncio.TimeoutError as exc:
                        self._rejected_total += 1
                        raise TimeoutError("Timed out waiting for proxy concurrency slot") from exc
                self._active += 1
            finally:
                if joined_queue:
                    self._waiting = max(0, self._waiting - 1)
        return ProxyConcurrencyLease(self)

    async def release(self) -> None:
        async with self._condition:
            if self._active <= 0:
                self._active = 0
            else:
                self._active -= 1
            self._condition.notify(1)


class UpstreamCircuitBreaker:
    def __init__(self, *, failure_threshold: int, window_seconds: float, open_seconds: float) -> None:
        self.failure_threshold = max(1, int(failure_threshold))
        self.window_seconds = max(0.1, float(window_seconds))
        self.open_seconds = max(0.1, float(open_seconds))
        self._failures: deque[float] = deque()
        self._open_until = 0.0

    def _prune(self, now: float) -> None:
        cutoff = now - self.window_seconds
        while self._failures and self._failures[0] < cutoff:
            self._failures.popleft()

    def is_open(self) -> bool:
        now = time.monotonic()
        if self._open_until <= now:
            return False
        return True

    def retry_after_seconds(self) -> float:
        return max(0.0, self._open_until - time.monotonic())

    def record_success(self) -> None:
        self._failures.clear()
        self._open_until = 0.0

    def record_failure(self) -> None:
        now = time.monotonic()
        self._prune(now)
        self._failures.append(now)
        if len(self._failures) >= self.failure_threshold:
            self._open_until = now + self.open_seconds

    def snapshot(self) -> dict[str, Any]:
        now = time.monotonic()
        self._prune(now)
        return {
            "open": self.is_open(),
            "retry_after_seconds": self.retry_after_seconds(),
            "recent_failures": len(self._failures),
            "failure_threshold": self.failure_threshold,
            "window_seconds": self.window_seconds,
            "open_seconds": self.open_seconds,
        }


class UpstreamCircuitRegistry:
    def __init__(self) -> None:
        self._breakers: dict[str, UpstreamCircuitBreaker] = {}

    def breaker_for(self, key: str) -> UpstreamCircuitBreaker:
        resolved_key = str(key or "default")
        breaker = self._breakers.get(resolved_key)
        if breaker is None:
            breaker = UpstreamCircuitBreaker(
                failure_threshold=_upstream_circuit_breaker_failure_threshold(),
                window_seconds=_upstream_circuit_breaker_window_seconds(),
                open_seconds=_upstream_circuit_breaker_open_seconds(),
            )
            self._breakers[resolved_key] = breaker
        return breaker

    def snapshot(self) -> dict[str, dict[str, Any]]:
        return {key: breaker.snapshot() for key, breaker in sorted(self._breakers.items())}


@dataclass
class UpstreamHTTPClientShard:
    key: str
    client: httpx.AsyncClient
    breaker: UpstreamCircuitBreaker


async def _sweep_httpx_client_idle_connections(client: httpx.AsyncClient) -> int:
    transport = getattr(client, "_transport", None)
    if isinstance(transport, _TimingAsyncHTTPTransport):
        transport = getattr(transport, "_transport", None)
    pool = getattr(transport, "_pool", None)
    assign_requests = getattr(pool, "_assign_requests_to_connections", None)
    close_connections = getattr(pool, "_close_connections", None)
    lock = getattr(pool, "_optional_thread_lock", None)
    if not callable(assign_requests) or not callable(close_connections):
        return 0

    # httpcore only reaps expired/readable idle sockets during pool operations.
    # Run that same cleanup path periodically so remote FINs do not sit in
    # CLOSE_WAIT until another request happens to touch this exact shard.
    if lock is not None:
        with lock:
            closing = list(assign_requests())
    else:
        closing = list(assign_requests())
    if not closing:
        return 0
    await close_connections(closing)
    return len(closing)


class UpstreamHTTPClientPool:
    def __init__(
        self,
        *,
        shard_count: int,
        max_connections: int,
        max_keepalive_connections: int,
        keepalive_expiry_seconds: float | None,
    ) -> None:
        self.shard_count = max(1, int(shard_count))
        self.max_connections = max(1, int(max_connections))
        self.max_keepalive_connections = max(0, int(max_keepalive_connections))
        self.keepalive_expiry_seconds = keepalive_expiry_seconds
        self.last_sweep_closed_connections = 0
        self.last_sweep_at: datetime | None = None
        per_shard_connections = max(1, math.ceil(self.max_connections / self.shard_count))
        per_shard_keepalive = min(
            per_shard_connections,
            max(0, math.ceil(self.max_keepalive_connections / self.shard_count)),
        )
        self._registry = UpstreamCircuitRegistry()
        self._shards = [
            UpstreamHTTPClientShard(
                key=f"shard-{index}",
                client=httpx.AsyncClient(
                    follow_redirects=True,
                    transport=_TimingAsyncHTTPTransport(
                        http2=False,
                        limits=httpx.Limits(
                            max_connections=per_shard_connections,
                            max_keepalive_connections=per_shard_keepalive,
                            keepalive_expiry=keepalive_expiry_seconds,
                        ),
                    ),
                ),
                breaker=self._registry.breaker_for(f"shard-{index}"),
            )
            for index in range(self.shard_count)
        ]

    def shard_for(self, shard_key: Any) -> UpstreamHTTPClientShard:
        if not self._shards:
            raise RuntimeError("Upstream HTTP client pool has no shards")
        normalized = str(shard_key or "default")
        digest = hashlib.blake2b(normalized.encode("utf-8"), digest_size=8).digest()
        index = int.from_bytes(digest, "big", signed=False) % len(self._shards)
        return self._shards[index]

    @property
    def default_client(self) -> httpx.AsyncClient:
        return self._shards[0].client

    def snapshot(self) -> dict[str, Any]:
        open_count = sum(1 for shard in self._shards if shard.breaker.is_open())
        return {
            "shard_count": len(self._shards),
            "max_connections": self.max_connections,
            "max_keepalive_connections": self.max_keepalive_connections,
            "keepalive_expiry_seconds": self.keepalive_expiry_seconds,
            "last_sweep_closed_connections": self.last_sweep_closed_connections,
            "last_sweep_at": self.last_sweep_at.isoformat() if self.last_sweep_at is not None else None,
            "open_breakers": open_count,
            "breakers": {
                shard.key: shard.breaker.snapshot()
                for shard in self._shards
            },
        }

    async def sweep_idle_connections(self) -> int:
        closed_by_shard = await asyncio.gather(
            *(_sweep_httpx_client_idle_connections(shard.client) for shard in self._shards),
            return_exceptions=True,
        )
        closed = 0
        for result in closed_by_shard:
            if isinstance(result, Exception):
                logger.error(
                    "Failed to sweep upstream HTTP shard idle connections",
                    exc_info=(type(result), result, result.__traceback__),
                )
                continue
            closed += int(result or 0)
        self.last_sweep_closed_connections = closed
        self.last_sweep_at = utcnow()
        return closed

    async def aclose(self) -> None:
        await asyncio.gather(*(shard.client.aclose() for shard in self._shards), return_exceptions=True)


async def _upstream_http_pool_maintenance_loop(pool: UpstreamHTTPClientPool) -> None:
    interval_seconds = _upstream_http_pool_sweep_interval_seconds()
    while True:
        await asyncio.sleep(interval_seconds)
        try:
            closed = await pool.sweep_idle_connections()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Failed to sweep upstream HTTP pool idle connections")
            continue
        if closed:
            logger.info("Closed stale upstream HTTP idle connections: count=%s", closed)


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


_MAX_REQUEST_ACCOUNT_RETRIES_WARNED: set[tuple[int, int]] = set()


def _max_effective_request_account_retries() -> int:
    return _int_env("MAX_EFFECTIVE_REQUEST_ACCOUNT_RETRIES", 8, minimum=1)


def _max_request_account_retries() -> int:
    requested = _int_env("MAX_REQUEST_ACCOUNT_RETRIES", 10, minimum=1)
    cap = _max_effective_request_account_retries()
    if requested <= cap:
        return requested
    warning_key = (requested, cap)
    if warning_key not in _MAX_REQUEST_ACCOUNT_RETRIES_WARNED:
        _MAX_REQUEST_ACCOUNT_RETRIES_WARNED.add(warning_key)
        logger.warning(
            "MAX_REQUEST_ACCOUNT_RETRIES=%s exceeds MAX_EFFECTIVE_REQUEST_ACCOUNT_RETRIES=%s; using capped value",
            requested,
            cap,
        )
    return cap


def _transport_error_max_retries() -> int:
    return _int_env("TRANSPORT_ERROR_MAX_RETRIES", 2, minimum=0)


def _upstream_5xx_max_retries() -> int:
    return _int_env("UPSTREAM_5XX_MAX_RETRIES", 3, minimum=0)


def _proxy_max_active_responses() -> int:
    return _int_env("PROXY_MAX_ACTIVE_RESPONSES", DEFAULT_PROXY_MAX_ACTIVE_RESPONSES, minimum=1)


def _proxy_queue_max_size() -> int:
    return _int_env("PROXY_QUEUE_MAX_SIZE", DEFAULT_PROXY_QUEUE_MAX_SIZE, minimum=0)


def _proxy_queue_timeout_seconds() -> float:
    return _float_env("PROXY_QUEUE_TIMEOUT_SECONDS", 1.0, minimum=0.0)


def _healthz_db_timeout_seconds() -> float:
    return _float_env("HEALTHZ_DB_TIMEOUT_SECONDS", 1.0, minimum=0.05)


def _upstream_connect_timeout_seconds() -> float:
    return _float_env("UPSTREAM_CONNECT_TIMEOUT_SECONDS", 5.0, minimum=0.1)


def _upstream_write_timeout_seconds() -> float:
    return _float_env("UPSTREAM_WRITE_TIMEOUT_SECONDS", 10.0, minimum=0.1)


def _upstream_pool_timeout_seconds() -> float:
    return _float_env("UPSTREAM_POOL_TIMEOUT_SECONDS", 2.0, minimum=0.1)


def _upstream_http_timeout(*, read: float | None = None) -> httpx.Timeout:
    return httpx.Timeout(
        connect=_upstream_connect_timeout_seconds(),
        read=read,
        write=_upstream_write_timeout_seconds(),
        pool=_upstream_pool_timeout_seconds(),
    )


def _upstream_circuit_breaker_failure_threshold() -> int:
    return _int_env("UPSTREAM_CIRCUIT_BREAKER_FAILURE_THRESHOLD", 5, minimum=1)


def _upstream_circuit_breaker_window_seconds() -> float:
    return _float_env("UPSTREAM_CIRCUIT_BREAKER_WINDOW_SECONDS", 10.0, minimum=0.1)


def _upstream_circuit_breaker_open_seconds() -> float:
    return _float_env("UPSTREAM_CIRCUIT_BREAKER_OPEN_SECONDS", 30.0, minimum=0.1)


def _upstream_http_shard_count() -> int:
    return _int_env("UPSTREAM_HTTP_SHARD_COUNT", DEFAULT_UPSTREAM_HTTP_SHARD_COUNT, minimum=1)


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


def _upstream_http_keepalive_expiry_seconds() -> float | None:
    raw = str(os.getenv("UPSTREAM_HTTP_KEEPALIVE_EXPIRY_SECONDS", "") or "").strip().lower()
    if raw in {"none", "null", "off", "disabled"}:
        return None
    return _float_env(
        "UPSTREAM_HTTP_KEEPALIVE_EXPIRY_SECONDS",
        DEFAULT_UPSTREAM_HTTP_KEEPALIVE_EXPIRY_SECONDS,
        minimum=0.0,
    )


def _upstream_http_pool_sweep_interval_seconds() -> float:
    return _float_env(
        "UPSTREAM_HTTP_POOL_SWEEP_INTERVAL_SECONDS",
        DEFAULT_UPSTREAM_HTTP_POOL_SWEEP_INTERVAL_SECONDS,
        minimum=1.0,
    )


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
    if isinstance(client, UpstreamHTTPClientPool):
        return client.default_client
    return app.state.http_client


async def _await_cleanup_task_safely(
    cleanup_coro: Awaitable[Any],
    *,
    label: str,
    false_is_failure: bool = False,
) -> bool:
    current_task = asyncio.current_task()
    if current_task is not None:
        while current_task.cancelling():
            current_task.uncancel()

    cleanup_task = asyncio.create_task(cleanup_coro)
    while True:
        try:
            await asyncio.shield(cleanup_task)
            break
        except asyncio.CancelledError:
            if cleanup_task.done():
                break
            if current_task is not None:
                while current_task.cancelling():
                    current_task.uncancel()
        except Exception:
            break

    try:
        cleanup_result = cleanup_task.result()
    except asyncio.CancelledError:
        logger.warning("%s cleanup was cancelled before it could finish", label)
        return False
    except Exception as exc:
        logger.warning(
            "%s cleanup failed",
            label,
            exc_info=(type(exc), exc, exc.__traceback__),
        )
        return False
    if false_is_failure and cleanup_result is False:
        return False
    return True


async def _close_async_iterator_safely(iterator: Any, *, label: str) -> bool:
    close_iterator = getattr(iterator, "aclose", None)
    if not callable(close_iterator):
        return True
    try:
        close_result = close_iterator()
    except asyncio.CancelledError as exc:
        logger.warning(
            "%s cleanup was cancelled before it could start",
            label,
            exc_info=(type(exc), exc, exc.__traceback__),
        )
        return False
    except Exception as exc:
        logger.warning(
            "%s cleanup failed before it could start",
            label,
            exc_info=(type(exc), exc, exc.__traceback__),
        )
        return False
    if inspect.isawaitable(close_result):
        return await _await_cleanup_task_safely(close_result, label=label)
    return True


async def _close_upstream_response_safely(upstream_response: Any | None) -> bool:
    return await UpstreamStreamOwner._close_upstream_response_safely(upstream_response)


async def _force_close_upstream_response_stream_safely(upstream_response: Any) -> bool:
    return await UpstreamStreamOwner._force_close_httpcore_stream_chain_safely(upstream_response)


async def _force_release_httpcore_pool_request_safely(stream: Any) -> bool:
    return await UpstreamStreamOwner._force_release_httpcore_pool_request_safely(stream)


async def _close_stream_cm_safely(stream_cm: Any) -> bool:
    return await _await_cleanup_task_safely(
        stream_cm.__aexit__(None, None, None),
        label="Upstream stream context manager",
    )


async def _close_upstream_response_stream_safely(
    stream_cm: Any | None,
    upstream_response: Any | None,
) -> bool:
    if isinstance(stream_cm, UpstreamStreamOwner):
        return await stream_cm.close(reason="legacy_close_helper")
    cleanup_ok = await _close_upstream_response_safely(upstream_response)
    if stream_cm is not None:
        cleanup_ok = await _close_stream_cm_safely(stream_cm) and cleanup_ok
    return cleanup_ok


class UpstreamStreamOwner:
    NEW = "NEW"
    OPENING = "OPENING"
    OPEN = "OPEN"
    STREAMING = "STREAMING"
    CLOSING = "CLOSING"
    CLOSED = "CLOSED"
    FAILED = "FAILED"

    def __init__(
        self,
        stream_cm: Any,
        *,
        label: str,
        compact: bool,
    ) -> None:
        self.owner_id = uuid.uuid4().hex
        self.label = str(label or "upstream")
        self.compact = bool(compact)
        self._stream_cm = stream_cm
        self._upstream_response: Any | None = None
        self._raw_iterator: AsyncIterator[bytes] | None = None
        self._state = self.NEW
        self.close_reason: str | None = None
        self.cleanup_error: str | None = None
        self._closed = False
        self._close_lock = asyncio.Lock()
        _STREAM_OWNER_COUNTERS["created_total"] += 1
        self._set_state(self.NEW)

    def _set_state(self, state: str) -> None:
        self._state = state
        if not self._closed:
            _STREAM_OWNER_ACTIVE[self.owner_id] = (self.label, state)

    @classmethod
    async def open(
        cls,
        client: httpx.AsyncClient,
        method: str,
        url: str,
        *,
        headers: dict[str, str],
        content: str | bytes,
        timeout: httpx.Timeout,
        compact: bool = False,
        label: str = "responses",
    ) -> "UpstreamStreamOwner":
        stream_cm = client.stream(
            method,
            url,
            headers=headers,
            content=content,
            timeout=timeout,
        )
        owner = cls(stream_cm, label=label, compact=compact)
        owner._set_state(cls.OPENING)
        try:
            owner._upstream_response = await _enter_upstream_stream_with_timing(
                stream_cm,
                compact=compact,
            )
        except BaseException:
            _STREAM_OWNER_COUNTERS["open_failed_total"] += 1
            await owner.close(reason="open_failed")
            raise

        _STREAM_OWNER_COUNTERS["open_success_total"] += 1
        owner._set_state(cls.OPEN)
        return owner

    @property
    def response(self) -> Any:
        if self._upstream_response is None:
            raise RuntimeError("upstream stream owner has no entered response")
        return self._upstream_response

    @property
    def status_code(self) -> int:
        return int(getattr(self.response, "status_code", 0) or 0)

    @property
    def state(self) -> str:
        return self._state

    @property
    def final_state(self) -> str:
        return self._state

    def iter_raw(self) -> AsyncIterator[bytes]:
        if self._raw_iterator is None:
            self._raw_iterator = self.response.aiter_raw()
        self._set_state(self.STREAMING)
        return self._raw_iterator

    def aiter_raw(self) -> AsyncIterator[bytes]:
        return self.iter_raw()

    async def aread(self) -> bytes:
        read = getattr(self.response, "aread", None)
        if not callable(read):
            return b""
        result = read()
        if inspect.isawaitable(result):
            return await result
        return result

    async def close(self, *, reason: str = "unknown") -> bool:
        return await _await_cleanup_task_safely(
            self._close_once(reason=reason),
            label=f"Upstream stream owner {self.label}",
            false_is_failure=True,
        )

    async def _close_once(self, *, reason: str = "unknown") -> bool:
        if self._closed:
            return True
        async with self._close_lock:
            if self._closed:
                return True
            if self._state == self.FAILED:
                _STREAM_OWNER_COUNTERS["close_retry_total"] += 1
                _STREAM_OWNER_COUNTERS["close_cancel_retry_total"] += 1
            self._set_state(self.CLOSING)
            self.close_reason = str(reason or "unknown")
            reason_key = re.sub(r"[^a-zA-Z0-9_]+", "_", str(reason or "unknown")).strip("_").lower() or "unknown"
            _STREAM_OWNER_COUNTERS[f"close_reason_{reason_key}_total"] += 1
            try:
                cleanup_ok = await self._cleanup_transport_safely()
            except asyncio.CancelledError as exc:
                _STREAM_OWNER_COUNTERS["close_cancel_retry_total"] += 1
                logger.warning(
                    "Upstream stream owner cleanup was cancelled: owner_id=%s label=%s reason=%s",
                    self.owner_id,
                    self.label,
                    reason,
                    exc_info=(type(exc), exc, exc.__traceback__),
                )
                self.cleanup_error = f"CancelledError: {exc}"
                cleanup_ok = False
            except Exception as exc:
                _STREAM_OWNER_COUNTERS["close_error_total"] += 1
                logger.warning(
                    "Upstream stream owner cleanup failed: owner_id=%s label=%s reason=%s",
                    self.owner_id,
                    self.label,
                    reason,
                    exc_info=(type(exc), exc, exc.__traceback__),
                )
                self.cleanup_error = f"{type(exc).__name__}: {exc}"
                cleanup_ok = False

            if not cleanup_ok:
                _STREAM_OWNER_COUNTERS["close_failed_total"] += 1
                _STREAM_OWNER_COUNTERS["close_error_total"] += 1
                self._set_state(self.FAILED)
                return False

            self._closed = True
            self._state = self.CLOSED
            self._upstream_response = None
            self._raw_iterator = None
            _STREAM_OWNER_ACTIVE.pop(self.owner_id, None)
            _STREAM_OWNER_COUNTERS["close_success_total"] += 1
            return True

    async def _cleanup_transport_safely(self) -> bool:
        cleanup_ok = True
        if self._raw_iterator is not None:
            cleanup_ok = await _close_async_iterator_safely(
                self._raw_iterator,
                label="Upstream raw response iterator",
            ) and cleanup_ok
        cleanup_ok = await self._close_upstream_response_safely(self._upstream_response) and cleanup_ok
        if self._stream_cm is not None:
            cleanup_ok = await _close_stream_cm_safely(self._stream_cm) and cleanup_ok
        return cleanup_ok

    @staticmethod
    async def _close_upstream_response_safely(upstream_response: Any | None) -> bool:
        if upstream_response is None:
            return True
        cleanup_ok = True
        aclose = getattr(upstream_response, "aclose", None)
        if callable(aclose):
            try:
                close_result = aclose()
            except Exception as exc:
                logger.warning(
                    "Upstream HTTP response cleanup failed",
                    exc_info=(type(exc), exc, exc.__traceback__),
                )
                cleanup_ok = False
            else:
                if inspect.isawaitable(close_result):
                    cleanup_ok = await _await_cleanup_task_safely(close_result, label="Upstream HTTP response") and cleanup_ok

        # httpx.Response.aclose() and httpcore.PoolByteStream.aclose() both have
        # early closed guards. If cancellation interrupts after those guards are
        # set, public close calls become no-ops while fd/pool request state can
        # still be live. Keep this private recovery inside the owner boundary.
        cleanup_ok = await UpstreamStreamOwner._force_close_httpcore_stream_chain_safely(upstream_response) and cleanup_ok
        return cleanup_ok

    @staticmethod
    async def _force_close_httpcore_stream_chain_safely(upstream_response: Any) -> bool:
        stream = getattr(upstream_response, "stream", None)
        candidates: list[Any] = []
        current = stream
        seen: set[int] = set()
        while current is not None:
            current_id = id(current)
            if current_id in seen:
                break
            seen.add(current_id)
            candidates.append(current)
            current = getattr(current, "_stream", None)

        cleanup_ok = True
        for candidate in candidates:
            if candidate is None:
                continue
            aclose = getattr(candidate, "aclose", None)
            if callable(aclose):
                _STREAM_OWNER_COUNTERS["force_httpcore_close_total"] += 1
                try:
                    close_result = aclose()
                except Exception as exc:
                    logger.warning(
                        "Upstream HTTP response stream cleanup failed",
                        exc_info=(type(exc), exc, exc.__traceback__),
                    )
                    cleanup_ok = False
                else:
                    if inspect.isawaitable(close_result):
                        cleanup_ok = (
                            await _await_cleanup_task_safely(close_result, label="Upstream HTTP response stream")
                            and cleanup_ok
                        )
            cleanup_ok = await UpstreamStreamOwner._force_release_httpcore_pool_request_safely(candidate) and cleanup_ok
        return cleanup_ok

    @staticmethod
    async def _force_release_httpcore_pool_request_safely(stream: Any) -> bool:
        pool = getattr(stream, "_pool", None)
        pool_request = getattr(stream, "_pool_request", None)
        if pool is None or pool_request is None:
            return True
        requests = getattr(pool, "_requests", None)
        if not isinstance(requests, list) or pool_request not in requests:
            return True

        try:
            lock = getattr(pool, "_optional_thread_lock", nullcontext())
            with lock:
                if pool_request in requests:
                    requests.remove(pool_request)
                    _STREAM_OWNER_COUNTERS["force_pool_release_total"] += 1
                assign = getattr(pool, "_assign_requests_to_connections", None)
                closing = assign() if callable(assign) else []

            close_connections = getattr(pool, "_close_connections", None)
            if callable(close_connections):
                close_result = close_connections(closing)
                if inspect.isawaitable(close_result):
                    if not await _await_cleanup_task_safely(close_result, label="Upstream HTTP pool request"):
                        return False
        except Exception as exc:
            logger.warning(
                "Upstream HTTP pool request cleanup failed",
                exc_info=(type(exc), exc, exc.__traceback__),
            )
            return False
        return True


async def _close_stream_handle_safely(stream_handle: Any | None, *, reason: str) -> bool:
    if stream_handle is None:
        return True
    if isinstance(stream_handle, UpstreamStreamOwner):
        return await stream_handle.close(reason=reason)
    return await _close_stream_cm_safely(stream_handle)


def _stream_owner_runtime_metrics() -> dict[str, Any]:
    active_by_label: Counter[str] = Counter()
    active_by_state: Counter[str] = Counter()
    for label, state in _STREAM_OWNER_ACTIVE.values():
        active_by_label[label] += 1
        active_by_state[state] += 1
    counters = dict(_STREAM_OWNER_COUNTERS)
    return {
        "active": len(_STREAM_OWNER_ACTIVE),
        "closing": active_by_state.get(UpstreamStreamOwner.CLOSING, 0),
        "failed": active_by_state.get(UpstreamStreamOwner.FAILED, 0),
        "close_success_total": counters.get("close_success_total", 0),
        "close_cancel_retry_total": counters.get("close_cancel_retry_total", 0),
        "close_error_total": counters.get("close_error_total", 0),
        "force_httpcore_close_total": counters.get("force_httpcore_close_total", 0),
        "force_pool_release_total": counters.get("force_pool_release_total", 0),
        "active_by_label": dict(active_by_label),
        "active_by_state": dict(active_by_state),
        "counters": counters,
    }


class StreamingProxySession:
    def __init__(self, *, label: str) -> None:
        self.label = str(label or "stream_proxy")
        self.owner: UpstreamStreamOwner | None = None
        self.worker_tasks: set[asyncio.Task[Any]] = set()
        self.close_reason: str | None = None
        self._close_lock = asyncio.Lock()
        self._closed = False

    async def open_owner(
        self,
        client: httpx.AsyncClient,
        method: str,
        url: str,
        *,
        headers: dict[str, str],
        content: str | bytes,
        timeout: httpx.Timeout,
        compact: bool,
    ) -> UpstreamStreamOwner:
        self.owner = await UpstreamStreamOwner.open(
            client,
            method,
            url,
            headers=headers,
            content=content,
            timeout=timeout,
            compact=compact,
            label=self.label,
        )
        return self.owner

    def add_worker(self, task: asyncio.Task[Any]) -> None:
        self.worker_tasks.add(task)
        task.add_done_callback(self.worker_tasks.discard)

    async def stop_workers(self, *, reason: str) -> bool:
        cleanup_ok = True
        for task in list(self.worker_tasks):
            if not task.done():
                task.cancel()
            cleanup_ok = await _await_cleanup_task_safely(
                _await_task_ignoring_cancelled(task),
                label=f"Streaming proxy worker {reason}",
            ) and cleanup_ok
        return cleanup_ok

    async def close(self, *, reason: str) -> bool:
        if self._closed:
            return True
        async with self._close_lock:
            if self._closed:
                return True
            self.close_reason = str(reason or "unknown")
            cleanup_ok = await self.stop_workers(reason=self.close_reason)
            if self.owner is not None:
                cleanup_ok = await self.owner.close(reason=self.close_reason) and cleanup_ok
            if cleanup_ok:
                self._closed = True
            return cleanup_ok


async def _await_task_ignoring_cancelled(task: asyncio.Task[Any]) -> None:
    try:
        await task
    except asyncio.CancelledError:
        return


class StreamTransform:
    async def prime(self, upstream_iter: AsyncIterator[bytes]) -> _PrimedResponsesStream:
        return await _prime_responses_upstream_stream_with_timing(upstream_iter)

    def model_name(self, primed_stream: _PrimedResponsesStream, fallback_model: str | None) -> str | None:
        return primed_stream.model_name or fallback_model

    async def stream(
        self,
        owner: UpstreamStreamOwner,
        upstream_iter: AsyncIterator[bytes],
        primed_stream: _PrimedResponsesStream,
        *,
        stream_capture: _ProxyStreamCapture | None,
        close_stream: Callable[[], Awaitable[Any]],
    ) -> AsyncIterator[bytes]:
        raise NotImplementedError


class RawResponsesTransform(StreamTransform):
    def __init__(self, *, response_model_alias: str | None = None) -> None:
        self.response_model_alias = response_model_alias

    async def prime(self, upstream_iter: AsyncIterator[bytes]) -> _PrimedResponsesStream:
        heartbeat_interval_seconds = (
            _stream_keepalive_interval_seconds()
            if self.response_model_alias is not None and _is_responses_image_compat_model(self.response_model_alias)
            else None
        )
        return await _prime_responses_upstream_stream_with_timing(
            upstream_iter,
            heartbeat_interval_seconds=heartbeat_interval_seconds,
        )

    def model_name(self, primed_stream: _PrimedResponsesStream, fallback_model: str | None) -> str | None:
        return self.response_model_alias or primed_stream.model_name or fallback_model

    async def stream(
        self,
        owner: UpstreamStreamOwner,
        upstream_iter: AsyncIterator[bytes],
        primed_stream: _PrimedResponsesStream,
        *,
        stream_capture: _ProxyStreamCapture | None,
        close_stream: Callable[[], Awaitable[Any]],
    ) -> AsyncIterator[bytes]:
        async for chunk in _stream_upstream_response(
            owner,
            upstream_iter,
            primed_stream.buffered_chunks,
            stream_committed=primed_stream.stream_committed,
            stream_capture=stream_capture,
            response_model_alias=self.response_model_alias,
            emit_initial_keepalive=primed_stream.emit_initial_keepalive,
            pending_chunk_task=primed_stream.pending_chunk_task,
            close_stream=close_stream,
        ):
            yield chunk


class ImageCompatTransform(StreamTransform):
    def __init__(
        self,
        *,
        response_format: str,
        stream_prefix: str,
        heartbeat_interval_seconds: float | None,
    ) -> None:
        self.response_format = response_format
        self.stream_prefix = stream_prefix
        self.heartbeat_interval_seconds = heartbeat_interval_seconds

    async def prime(self, upstream_iter: AsyncIterator[bytes]) -> _PrimedResponsesStream:
        return await _prime_responses_upstream_stream_with_timing(
            upstream_iter,
            heartbeat_interval_seconds=self.heartbeat_interval_seconds,
        )

    async def stream(
        self,
        owner: UpstreamStreamOwner,
        upstream_iter: AsyncIterator[bytes],
        primed_stream: _PrimedResponsesStream,
        *,
        stream_capture: _ProxyStreamCapture | None,
        close_stream: Callable[[], Awaitable[Any]],
    ) -> AsyncIterator[bytes]:
        async for chunk in _stream_upstream_image_response(
            owner,
            upstream_iter,
            primed_stream.buffered_chunks,
            stream_committed=primed_stream.stream_committed,
            response_format=self.response_format,
            stream_prefix=self.stream_prefix,
            heartbeat_interval_seconds=self.heartbeat_interval_seconds,
            emit_initial_keepalive=primed_stream.emit_initial_keepalive,
            pending_chunk_task=primed_stream.pending_chunk_task,
            stream_capture=stream_capture,
            close_stream=close_stream,
        ):
            yield chunk


class ChatCompatTransform:
    def __init__(self, *, request_model: str, include_usage: bool) -> None:
        self.request_model = request_model
        self.include_usage = include_usage

    def stream_downstream(self, body_iterator: AsyncIterator[bytes]) -> AsyncIterator[bytes]:
        return _stream_responses_to_chat_completions(
            body_iterator,
            request_model=self.request_model,
            include_usage=self.include_usage,
        )


class StreamingResponseAdapter:
    @staticmethod
    def sse_response(
        body_iterator: AsyncIterator[bytes],
        *,
        heartbeat_interval_seconds: float | None = None,
        on_close: Callable[[], Awaitable[Any]] | None = None,
        status_code: int = 200,
    ) -> StreamingResponse:
        if heartbeat_interval_seconds is not None:
            body_iterator = _wrap_sse_stream_with_initial_keepalive(
                body_iterator,
                heartbeat_interval_seconds=heartbeat_interval_seconds,
            )
        if on_close is not None:
            body_iterator = _wrap_streaming_body_iterator(
                body_iterator,
                on_close=on_close,
            )
        return StreamingResponse(
            body_iterator,
            status_code=status_code,
            media_type="text/event-stream",
            headers=_sse_response_headers(),
        )


async def stream_proxy(
    client: httpx.AsyncClient,
    *,
    method: str,
    url: str,
    headers: dict[str, str],
    content: str | bytes,
    timeout: httpx.Timeout,
    transform: StreamTransform,
    compact: bool,
    label: str,
    model_name: str | None,
    stream_capture: _ProxyStreamCapture,
    heartbeat_interval_seconds: float | None = None,
    open_before_return: bool = False,
) -> ProxyRequestResult:
    session = StreamingProxySession(label=label)
    close_session = _IdempotentAsyncCallback(lambda: session.close(reason=f"{label}_close"))

    async def open_and_prime() -> tuple[UpstreamStreamOwner, AsyncIterator[bytes], _PrimedResponsesStream]:
        owner = await session.open_owner(
            client,
            method,
            url,
            headers=headers,
            content=content,
            timeout=timeout,
            compact=compact,
        )
        if owner.status_code < 200 or owner.status_code >= 300:
            try:
                raw = await owner.aread()
            finally:
                await close_session()
            raise _upstream_error_http_exception(owner.status_code, _decode_error_body(raw))

        upstream_iter = owner.iter_raw()
        try:
            primed = await transform.prime(upstream_iter)
        except Exception:
            await close_session()
            raise
        return owner, upstream_iter, primed

    opened: tuple[UpstreamStreamOwner, AsyncIterator[bytes], _PrimedResponsesStream] | None = None
    status_code = 200
    effective_model_name = model_name
    first_token_at: datetime | None = None

    if open_before_return:
        opened = await open_and_prime()
        owner, _, primed_stream = opened
        status_code = owner.status_code
        effective_model_name = transform.model_name(primed_stream, model_name)
        first_token_at = utcnow() if primed_stream.first_token_observed else None

    async def body_iterator() -> AsyncGenerator[bytes, None]:
        nonlocal opened
        if opened is None:
            opened = await open_and_prime()
        owner, upstream_iter, primed_stream = opened
        async for chunk in transform.stream(
            owner,
            upstream_iter,
            primed_stream,
            stream_capture=stream_capture,
            close_stream=close_session,
        ):
            yield chunk

    return ProxyRequestResult(
        response=StreamingResponseAdapter.sse_response(
            body_iterator(),
            heartbeat_interval_seconds=heartbeat_interval_seconds,
            on_close=close_session,
        ),
        status_code=status_code,
        model_name=effective_model_name,
        first_token_at=first_token_at,
        on_close=close_session,
        stream_capture=stream_capture,
    )


def _proxy_concurrency_limiter(app: FastAPI) -> ProxyConcurrencyLimiter:
    limiter = getattr(app.state, "proxy_concurrency", None)
    max_active = _proxy_max_active_responses()
    max_queue = _proxy_queue_max_size()
    if (
        not isinstance(limiter, ProxyConcurrencyLimiter)
        or limiter.max_active != max_active
        or limiter.max_queue != max_queue
    ):
        limiter = ProxyConcurrencyLimiter(max_active, max_queue=max_queue)
        app.state.proxy_concurrency = limiter
    return limiter


def _upstream_circuit_registry(app: FastAPI) -> UpstreamCircuitRegistry:
    registry = getattr(app.state, "upstream_circuit_registry", None)
    if isinstance(registry, UpstreamCircuitRegistry):
        return registry
    legacy_breaker = getattr(app.state, "upstream_circuit_breaker", None)
    registry = UpstreamCircuitRegistry()
    if isinstance(legacy_breaker, UpstreamCircuitBreaker):
        registry._breakers["default"] = legacy_breaker
    app.state.upstream_circuit_registry = registry
    return registry


def _upstream_circuit_breaker(app: FastAPI, key: str = "default") -> UpstreamCircuitBreaker:
    return _upstream_circuit_registry(app).breaker_for(key)


def _upstream_circuit_open_exception(breaker: UpstreamCircuitBreaker) -> HTTPException:
    retry_after = max(1, int(math.ceil(breaker.retry_after_seconds())))
    exc = HTTPException(
        status_code=503,
        detail=f"Upstream temporarily unavailable; retry after {retry_after}s",
        headers={"Retry-After": str(retry_after)},
    )
    setattr(exc, "retryable", False)
    return exc


def _raise_if_upstream_circuit_open(breaker: UpstreamCircuitBreaker) -> None:
    if breaker.is_open():
        raise _upstream_circuit_open_exception(breaker)


def _is_upstream_connectivity_exception(exc: Exception) -> bool:
    return isinstance(exc, (httpx.ConnectTimeout, httpx.ConnectError))


def _is_local_upstream_pool_timeout(exc: Exception) -> bool:
    return isinstance(exc, httpx.PoolTimeout)


def _should_record_upstream_circuit_failure(exc: Exception) -> bool:
    return _is_upstream_connectivity_exception(exc) and not _is_local_upstream_pool_timeout(exc)


def _upstream_shard_key(
    *,
    token_id: int | None = None,
    account_id: str | None = None,
    request_id: str | None = None,
) -> str:
    if token_id is not None:
        return f"token:{int(token_id)}"
    if account_id:
        return f"account:{account_id}"
    if request_id:
        return f"request:{request_id}"
    return "default"


def _response_http_client_pool(app: FastAPI) -> UpstreamHTTPClientPool | None:
    pool = getattr(app.state, "response_http_client", None)
    return pool if isinstance(pool, UpstreamHTTPClientPool) else None


def _select_response_http_client(
    app: FastAPI,
    *,
    token_id: int | None = None,
    account_id: str | None = None,
    request_id: str | None = None,
) -> tuple[httpx.AsyncClient, UpstreamCircuitBreaker, str]:
    key = _upstream_shard_key(token_id=token_id, account_id=account_id, request_id=request_id)
    pool = _response_http_client_pool(app)
    if pool is not None:
        shard = pool.shard_for(key)
        return shard.client, shard.breaker, shard.key
    client = _state_http_client(app, "response_http_client")
    return client, _upstream_circuit_breaker(app, key), key


def _image_request_max_account_retries() -> int:
    return _int_env("IMAGE_REQUEST_MAX_ACCOUNT_RETRIES", 8, minimum=1)


def _image_input_max_per_request() -> int:
    return _int_env("IMAGE_INPUT_MAX_PER_REQUEST", 249, minimum=1)


def _image_upload_max_bytes() -> int:
    return _int_env("IMAGE_UPLOAD_MAX_BYTES", 25 * 1024 * 1024, minimum=1024)


def _upstream_non_stream_max_response_bytes() -> int:
    return _int_env("UPSTREAM_NON_STREAM_MAX_RESPONSE_BYTES", 64 * 1024 * 1024, minimum=1024 * 1024)


def _stream_capture_max_bytes() -> int:
    return _int_env("STREAM_CAPTURE_MAX_BYTES", 8 * 1024 * 1024, minimum=1024 * 1024)


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


def _admin_quota_sync_refresh_limit() -> int:
    return _int_env("ADMIN_QUOTA_SYNC_REFRESH_LIMIT", 12, minimum=0)


def _admin_quota_background_refresh_limit() -> int:
    return _int_env("ADMIN_QUOTA_BACKGROUND_REFRESH_LIMIT", 100, minimum=1)


def _admin_requests_cache_ttl_seconds() -> float:
    return _float_env("ADMIN_REQUESTS_CACHE_TTL_SECONDS", 15.0, minimum=0.0)


def _admin_requests_cache_refresh_seconds() -> float:
    ttl_seconds = _admin_requests_cache_ttl_seconds()
    default_refresh_seconds = ttl_seconds * 0.8 if ttl_seconds > 0 else 15.0
    return _float_env(
        "ADMIN_REQUESTS_CACHE_REFRESH_SECONDS",
        default_refresh_seconds,
        minimum=1.0,
    )


def _admin_token_counts_cache_ttl_seconds() -> float:
    return _float_env("ADMIN_TOKEN_COUNTS_CACHE_TTL_SECONDS", 2.0, minimum=0.0)


def _request_log_retention_days() -> int:
    return _int_env("REQUEST_LOG_RETENTION_DAYS", 30, minimum=0)


def _request_log_cleanup_interval_seconds() -> float:
    return _float_env("REQUEST_LOG_CLEANUP_INTERVAL_SECONDS", 3600.0, minimum=60.0)


def _token_active_runtime_metrics() -> dict[str, Any]:
    values = [max(0, int(value or 0)) for value in _TOKEN_ACTIVE_REQUESTS.values()]
    return {
        "tokens_with_active_streams": len(values),
        "total_active_streams": sum(values),
        "p50": _percentile(values, 50),
        "p95": _percentile(values, 95),
        "max": max(values) if values else 0,
    }


def _runtime_fd_metrics() -> dict[str, Any]:
    metrics: dict[str, Any] = {
        "open_fds": None,
        "tcp_states": {},
        "tcp_close_wait": 0,
        "tcp_close_wait_443": 0,
    }
    fd_dir = Path("/proc/self/fd")
    with suppress(Exception):
        metrics["open_fds"] = len(list(fd_dir.iterdir()))

    state_names = {
        "01": "ESTABLISHED",
        "02": "SYN_SENT",
        "03": "SYN_RECV",
        "04": "FIN_WAIT1",
        "05": "FIN_WAIT2",
        "06": "TIME_WAIT",
        "07": "CLOSE",
        "08": "CLOSE_WAIT",
        "09": "LAST_ACK",
        "0A": "LISTEN",
        "0B": "CLOSING",
    }
    tcp_states: dict[str, int] = {}
    close_wait_443 = 0
    for proc_path in (Path("/proc/net/tcp"), Path("/proc/net/tcp6")):
        try:
            lines = proc_path.read_text(encoding="utf-8").splitlines()[1:]
        except OSError:
            continue
        for line in lines:
            parts = line.split()
            if len(parts) < 4:
                continue
            state_hex = parts[3].upper()
            state_name = state_names.get(state_hex, state_hex)
            tcp_states[state_name] = tcp_states.get(state_name, 0) + 1
            if state_name == "CLOSE_WAIT":
                metrics["tcp_close_wait"] = int(metrics["tcp_close_wait"] or 0) + 1
                remote = parts[2]
                remote_port_hex = remote.rsplit(":", 1)[-1].upper()
                if remote_port_hex == "01BB":
                    close_wait_443 += 1
    metrics["tcp_states"] = tcp_states
    metrics["tcp_close_wait_443"] = close_wait_443
    return metrics


def _upstream_runtime_metrics(app: FastAPI) -> dict[str, Any]:
    pool = _response_http_client_pool(app)
    if pool is not None:
        return pool.snapshot()
    return {
        "shard_count": 1,
        "breakers": _upstream_circuit_registry(app).snapshot(),
    }


def _proxy_limiter_runtime_metrics(limiter: ProxyConcurrencyLimiter) -> dict[str, Any]:
    return {
        "active": limiter.active,
        "queued": limiter.queued,
        "rejected_total": limiter.rejected_total,
        "max_active": limiter.max_active,
        "max_queue": limiter.max_queue,
    }


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


def _build_upstream_headers(
    http_request: Request,
    access_token: str,
    account_id: str | None,
    stream: bool,
    *,
    prompt_cache_context: _PromptCacheRequestContext | None = None,
    session_id: str | None = None,
) -> dict[str, str]:
    resolved_session_id = session_id
    if not resolved_session_id:
        resolved_session_id, _ = _prompt_cache_session_context(http_request, prompt_cache_context)

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Openai-Beta": (http_request.headers.get("Openai-Beta") or "responses=experimental").strip(),
        "Originator": (http_request.headers.get("Originator") or "codex_cli_rs").strip(),
        "Version": CODEX_CLI_VERSION,
        "Session_id": resolved_session_id,
        "User-Agent": CODEX_USER_AGENT,
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
    _remove_reasoning_content_fields(payload)
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


def _remove_reasoning_content_fields(value: Any) -> None:
    if isinstance(value, dict):
        value.pop("reasoning_content", None)
        for nested_value in value.values():
            _remove_reasoning_content_fields(nested_value)
        return

    if isinstance(value, (list, tuple)):
        for nested_value in value:
            _remove_reasoning_content_fields(nested_value)


def _trim_invalid_encrypted_reasoning_items(payload: dict[str, Any]) -> bool:
    input_value = payload.get("input")
    if isinstance(input_value, list):
        filtered: list[Any] = []
        changed = False
        for item in input_value:
            next_item, item_changed, keep_item = _trim_invalid_encrypted_reasoning_item(item)
            changed = changed or item_changed
            if keep_item:
                filtered.append(next_item)
        if not changed:
            return False
        if filtered:
            payload["input"] = filtered
        else:
            payload.pop("input", None)
        return True

    if isinstance(input_value, dict):
        next_item, changed, keep_item = _trim_invalid_encrypted_reasoning_item(input_value)
        if not changed:
            return False
        if keep_item:
            payload["input"] = next_item
        else:
            payload.pop("input", None)
        return True

    return False


def _trim_invalid_encrypted_reasoning_item(item: Any) -> tuple[Any, bool, bool]:
    if not isinstance(item, dict):
        return item, False, True

    if str(item.get("type") or "").strip() != "reasoning":
        return item, False, True

    if "encrypted_content" not in item:
        return item, False, True

    item.pop("encrypted_content", None)
    if set(item.keys()) == {"type"}:
        return None, True, False
    return item, True, True


def _is_invalid_encrypted_content_error(status_code: int, error_text: str) -> bool:
    if status_code != 400:
        return False

    error = _extract_error_object(error_text)
    if isinstance(error, dict):
        error_code = str(error.get("code") or "").strip().lower()
        if error_code == "invalid_encrypted_content":
            return True

    return "invalid_encrypted_content" in str(error_text or "").lower()


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


def _bool_env(name: str, default: bool) -> bool:
    return _coerce_bool(os.getenv(name), default)


def _prompt_cache_affinity_enabled() -> bool:
    return _bool_env("PROMPT_CACHE_AFFINITY_ENABLED", True)


def _prompt_cache_auto_key_enabled() -> bool:
    return _bool_env("PROMPT_CACHE_AUTO_KEY_ENABLED", True)


def _prompt_cache_max_lanes_per_key() -> int:
    return _int_env("PROMPT_CACHE_MAX_LANES_PER_KEY", 3, minimum=1)


def _prompt_cache_primary_wait_ms() -> int:
    return _int_env("PROMPT_CACHE_PRIMARY_WAIT_MS", 500, minimum=0)


def _prompt_cache_lane_wait_ms() -> int:
    return _int_env("PROMPT_CACHE_LANE_WAIT_MS", 100, minimum=0)


def _prompt_cache_previous_owner_wait_ms() -> int:
    return _int_env("PROMPT_CACHE_PREVIOUS_OWNER_WAIT_MS", 800, minimum=0)


def _prompt_cache_prune_interval_seconds() -> float:
    return _float_env("PROMPT_CACHE_PRUNE_INTERVAL_SECONDS", 1.0, minimum=0.0)


def _prompt_cache_global_fallback_enabled() -> bool:
    return _bool_env("PROMPT_CACHE_GLOBAL_FALLBACK_ENABLED", True)


def _prompt_cache_rebind_primary_enabled() -> bool:
    return _bool_env("PROMPT_CACHE_REBIND_PRIMARY", False)


def _prompt_cache_previous_replay_fallback_enabled() -> bool:
    return _bool_env("PROMPT_CACHE_PREVIOUS_REPLAY_FALLBACK_ENABLED", False)


def _prompt_cache_previous_strict_enabled() -> bool:
    explicit = os.getenv("PROMPT_CACHE_PREVIOUS_STRICT_ENABLED")
    if explicit is not None:
        return _coerce_bool(explicit, True)
    return not _prompt_cache_previous_replay_fallback_enabled()


def _prompt_cache_session_prefer_header() -> bool:
    value = str(os.getenv("PROMPT_CACHE_SESSION_ID_MODE") or "").strip().lower()
    return value in {"header", "prefer_header", "client", "client_header"}


def _prompt_cache_lane_ttl_seconds() -> float:
    return _float_env("PROMPT_CACHE_LANE_TTL_SECONDS", 3600.0, minimum=60.0)


def _prompt_cache_response_ttl_seconds() -> float:
    return _float_env("PROMPT_CACHE_RESPONSE_TTL_SECONDS", 86400.0, minimum=60.0)


def _prompt_cache_max_entries() -> int:
    return _int_env("PROMPT_CACHE_MAX_ENTRIES", 10000, minimum=100)


def _prompt_cache_response_max_entries() -> int:
    return _int_env("PROMPT_CACHE_RESPONSE_MAX_ENTRIES", 20000, minimum=100)


def _build_prompt_cache_affinity_key(
    *,
    endpoint: str,
    model: str | None,
    compact: bool,
    client_scope: str,
    prompt_key_hash: str,
) -> str:
    affinity_endpoint, affinity_compact = _prompt_cache_affinity_family(endpoint=endpoint, compact=compact)
    payload = {
        "endpoint": affinity_endpoint,
        "model": model,
        "compact": affinity_compact,
        "client": client_scope,
        "prompt": prompt_key_hash,
    }
    return short_hash(normalize_seed_json(payload), length=32)


def _prompt_cache_affinity_family(*, endpoint: str, compact: bool) -> tuple[str, bool]:
    if endpoint in {"/v1/responses", "/v1/responses/compact"}:
        return "/v1/responses", False
    return endpoint, bool(compact)


def _prompt_cache_session_context(
    http_request: Request,
    prompt_cache_context: _PromptCacheRequestContext | None,
) -> tuple[str, str]:
    explicit_session_id = (http_request.headers.get("Session_id") or "").strip()
    prompt_session_id = _prompt_cache_session_uuid(prompt_cache_context)

    if explicit_session_id and (prompt_session_id is None or _prompt_cache_session_prefer_header()):
        return explicit_session_id, "header"

    if prompt_session_id:
        return prompt_session_id, "prompt_cache"

    return str(uuid.uuid4()), "generated"


def _prompt_cache_lane_count(app: FastAPI, prompt_cache_context: _PromptCacheRequestContext | None) -> int | None:
    if prompt_cache_context is None or not prompt_cache_context.affinity_key:
        return None
    store = _prompt_cache_lane_store(app)
    state = store.get(prompt_cache_context.affinity_key)
    if state is None:
        return None
    return len(state.lanes)


def _build_prompt_cache_request_context(
    http_request: Request,
    *,
    endpoint: str,
    request_model: str | None,
    raw_payload: dict[str, Any],
    compact: bool = False,
) -> _PromptCacheRequestContext | None:
    if not _prompt_cache_affinity_enabled():
        return None

    previous_response_id = extract_previous_response_id(raw_payload)
    explicit_key = extract_prompt_cache_key(raw_payload)
    prompt_cache_key = explicit_key
    source = "explicit" if explicit_key else "none"
    if prompt_cache_key is None and _prompt_cache_auto_key_enabled():
        if endpoint == "/v1/chat/completions":
            prompt_cache_key = derive_chat_prompt_cache_key(raw_payload)
            source = "derived_chat" if prompt_cache_key else "none"
        elif endpoint in {"/v1/responses", "/v1/responses/compact"}:
            prompt_cache_key = derive_responses_prompt_cache_key(raw_payload)
            source = "derived_responses" if prompt_cache_key else "none"

    key_hash = prompt_cache_key_hash(prompt_cache_key) if prompt_cache_key else None
    client_scope = client_scope_hash(http_request.headers)
    affinity_key = (
        _build_prompt_cache_affinity_key(
            endpoint=endpoint,
            model=request_model or _normalize_optional_text(raw_payload.get("model")),
            compact=compact,
            client_scope=client_scope,
            prompt_key_hash=key_hash,
        )
        if key_hash
        else None
    )
    if previous_response_id is None and prompt_cache_key is None:
        return None
    return _PromptCacheRequestContext(
        endpoint=endpoint,
        model=request_model or _normalize_optional_text(raw_payload.get("model")),
        compact=compact,
        previous_response_id=previous_response_id,
        prompt_cache_key=prompt_cache_key,
        prompt_cache_key_hash=key_hash,
        affinity_key=affinity_key,
        client_scope=client_scope,
        source=source,
        raw_payload=copy.deepcopy(raw_payload) if isinstance(raw_payload, dict) else None,
    )


def _apply_prompt_cache_context_to_payload(
    payload: dict[str, Any],
    context: _PromptCacheRequestContext | None,
) -> dict[str, Any]:
    if context is not None and context.prompt_cache_key:
        payload["prompt_cache_key"] = context.prompt_cache_key
    return payload


def _prompt_cache_session_uuid(context: _PromptCacheRequestContext | None) -> str | None:
    if context is None:
        return None
    seed = context.prompt_cache_key or context.affinity_key
    if not seed:
        return None
    return deterministic_uuid(f"{context.client_scope}:{seed}")


def _prompt_cache_update_runtime_trace_fields(
    trace: dict[str, Any] | None,
    *,
    session_id: str | None = None,
    session_id_source: str | None = None,
    upstream_response_id: str | None = None,
    status_code: int | None = None,
    usage_metrics: UsageMetrics | None = None,
    cache_affinity_result: str | None = None,
    cache_affinity_lane_index: int | None = None,
    cache_affinity_lane_count: int | None = None,
) -> dict[str, Any] | None:
    if not isinstance(trace, dict):
        return None

    if session_id is not None:
        trace["session_id_hash"] = short_hash(session_id, length=64)
    if session_id_source is not None:
        trace["session_id_source"] = session_id_source

    route = trace.get("route")
    if not isinstance(route, dict):
        route = {}
        trace["route"] = route
    if cache_affinity_result is not None:
        route["cache_affinity_result"] = cache_affinity_result
    if cache_affinity_lane_index is not None:
        route["cache_affinity_lane_index"] = cache_affinity_lane_index
    if cache_affinity_lane_count is not None:
        route["cache_affinity_lane_count"] = cache_affinity_lane_count

    usage = trace.get("usage")
    if not isinstance(usage, dict):
        usage = {}
        trace["usage"] = usage
    if usage_metrics is not None:
        usage["input_tokens"] = usage_metrics.input_tokens
        usage["cached_input_tokens"] = usage_metrics.cached_input_tokens
        usage["output_tokens"] = usage_metrics.output_tokens
        usage["total_tokens"] = usage_metrics.total_tokens
        usage["cache_hit_ratio"] = usage_metrics.cache_hit_ratio

    response = trace.get("response")
    if not isinstance(response, dict):
        response = {}
        trace["response"] = response
    if upstream_response_id is not None:
        response["response_id"] = upstream_response_id
    if status_code is not None:
        response["status_code"] = status_code

    return trace


def _extract_response_id_from_payload(payload: Any) -> str | None:
    if not isinstance(payload, dict):
        return None
    candidates = (
        payload.get("id"),
        payload.get("response", {}).get("id") if isinstance(payload.get("response"), dict) else None,
    )
    for candidate in candidates:
        text = _normalize_optional_text(candidate)
        if text:
            return text
    return None


def _prompt_cache_finalize_kwargs(
    context: _PromptCacheRequestContext | None,
    *,
    app: FastAPI | None = None,
    prompt_cache_trace: dict[str, Any] | None = None,
    affinity_result: str | None = None,
    lane_index: int | None = None,
    status_code: int | None = None,
    usage_metrics: UsageMetrics | None = None,
    upstream_response_id: str | None = None,
) -> dict[str, Any]:
    if context is None:
        return {}
    trace = prompt_cache_trace if isinstance(prompt_cache_trace, dict) else {}
    route = trace.get("route") if isinstance(trace.get("route"), dict) else {}
    response = trace.get("response") if isinstance(trace.get("response"), dict) else {}
    usage = trace.get("usage") if isinstance(trace.get("usage"), dict) else {}
    prompt = trace.get("prompt") if isinstance(trace.get("prompt"), dict) else {}
    if not usage and usage_metrics is not None:
        usage = {
            "input_tokens": usage_metrics.input_tokens,
            "cached_input_tokens": usage_metrics.cached_input_tokens,
            "output_tokens": usage_metrics.output_tokens,
            "total_tokens": usage_metrics.total_tokens,
            "cache_hit_ratio": usage_metrics.cache_hit_ratio,
        }
    return {
        "request_payload_hash": trace.get("request_payload_hash"),
        "upstream_payload_hash": trace.get("upstream_payload_hash"),
        "prompt_template_hash": prompt.get("template_hash"),
        "prompt_dynamic_hash": prompt.get("dynamic_hash"),
        "prompt_cache_source": context.source,
        "prompt_cache_key_hash": context.prompt_cache_key_hash,
        "prompt_cache_retention_requested": trace.get("prompt_cache_retention_requested"),
        "prompt_cache_retention_sent": trace.get("prompt_cache_retention_sent"),
        "session_id_hash": trace.get("session_id_hash"),
        "session_id_source": trace.get("session_id_source"),
        "previous_response_id_hash": trace.get("previous_response_id_hash"),
        "upstream_response_id": upstream_response_id or response.get("response_id"),
        "cache_hit_ratio": usage.get("cache_hit_ratio"),
        "cache_affinity_result": affinity_result or route.get("cache_affinity_result"),
        "cache_affinity_lane_index": lane_index if lane_index is not None else route.get("cache_affinity_lane_index"),
        "prompt_cache_trace": trace,
    }


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


def _model_unsupported_codex_token_unavailable_detail(model_name: str) -> str:
    return f"No compatible Codex token available for {model_name} requests"


def _model_unsupported_scoped_cooldown_seconds() -> int:
    return _int_env("MODEL_UNSUPPORTED_SCOPED_COOLDOWN_SECONDS", 3600, minimum=60)


def _model_compatibility_max_token_probes() -> int:
    requested_account_retries = _int_env("MAX_REQUEST_ACCOUNT_RETRIES", 10, minimum=1)
    configured_probe_limit = _int_env(
        "MODEL_COMPATIBILITY_MAX_TOKEN_PROBES",
        requested_account_retries,
        minimum=1,
    )
    return max(configured_probe_limit, _max_request_account_retries())


def _model_compatibility_scoped_cooldown_scope(request_model: str | None) -> str | None:
    restricted_model_name = _non_free_only_codex_model_name(request_model)
    if restricted_model_name is None or _is_responses_image_compat_model(restricted_model_name):
        return None
    return f"{restricted_model_name}:model-compatibility"


def _codex_request_scoped_cooldown_scope(request_model: str | None) -> str | None:
    return _image_rate_limit_scoped_cooldown_scope(request_model) or _model_compatibility_scoped_cooldown_scope(
        request_model
    )


def _is_model_unsupported_for_chatgpt_account_error(
    status_code: int,
    detail: str,
    *,
    request_model: str | None,
) -> bool:
    restricted_model_name = _non_free_only_codex_model_name(request_model)
    if status_code != 400 or restricted_model_name is None:
        return False
    normalized_detail = str(detail or "").lower()
    return (
        restricted_model_name.lower() in normalized_detail
        and "model is not supported" in normalized_detail
        and "chatgpt account" in normalized_detail
    )


async def _mark_model_unsupported_token_for_request(
    http_request: Request,
    token_row: Any,
    *,
    request_model: str | None,
    detail: str,
    excluded_token_ids: set[int],
) -> None:
    token_id = int(getattr(token_row, "id", 0) or 0)
    if token_id <= 0:
        return
    excluded_token_ids.add(token_id)
    _prompt_cache_remove_token(http_request.app, token_id)
    cooldown_scope = _model_compatibility_scoped_cooldown_scope(request_model)
    if cooldown_scope is None:
        await mark_token_error(token_id, detail)
        return
    await mark_token_scoped_cooldown(
        token_id,
        cooldown_scope,
        detail,
        cooldown_seconds=_model_unsupported_scoped_cooldown_seconds(),
    )


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

    max_bytes = _image_upload_max_bytes()
    if len(payload) > max_bytes:
        raise HTTPException(
            status_code=413,
            detail=f"Uploaded image is too large; limit is {max_bytes} bytes",
        )

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
    total_bytes = 0
    max_response_bytes = _upstream_non_stream_max_response_bytes()

    while True:
        try:
            chunk = await upstream_iter.__anext__()
        except StopAsyncIteration:
            if text_buffer.strip():
                raise HTTPException(status_code=502, detail="Upstream closed stream with an incomplete SSE event")
            break

        total_bytes += len(chunk)
        if total_bytes > max_response_bytes:
            raise HTTPException(
                status_code=502,
                detail=f"Upstream non-stream response exceeded {max_response_bytes} bytes",
            )
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
    total_bytes = 0
    max_response_bytes = _upstream_non_stream_max_response_bytes()

    async for chunk in upstream_iter:
        if chunk and first_chunk_at is None:
            first_chunk_at = utcnow()
        total_bytes += len(chunk)
        if total_bytes > max_response_bytes:
            raise HTTPException(
                status_code=502,
                detail=f"Upstream non-stream response exceeded {max_response_bytes} bytes",
            )
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

                if event_type == "keepalive":
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
    total_bytes = 0
    max_response_bytes = _upstream_non_stream_max_response_bytes()

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

        total_bytes += len(chunk)
        if total_bytes > max_response_bytes:
            raise _retryable_gateway_http_exception(
                status_code=502,
                detail=f"Upstream image response exceeded {max_response_bytes} bytes",
            )

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


def _build_stream_keepalive_event(sequence_number: int = 0) -> bytes:
    payload = json.dumps(
        {
            "type": "keepalive",
            "sequence_number": sequence_number,
        },
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return b"event: keepalive\n" + b"data: " + payload + b"\n\n"


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


def _stream_error_status_code_from_chunk(chunk: bytes) -> int | None:
    stream_error = _extract_stream_error_from_chunk(chunk)
    if stream_error is not None:
        return int(getattr(stream_error, "status_code", 500) or 500)
    return None


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
    close_stream: Callable[[], Awaitable[None]] | None = None,
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
        if close_stream is not None:
            await close_stream()
        else:
            await _close_stream_handle_safely(stream_cm, reason="image_stream_generator_finally")


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
    timeout = _upstream_http_timeout(read=None)
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
        return await stream_proxy(
            client,
            method="POST",
            url=upstream_url,
            headers=headers,
            content=json_payload,
            timeout=timeout,
            transform=ImageCompatTransform(
                response_format=image_request.response_format,
                stream_prefix=image_request.stream_prefix,
                heartbeat_interval_seconds=stream_keepalive_interval_seconds,
            ),
            compact=False,
            label="image_stream",
            model_name=image_request.model_name,
            stream_capture=stream_capture,
            heartbeat_interval_seconds=stream_keepalive_interval_seconds,
            open_before_return=False,
        )

    session = StreamingProxySession(label="image_non_stream")
    stream_owner = await session.open_owner(
        client,
        "POST",
        upstream_url,
        headers=headers,
        content=json_payload,
        timeout=timeout,
        compact=False,
    )
    upstream_response = stream_owner.response
    try:
        if upstream_response.status_code < 200 or upstream_response.status_code >= 300:
            raw = await stream_owner.aread()
            raise _upstream_error_http_exception(upstream_response.status_code, _decode_error_body(raw))

        response_body, first_token_at = await _collect_images_api_response_from_sse(
            stream_owner.iter_raw(),
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
        await session.close(reason="image_non_stream_finished")


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

    payload = _load_error_mapping(error_text)
    if not isinstance(payload, dict):
        return False

    if status_code == 402:
        detail = payload.get("detail")
        if isinstance(detail, dict):
            detail_code = str(detail.get("code") or "").strip()
            if detail_code == "deactivated_workspace":
                return True

    error = _extract_error_object(error_text) if status_code == 401 else None
    if isinstance(error, dict):
        error_code = str(error.get("code") or "").strip().lower()
        if error_code in {"account_deactivated", "token_invalidated"}:
            return True
        error_message = str(error.get("message") or "").strip().lower()
        if "authentication token has been invalidated" in error_message and "signing in again" in error_message:
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


def _sanitized_permanent_refresh_failure_detail() -> str:
    return "Codex token credentials are no longer valid; the key was disabled and failover was attempted."


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
        "cached_input_tokens": usage_metrics.cached_input_tokens,
        "estimated_cost_usd": usage_metrics.estimated_cost_usd,
    }


def _response_id_from_proxy_result(proxy_result: ProxyRequestResult) -> str | None:
    response_id = _normalize_optional_text(getattr(proxy_result, "response_id", None))
    if response_id:
        return response_id
    stream_capture = getattr(proxy_result, "stream_capture", None)
    if stream_capture is not None:
        return _extract_response_id_from_payload(stream_capture.response_payload)
    body = getattr(getattr(proxy_result, "response", None), "body", None)
    if isinstance(body, (bytes, bytearray)):
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            return None
        return _extract_response_id_from_payload(payload)
    return None


async def _record_prompt_cache_success(
    app: FastAPI,
    *,
    prompt_cache_context: _PromptCacheRequestContext | None,
    token_id: int,
    proxy_result: ProxyRequestResult,
) -> None:
    if prompt_cache_context is None:
        return
    try:
        _prompt_cache_bind_lane(app, prompt_cache_context, int(token_id))
        _prompt_cache_bind_response(app, _response_id_from_proxy_result(proxy_result), int(token_id))
    except Exception:
        logger.exception("Failed to update prompt cache affinity state")


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
    client, _, _ = _select_response_http_client(
        app,
        token_id=int(getattr(token_row, "id", 0) or 0),
        account_id=getattr(token_row, "account_id", None),
    )
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
    extra_finalize_kwargs: dict[str, Any] | None = None,
) -> int | None:
    try:
        stream_capture.finalize_usage_metrics()
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
            **(extra_finalize_kwargs or {}),
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
    proxy_lease: ProxyConcurrencyLease,
    timing: _RequestTimingRecorder | None,
) -> StreamingResponse:
    output_queue: asyncio.Queue[bytes | object] = asyncio.Queue()
    queue_done_sentinel = object()
    heartbeat_interval_seconds = _stream_keepalive_interval_seconds()
    first_output_timeout_seconds = _image_stream_first_output_timeout_seconds()
    response_body_started = False
    worker_task: asyncio.Task[None] | None = None
    worker_cleanup_lock = asyncio.Lock()
    response_session = StreamingProxySession(label="gpt_image_keepalive_response")

    async def worker() -> None:
        timing_context_token = _REQUEST_TIMING.set(timing) if timing is not None else None
        db_timing_context_token = set_db_timing_recorder(timing) if timing is not None else None
        oauth_manager: CodexOAuthManager = http_request.app.state.oauth_manager
        selection_settings = _current_token_selection_settings(http_request.app)

        attempt_count = 0
        last_token_id: int | None = None
        last_account_id: str | None = None
        stream_finalized = False
        restricted_model_name = _non_free_only_codex_model_name(request_model)
        require_non_free_token = restricted_model_name is not None
        restricted_model_label = restricted_model_name or "requested"
        scoped_cooldown_scope = _codex_request_scoped_cooldown_scope(request_model)
        skipped_free_token_ids: set[int] = set()
        incompatible_model_token_ids: set[int] = set()
        excluded_token_ids: set[int] = set()
        postponed_unknown_plan_tokens: list[Any] = []

        try:
            await _record_event_loop_lag(timing)
            max_attempts = _effective_proxy_max_attempts(endpoint=endpoint)
            max_claims = _max_request_account_retries()
            if _model_compatibility_scoped_cooldown_scope(request_model) is not None:
                max_claims = max(max_claims, _model_compatibility_max_token_probes())
            claim_count = 0
            last_error: HTTPException | None = None
            transport_error_count = 0
            upstream_5xx_error_count = 0

            while attempt_count - len(incompatible_model_token_ids) < max_attempts and claim_count < max_claims:
                claim_count += 1
                token_row = await _await_timed(
                    _claim_next_token_for_model_request(
                        selection_settings,
                        exclude_token_ids=excluded_token_ids,
                        scoped_cooldown_scope=scoped_cooldown_scope,
                        timing=timing,
                        token_pool_snapshot=_fresh_token_pool_snapshot(http_request.app, timing=timing),
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
                client, upstream_breaker, upstream_shard_key = _select_response_http_client(
                    http_request.app,
                    token_id=token_row.id,
                    account_id=token_row.account_id,
                    request_id=request_log.request_id,
                )
                if upstream_breaker.is_open():
                    excluded_token_ids.add(token_row.id)
                    last_error = _upstream_circuit_open_exception(upstream_breaker)
                    logger.info(
                        "Skipping Codex token because upstream shard circuit is open: token_id=%s account_id=%s shard=%s",
                        token_row.id,
                        token_row.account_id,
                        upstream_shard_key,
                    )
                    _record_selected_token_observation_finish(
                        token_row.id,
                        started_at=request_log.started_at,
                        first_token_at=None,
                    )
                    continue

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
                    if permanent_refresh_failure or exc.status_code in (401, 403):
                        _exclude_auth_failed_token_for_request(excluded_token_ids, token_row)
                        last_error = (
                            HTTPException(
                                status_code=503,
                                detail=_sanitized_permanent_refresh_failure_detail(),
                            )
                            if permanent_refresh_failure
                            else exc
                        )
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
                        proxy_result_on_close = getattr(proxy_result, "on_close", None)
                        if proxy_result_on_close is not None:
                            await _await_cleanup_task_safely(
                                proxy_result_on_close(),
                                label="GPT image proxy result close",
                            )

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
                    upstream_breaker.record_success()
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
                    if _is_model_unsupported_for_chatgpt_account_error(
                        status_code,
                        detail,
                        request_model=request_model,
                    ):
                        incompatible_model_token_ids.add(token_row.id)
                        await _mark_model_unsupported_token_for_request(
                            http_request,
                            token_row,
                            request_model=request_model,
                            detail=detail,
                            excluded_token_ids=excluded_token_ids,
                        )
                        logger.info(
                            "Skipping Codex token because account does not support %s: token_id=%s account_id=%s attempt=%s/%s",
                            restricted_model_label,
                            token_row.id,
                            token_row.account_id,
                            attempt,
                            max_attempts,
                        )
                        last_error = exc
                        continue

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
                    auth_failed_after_refresh = status_code in (401, 403) and (
                        access_token_refreshed or is_access_token_only_refresh_token(token_row.refresh_token)
                    )

                    if permanently_disabled or auth_failed_after_refresh:
                        oauth_manager.invalidate(token_row.id)
                        _exclude_auth_failed_token_for_request(excluded_token_ids, token_row)
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
                        _exclude_auth_failed_token_for_request(excluded_token_ids, token_row)
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
                    retryable_server_error = _should_retry_http_exception(exc)
                    if retryable_server_error:
                        upstream_5xx_error_count += 1
                    if (
                        retryable_server_error
                        and upstream_5xx_error_count <= _upstream_5xx_max_retries()
                        and attempt < max_attempts
                    ):
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

                    if retryable_server_error:
                        await mark_token_error(token_row.id, detail)

                    raise
                except httpx.HTTPError as exc:
                    message = f"Upstream request failed: {type(exc).__name__}: {exc}"
                    if _should_record_upstream_circuit_failure(exc):
                        upstream_breaker.record_failure()
                    transport_error_count += 1
                    compact_server_error_cooling_time = _get_compact_codex_server_error_cooling_time(
                        compact=compact,
                        status_code=500,
                        error_text=message,
                    )
                    if (
                        transport_error_count <= _transport_error_max_retries()
                        and attempt < max_attempts
                        and not upstream_breaker.is_open()
                    ):
                        excluded_token_ids.add(token_row.id)
                        mark_kwargs: dict[str, Any] = {}
                        if compact_server_error_cooling_time > 0:
                            mark_kwargs["cooldown_seconds"] = compact_server_error_cooling_time
                        if _is_local_upstream_pool_timeout(exc):
                            logger.warning(
                                "Local upstream HTTP pool exhausted before response commit: token_id=%s account_id=%s shard=%s attempt=%s/%s",
                                token_row.id,
                                token_row.account_id,
                                upstream_shard_key,
                                attempt,
                                max_attempts,
                            )
                        else:
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
                    if not _is_local_upstream_pool_timeout(exc):
                        await mark_token_error(token_row.id, message)
                    if upstream_breaker.is_open():
                        raise _upstream_circuit_open_exception(upstream_breaker) from exc
                    raise HTTPException(status_code=502, detail=message) from exc
                finally:
                    _record_selected_token_observation_finish(
                        token_row.id,
                        started_at=request_log.started_at,
                        first_token_at=token_observation_first_token_at,
                    )

            if require_non_free_token and incompatible_model_token_ids:
                raise HTTPException(
                    status_code=503,
                    detail=_model_unsupported_codex_token_unavailable_detail(restricted_model_label),
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
                await proxy_lease.release()
            finally:
                if timing_context_token is not None:
                    _REQUEST_TIMING.reset(timing_context_token)
                if db_timing_context_token is not None:
                    reset_db_timing_recorder(db_timing_context_token)

    async def cancel_worker_task() -> None:
        task = worker_task
        if task is None:
            return
        async with worker_cleanup_lock:
            try:
                await response_session.stop_workers(reason="gpt_image_response_close")
            except Exception:
                logger.exception("GPT image stream worker failed during shutdown")

    async def response_body() -> AsyncGenerator[bytes, None]:
        nonlocal response_body_started, worker_task
        response_body_started = True
        worker_task = asyncio.create_task(worker())
        response_session.add_worker(worker_task)
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
            await _await_cleanup_task_safely(
                cancel_worker_task(),
                label="GPT image stream worker",
            )

    async def on_response_close() -> None:
        if response_body_started:
            await cancel_worker_task()
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
            await proxy_lease.release()

    return _FinalizingStreamingResponse(
        response_body(),
        on_close=on_response_close,
        media_type="text/event-stream",
        headers=_sse_response_headers(),
    )


async def _responses_stream_keepalive_response(
    http_request: Request,
    *,
    endpoint: str,
    request_model: str | None,
    compact: bool,
    request_data: ResponsesRequest,
    prompt_cache_context: _PromptCacheRequestContext | None = None,
    session_id: str | None = None,
    session_id_source: str | None = None,
    prompt_cache_trace: dict[str, Any] | None = None,
) -> Response:
    try:
        proxy_lease = await _proxy_concurrency_limiter(http_request.app).acquire(
            timeout_seconds=_proxy_queue_timeout_seconds(),
        )
    except TimeoutError:
        retry_after = max(1, int(math.ceil(_proxy_queue_timeout_seconds())))
        return JSONResponse(
            status_code=503,
            content={"error": "Too many active proxy requests; retry later"},
            headers={"Retry-After": str(retry_after)},
        )

    response_traffic: ResponseTrafficController = http_request.app.state.response_traffic
    response_lease = response_traffic.start_response()
    timing = _RequestTimingRecorder()
    request_started_at = utcnow()
    request_log = _RequestLogHandle.start(
        endpoint=endpoint,
        model=request_model,
        is_stream=True,
        started_at=request_started_at,
        client_ip=http_request.client.host if http_request.client is not None else None,
        user_agent=(http_request.headers.get("User-Agent") or "").strip() or None,
        timing=timing,
        prompt_cache_trace=prompt_cache_trace if isinstance(prompt_cache_trace, dict) else None,
    )

    raw_payload = request_data.model_dump(exclude_unset=True)
    payload = _sanitize_codex_payload(
        copy.deepcopy(raw_payload),
        compact=compact,
        preserve_previous_response_id=False,
    )
    _apply_prompt_cache_context_to_payload(payload, prompt_cache_context)
    payload["stream"] = True
    json_payload = json.dumps(payload, ensure_ascii=False)

    resolved_session_id = session_id
    resolved_session_id_source = session_id_source
    if not resolved_session_id or not resolved_session_id_source:
        resolved_session_id, resolved_session_id_source = _prompt_cache_session_context(
            http_request,
            prompt_cache_context,
        )

    if isinstance(prompt_cache_trace, dict):
        prompt_cache_trace.clear()
        prompt_cache_trace.update(
            build_prompt_cache_trace(
                endpoint=endpoint,
                model=request_data.model,
                compact=compact,
                raw_payload=raw_payload,
                upstream_payload=payload,
                prompt_cache_context=prompt_cache_context,
                session_id=resolved_session_id,
                session_id_source=resolved_session_id_source,
            )
        )

    attempt_count = 0
    last_token_id: int | None = None
    last_account_id: str | None = None
    stream_finalized = False
    restricted_model_name = _non_free_only_codex_model_name(request_model)
    require_non_free_token = restricted_model_name is not None
    restricted_model_label = restricted_model_name or "requested"
    scoped_cooldown_scope = _codex_request_scoped_cooldown_scope(request_model)
    skipped_free_token_ids: set[int] = set()
    incompatible_model_token_ids: set[int] = set()
    excluded_token_ids: set[int] = set()
    postponed_unknown_plan_tokens: list[Any] = []
    typed_keepalive_sent = False
    last_affinity_result: str | None = None
    last_affinity_lane_index: int | None = None
    output_queue: asyncio.Queue[bytes | object] = asyncio.Queue()
    queue_done_sentinel = object()
    worker_task: asyncio.Task[None] | None = None
    worker_cleanup_lock = asyncio.Lock()
    response_session = StreamingProxySession(label="responses_keepalive_response")

    def prompt_cache_log_kwargs(
        stream_capture: _ProxyStreamCapture | None = None,
        *,
        status_code: int | None = None,
    ) -> dict[str, Any]:
        trace = request_log.prompt_cache_trace if isinstance(request_log.prompt_cache_trace, dict) else None
        usage_metrics = getattr(stream_capture, "usage_metrics", None) if stream_capture is not None else None
        upstream_response_id = None
        if stream_capture is not None:
            stream_capture.finalize_usage_metrics()
            usage_metrics = stream_capture.usage_metrics or usage_metrics
            upstream_response_id = _extract_response_id_from_payload(stream_capture.response_payload)
        _prompt_cache_update_runtime_trace_fields(
            trace,
            session_id=resolved_session_id,
            session_id_source=resolved_session_id_source,
            upstream_response_id=upstream_response_id,
            status_code=status_code,
            usage_metrics=usage_metrics,
            cache_affinity_result=last_affinity_result,
            cache_affinity_lane_index=last_affinity_lane_index,
            cache_affinity_lane_count=_prompt_cache_lane_count(http_request.app, prompt_cache_context),
        )
        return _prompt_cache_finalize_kwargs(
            prompt_cache_context,
            prompt_cache_trace=trace,
            affinity_result=last_affinity_result,
            lane_index=last_affinity_lane_index,
            status_code=status_code,
            usage_metrics=usage_metrics,
            upstream_response_id=upstream_response_id,
        )

    async def worker(output_queue: asyncio.Queue[bytes | object], queue_done_sentinel: object) -> None:
        nonlocal attempt_count, last_token_id, last_account_id, stream_finalized, typed_keepalive_sent
        nonlocal last_affinity_result, last_affinity_lane_index
        last_error: HTTPException | None = None
        transport_error_count = 0
        upstream_5xx_error_count = 0
        oauth_manager: CodexOAuthManager = http_request.app.state.oauth_manager
        selection_settings = _current_token_selection_settings(http_request.app)
        current_json_payload = json_payload
        invalid_encrypted_content_recovery_tried = False
        retry_token_row: Any | None = None
        timing_context_token = _REQUEST_TIMING.set(timing)
        db_timing_context_token = set_db_timing_recorder(timing)

        async def finalize_success(
            *,
            token_row: Any,
            stream_capture: _ProxyStreamCapture,
            attempt: int,
            upstream_breaker: UpstreamCircuitBreaker,
        ) -> None:
            nonlocal stream_finalized
            await _finalize_stream_request_log(
                request_log_ref=request_log,
                status_code=200,
                attempt_count=attempt,
                token_id=token_row.id,
                account_id=token_row.account_id,
                fallback_model_name=stream_capture.model_name or request_model,
                fallback_first_token_at=stream_capture.first_token_at,
                stream_capture=stream_capture,
                timing=timing,
                success_hook=None,
                extra_finalize_kwargs=prompt_cache_log_kwargs(stream_capture, status_code=200),
            )
            await _record_prompt_cache_success(
                http_request.app,
                prompt_cache_context=prompt_cache_context,
                token_id=token_row.id,
                proxy_result=SimpleNamespace(
                    response_id=_extract_response_id_from_payload(stream_capture.response_payload),
                    stream_capture=stream_capture,
                ),
            )
            await _mark_token_success_with_timing(
                timing,
                token_row.id,
                token_row=token_row,
            )
            upstream_breaker.record_success()
            stream_finalized = True

        async def finalize_failure(
            *,
            token_row: Any,
            attempt: int,
            stream_capture: _ProxyStreamCapture | None,
            status_code: int,
            detail: str,
        ) -> None:
            nonlocal stream_finalized
            await _finalize_request_log_with_timing(
                timing,
                request_log,
                status_code=status_code,
                success=False,
                attempt_count=attempt,
                finished_at=utcnow(),
                first_token_at=(
                    stream_capture.first_token_at if stream_capture is not None else None
                ),
                token_id=token_row.id,
                account_id=token_row.account_id,
                model_name=(
                    stream_capture.model_name if stream_capture is not None else request_model
                ),
                error_message=detail,
                **prompt_cache_log_kwargs(stream_capture, status_code=status_code),
            )
            stream_finalized = True

        async def emit_terminal_error(status_code: int, detail: str) -> None:
            await output_queue.put(_build_stream_error_event(status_code, detail))
            await output_queue.put(b"data: [DONE]\n\n")

        try:
            await _record_event_loop_lag(timing)
            max_attempts = _effective_proxy_max_attempts(endpoint=endpoint)
            max_claims = _max_request_account_retries()
            if _model_compatibility_scoped_cooldown_scope(request_model) is not None:
                max_claims = max(max_claims, _model_compatibility_max_token_probes())
            claim_count = 0

            while retry_token_row is not None or (
                attempt_count - len(incompatible_model_token_ids) < max_attempts and claim_count < max_claims
            ):
                using_postponed_unknown_plan = False
                if retry_token_row is not None:
                    token_row = retry_token_row
                    retry_token_row = None
                    _record_selected_token_observation_start(token_row.id, timing)
                else:
                    claim_count += 1
                    token_pool_snapshot = _fresh_token_pool_snapshot(http_request.app, timing=timing)
                    affinity_claim = await _await_timed(
                        _claim_prompt_cache_affinity_token(
                            http_request.app,
                            selection_settings,
                            prompt_cache_context=prompt_cache_context,
                            exclude_token_ids=excluded_token_ids,
                            scoped_cooldown_scope=scoped_cooldown_scope,
                            timing=timing,
                            token_pool_snapshot=token_pool_snapshot,
                            require_non_free_token=require_non_free_token,
                        ),
                        timing=timing,
                        span_name="claim_prompt_cache_affinity_ms",
                        db_wait=False,
                    )
                    if affinity_claim is not None:
                        token_row = affinity_claim.token_row
                        last_affinity_result = affinity_claim.result
                        last_affinity_lane_index = affinity_claim.lane_index
                        if timing is not None:
                            timing.set_tag("cache_affinity_result", affinity_claim.result)
                            if affinity_claim.lane_index is not None:
                                timing.set_ms("cache_affinity_lane_index", affinity_claim.lane_index)
                    elif (
                        prompt_cache_context is not None
                        and prompt_cache_context.previous_response_id
                        and _prompt_cache_previous_strict_enabled()
                        and _prompt_cache_response_owner(http_request.app, prompt_cache_context.previous_response_id)
                        is not None
                    ):
                        last_affinity_result = "previous_owner_busy"
                        last_affinity_lane_index = None
                        raise HTTPException(status_code=429, detail="previous_response_busy")
                    else:
                        if prompt_cache_context is not None and prompt_cache_context.affinity_key and not _prompt_cache_global_fallback_enabled():
                            raise HTTPException(status_code=429, detail="Prompt cache affinity lanes are busy")
                        if prompt_cache_context is not None and prompt_cache_context.affinity_key:
                            last_affinity_result = "global_fallback"
                            last_affinity_lane_index = None
                        token_row = await _await_timed(
                            _claim_next_token_for_model_request(
                                selection_settings,
                                exclude_token_ids=excluded_token_ids,
                                scoped_cooldown_scope=scoped_cooldown_scope,
                                timing=timing,
                                token_pool_snapshot=token_pool_snapshot,
                                require_non_free_token=require_non_free_token,
                            ),
                            timing=timing,
                            span_name="claim_token_ms",
                            db_wait=True,
                        )

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
                        _record_selected_token_observation_finish(
                            token_row.id,
                            started_at=request_started_at,
                            first_token_at=None,
                        )
                        continue
                    if effective_plan_type is None and not using_postponed_unknown_plan:
                        excluded_token_ids.add(token_row.id)
                        postponed_unknown_plan_tokens.append(token_row)
                        _record_selected_token_observation_finish(
                            token_row.id,
                            started_at=request_started_at,
                            first_token_at=None,
                        )
                        continue

                if prompt_cache_context is not None and prompt_cache_context.affinity_key:
                    bound_lane_index = _prompt_cache_bind_lane(
                        http_request.app,
                        prompt_cache_context,
                        int(token_row.id),
                        prefer_primary=last_affinity_result in {"previous_owner_hit", "primary_hit"},
                    )
                    if last_affinity_lane_index is None:
                        last_affinity_lane_index = bound_lane_index
                    if timing is not None:
                        if last_affinity_result:
                            timing.set_tag("cache_affinity_result", last_affinity_result)
                        if last_affinity_lane_index is not None:
                            timing.set_ms("cache_affinity_lane_index", last_affinity_lane_index)

                attempt_count += 1
                attempt = attempt_count
                last_token_id = token_row.id
                last_account_id = token_row.account_id
                client, upstream_breaker, upstream_shard_key = _select_response_http_client(
                    http_request.app,
                    token_id=token_row.id,
                    account_id=token_row.account_id,
                    request_id=request_log.request_id,
                )
                if upstream_breaker.is_open():
                    excluded_token_ids.add(token_row.id)
                    last_error = _upstream_circuit_open_exception(upstream_breaker)
                    logger.info(
                        "Skipping Codex token because upstream shard circuit is open: token_id=%s account_id=%s shard=%s",
                        token_row.id,
                        token_row.account_id,
                        upstream_shard_key,
                    )
                    _record_selected_token_observation_finish(
                        token_row.id,
                        started_at=request_started_at,
                        first_token_at=None,
                    )
                    continue

                try:
                    with timing.measure("oauth_ms") if timing is not None else suppress():
                        access_token, access_token_refreshed = await oauth_manager.get_access_token(
                            token_row,
                            client,
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
                    if permanent_refresh_failure or exc.status_code in (401, 403):
                        _exclude_auth_failed_token_for_request(
                            excluded_token_ids,
                            token_row,
                            app=http_request.app,
                        )
                        last_error = (
                            HTTPException(
                                status_code=503,
                                detail=_sanitized_permanent_refresh_failure_detail(),
                            )
                            if permanent_refresh_failure
                            else exc
                        )
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

                token_observation_first_token_at: datetime | None = None
                attempt_session: StreamingProxySession | None = None
                try:
                    headers = _build_upstream_headers(
                        http_request,
                        access_token=access_token,
                        account_id=token_row.account_id,
                        stream=True,
                        prompt_cache_context=prompt_cache_context,
                        session_id=resolved_session_id,
                    )
                    attempt_session = StreamingProxySession(label="responses_keepalive_worker")
                    stream_owner = await attempt_session.open_owner(
                        client,
                        "POST",
                        _codex_responses_url(compact=compact),
                        headers=headers,
                        content=current_json_payload,
                        timeout=_upstream_http_timeout(read=None),
                        compact=compact,
                    )
                    upstream_response = stream_owner.response
                    if upstream_response.status_code < 200 or upstream_response.status_code >= 300:
                        upstream_status_code = upstream_response.status_code
                        raw = await upstream_response.aread()
                        raise _upstream_error_http_exception(
                            upstream_status_code,
                            _decode_error_body(raw),
                        )

                    upstream_iter = stream_owner.iter_raw()
                    stream_capture = _ProxyStreamCapture(
                        initial_model_name=request_model,
                        initial_first_token_at=None,
                    )
                    decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
                    text_buffer = ""
                    sse_scan_start = 0
                    buffered_chunks: list[bytes] = []
                    stream_committed = False

                    try:
                        while not stream_committed:
                            try:
                                chunk = await upstream_iter.__anext__()
                            except StopAsyncIteration as exc:
                                if not buffered_chunks:
                                    raise HTTPException(
                                        status_code=502,
                                        detail="Upstream closed stream without data",
                                    ) from exc
                                if text_buffer.strip():
                                    raise HTTPException(
                                        status_code=502,
                                        detail="Upstream closed stream with an incomplete SSE event",
                                    ) from exc
                                raise HTTPException(
                                    status_code=502,
                                    detail="Upstream closed stream without producing a response",
                                ) from exc

                            stream_capture.feed(chunk)
                            buffered_chunks.append(chunk)
                            text_buffer += decoder.decode(chunk)

                            while True:
                                raw_event, text_buffer, sse_scan_start = _pop_sse_event_from_buffer(
                                    text_buffer,
                                    sse_scan_start,
                                )
                                if raw_event is None:
                                    break

                                if not raw_event.strip():
                                    continue

                                event_type, event_payload = _extract_responses_stream_event(raw_event)
                                if event_type == "[DONE]":
                                    raise HTTPException(
                                        status_code=502,
                                        detail="Upstream closed stream without producing a response",
                                    )

                                semantic_failure = _responses_failure_http_exception(event_payload)
                                if semantic_failure is not None:
                                    raise semantic_failure

                                if event_type == "response.created" and not typed_keepalive_sent:
                                    await output_queue.put(_build_stream_keepalive_event())
                                    typed_keepalive_sent = True

                                if event_type and event_type not in RESPONSES_STREAM_PREFLIGHT_EVENTS:
                                    stream_committed = True
                                    break

                            if stream_committed:
                                for buffered_chunk in buffered_chunks:
                                    await output_queue.put(buffered_chunk)
                                buffered_chunks.clear()

                        async for chunk in upstream_iter:
                            stream_capture.feed(chunk)
                            await output_queue.put(chunk)
                    except RESPONSES_STREAM_NETWORK_ERRORS as exc:
                        if stream_committed:
                            error_event = _build_stream_error_event(
                                502,
                                f"Upstream stream aborted before completion: {type(exc).__name__}: {str(exc) or type(exc).__name__}",
                            )
                            stream_capture.feed(error_event)
                            await output_queue.put(error_event)
                            await output_queue.put(b"data: [DONE]\n\n")
                            await finalize_failure(
                                token_row=token_row,
                                attempt=attempt,
                                stream_capture=stream_capture,
                                status_code=502,
                                detail=(
                                    f"Upstream stream aborted before completion: {type(exc).__name__}: {str(exc) or type(exc).__name__}"
                                ),
                            )
                            await mark_token_error(token_row.id, stream_capture.error_message or str(exc))
                            token_observation_first_token_at = stream_capture.first_token_at
                            stream_finalized = True
                            return
                        raise HTTPException(
                            status_code=502,
                            detail=f"Upstream request failed: {type(exc).__name__}: {exc}",
                        ) from exc

                    token_observation_first_token_at = stream_capture.first_token_at
                    await finalize_success(
                        token_row=token_row,
                        stream_capture=stream_capture,
                        attempt=attempt,
                        upstream_breaker=upstream_breaker,
                    )
                    stream_finalized = True
                    return
                except HTTPException as exc:
                    status_code = getattr(exc, "status_code", 500)
                    detail = str(getattr(exc, "detail", "") or exc)
                    if (
                        not invalid_encrypted_content_recovery_tried
                        and _is_invalid_encrypted_content_error(status_code, detail)
                        and _trim_invalid_encrypted_reasoning_items(payload)
                    ):
                        invalid_encrypted_content_recovery_tried = True
                        current_json_payload = json.dumps(payload, ensure_ascii=False)
                        retry_token_row = token_row
                        logger.info(
                            "Retrying Codex stream once on the same account after invalid encrypted reasoning content: "
                            "token_id=%s account_id=%s attempt=%s",
                            token_row.id,
                            token_row.account_id,
                            attempt,
                        )
                        continue

                    if _is_model_unsupported_for_chatgpt_account_error(
                        status_code,
                        detail,
                        request_model=request_model,
                    ):
                        incompatible_model_token_ids.add(token_row.id)
                        await _mark_model_unsupported_token_for_request(
                            http_request,
                            token_row,
                            request_model=request_model,
                            detail=detail,
                            excluded_token_ids=excluded_token_ids,
                        )
                        logger.info(
                            "Skipping Codex token because account does not support %s: token_id=%s account_id=%s attempt=%s/%s",
                            restricted_model_label,
                            token_row.id,
                            token_row.account_id,
                            attempt,
                            max_attempts,
                        )
                        last_error = exc
                        continue

                    cooldown_seconds = _extract_usage_limit_cooldown_seconds(status_code, detail)

                    if cooldown_seconds is not None:
                        await mark_token_error(
                            token_row.id,
                            detail,
                            cooldown_seconds=cooldown_seconds,
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
                        last_error = exc
                        continue

                    if _should_record_http_exception_token_error(exc):
                        await mark_token_error(token_row.id, detail)

                    permanently_disabled = _is_permanent_account_disable_error(status_code, detail)
                    auth_failed_after_refresh = status_code in (401, 403) and (
                        access_token_refreshed or is_access_token_only_refresh_token(token_row.refresh_token)
                    )

                    if permanently_disabled or auth_failed_after_refresh:
                        oauth_manager.invalidate(token_row.id)
                        _exclude_auth_failed_token_for_request(
                            excluded_token_ids,
                            token_row,
                            app=http_request.app,
                        )
                        await mark_token_error(
                            token_row.id,
                            detail,
                            deactivate=True,
                            clear_access_token=True,
                        )
                        last_error = exc
                        continue

                    if status_code in (401, 403):
                        oauth_manager.invalidate(token_row.id)
                        _exclude_auth_failed_token_for_request(
                            excluded_token_ids,
                            token_row,
                            app=http_request.app,
                        )
                        await mark_token_error(token_row.id, detail, clear_access_token=True)
                        last_error = exc
                        continue

                    compact_server_error_cooling_time = _get_compact_codex_server_error_cooling_time(
                        compact=compact,
                        status_code=status_code,
                        error_text=detail,
                    )
                    retryable_server_error = _should_retry_http_exception(exc)
                    if retryable_server_error:
                        upstream_5xx_error_count += 1
                    if (
                        retryable_server_error
                        and upstream_5xx_error_count <= _upstream_5xx_max_retries()
                        and attempt < max_attempts
                    ):
                        excluded_token_ids.add(token_row.id)
                        mark_kwargs: dict[str, Any] = {}
                        if compact_server_error_cooling_time > 0:
                            mark_kwargs["cooldown_seconds"] = compact_server_error_cooling_time
                        await mark_token_error(token_row.id, detail, **mark_kwargs)
                        if compact_server_error_cooling_time > 0:
                            _prompt_cache_remove_token(http_request.app, token_row.id)
                        last_error = exc
                        continue

                    if retryable_server_error:
                        await mark_token_error(token_row.id, detail)

                    await emit_terminal_error(status_code, detail)
                    await finalize_failure(
                        token_row=token_row,
                        attempt=attempt,
                        stream_capture=None,
                        status_code=status_code,
                        detail=detail,
                    )
                    stream_finalized = True
                    return
                except httpx.HTTPError as exc:
                    message = f"Upstream request failed: {type(exc).__name__}: {exc}"
                    if _should_record_upstream_circuit_failure(exc):
                        upstream_breaker.record_failure()
                    transport_error_count += 1
                    compact_server_error_cooling_time = _get_compact_codex_server_error_cooling_time(
                        compact=compact,
                        status_code=500,
                        error_text=message,
                    )
                    if (
                        transport_error_count <= _transport_error_max_retries()
                        and attempt < max_attempts
                        and not upstream_breaker.is_open()
                    ):
                        excluded_token_ids.add(token_row.id)
                        mark_kwargs: dict[str, Any] = {}
                        if compact_server_error_cooling_time > 0:
                            mark_kwargs["cooldown_seconds"] = compact_server_error_cooling_time
                        if _is_local_upstream_pool_timeout(exc):
                            logger.warning(
                                "Local upstream HTTP pool exhausted before response commit: token_id=%s account_id=%s shard=%s attempt=%s/%s",
                                token_row.id,
                                token_row.account_id,
                                upstream_shard_key,
                                attempt,
                                max_attempts,
                            )
                        else:
                            await mark_token_error(token_row.id, message, **mark_kwargs)
                            if compact_server_error_cooling_time > 0:
                                _prompt_cache_remove_token(http_request.app, token_row.id)
                        last_error = HTTPException(status_code=502, detail=message)
                        continue
                    if not _is_local_upstream_pool_timeout(exc):
                        await mark_token_error(token_row.id, message)
                    if upstream_breaker.is_open():
                        raise _upstream_circuit_open_exception(upstream_breaker) from exc
                    await emit_terminal_error(502, message)
                    await finalize_failure(
                        token_row=token_row,
                        attempt=attempt,
                        stream_capture=None,
                        status_code=502,
                        detail=message,
                    )
                    stream_finalized = True
                    return
                except Exception as exc:
                    if not _is_retryable_upstream_transport_exception(exc):
                        raise
                    message = f"Upstream request failed: {type(exc).__name__}: {exc}"
                    transport_error_count += 1
                    compact_server_error_cooling_time = _get_compact_codex_server_error_cooling_time(
                        compact=compact,
                        status_code=500,
                        error_text=message,
                    )
                    if transport_error_count <= _transport_error_max_retries() and attempt < max_attempts:
                        excluded_token_ids.add(token_row.id)
                        mark_kwargs: dict[str, Any] = {}
                        if compact_server_error_cooling_time > 0:
                            mark_kwargs["cooldown_seconds"] = compact_server_error_cooling_time
                        await mark_token_error(token_row.id, message, **mark_kwargs)
                        if compact_server_error_cooling_time > 0:
                            _prompt_cache_remove_token(http_request.app, token_row.id)
                        last_error = HTTPException(status_code=502, detail=message)
                        continue
                    await mark_token_error(token_row.id, message)
                    await emit_terminal_error(502, message)
                    await finalize_failure(
                        token_row=token_row,
                        attempt=attempt,
                        stream_capture=None,
                        status_code=502,
                        detail=message,
                    )
                    stream_finalized = True
                    return
                finally:
                    if attempt_session is not None:
                        await attempt_session.close(reason="responses_keepalive_attempt_finished")
                    _record_selected_token_observation_finish(
                        token_row.id,
                        started_at=request_started_at,
                        first_token_at=token_observation_first_token_at,
                    )

            if require_non_free_token and incompatible_model_token_ids:
                detail = _model_unsupported_codex_token_unavailable_detail(restricted_model_label)
                await output_queue.put(_build_stream_error_event(503, detail))
                await output_queue.put(b"data: [DONE]\n\n")
                await _finalize_request_log_with_timing(
                    timing,
                    request_log,
                    status_code=503,
                    success=False,
                    attempt_count=attempt_count,
                    finished_at=utcnow(),
                    token_id=last_token_id,
                    account_id=last_account_id,
                    error_message=detail,
                    **prompt_cache_log_kwargs(status_code=503),
                )
                stream_finalized = True
                return

            if last_error is not None:
                await output_queue.put(
                    _build_stream_error_event(
                        getattr(last_error, "status_code", 500),
                        str(getattr(last_error, "detail", "") or last_error),
                    )
                )
                await output_queue.put(b"data: [DONE]\n\n")
                await _finalize_request_log_with_timing(
                    timing,
                    request_log,
                    status_code=getattr(last_error, "status_code", 500),
                    success=False,
                    attempt_count=attempt_count,
                    finished_at=utcnow(),
                    token_id=last_token_id,
                    account_id=last_account_id,
                    error_message=str(getattr(last_error, "detail", "") or last_error),
                    **prompt_cache_log_kwargs(status_code=getattr(last_error, "status_code", 500)),
                )
                stream_finalized = True
                return

            if require_non_free_token and skipped_free_token_ids:
                detail = _non_free_codex_token_unavailable_detail(restricted_model_label)
                await output_queue.put(_build_stream_error_event(503, detail))
                await output_queue.put(b"data: [DONE]\n\n")
                await _finalize_request_log_with_timing(
                    timing,
                    request_log,
                    status_code=503,
                    success=False,
                    attempt_count=attempt_count,
                    finished_at=utcnow(),
                    token_id=last_token_id,
                    account_id=last_account_id,
                    error_message=detail,
                    **prompt_cache_log_kwargs(status_code=503),
                )
                stream_finalized = True
                return

            detail = "No available Codex token could satisfy this request"
            await output_queue.put(_build_stream_error_event(503, detail))
            await output_queue.put(b"data: [DONE]\n\n")
            await _finalize_request_log_with_timing(
                timing,
                request_log,
                status_code=503,
                success=False,
                attempt_count=attempt_count,
                finished_at=utcnow(),
                token_id=last_token_id,
                account_id=last_account_id,
                error_message=detail,
                **prompt_cache_log_kwargs(status_code=503),
            )
            stream_finalized = True
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
                await proxy_lease.release()
            finally:
                if timing_context_token is not None:
                    _REQUEST_TIMING.reset(timing_context_token)
                if db_timing_context_token is not None:
                    reset_db_timing_recorder(db_timing_context_token)

    async def cancel_worker_task() -> None:
        task = worker_task
        if task is None:
            return
        async with worker_cleanup_lock:
            try:
                await response_session.stop_workers(reason="responses_body_close")
            except Exception:
                logger.exception("Responses stream worker failed during shutdown")

    async def read_first_body_item() -> bytes | None:
        while True:
            item = await output_queue.get()
            if item is queue_done_sentinel:
                return None
            if isinstance(item, bytes):
                return item

    async def response_body(first_item: bytes) -> AsyncGenerator[bytes, None]:
        try:
            yield first_item
            while True:
                item = await output_queue.get()
                if item is queue_done_sentinel:
                    break
                if isinstance(item, bytes):
                    yield item
        finally:
            await _await_cleanup_task_safely(
                cancel_worker_task(),
                label="Responses stream worker",
            )

    async def on_response_close() -> None:
        if worker_task is not None and not worker_task.done():
            await cancel_worker_task()

    worker_task = asyncio.create_task(worker(output_queue, queue_done_sentinel))
    response_session.add_worker(worker_task)
    try:
        first_item = await read_first_body_item()
    except BaseException:
        await cancel_worker_task()
        raise

    if first_item is None:
        first_item = _build_stream_error_event(500, "Responses stream worker finished without a body event")
        await output_queue.put(b"data: [DONE]\n\n")

    status_code = _stream_error_status_code_from_chunk(first_item) or 200
    return _FinalizingStreamingResponse(
        response_body(first_item),
        on_close=on_response_close,
        status_code=status_code,
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
    prompt_cache_context: _PromptCacheRequestContext | None = None,
    prompt_cache_trace: dict[str, Any] | None = None,
    session_id: str | None = None,
    session_id_source: str | None = None,
) -> Response:
    try:
        proxy_lease = await _proxy_concurrency_limiter(http_request.app).acquire(
            timeout_seconds=_proxy_queue_timeout_seconds(),
        )
    except TimeoutError:
        retry_after = max(1, int(math.ceil(_proxy_queue_timeout_seconds())))
        return JSONResponse(
            status_code=503,
            content={"error": "Too many active proxy requests; retry later"},
            headers={"Retry-After": str(retry_after)},
        )

    response_traffic: ResponseTrafficController = http_request.app.state.response_traffic
    response_lease = response_traffic.start_response()
    release_response_traffic = True
    release_proxy_concurrency = True
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
        prompt_cache_trace=prompt_cache_trace if isinstance(prompt_cache_trace, dict) else None,
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
            release_proxy_concurrency = False
            return _gpt_image_stream_keepalive_response(
                http_request,
                endpoint=endpoint,
                request_model=request_model,
                compact=compact,
                proxy_call=proxy_call,
                request_log=request_log,
                response_lease=response_lease,
                proxy_lease=proxy_lease,
                timing=timing,
            )

        max_attempts = _effective_proxy_max_attempts(endpoint=endpoint)
        max_claims = _max_request_account_retries()
        oauth_manager: CodexOAuthManager = http_request.app.state.oauth_manager
        selection_settings = _current_token_selection_settings(http_request.app)
        last_error: HTTPException | None = None
        restricted_model_name = _non_free_only_codex_model_name(request_model)
        require_non_free_token = restricted_model_name is not None
        restricted_model_label = restricted_model_name or "requested"
        scoped_cooldown_scope = _codex_request_scoped_cooldown_scope(request_model)
        excluded_token_ids: set[int] = set()
        skipped_free_token_ids: set[int] = set()
        incompatible_model_token_ids: set[int] = set()
        postponed_unknown_plan_tokens: list[Any] = []
        transport_error_count = 0
        upstream_5xx_error_count = 0
        last_affinity_result: str | None = None
        last_affinity_lane_index: int | None = None
        if _model_compatibility_scoped_cooldown_scope(request_model) is not None:
            max_claims = max(max_claims, _model_compatibility_max_token_probes())
        claim_count = 0

        def prompt_cache_log_kwargs(
            proxy_result: ProxyRequestResult | None = None,
            *,
            status_code: int | None = None,
        ) -> dict[str, Any]:
            trace = request_log.prompt_cache_trace if isinstance(request_log.prompt_cache_trace, dict) else None
            usage_metrics = getattr(proxy_result, "usage_metrics", None) if proxy_result is not None else None
            stream_capture = getattr(proxy_result, "stream_capture", None) if proxy_result is not None else None
            if stream_capture is not None:
                stream_capture.finalize_usage_metrics()
                usage_metrics = stream_capture.usage_metrics or usage_metrics
            upstream_response_id = _response_id_from_proxy_result(proxy_result) if proxy_result is not None else None
            _prompt_cache_update_runtime_trace_fields(
                trace,
                session_id=session_id,
                session_id_source=session_id_source,
                upstream_response_id=upstream_response_id,
                status_code=status_code
                if status_code is not None
                else (getattr(proxy_result, "status_code", None) if proxy_result is not None else None),
                usage_metrics=usage_metrics,
                cache_affinity_result=last_affinity_result,
                cache_affinity_lane_index=last_affinity_lane_index,
                cache_affinity_lane_count=_prompt_cache_lane_count(http_request.app, prompt_cache_context),
            )
            return _prompt_cache_finalize_kwargs(
                prompt_cache_context,
                prompt_cache_trace=trace,
                affinity_result=last_affinity_result,
                lane_index=last_affinity_lane_index,
                status_code=status_code,
                usage_metrics=usage_metrics,
                upstream_response_id=upstream_response_id,
            )

        while attempt_count - len(incompatible_model_token_ids) < max_attempts and claim_count < max_claims:
            claim_count += 1
            token_pool_snapshot = _fresh_token_pool_snapshot(http_request.app, timing=timing)
            affinity_claim = await _await_timed(
                _claim_prompt_cache_affinity_token(
                    http_request.app,
                    selection_settings,
                    prompt_cache_context=prompt_cache_context,
                    exclude_token_ids=excluded_token_ids,
                    scoped_cooldown_scope=scoped_cooldown_scope,
                    timing=timing,
                    token_pool_snapshot=token_pool_snapshot,
                    require_non_free_token=require_non_free_token,
                ),
                timing=timing,
                span_name="claim_prompt_cache_affinity_ms",
                db_wait=False,
            )
            if affinity_claim is not None:
                token_row = affinity_claim.token_row
                last_affinity_result = affinity_claim.result
                last_affinity_lane_index = affinity_claim.lane_index
                if timing is not None:
                    timing.set_tag("cache_affinity_result", affinity_claim.result)
                    if affinity_claim.lane_index is not None:
                        timing.set_ms("cache_affinity_lane_index", affinity_claim.lane_index)
            elif (
                prompt_cache_context is not None
                and prompt_cache_context.previous_response_id
                and _prompt_cache_previous_strict_enabled()
                and _prompt_cache_response_owner(http_request.app, prompt_cache_context.previous_response_id) is not None
            ):
                last_affinity_result = "previous_owner_busy"
                last_affinity_lane_index = None
                raise HTTPException(status_code=429, detail="previous_response_busy")
            else:
                if prompt_cache_context is not None and prompt_cache_context.affinity_key and not _prompt_cache_global_fallback_enabled():
                    raise HTTPException(status_code=429, detail="Prompt cache affinity lanes are busy")
                if prompt_cache_context is not None and prompt_cache_context.affinity_key:
                    last_affinity_result = "global_fallback"
                    last_affinity_lane_index = None
                token_row = await _await_timed(
                    _claim_next_token_for_model_request(
                        selection_settings,
                        exclude_token_ids=excluded_token_ids,
                        scoped_cooldown_scope=scoped_cooldown_scope,
                        timing=timing,
                        token_pool_snapshot=token_pool_snapshot,
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
                    _prompt_cache_remove_token(http_request.app, token_row.id)
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

            if prompt_cache_context is not None and prompt_cache_context.affinity_key:
                bound_lane_index = _prompt_cache_bind_lane(
                    http_request.app,
                    prompt_cache_context,
                    int(token_row.id),
                    prefer_primary=last_affinity_result in {"previous_owner_hit", "primary_hit"},
                )
                if last_affinity_lane_index is None:
                    last_affinity_lane_index = bound_lane_index
                if timing is not None:
                    if last_affinity_result:
                        timing.set_tag("cache_affinity_result", last_affinity_result)
                    if last_affinity_lane_index is not None:
                        timing.set_ms("cache_affinity_lane_index", last_affinity_lane_index)

            attempt_count += 1
            attempt = attempt_count
            last_token_id = token_row.id
            last_account_id = token_row.account_id
            client, upstream_breaker, upstream_shard_key = _select_response_http_client(
                http_request.app,
                token_id=token_row.id,
                account_id=token_row.account_id,
                request_id=request_log.request_id,
            )
            if upstream_breaker.is_open():
                excluded_token_ids.add(token_row.id)
                last_error = _upstream_circuit_open_exception(upstream_breaker)
                logger.info(
                    "Skipping Codex token because upstream shard circuit is open: token_id=%s account_id=%s shard=%s",
                    token_row.id,
                    token_row.account_id,
                    upstream_shard_key,
                )
                _record_selected_token_observation_finish(
                    token_row.id,
                    started_at=request_started_at,
                    first_token_at=None,
                )
                continue

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
                if permanent_refresh_failure or exc.status_code in (401, 403):
                    _exclude_auth_failed_token_for_request(
                        excluded_token_ids,
                        token_row,
                        app=http_request.app,
                    )
                    last_error = (
                        HTTPException(
                            status_code=503,
                            detail=_sanitized_permanent_refresh_failure_detail(),
                        )
                        if permanent_refresh_failure
                        else exc
                    )
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
                        selected_upstream_breaker: UpstreamCircuitBreaker = upstream_breaker,
                    ) -> None:
                        db_timing_context_token = set_db_timing_recorder(timing)
                        try:
                            proxied_on_close = getattr(proxied_result, "on_close", None)
                            if proxied_on_close is not None:
                                await proxied_on_close()
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
                                    **prompt_cache_log_kwargs(proxied_result, status_code=499),
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
                                    extra_finalize_kwargs=prompt_cache_log_kwargs(
                                        proxied_result,
                                        status_code=proxied_result.status_code,
                                    ),
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
                                    **prompt_cache_log_kwargs(proxied_result, status_code=proxied_result.status_code),
                                )
                            if (
                                not disconnected_before_completion
                                and (
                                    proxied_result.stream_capture is None
                                    or proxied_result.stream_capture.error_message is None
                                    or proxied_result.stream_capture.completed
                                )
                            ):
                                await _record_prompt_cache_success(
                                    http_request.app,
                                    prompt_cache_context=prompt_cache_context,
                                    token_id=token_id,
                                    proxy_result=proxied_result,
                                )
                                await _mark_token_success_with_timing(
                                    timing,
                                    token_id,
                                    token_row=selected_token_row,
                                )
                                selected_upstream_breaker.record_success()
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
                            await proxy_lease.release()

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
                    release_proxy_concurrency = False
                    token_observation_transferred = True
                    return _FinalizingStreamingResponse(
                        body_iterator,
                        on_close=on_stream_close_once,
                        status_code=proxy_result.response.status_code,
                        headers=dict(proxy_result.response.headers),
                        media_type=proxy_result.response.media_type,
                    )

                await _mark_token_success_with_timing(timing, token_row.id, token_row=token_row)
                upstream_breaker.record_success()
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
                    **prompt_cache_log_kwargs(proxy_result, status_code=proxy_result.status_code),
                )
                await _record_prompt_cache_success(
                    http_request.app,
                    prompt_cache_context=prompt_cache_context,
                    token_id=token_row.id,
                    proxy_result=proxy_result,
                )
                return proxy_result.response
            except HTTPException as exc:
                status_code = getattr(exc, "status_code", 500)
                detail = str(getattr(exc, "detail", "") or exc)
                if _is_model_unsupported_for_chatgpt_account_error(
                    status_code,
                    detail,
                    request_model=request_model,
                ):
                    incompatible_model_token_ids.add(token_row.id)
                    await _mark_model_unsupported_token_for_request(
                        http_request,
                        token_row,
                        request_model=request_model,
                        detail=detail,
                        excluded_token_ids=excluded_token_ids,
                    )
                    logger.info(
                        "Skipping Codex token because account does not support %s: token_id=%s account_id=%s attempt=%s/%s",
                        restricted_model_label,
                        token_row.id,
                        token_row.account_id,
                        attempt,
                        max_attempts,
                    )
                    last_error = exc
                    continue

                cooldown_seconds = _extract_usage_limit_cooldown_seconds(status_code, detail)

                if cooldown_seconds is not None:
                    await mark_token_error(
                        token_row.id,
                        detail,
                        cooldown_seconds=cooldown_seconds,
                    )
                    _prompt_cache_remove_token(http_request.app, token_row.id)
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
                    _prompt_cache_remove_token(http_request.app, token_row.id)
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
                auth_failed_after_refresh = status_code in (401, 403) and (
                    access_token_refreshed or is_access_token_only_refresh_token(token_row.refresh_token)
                )

                if permanently_disabled or auth_failed_after_refresh:
                    oauth_manager.invalidate(token_row.id)
                    _exclude_auth_failed_token_for_request(
                        excluded_token_ids,
                        token_row,
                        app=http_request.app,
                    )
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
                    _exclude_auth_failed_token_for_request(
                        excluded_token_ids,
                        token_row,
                        app=http_request.app,
                    )
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
                retryable_server_error = _should_retry_http_exception(exc)
                if retryable_server_error:
                    upstream_5xx_error_count += 1
                if (
                    retryable_server_error
                    and upstream_5xx_error_count <= _upstream_5xx_max_retries()
                    and attempt < max_attempts
                ):
                    excluded_token_ids.add(token_row.id)
                    mark_kwargs: dict[str, Any] = {}
                    if compact_server_error_cooling_time > 0:
                        mark_kwargs["cooldown_seconds"] = compact_server_error_cooling_time
                    await mark_token_error(token_row.id, detail, **mark_kwargs)
                    if compact_server_error_cooling_time > 0:
                        _prompt_cache_remove_token(http_request.app, token_row.id)
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
                if _should_record_upstream_circuit_failure(exc):
                    upstream_breaker.record_failure()
                transport_error_count += 1
                compact_server_error_cooling_time = _get_compact_codex_server_error_cooling_time(
                    compact=compact,
                    status_code=500,
                    error_text=message,
                )
                if (
                    transport_error_count <= _transport_error_max_retries()
                    and attempt < max_attempts
                    and not upstream_breaker.is_open()
                ):
                    excluded_token_ids.add(token_row.id)
                    mark_kwargs: dict[str, Any] = {}
                    if compact_server_error_cooling_time > 0:
                        mark_kwargs["cooldown_seconds"] = compact_server_error_cooling_time
                    if _is_local_upstream_pool_timeout(exc):
                        logger.warning(
                            "Local upstream HTTP pool exhausted before response commit: token_id=%s account_id=%s shard=%s attempt=%s/%s",
                            token_row.id,
                            token_row.account_id,
                            upstream_shard_key,
                            attempt,
                            max_attempts,
                        )
                    else:
                        await mark_token_error(token_row.id, message, **mark_kwargs)
                        if compact_server_error_cooling_time > 0:
                            _prompt_cache_remove_token(http_request.app, token_row.id)
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
                if not _is_local_upstream_pool_timeout(exc):
                    await mark_token_error(token_row.id, message)
                if upstream_breaker.is_open():
                    raise _upstream_circuit_open_exception(upstream_breaker) from exc
                raise HTTPException(status_code=502, detail=message) from exc
            except Exception as exc:
                if not _is_retryable_upstream_transport_exception(exc):
                    raise
                message = f"Upstream request failed: {type(exc).__name__}: {exc}"
                transport_error_count += 1
                compact_server_error_cooling_time = _get_compact_codex_server_error_cooling_time(
                    compact=compact,
                    status_code=500,
                    error_text=message,
                )
                if transport_error_count <= _transport_error_max_retries() and attempt < max_attempts:
                    excluded_token_ids.add(token_row.id)
                    mark_kwargs: dict[str, Any] = {}
                    if compact_server_error_cooling_time > 0:
                        mark_kwargs["cooldown_seconds"] = compact_server_error_cooling_time
                    await mark_token_error(token_row.id, message, **mark_kwargs)
                    if compact_server_error_cooling_time > 0:
                        _prompt_cache_remove_token(http_request.app, token_row.id)
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

        if require_non_free_token and incompatible_model_token_ids:
            raise HTTPException(
                status_code=503,
                detail=_model_unsupported_codex_token_unavailable_detail(restricted_model_label),
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
            **prompt_cache_log_kwargs(status_code=499),
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
            **prompt_cache_log_kwargs(status_code=getattr(exc, "status_code", 500)),
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
            **prompt_cache_log_kwargs(status_code=500),
        )
        raise
    finally:
        if release_response_traffic:
            await response_lease.release()
        if release_proxy_concurrency:
            await proxy_lease.release()
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
        await _close_async_iterator_safely(
            body_iterator,
            label="Streamed response body iterator",
        )

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


async def _cached_admin_token_counts(app: FastAPI) -> Any:
    ttl_seconds = _admin_token_counts_cache_ttl_seconds()
    cache_entry = getattr(app.state, "admin_token_counts_cache", None)
    now = time.monotonic()
    if (
        ttl_seconds > 0
        and isinstance(cache_entry, tuple)
        and len(cache_entry) == 2
        and float(cache_entry[0]) > now
    ):
        return cache_entry[1]

    counts = await get_token_counts()
    if ttl_seconds > 0:
        app.state.admin_token_counts_cache = (now + ttl_seconds, counts)
    return counts


def _admin_quota_refresh_tasks(app: FastAPI) -> dict[int, asyncio.Task[None]]:
    tasks = getattr(app.state, "admin_quota_refresh_tasks", None)
    if not isinstance(tasks, dict):
        tasks = {}
        app.state.admin_quota_refresh_tasks = tasks
    return tasks


def _schedule_admin_quota_refresh(
    app: FastAPI,
    token_rows: list[Any],
    *,
    account_ids: dict[int, str | None],
) -> tuple[int, ...]:
    if not token_rows:
        return ()

    task_map = _admin_quota_refresh_tasks(app)
    pending_rows = [token_row for token_row in token_rows if int(token_row.id) not in task_map]
    if not pending_rows:
        return ()

    batch = pending_rows[: _admin_quota_background_refresh_limit()]
    if not batch:
        return ()

    token_ids = tuple(int(token_row.id) for token_row in batch)
    quota_service: CodexQuotaService = app.state.quota_service
    client: httpx.AsyncClient = _state_http_client(app, "admin_http_client")
    oauth_manager: CodexOAuthManager = app.state.oauth_manager

    async def refresh() -> None:
        try:
            await quota_service.get_many(
                batch,
                client=client,
                oauth_manager=oauth_manager,
                account_ids=account_ids,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Failed to refresh admin token quota snapshots in background")
        finally:
            for token_id in token_ids:
                task_map.pop(token_id, None)

    task = asyncio.create_task(refresh(), name="oaix-admin-quota-refresh")
    for token_id in token_ids:
        task_map[token_id] = task
    return token_ids


async def _collect_admin_token_quota_snapshots(
    app: FastAPI,
    *,
    token_rows: list[Any],
) -> tuple[dict[int, CodexQuotaSnapshot], tuple[int, ...]]:
    if not token_rows:
        return {}, ()

    quota_service: CodexQuotaService = app.state.quota_service
    client: httpx.AsyncClient = _state_http_client(app, "admin_http_client")
    oauth_manager: CodexOAuthManager = app.state.oauth_manager
    effective_account_ids = {
        token_row.id: (
            extract_codex_plan_info(
                token_row.id_token,
                account_id=token_row.account_id,
                raw_payload=token_row.raw_payload,
            ).chatgpt_account_id
            or token_row.account_id
        )
        for token_row in token_rows
    }

    quota_by_id: dict[int, CodexQuotaSnapshot] = {}
    get_cached_snapshot = getattr(quota_service, "get_cached_snapshot", None)
    has_fresh_cached_snapshot = getattr(quota_service, "has_fresh_cached_snapshot", None)
    if not callable(get_cached_snapshot) or not callable(has_fresh_cached_snapshot):
        return (
            await quota_service.get_many(
                token_rows,
                client=client,
                oauth_manager=oauth_manager,
                account_ids=effective_account_ids,
            ),
            (),
        )

    refresh_rows: list[Any] = []
    pending_ids: list[int] = []
    inflight_refresh_ids = set(_admin_quota_refresh_tasks(app))
    for token_row in token_rows:
        token_id = int(token_row.id)
        cached_snapshot = get_cached_snapshot(token_id, include_stale=True)
        if cached_snapshot is not None:
            quota_by_id[token_id] = cached_snapshot
        if has_fresh_cached_snapshot(token_id):
            continue
        if token_id in inflight_refresh_ids:
            pending_ids.append(token_id)
            continue
        if token_id not in inflight_refresh_ids:
            refresh_rows.append(token_row)

    sync_refresh_limit = _admin_quota_sync_refresh_limit()
    sync_rows = refresh_rows[:sync_refresh_limit]
    background_rows = refresh_rows[sync_refresh_limit:]

    if sync_rows:
        try:
            quota_by_id.update(
                await quota_service.get_many(
                    sync_rows,
                    client=client,
                    oauth_manager=oauth_manager,
                    account_ids=effective_account_ids,
                )
            )
        except Exception:
            logger.exception("Failed to collect admin token quota snapshots")

    pending_ids.extend(
        _schedule_admin_quota_refresh(
            app,
            background_rows,
            account_ids=effective_account_ids,
        )
    )
    return quota_by_id, tuple(dict.fromkeys(pending_ids))


async def _build_admin_token_items(
    app: FastAPI,
    *,
    token_rows: list[Any],
    include_quota: bool,
    include_observed_cost: bool = True,
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
    if include_observed_cost and observed_token_ids:
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
    if include_observed_cost and fallback_account_ids:
        try:
            observed_costs_by_account, token_counts_by_account = await asyncio.gather(
                get_request_costs_by_account(fallback_account_ids),
                get_token_counts_by_account_ids(fallback_account_ids),
            )
        except Exception:
            logger.exception("Failed to collect admin token observed cost fallbacks")
    if include_quota and token_rows:
        quota_token_rows = [
            token_row
            for token_row in token_rows
            if bool(getattr(token_row, "is_active", False))
            and _normalize_optional_text(getattr(token_row, "refresh_token", None)) is not None
        ]
        quota_by_id, _ = await _collect_admin_token_quota_snapshots(app, token_rows=quota_token_rows)

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


async def _build_admin_token_observed_cost_items(token_rows: list[Any]) -> list[dict[str, Any]]:
    if not token_rows:
        return []

    observed_costs_by_token: dict[int, float] = {}
    observed_costs_by_account: dict[str, float] = {}
    token_counts_by_account: dict[str, int] = {}
    observed_token_ids = {int(token_row.id) for token_row in token_rows}
    if observed_token_ids:
        try:
            observed_costs_by_token = await get_request_costs_by_token(observed_token_ids)
        except Exception:
            logger.exception("Failed to collect admin token observed costs by token")

    fallback_account_ids = {
        account_id
        for token_row in token_rows
        if int(token_row.id) not in observed_costs_by_token
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

    return [
        {
            "id": int(token_row.id),
            "observed_cost_usd": _resolve_token_observed_cost_usd(
                observed_costs_by_token,
                observed_costs_by_account,
                token_counts_by_account,
                token_row=token_row,
            ),
        }
        for token_row in token_rows
    ]


def _json_response_body(payload: Any) -> bytes:
    return json.dumps(
        jsonable_encoder(payload),
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")


def _serialize_admin_request_log_item(item: Any) -> dict[str, Any]:
    error_message = str(getattr(item, "error_message", "") or "")
    return {
        "endpoint": getattr(item, "endpoint", None),
        "model": getattr(item, "model", None),
        "model_name": getattr(item, "model_name", None),
        "is_stream": bool(getattr(item, "is_stream", False)),
        "status_code": getattr(item, "status_code", None),
        "success": getattr(item, "success", None),
        "attempt_count": getattr(item, "attempt_count", None),
        "started_at": getattr(item, "started_at", None),
        "ttft_ms": getattr(item, "ttft_ms", None),
        "cache_hit_ratio": getattr(item, "cache_hit_ratio", None),
        "prompt_cache_source": getattr(item, "prompt_cache_source", None),
        "cache_affinity_result": getattr(item, "cache_affinity_result", None),
        "cache_affinity_lane_index": getattr(item, "cache_affinity_lane_index", None),
        "error_message": error_message[:500] if error_message else None,
    }


async def _cached_admin_requests_payload(
    app: FastAPI,
    *,
    limit: int,
    force_refresh: bool = False,
) -> tuple[bytes, str]:
    ttl_seconds = _admin_requests_cache_ttl_seconds()
    cache: dict[int, tuple[float, bytes]] = getattr(app.state, "admin_requests_cache", {})
    if not isinstance(cache, dict):
        cache = {}
        app.state.admin_requests_cache = cache

    cache_key = max(1, min(int(limit), 500))
    now = time.monotonic()
    cached = cache.get(cache_key)
    if not force_refresh and ttl_seconds > 0 and cached is not None and cached[0] > now:
        return cached[1], "hit"
    if not force_refresh and cached is not None:
        return cached[1], "stale"

    lock = getattr(app.state, "admin_requests_cache_lock", None)
    if not isinstance(lock, asyncio.Lock):
        lock = asyncio.Lock()
        app.state.admin_requests_cache_lock = lock
    if not force_refresh and cached is not None and lock.locked():
        return cached[1], "stale"

    async with lock:
        now = time.monotonic()
        cached = cache.get(cache_key)
        if not force_refresh and ttl_seconds > 0 and cached is not None and cached[0] > now:
            return cached[1], "hit"
        if not force_refresh and cached is not None:
            return cached[1], "stale"

        summary, analytics, raw_items = await asyncio.gather(
            get_request_log_summary(hours=24),
            get_request_log_analytics(hours=24, bucket_minutes=60, top_models=6),
            list_request_logs(limit=cache_key),
        )
        payload = {
            "summary": asdict(summary),
            "analytics": asdict(analytics),
            "items": [_serialize_admin_request_log_item(item) for item in raw_items],
        }
        body = _json_response_body(payload)
        if ttl_seconds > 0:
            cache[cache_key] = (time.monotonic() + ttl_seconds, body)
            if len(cache) > 8:
                for stale_key in sorted(cache, key=lambda key: cache[key][0])[:-8]:
                    cache.pop(stale_key, None)
        return body, "refresh" if force_refresh else "miss"


async def _admin_requests_cache_refresh_loop(app: FastAPI) -> None:
    while True:
        try:
            await _cached_admin_requests_payload(app, limit=80, force_refresh=True)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Failed to refresh admin requests cache")
        await asyncio.sleep(_admin_requests_cache_refresh_seconds())


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
    quota_token_rows = [
        token_row
        for token_row in token_rows
        if bool(getattr(token_row, "is_active", False))
        and _normalize_optional_text(getattr(token_row, "refresh_token", None)) is not None
    ]
    quota_by_id: dict[int, CodexQuotaSnapshot] = {}
    refresh_pending_ids: tuple[int, ...] = ()
    if quota_token_rows:
        quota_by_id, refresh_pending_ids = await _collect_admin_token_quota_snapshots(app, token_rows=quota_token_rows)
    app.state.admin_token_quota_refresh_pending_ids = refresh_pending_ids

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
    app.state.response_http_client = UpstreamHTTPClientPool(
        shard_count=_upstream_http_shard_count(),
        max_connections=_upstream_http_max_connections(),
        max_keepalive_connections=_upstream_http_max_keepalive_connections(),
        keepalive_expiry_seconds=_upstream_http_keepalive_expiry_seconds(),
    )
    app.state.http_client = app.state.response_http_client.default_client
    app.state.upstream_http_pool_maintenance_task = asyncio.create_task(
        _upstream_http_pool_maintenance_loop(app.state.response_http_client),
        name="oaix-upstream-http-pool-maintenance",
    )
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
    app.state.request_log_retention_task = asyncio.create_task(
        _request_log_retention_worker(),
        name="oaix-request-log-retention",
    )
    try:
        await _cached_admin_requests_payload(app, limit=80)
    except Exception:
        logger.exception("Failed to prewarm admin requests cache")
    app.state.admin_requests_cache_refresh_task = asyncio.create_task(
        _admin_requests_cache_refresh_loop(app),
        name="oaix-admin-requests-cache-refresh",
    )

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
        quota_refresh_tasks = set(_admin_quota_refresh_tasks(app).values())
        for task in quota_refresh_tasks:
            task.cancel()
        if quota_refresh_tasks:
            await asyncio.gather(*quota_refresh_tasks, return_exceptions=True)
        retention_task = getattr(app.state, "request_log_retention_task", None)
        if isinstance(retention_task, asyncio.Task):
            retention_task.cancel()
            await asyncio.gather(retention_task, return_exceptions=True)
        admin_requests_cache_task = getattr(app.state, "admin_requests_cache_refresh_task", None)
        if isinstance(admin_requests_cache_task, asyncio.Task):
            admin_requests_cache_task.cancel()
            await asyncio.gather(admin_requests_cache_task, return_exceptions=True)
        upstream_pool_task = getattr(app.state, "upstream_http_pool_maintenance_task", None)
        if isinstance(upstream_pool_task, asyncio.Task):
            upstream_pool_task.cancel()
            await asyncio.gather(upstream_pool_task, return_exceptions=True)
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
    app.state.proxy_concurrency = ProxyConcurrencyLimiter(
        _proxy_max_active_responses(),
        max_queue=_proxy_queue_max_size(),
    )
    app.state.upstream_circuit_registry = UpstreamCircuitRegistry()
    app.state.prompt_cache_affinity_lanes = OrderedDict()
    app.state.prompt_cache_response_bindings = OrderedDict()
    app.state.token_import_worker = None
    app.add_middleware(GZipMiddleware, minimum_size=1024)
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
        web_version_hash = _web_bundle_version_hash()
        web_version_time = _web_bundle_version_time()
        html = html.replace("/assets/styles.css", f"/assets/styles.css?v={css_version}")
        html = html.replace("/assets/app.js", f"/assets/app.js?v={js_version}")
        html = html.replace("__OAIX_WEB_VERSION_HASH__", web_version_hash)
        html = html.replace("__OAIX_WEB_VERSION_TIME__", web_version_time)
        return HTMLResponse(
            content=html,
            headers={"Cache-Control": "public, max-age=60, stale-while-revalidate=300"},
        )

    @app.get("/favicon.ico", include_in_schema=False)
    @app.get("/apple-touch-icon.png", include_in_schema=False)
    @app.get("/apple-touch-icon-precomposed.png", include_in_schema=False)
    async def empty_icon() -> Response:
        return Response(status_code=204, headers={"Cache-Control": "public, max-age=86400"})

    @app.get("/livez")
    async def livez() -> JSONResponse:
        response_traffic: ResponseTrafficController = app.state.response_traffic
        proxy_limiter = _proxy_concurrency_limiter(app)
        upstream_metrics = _upstream_runtime_metrics(app)
        breaker_snapshots = upstream_metrics.get("breakers") if isinstance(upstream_metrics, dict) else {}
        upstream_circuit_open = any(
            bool(snapshot.get("open"))
            for snapshot in (breaker_snapshots or {}).values()
            if isinstance(snapshot, dict)
        )
        proxy_metrics = _proxy_limiter_runtime_metrics(proxy_limiter)
        runtime_metrics = _runtime_fd_metrics()
        guard_window = _gateway_guard_window_metrics()
        tcp_close_wait_443 = int(runtime_metrics.get("tcp_close_wait_443") or 0)
        open_fds = runtime_metrics.get("open_fds")
        _record_runtime_guard_sample(
            open_fds=open_fds if isinstance(open_fds, int) else None,
            tcp_close_wait_443=tcp_close_wait_443,
            active_responses=response_traffic.active_responses,
            responses_499=guard_window["responses_499"],
        )
        runtime_guard_window = _runtime_guard_window_metrics()
        return JSONResponse(
            status_code=200,
            content={
                "ok": True,
                "active_responses": response_traffic.active_responses,
                "active_proxy_requests": proxy_metrics["active"],
                "queued_proxy_requests": proxy_metrics["queued"],
                "rejected_proxy_requests_total": proxy_metrics["rejected_total"],
                "max_proxy_requests": proxy_metrics["max_active"],
                "max_proxy_queue": proxy_metrics["max_queue"],
                "upstream_circuit_open": upstream_circuit_open,
                "upstream": upstream_metrics,
                "runtime": runtime_metrics,
                "stream_owner": _stream_owner_runtime_metrics(),
                "gateway": {
                    "499_per_5m": guard_window["responses_499"],
                    "pooltimeout_per_5m": guard_window["responses_pooltimeout"],
                    "5xx_per_5m": guard_window["responses_5xx"],
                },
                "guards": {
                    "window": guard_window,
                    "runtime_window": runtime_guard_window,
                    "alerts": {
                        "responses_499": guard_window["responses_499"] > 0,
                        "responses_5xx": guard_window["responses_5xx"] > 0,
                        "responses_pooltimeout": guard_window["responses_pooltimeout"] > 0,
                        "tcp_close_wait_443_warning": tcp_close_wait_443 > 0,
                        "tcp_close_wait_443_critical": tcp_close_wait_443 >= 5,
                        "open_fds_rising_with_499": runtime_guard_window["open_fds_rising_with_499"],
                        "active_low_open_fds_high": runtime_guard_window["active_low_open_fds_high"],
                        "open_fds": open_fds,
                    },
                },
                "token_active": _token_active_runtime_metrics(),
                "request_log": _request_log_write_queue_stats(),
            },
        )

    @app.get("/healthz")
    async def healthz() -> JSONResponse:
        try:
            counts = await asyncio.wait_for(
                get_token_counts(),
                timeout=_healthz_db_timeout_seconds(),
            )
        except asyncio.TimeoutError:
            return JSONResponse(
                status_code=503,
                content={
                    "ok": False,
                    "degraded": True,
                    "error": "token count query timed out",
                    "upstream": _codex_responses_url(),
                    "service_key_protected": bool(_get_service_api_keys()),
                },
            )
        except Exception as exc:
            logger.exception("Health check token count query failed")
            return JSONResponse(
                status_code=503,
                content={
                    "ok": False,
                    "degraded": True,
                    "error": str(exc) or type(exc).__name__,
                    "upstream": _codex_responses_url(),
                    "service_key_protected": bool(_get_service_api_keys()),
                },
            )
        ok = counts.available > 0
        return JSONResponse(
            status_code=200 if ok else 503,
            content={
                "ok": ok,
                "degraded": not ok,
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
        selected_import_batch_failed_items = (
            await list_token_import_batch_failed_items(import_batch_id)
            if selected_import_batch is not None and import_batch_id is not None
            else []
        )

        counts, filtered_counts, plan_counts, filtered_total, token_rows = await asyncio.gather(
            _cached_admin_token_counts(http_request.app),
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
            include_observed_cost=False,
        )
        total = max(0, int(filtered_total))
        total_pages = max(1, int(math.ceil(total / limit))) if limit else 1
        selected_import_batch_payload = (
            _serialize_token_import_batch_summary(selected_import_batch)
            if selected_import_batch is not None
            else None
        )
        if selected_import_batch_payload is not None:
            selected_import_batch_payload["failed_items"] = selected_import_batch_failed_items

        return {
            "counts": asdict(counts),
            "filtered_counts": asdict(filtered_counts),
            "plan_counts": asdict(plan_counts),
            "selection": _serialize_token_selection_settings(_current_token_selection_settings(http_request.app)),
            "import_batches": [_serialize_token_import_batch_summary(batch) for batch in import_batches],
            "selected_import_batch": selected_import_batch_payload,
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

    @app.get("/admin/tokens/costs")
    async def list_token_costs_route(
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
        return {"items": await _build_admin_token_observed_cost_items(token_rows)}

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
        refresh_pending_ids = getattr(http_request.app.state, "admin_token_quota_refresh_pending_ids", ())
        requested_token_ids = {int(row.id) for row in token_rows}
        response = {"items": items}
        pending_ids = [int(token_id) for token_id in refresh_pending_ids if int(token_id) in requested_token_ids]
        if pending_ids:
            response["refresh_pending_ids"] = pending_ids
        return response

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
    ) -> Response:
        try:
            body, cache_status = await _cached_admin_requests_payload(app, limit=limit)
        except SQLAlchemyError as exc:
            raise HTTPException(status_code=503, detail="Request logs temporarily unavailable") from exc
        return Response(
            content=body,
            media_type="application/json",
            headers={"X-OAIX-Admin-Requests-Cache": cache_status},
        )

    @app.get("/admin/runtime")
    async def runtime_route(
        _: None = Depends(verify_service_api_key),
    ) -> dict[str, Any]:
        response_traffic: ResponseTrafficController = app.state.response_traffic
        proxy_limiter = _proxy_concurrency_limiter(app)
        return {
            "active_responses": response_traffic.active_responses,
            "proxy": _proxy_limiter_runtime_metrics(proxy_limiter),
            "upstream": _upstream_runtime_metrics(app),
            "runtime": _runtime_fd_metrics(),
            "token_active": _token_active_runtime_metrics(),
            "request_log": _request_log_write_queue_stats(),
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
        raw_payload = request_data.model_dump(exclude_unset=True)
        prompt_cache_context = _build_prompt_cache_request_context(
            http_request,
            endpoint="/v1/chat/completions",
            request_model=request_data.model,
            raw_payload=raw_payload,
        )
        session_id, session_id_source = (
            _prompt_cache_session_context(http_request, prompt_cache_context)
            if prompt_cache_context is not None
            else (None, None)
        )
        prompt_cache_trace = (
            build_prompt_cache_trace(
                endpoint="/v1/chat/completions",
                model=request_data.model,
                compact=False,
                raw_payload=raw_payload,
                upstream_payload=copy.deepcopy(raw_payload),
                prompt_cache_context=prompt_cache_context,
                session_id=session_id,
                session_id_source=session_id_source,
            )
            if prompt_cache_context is not None
            else None
        )

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
                prompt_cache_context=prompt_cache_context,
                session_id=session_id,
                session_id_source=session_id_source,
                prompt_cache_trace=prompt_cache_trace,
            )

        return await _execute_proxy_request_with_failover(
            http_request,
            endpoint="/v1/chat/completions",
            request_model=request_data.model,
            is_stream=bool(request_data.stream),
            proxy_call=proxy_call,
            prompt_cache_context=prompt_cache_context,
            prompt_cache_trace=prompt_cache_trace,
            session_id=session_id,
            session_id_source=session_id_source,
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
        raw_payload = request_data.model_dump(exclude_unset=True)
        prompt_cache_context = _build_prompt_cache_request_context(
            http_request,
            endpoint=endpoint,
            request_model=request_data.model,
            raw_payload=raw_payload,
            compact=compact,
        )
        session_id, session_id_source = (
            _prompt_cache_session_context(http_request, prompt_cache_context)
            if prompt_cache_context is not None
            else (None, None)
        )
        prompt_cache_trace = (
            build_prompt_cache_trace(
                endpoint=endpoint,
                model=request_data.model,
                compact=compact,
                raw_payload=raw_payload,
                upstream_payload=copy.deepcopy(raw_payload),
                prompt_cache_context=prompt_cache_context,
                session_id=session_id,
                session_id_source=session_id_source,
            )
            if prompt_cache_context is not None
            else None
        )

        if bool(request_data.stream) and not _is_responses_image_compat_model(request_data.model):
            return await _responses_stream_keepalive_response(
                http_request,
                endpoint=endpoint,
                request_model=request_data.model,
                compact=compact,
                request_data=request_data,
                prompt_cache_context=prompt_cache_context,
                session_id=session_id,
                session_id_source=session_id_source,
                prompt_cache_trace=prompt_cache_trace,
            )

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
                prompt_cache_context=prompt_cache_context,
                session_id=session_id,
                session_id_source=session_id_source,
                prompt_cache_trace=prompt_cache_trace,
            )

        return await _execute_proxy_request_with_failover(
            http_request,
            endpoint=endpoint,
            request_model=request_data.model,
            is_stream=bool(request_data.stream),
            proxy_call=proxy_call,
            compact=compact,
            prompt_cache_context=prompt_cache_context,
            prompt_cache_trace=prompt_cache_trace,
            session_id=session_id,
            session_id_source=session_id_source,
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
    close_stream: Callable[[], Awaitable[None]] | None = None,
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
            if close_stream is not None:
                await close_stream()
            else:
                await _close_stream_handle_safely(stream_cm, reason="responses_stream_generator_finally")
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
        if close_stream is not None:
            await close_stream()
        else:
            await _close_stream_handle_safely(stream_cm, reason="responses_stream_generator_finally")


async def _proxy_request_with_token(
    client: httpx.AsyncClient,
    http_request: Request,
    request_data: ResponsesRequest,
    *,
    access_token: str,
    account_id: str | None,
    compact: bool = False,
    prompt_cache_context: _PromptCacheRequestContext | None = None,
    session_id: str | None = None,
    session_id_source: str | None = None,
    prompt_cache_trace: dict[str, Any] | None = None,
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
    _apply_prompt_cache_context_to_payload(payload, prompt_cache_context)
    if compact and not downstream_stream:
        payload.pop("stream", None)
    elif not downstream_stream:
        payload["stream"] = True

    resolved_session_id = session_id
    resolved_session_id_source = session_id_source
    if not resolved_session_id or not resolved_session_id_source:
        resolved_session_id, resolved_session_id_source = _prompt_cache_session_context(
            http_request,
            prompt_cache_context,
        )

    headers = _build_upstream_headers(
        http_request,
        access_token=access_token,
        account_id=account_id,
        stream=upstream_stream,
        prompt_cache_context=prompt_cache_context,
        session_id=resolved_session_id,
    )
    if isinstance(prompt_cache_trace, dict):
        prompt_cache_trace.clear()
        prompt_cache_trace.update(
            build_prompt_cache_trace(
                endpoint=prompt_cache_context.endpoint
                if prompt_cache_context is not None
                else _responses_endpoint_path(compact=compact),
                model=prompt_cache_context.model if prompt_cache_context is not None else effective_requested_model,
                compact=compact,
                raw_payload=(
                    prompt_cache_context.raw_payload
                    if prompt_cache_context is not None and prompt_cache_context.raw_payload is not None
                    else raw_payload
                ),
                upstream_payload=payload,
                prompt_cache_context=prompt_cache_context,
                session_id=resolved_session_id,
                session_id_source=resolved_session_id_source,
            )
        )
    upstream_url = _codex_responses_url(compact=compact)
    json_payload = json.dumps(payload, ensure_ascii=False)

    if downstream_stream and response_model_alias is not None:
        heartbeat_interval_seconds = _stream_keepalive_interval_seconds()
        stream_capture = _ProxyStreamCapture(
            initial_model_name=effective_requested_model,
            initial_first_token_at=None,
        )
        _prompt_cache_update_runtime_trace_fields(
            prompt_cache_trace,
            session_id=resolved_session_id,
            session_id_source=resolved_session_id_source,
            status_code=200,
        )
        return await stream_proxy(
            client,
            method="POST",
            url=upstream_url,
            headers=headers,
            content=json_payload,
            timeout=_upstream_http_timeout(read=None),
            transform=RawResponsesTransform(response_model_alias=response_model_alias),
            compact=compact,
            label="responses_image_compat_stream",
            model_name=effective_requested_model,
            stream_capture=stream_capture,
            heartbeat_interval_seconds=heartbeat_interval_seconds,
            open_before_return=False,
        )

    if downstream_stream:
        stream_capture = _ProxyStreamCapture(
            initial_model_name=request_data.model,
            initial_first_token_at=None,
        )
        proxy_result = await stream_proxy(
            client,
            method="POST",
            url=upstream_url,
            headers=headers,
            content=json_payload,
            timeout=_upstream_http_timeout(read=None),
            transform=RawResponsesTransform(),
            compact=compact,
            label="responses_stream",
            model_name=request_data.model,
            stream_capture=stream_capture,
            open_before_return=True,
        )
        if proxy_result.stream_capture is not None:
            proxy_result.stream_capture.model_name = proxy_result.model_name
            proxy_result.stream_capture.first_token_at = proxy_result.first_token_at

        _prompt_cache_update_runtime_trace_fields(
            prompt_cache_trace,
            session_id=resolved_session_id,
            session_id_source=resolved_session_id_source,
            status_code=proxy_result.status_code,
        )
        return proxy_result

    if upstream_stream:
        invalid_encrypted_content_recovery_tried = False
        while True:
            session = StreamingProxySession(label="responses_non_stream")
            stream_owner = await session.open_owner(
                client,
                "POST",
                upstream_url,
                headers=headers,
                content=json_payload,
                timeout=_upstream_http_timeout(read=None),
                compact=compact,
            )
            upstream_response = stream_owner.response
            try:
                if upstream_response.status_code < 200 or upstream_response.status_code >= 300:
                    raw = await stream_owner.aread()
                    raise _upstream_error_http_exception(upstream_response.status_code, _decode_error_body(raw))

                data, first_token_at = await _collect_responses_json_from_sse(
                    stream_owner.iter_raw(),
                    model=effective_requested_model,
                )
                if response_model_alias is not None:
                    data = _apply_response_model_alias(data, response_model_alias)
                    effective_model_name = response_model_alias
                else:
                    effective_model_name = _extract_response_model_name(data) or request_data.model
                usage_metrics = extract_usage_metrics(data, model_name=effective_model_name)
                _prompt_cache_update_runtime_trace_fields(
                    prompt_cache_trace,
                    session_id=resolved_session_id,
                    session_id_source=resolved_session_id_source,
                    upstream_response_id=_extract_response_id_from_payload(data),
                    status_code=upstream_response.status_code,
                    usage_metrics=usage_metrics,
                )
                return ProxyRequestResult(
                    response=JSONResponse(status_code=upstream_response.status_code, content=data),
                    status_code=upstream_response.status_code,
                    model_name=effective_model_name,
                    first_token_at=first_token_at,
                    usage_metrics=usage_metrics,
                    response_id=_extract_response_id_from_payload(data),
                )
            except HTTPException as exc:
                status_code = getattr(exc, "status_code", 500)
                detail = str(getattr(exc, "detail", "") or exc)
                if (
                    not invalid_encrypted_content_recovery_tried
                    and _is_invalid_encrypted_content_error(status_code, detail)
                    and _trim_invalid_encrypted_reasoning_items(payload)
                ):
                    invalid_encrypted_content_recovery_tried = True
                    json_payload = json.dumps(payload, ensure_ascii=False)
                    logger.info(
                        "Retrying Codex request once on the same account after invalid encrypted reasoning content: "
                        "account_id=%s compact=%s",
                        account_id,
                        compact,
                    )
                    continue
                raise
            finally:
                await session.close(reason="responses_non_stream_attempt_finished")

    upstream_response = await client.post(
        upstream_url,
        headers=headers,
        content=json_payload,
        timeout=_upstream_http_timeout(read=None),
    )

    if upstream_response.status_code < 200 or upstream_response.status_code >= 300:
        raw = await upstream_response.aread()
        raise _upstream_error_http_exception(upstream_response.status_code, _decode_error_body(raw))

    raw = await upstream_response.aread()
    data = _decode_responses_json_body(raw)
    if response_model_alias is not None:
        data = _apply_response_model_alias(data, response_model_alias)
        effective_model_name = response_model_alias
    else:
        effective_model_name = _extract_response_model_name(data) or request_data.model
    usage_metrics = extract_usage_metrics(data, model_name=effective_model_name)
    _prompt_cache_update_runtime_trace_fields(
        prompt_cache_trace,
        session_id=resolved_session_id,
        session_id_source=resolved_session_id_source,
        upstream_response_id=_extract_response_id_from_payload(data),
        status_code=upstream_response.status_code,
        usage_metrics=usage_metrics,
    )
    return ProxyRequestResult(
        response=JSONResponse(status_code=upstream_response.status_code, content=data),
        status_code=upstream_response.status_code,
        model_name=effective_model_name,
        first_token_at=None,
        usage_metrics=usage_metrics,
        response_id=_extract_response_id_from_payload(data),
    )


async def _proxy_chat_completions_with_token(
    client: httpx.AsyncClient,
    http_request: Request,
    request_data: ChatCompletionsRequest,
    *,
    access_token: str,
    account_id: str | None,
    prompt_cache_context: _PromptCacheRequestContext | None = None,
    session_id: str | None = None,
    session_id_source: str | None = None,
    prompt_cache_trace: dict[str, Any] | None = None,
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
        prompt_cache_context=prompt_cache_context,
        session_id=session_id,
        session_id_source=session_id_source,
        prompt_cache_trace=prompt_cache_trace,
    )

    if isinstance(proxy_result.response, StreamingResponse):
        chat_transform = ChatCompatTransform(
            request_model=request_data.model,
            include_usage=include_usage,
        )
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
                chat_transform.stream_downstream(proxy_result.response.body_iterator),
                media_type="text/event-stream",
                headers=_sse_response_headers(),
            ),
            status_code=proxy_result.status_code,
            model_name=request_data.model,
            first_token_at=proxy_result.first_token_at,
            usage_metrics=proxy_result.usage_metrics,
            on_success=checkpoint_callback,
            on_close=getattr(proxy_result, "on_close", None),
            stream_capture=proxy_result.stream_capture,
            response_id=proxy_result.response_id,
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
        response_id=proxy_result.response_id,
    )


app = create_app()


def main() -> None:
    uvicorn.run(
        "oaix_gateway.api_server:app",
        host=os.getenv("HOST", "0.0.0.0"),
        port=int(os.getenv("PORT", "8000")),
    )
