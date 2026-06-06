from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import random
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import httpx


logger = logging.getLogger("oaix.gateway")

_TRACE_ENDPOINT = "/v1/traces"
_LOG_ENDPOINT = "/v1/logs"
_METRIC_ENDPOINT = "/v1/metrics"
_DEFAULT_SERVICE_NAME = "oaix"
_DEFAULT_QUEUE_MAX_SIZE = 1000
_DEFAULT_EXPORT_TIMEOUT_SECONDS = 2.0
_DEFAULT_SAMPLE_RATE = 1.0


@dataclass(frozen=True)
class FugueObservabilityConfig:
    endpoint: str | None
    service_name: str = _DEFAULT_SERVICE_NAME
    service_version: str | None = None
    queue_max_size: int = _DEFAULT_QUEUE_MAX_SIZE
    export_timeout_seconds: float = _DEFAULT_EXPORT_TIMEOUT_SECONDS
    sample_rate: float = _DEFAULT_SAMPLE_RATE
    identity_attrs: dict[str, str] = field(default_factory=dict)
    emit_request_summaries: bool = True
    emit_stage_spans: bool = True
    emit_metrics: bool = True

    @property
    def enabled(self) -> bool:
        return bool((self.endpoint or "").strip())


class FugueObservabilityClient:
    def __init__(self, config: FugueObservabilityConfig) -> None:
        self.config = config
        self._queue: asyncio.Queue[tuple[str, dict[str, Any]]] | None = None
        self._task: asyncio.Task[None] | None = None
        self._client: httpx.AsyncClient | None = None
        self._dropped = 0
        self._export_errors = 0

    async def start(self) -> None:
        if not self.config.enabled or self._task is not None:
            return
        self._queue = asyncio.Queue(maxsize=max(1, int(self.config.queue_max_size)))
        self._client = httpx.AsyncClient(timeout=self.config.export_timeout_seconds)
        self._task = asyncio.create_task(self._worker(), name="oaix-fugue-observability-exporter")
        logger.info("Fugue observability exporter enabled for service=%s", self.config.service_name)

    async def stop(self) -> None:
        task = self._task
        self._task = None
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        client = self._client
        self._client = None
        if client is not None:
            await client.aclose()
        self._queue = None

    def emit_oaix_request(
        self,
        *,
        request_id: str | None,
        endpoint: str | None,
        model: str | None,
        is_stream: bool | None,
        request_log_id: int | None,
        status_code: int,
        success: bool | None,
        attempt_count: int | None,
        started_at: datetime | None,
        finished_at: datetime | None,
        first_token_at: datetime | None,
        spans: dict[str, Any] | None,
        error_message: str | None,
        request_log_queue_depth: int | None = None,
        runtime_metric_values: dict[str, int | None] | None = None,
    ) -> None:
        if not self.config.enabled:
            return
        if status_code < 400 and self.config.sample_rate < 1.0 and random.random() > self.config.sample_rate:
            return
        telemetry = build_oaix_request_telemetry(
            service_name=self.config.service_name,
            service_version=self.config.service_version,
            identity_attrs=self.config.identity_attrs,
            request_id=request_id,
            endpoint=endpoint,
            model=model,
            is_stream=is_stream,
            request_log_id=request_log_id,
            status_code=status_code,
            success=success,
            attempt_count=attempt_count,
            started_at=started_at,
            finished_at=finished_at,
            first_token_at=first_token_at,
            spans=spans,
            error_message=error_message,
            request_log_queue_depth=request_log_queue_depth,
            runtime_metric_values=runtime_metric_values,
        )
        if self.config.emit_request_summaries:
            self._emit_events(_LOG_ENDPOINT, telemetry["logs"])
        if self.config.emit_stage_spans:
            self._emit_events(_TRACE_ENDPOINT, telemetry["traces"])
        if self.config.emit_metrics:
            self._emit_events(_METRIC_ENDPOINT, telemetry["metrics"])

    def emit_request_log_written(
        self,
        *,
        payload: dict[str, Any],
        request_log_id: int | None,
        write_ms: int,
        db_spans: dict[str, Any] | None = None,
    ) -> None:
        if not self.config.enabled or not self.config.emit_stage_spans:
            return
        event = build_oaix_request_log_written_event(
            service_name=self.config.service_name,
            service_version=self.config.service_version,
            identity_attrs=self.config.identity_attrs,
            payload=payload,
            request_log_id=request_log_id,
            write_ms=write_ms,
            db_spans=db_spans,
        )
        if event:
            self._emit_events(_TRACE_ENDPOINT, [event])
        if self.config.emit_metrics:
            self._emit_events(
                _METRIC_ENDPOINT,
                build_oaix_request_log_write_metric_events(
                    service_name=self.config.service_name,
                    identity_attrs=self.config.identity_attrs,
                    payload=payload,
                    write_ms=write_ms,
                    db_spans=db_spans,
                ),
            )

    def _emit_events(self, path: str, events: list[dict[str, Any]]) -> None:
        if not events:
            return
        queue = self._queue
        if queue is None:
            return
        payload = {"events": events}
        try:
            queue.put_nowait((path, payload))
        except asyncio.QueueFull:
            self._dropped += len(events)
            if self._dropped == len(events) or self._dropped % 100 == 0:
                logger.warning("Fugue observability queue full; dropped %s event(s)", self._dropped)

    async def _worker(self) -> None:
        assert self._queue is not None
        while True:
            path, payload = await self._queue.get()
            try:
                await self._post_json(path, payload)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._export_errors += 1
                if self._export_errors == 1 or self._export_errors % 100 == 0:
                    logger.warning("Fugue observability export failed: %s", type(exc).__name__)
            finally:
                self._queue.task_done()

    async def _post_json(self, path: str, payload: dict[str, Any]) -> None:
        client = self._client
        if client is None:
            return
        response = await client.post(_endpoint_url(self.config.endpoint or "", path), json=payload)
        if response.status_code >= 400:
            raise RuntimeError(f"observability endpoint returned HTTP {response.status_code}")


_client: FugueObservabilityClient | None = None


async def start_fugue_observability_from_env(*, service_version: str | None = None) -> None:
    global _client
    config = fugue_observability_config_from_env(service_version=service_version)
    if not config.enabled:
        _client = None
        return
    client = FugueObservabilityClient(config)
    await client.start()
    _client = client


async def stop_fugue_observability() -> None:
    global _client
    client = _client
    _client = None
    if client is not None:
        await client.stop()


def fugue_observability_config_from_env(*, service_version: str | None = None) -> FugueObservabilityConfig:
    endpoint = _env_text("FUGUE_OBSERVABILITY_ENDPOINT") or _env_text("OTEL_EXPORTER_OTLP_ENDPOINT")
    return FugueObservabilityConfig(
        endpoint=endpoint,
        service_name=_env_text("FUGUE_OBSERVABILITY_SERVICE_NAME") or _DEFAULT_SERVICE_NAME,
        service_version=_env_text("FUGUE_OBSERVABILITY_SERVICE_VERSION") or service_version,
        queue_max_size=_env_int("FUGUE_OBSERVABILITY_QUEUE_MAX_SIZE", _DEFAULT_QUEUE_MAX_SIZE),
        export_timeout_seconds=_env_float(
            "FUGUE_OBSERVABILITY_EXPORT_TIMEOUT_SECONDS",
            _DEFAULT_EXPORT_TIMEOUT_SECONDS,
        ),
        sample_rate=max(0.0, min(1.0, _env_float("FUGUE_OBSERVABILITY_SAMPLE_RATE", _DEFAULT_SAMPLE_RATE))),
        identity_attrs=_identity_attrs_from_env(),
        emit_request_summaries=_env_bool("FUGUE_OBSERVABILITY_REQUEST_SUMMARY_ENABLED", True),
        emit_stage_spans=_env_bool("FUGUE_OBSERVABILITY_STAGE_SPANS_ENABLED", True),
        emit_metrics=_env_bool("FUGUE_OBSERVABILITY_METRICS_ENABLED", True),
    )


def emit_oaix_request_observability(**kwargs: Any) -> None:
    client = _client
    if client is None:
        return
    try:
        client.emit_oaix_request(**kwargs)
    except Exception:
        logger.exception("Failed to enqueue Fugue request observability event")


def emit_oaix_request_log_write_observability(**kwargs: Any) -> None:
    client = _client
    if client is None:
        return
    try:
        client.emit_request_log_written(**kwargs)
    except Exception:
        logger.exception("Failed to enqueue Fugue request-log observability event")


def build_oaix_request_telemetry(
    *,
    service_name: str,
    service_version: str | None,
    identity_attrs: dict[str, str] | None,
    request_id: str | None,
    endpoint: str | None,
    model: str | None,
    is_stream: bool | None,
    request_log_id: int | None,
    status_code: int,
    success: bool | None,
    attempt_count: int | None,
    started_at: datetime | None,
    finished_at: datetime | None,
    first_token_at: datetime | None,
    spans: dict[str, Any] | None,
    error_message: str | None,
    request_log_queue_depth: int | None = None,
    runtime_metric_values: dict[str, int | None] | None = None,
) -> dict[str, list[dict[str, Any]]]:
    safe_spans = dict(spans or {})
    started = _as_datetime(started_at)
    finished = _as_datetime(finished_at) or datetime.now(timezone.utc)
    first_token = _as_datetime(first_token_at)
    trace_id = _safe_text(safe_spans.get("trace_id")) or _safe_text(request_id)
    req_id = _safe_text(request_id)
    route_id = _route_id(endpoint)
    duration_ms = _duration_ms(started, finished)
    ttft_ms = _duration_ms(started, first_token) if first_token is not None else None
    upstream_headers_ms = _span_ms(safe_spans, "upstream_headers_ms") + _span_ms(
        safe_spans,
        "upstream_compact_headers_ms",
    )
    token_acquire_ms = _span_ms(safe_spans, "claim_prompt_cache_affinity_ms") + _span_ms(
        safe_spans,
        "claim_token_ms",
    )
    error_type = _classify_error(status_code, error_message)

    base = _base_attrs(
        service_name=service_name,
        service_version=service_version,
        identity_attrs=identity_attrs,
        trace_id=trace_id,
        request_id=req_id,
        endpoint=endpoint,
        model=model,
        is_stream=is_stream,
        status_code=status_code,
        request_log_id=request_log_id,
        route_id=route_id,
        error_type=error_type,
    )
    if attempt_count is not None:
        base["attempt_count"] = str(max(0, int(attempt_count)))
        base["retry_count"] = str(max(0, int(attempt_count) - 1))
    if success is not None:
        base["success"] = "true" if success else "false"

    logs = [
        {
            "timestamp": _iso_timestamp(finished),
            "level": _event_level(status_code),
            "service": service_name,
            "trace_id": trace_id,
            "request_id": req_id,
            "event": "request_summary",
            "event_type": "request_summary",
            "source": service_name,
            "message": "oaix request finished",
            "attributes": _drop_empty(
                {
                    **base,
                    "duration_ms": _int_text(duration_ms),
                    "total_ms": _int_text(duration_ms),
                    "ttfb_ms": _int_text(ttft_ms),
                    "ttft_ms": _int_text(ttft_ms),
                    "upstream_ms": _int_text(upstream_headers_ms),
                    "method": "POST",
                    "path_template": _safe_text(endpoint),
                    "status_class": _status_class(status_code),
                }
            ),
            "summary": _drop_empty(
                {
                    "token_acquire_ms": _int_text(token_acquire_ms),
                    "http_pool_wait_ms": _int_text(_span_ms(safe_spans, "http_pool_wait_ms")),
                    "event_loop_lag_ms": _int_text(_span_ms(safe_spans, "event_loop_lag_ms")),
                    "request_log_queue_wait_ms": _int_text(_span_ms(safe_spans, "request_log_queue_wait_ms")),
                    "request_log_queue_depth": _int_text(request_log_queue_depth),
                }
            ),
        }
    ]

    traces = []
    parent_span_id = _safe_text(safe_spans.get("parent_span_id"))
    for stage, stage_ms, stage_attrs in _oaix_stage_rows(
        safe_spans,
        started=started,
        finished=finished,
        first_token=first_token,
    ):
        traces.append(
            {
                "timestamp": _iso_timestamp(finished),
                "kind": "span",
                "event_type": "request_span",
                "source": service_name,
                "message": stage,
                "attributes": _drop_empty(
                    {
                        **base,
                        **stage_attrs,
                        "span_id": _span_id(trace_id, req_id, stage),
                        "parent_span_id": parent_span_id,
                        "stage": stage,
                        "stage_ms": _int_text(stage_ms),
                    }
                ),
            }
        )

    metrics = _request_metric_events(
        service_name=service_name,
        identity_attrs=identity_attrs,
        timestamp=finished,
        status_code=status_code,
        route_id=route_id,
        values={
            "oaix_request_duration_ms": duration_ms,
            "oaix_request_ttfb_ms": ttft_ms,
            "oaix_http_pool_wait_ms": _span_ms(safe_spans, "http_pool_wait_ms"),
            "oaix_token_pool_wait_ms": token_acquire_ms,
            "oaix_token_acquire_ms": token_acquire_ms,
            "oaix_event_loop_lag_ms": _span_ms(safe_spans, "event_loop_lag_ms"),
            "oaix_request_log_queue_depth": request_log_queue_depth,
            **(runtime_metric_values or {}),
        },
    )
    return {"logs": logs, "traces": traces, "metrics": metrics}


def build_oaix_request_log_written_event(
    *,
    service_name: str,
    service_version: str | None,
    identity_attrs: dict[str, str] | None,
    payload: dict[str, Any],
    request_log_id: int | None,
    write_ms: int,
    db_spans: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    request_id = _safe_text(payload.get("request_id"))
    if not request_id:
        return None
    spans = dict(payload.get("timing_spans") or {})
    if db_spans:
        spans.update(db_spans)
    trace_id = _safe_text(spans.get("trace_id")) or request_id
    endpoint = _safe_text(payload.get("endpoint"))
    status_code = _safe_int(payload.get("status_code"), 0)
    attrs = _base_attrs(
        service_name=service_name,
        service_version=service_version,
        identity_attrs=identity_attrs,
        trace_id=trace_id,
        request_id=request_id,
        endpoint=endpoint,
        model=_safe_text(payload.get("model_name") or payload.get("model")),
        is_stream=_safe_bool(payload.get("is_stream")),
        status_code=status_code,
        request_log_id=request_log_id,
        route_id=_route_id(endpoint),
        error_type=_classify_error(status_code, _safe_text(payload.get("error_message"))),
    )
    attrs.update(
        {
            "span_id": _span_id(trace_id, request_id, "request_log_written"),
            "stage": "request_log_written",
            "stage_ms": _int_text(write_ms),
            "request_log_queue_wait_ms": _int_text(_span_ms(spans, "request_log_queue_wait_ms")),
            "db_query_ms": _int_text(_span_ms(spans, "db_query_ms")),
        }
    )
    return {
        "timestamp": _iso_timestamp(_as_datetime(payload.get("finished_at")) or datetime.now(timezone.utc)),
        "kind": "span",
        "event_type": "request_span",
        "source": service_name,
        "message": "request_log_written",
        "attributes": _drop_empty(attrs),
    }


def build_oaix_request_log_write_metric_events(
    *,
    service_name: str,
    identity_attrs: dict[str, str] | None,
    payload: dict[str, Any],
    write_ms: int,
    db_spans: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    spans = dict(payload.get("timing_spans") or {})
    if db_spans:
        spans.update(db_spans)
    return _request_metric_events(
        service_name=service_name,
        identity_attrs=identity_attrs,
        timestamp=_as_datetime(payload.get("finished_at")) or datetime.now(timezone.utc),
        status_code=_safe_int(payload.get("status_code"), 0) or 0,
        route_id=_route_id(_safe_text(payload.get("endpoint"))),
        values={
            "oaix_request_log_queue_wait_ms": _span_ms(spans, "request_log_queue_wait_ms"),
            "oaix_request_log_write_ms": max(0, int(write_ms or 0)),
        },
    )


def _oaix_stage_rows(
    spans: dict[str, Any],
    *,
    started: datetime | None,
    finished: datetime,
    first_token: datetime | None,
) -> list[tuple[str, int, dict[str, str]]]:
    token_acquire_ms = _span_ms(spans, "claim_prompt_cache_affinity_ms") + _span_ms(spans, "claim_token_ms")
    headers_ms = _span_ms(spans, "upstream_headers_ms") + _span_ms(spans, "upstream_compact_headers_ms")
    rows = [
        (
            "request_received",
            0,
            {
                "event_loop_lag_ms": _int_text(_span_ms(spans, "event_loop_lag_ms")),
            },
        ),
        (
            "token_pool_checked",
            0,
            {
                "token_pool_snapshot_age_ms": _int_text(_span_ms(spans, "token_pool_snapshot_age_ms")),
                "token_pool_snapshot_stale_count": _int_text(_span_ms(spans, "token_pool_snapshot_stale_count")),
            },
        ),
        (
            "token_acquired",
            token_acquire_ms,
            {
                "claim_token_source": _safe_text(spans.get("claim_token_source")),
                "claim_token_memory_count": _int_text(_span_ms(spans, "claim_token_memory_count")),
                "claim_token_memory_miss_count": _int_text(_span_ms(spans, "claim_token_memory_miss_count")),
                "claim_token_db_fallback_count": _int_text(_span_ms(spans, "claim_token_db_fallback_count")),
            },
        ),
        ("http_pool_acquired", _span_ms(spans, "http_pool_wait_ms"), {}),
        ("http_connect_done", _span_ms(spans, "http_connect_ms"), {}),
        ("http_tls_done", _span_ms(spans, "http_tls_ms"), {}),
        ("upstream_send_start", _span_ms(spans, "http_request_write_ms"), {}),
        ("upstream_headers_received", headers_ms, {}),
        ("upstream_first_token", _duration_ms(started, first_token), {}),
        ("response_stream_end", _duration_ms(started, finished), {}),
        (
            "request_log_enqueued",
            0,
            {
                "request_log_queue_wait_ms": _int_text(_span_ms(spans, "request_log_queue_wait_ms")),
            },
        ),
    ]
    return [(stage, max(0, int(ms or 0)), attrs) for stage, ms, attrs in rows]


def _request_metric_events(
    *,
    service_name: str,
    identity_attrs: dict[str, str] | None,
    timestamp: datetime,
    status_code: int,
    route_id: str | None,
    values: dict[str, int | None],
) -> list[dict[str, Any]]:
    base_attrs = _drop_empty(
        {
            **(identity_attrs or {}),
            "component": service_name,
            "route_id": route_id,
            "method": "POST",
            "status_class": _status_class(status_code),
        }
    )
    events = []
    for metric, value in values.items():
        if value is None:
            continue
        events.append(
            {
                "timestamp": _iso_timestamp(timestamp),
                "kind": "metric",
                "source": service_name,
                "message": metric,
                "metric": metric,
                "value": max(0, int(value)),
                "attributes": base_attrs,
            }
        )
    return events


def _base_attrs(
    *,
    service_name: str,
    service_version: str | None,
    identity_attrs: dict[str, str] | None,
    trace_id: str | None,
    request_id: str | None,
    endpoint: str | None,
    model: str | None,
    is_stream: bool | None,
    status_code: int,
    request_log_id: int | None,
    route_id: str | None,
    error_type: str | None,
) -> dict[str, str]:
    attrs = {
        **(identity_attrs or {}),
        "service": service_name,
        "component": service_name,
        "service_version": _safe_text(service_version),
        "trace_id": _safe_text(trace_id),
        "request_id": _safe_text(request_id),
        "oaix_request_log_id": _int_text(request_log_id),
        "route": _safe_text(endpoint),
        "route_id": route_id,
        "path_template": _safe_text(endpoint),
        "request_kind": _safe_text(endpoint),
        "model": _safe_text(model),
        "stream": _bool_text(is_stream),
        "streaming": _bool_text(is_stream),
        "status_code": _int_text(status_code),
        "status_class": _status_class(status_code),
        "error_type": error_type,
    }
    return _drop_empty(attrs)


def _identity_attrs_from_env() -> dict[str, str]:
    env_map = {
        "tenant_id": "FUGUE_OBSERVABILITY_TENANT_ID",
        "project_id": "FUGUE_OBSERVABILITY_PROJECT_ID",
        "app_id": "FUGUE_OBSERVABILITY_APP_ID",
        "runtime_id": "FUGUE_OBSERVABILITY_RUNTIME_ID",
        "pod": "HOSTNAME",
    }
    return _drop_empty({key: _env_text(env_name) for key, env_name in env_map.items()})


def _endpoint_url(endpoint: str, path: str) -> str:
    base = endpoint.strip().rstrip("/")
    if base.endswith(("/v1/logs", "/v1/metrics", "/v1/traces")):
        base = base.rsplit("/v1/", 1)[0]
    return base + path


def _env_text(name: str) -> str | None:
    value = str(os.getenv(name, "")).strip()
    return value or None


def _env_bool(name: str, default: bool) -> bool:
    value = str(os.getenv(name, "")).strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    return default


def _env_int(name: str, default: int) -> int:
    try:
        return int(str(os.getenv(name, "")).strip() or default)
    except (TypeError, ValueError):
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(str(os.getenv(name, "")).strip() or default)
    except (TypeError, ValueError):
        return default


def _safe_text(value: Any, *, max_length: int = 160) -> str | None:
    if value is None:
        return None
    text = str(value).strip().replace("\n", " ").replace("\r", " ")
    if not text:
        return None
    return text[:max_length]


def _safe_int(value: Any, default: int | None = None) -> int | None:
    try:
        if value is None:
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _safe_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return None


def _span_ms(spans: dict[str, Any], name: str) -> int:
    value = _safe_int(spans.get(name), 0)
    return max(0, int(value or 0))


def _as_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    return None


def _duration_ms(started: datetime | None, finished: datetime | None) -> int | None:
    if started is None or finished is None:
        return None
    return max(0, int((finished - started).total_seconds() * 1000))


def _iso_timestamp(value: datetime | None) -> str:
    timestamp = _as_datetime(value) or datetime.now(timezone.utc)
    return timestamp.isoformat().replace("+00:00", "Z")


def _int_text(value: int | None) -> str | None:
    if value is None:
        return None
    return str(max(0, int(value)))


def _bool_text(value: bool | None) -> str | None:
    if value is None:
        return None
    return "true" if value else "false"


def _drop_empty(values: dict[str, Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    for key, value in values.items():
        if value is None:
            continue
        text = _safe_text(value)
        if text is None:
            continue
        out[str(key)] = text
    return out


def _status_class(status_code: int) -> str:
    code = max(0, int(status_code or 0))
    if code <= 0:
        return "unknown"
    return f"{code // 100}xx"


def _event_level(status_code: int) -> str:
    code = max(0, int(status_code or 0))
    if code >= 500:
        return "error"
    if code >= 400:
        return "warning"
    return "info"


def _classify_error(status_code: int, error_message: str | None) -> str | None:
    if status_code < 400 and not error_message:
        return None
    text = (error_message or "").lower()
    if status_code == 499 or "client disconnected" in text:
        return "client_disconnected"
    if "pool" in text and "timeout" in text:
        return "pool_timeout"
    if "timeout" in text:
        return "timeout"
    if status_code == 429:
        return "rate_limited"
    if status_code in (401, 403):
        return "auth_error"
    if status_code >= 500:
        return "upstream_or_gateway_5xx"
    if status_code >= 400:
        return "client_error"
    return "error"


def _route_id(endpoint: str | None) -> str | None:
    text = _safe_text(endpoint)
    if text is None:
        return None
    route = text.strip("/").replace("/", "_").replace("-", "_")
    return route or "root"


def _span_id(trace_id: str | None, request_id: str | None, stage: str) -> str:
    seed = "|".join(part or "" for part in (trace_id, request_id, stage))
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()[:16]
