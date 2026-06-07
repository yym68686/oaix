from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from time import perf_counter
from typing import Any

from sqlalchemy import case, delete, func, or_, select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from .database import CodexToken, GatewayRequestHourlyStat, GatewayRequestLog, get_read_session, get_request_log_session, utcnow


@dataclass(frozen=True)
class RequestLogItem:
    id: int
    request_id: str
    endpoint: str
    model: str | None
    model_name: str | None
    is_stream: bool
    status_code: int | None
    success: bool | None
    attempt_count: int
    token_id: int | None
    account_id: str | None
    client_ip: str | None
    user_agent: str | None
    started_at: datetime
    finished_at: datetime | None
    first_token_at: datetime | None
    ttft_ms: int | None
    duration_ms: int | None
    timing_spans: dict[str, Any] | None
    input_tokens: int | None
    cached_input_tokens: int | None
    output_tokens: int | None
    total_tokens: int | None
    estimated_cost_usd: float | None
    request_payload_hash: str | None
    upstream_payload_hash: str | None
    prompt_template_hash: str | None
    prompt_dynamic_hash: str | None
    prompt_cache_source: str | None
    prompt_cache_key_hash: str | None
    prompt_cache_retention_requested: str | None
    prompt_cache_retention_sent: str | None
    session_id_hash: str | None
    session_id_source: str | None
    previous_response_id_hash: str | None
    upstream_response_id: str | None
    cache_hit_ratio: float | None
    cache_affinity_result: str | None
    cache_affinity_lane_index: int | None
    prompt_cache_trace: dict[str, Any] | None
    error_message: str | None


@dataclass(frozen=True)
class RequestLogSummary:
    total: int
    successful: int
    failed: int
    streaming: int
    input_tokens: int
    output_tokens: int
    total_tokens: int
    estimated_cost_usd: float
    avg_ttft_ms: int | None


def _ms_between(started_at: datetime | None, ended_at: datetime | None) -> int | None:
    if started_at is None or ended_at is None:
        return None
    return max(0, int((ended_at - started_at).total_seconds() * 1000))


def _cache_hit_ratio(input_tokens: int | None, cached_input_tokens: int | None) -> float | None:
    try:
        resolved_input_tokens = int(input_tokens)
        resolved_cached_input_tokens = int(cached_input_tokens)
    except (TypeError, ValueError, ZeroDivisionError):
        return None
    if resolved_input_tokens <= 0:
        return None
    ratio = float(resolved_cached_input_tokens) / float(resolved_input_tokens)
    return max(0.0, min(1.0, ratio))


def _normalize_timing_spans(timing_spans: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(timing_spans, dict):
        return None

    normalized: dict[str, Any] = {}
    for key, value in timing_spans.items():
        name = str(key or "").strip()
        if not name or isinstance(value, bool):
            continue
        if isinstance(value, str):
            text = value.strip()
            if text:
                normalized[name] = text[:128]
            continue
        try:
            normalized[name] = max(0, int(round(float(value))))
        except (TypeError, ValueError):
            continue
    return normalized or None


@dataclass(frozen=True)
class RequestAnalyticsBucket:
    bucket_start: datetime
    request_count: int
    input_tokens: int
    output_tokens: int
    total_tokens: int
    estimated_cost_usd: float


@dataclass(frozen=True)
class RequestAnalyticsModel:
    model_name: str
    request_count: int
    total_tokens: int
    estimated_cost_usd: float


@dataclass(frozen=True)
class RequestLogAnalytics:
    period_hours: int
    bucket_minutes: int
    buckets: list[RequestAnalyticsBucket]
    models: list[RequestAnalyticsModel]


def _normalize_request_account_ids(account_ids: list[str] | set[str] | tuple[str, ...]) -> list[str]:
    return sorted({str(value or "").strip() for value in account_ids if str(value or "").strip()})


def _normalize_request_token_ids(token_ids: list[int] | set[int] | tuple[int, ...]) -> list[int]:
    normalized: set[int] = set()
    for value in token_ids:
        try:
            token_id = int(value)
        except (TypeError, ValueError):
            continue
        if token_id > 0:
            normalized.add(token_id)
    return sorted(normalized)


def _build_request_account_costs_stmt(*, account_ids: list[str]):
    return (
        select(
            GatewayRequestLog.account_id,
            func.sum(GatewayRequestLog.estimated_cost_usd),
        )
        .where(GatewayRequestLog.account_id.in_(account_ids))
        .group_by(GatewayRequestLog.account_id)
    )


def _build_request_token_costs_stmt(*, token_ids: list[int]):
    canonical_token_id = func.coalesce(CodexToken.merged_into_token_id, CodexToken.id)
    return (
        select(
            canonical_token_id,
            func.sum(GatewayRequestLog.estimated_cost_usd),
        )
        .select_from(GatewayRequestLog)
        .join(CodexToken, GatewayRequestLog.token_id == CodexToken.id)
        .where(canonical_token_id.in_(token_ids))
        .group_by(canonical_token_id)
    )


def _int_or_zero(value) -> int:
    if value is None:
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _float_or_zero(value) -> float:
    if value is None:
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _round_cost(value: float) -> float:
    return round(max(0.0, float(value)), 6)


def _floor_bucket_start(value: datetime, *, bucket_minutes: int) -> datetime:
    bucket = max(1, int(bucket_minutes))
    value = value.replace(second=0, microsecond=0)
    minute = (value.minute // bucket) * bucket
    return value.replace(minute=minute)


def _hour_bucket_start(value: datetime) -> datetime:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)


def _request_model_label_expr():
    return func.coalesce(GatewayRequestLog.model_name, GatewayRequestLog.model, "未识别模型")


def _request_hourly_model_label_expr():
    return func.coalesce(GatewayRequestHourlyStat.model_name, "未识别模型")


def _hour_floor(value: datetime) -> datetime:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)


def _build_request_log_summary_stmt(*, since: datetime | None = None):
    stmt = select(
        func.count().label("total"),
        func.sum(case((GatewayRequestLog.success.is_(True), 1), else_=0)).label("successful"),
        func.sum(case((GatewayRequestLog.success.is_(False), 1), else_=0)).label("failed"),
        func.sum(case((GatewayRequestLog.is_stream.is_(True), 1), else_=0)).label("streaming"),
        func.avg(GatewayRequestLog.ttft_ms).label("avg_ttft_ms"),
        func.sum(GatewayRequestLog.input_tokens).label("input_tokens"),
        func.sum(GatewayRequestLog.output_tokens).label("output_tokens"),
        func.sum(GatewayRequestLog.total_tokens).label("total_tokens"),
        func.sum(GatewayRequestLog.estimated_cost_usd).label("estimated_cost_usd"),
    ).select_from(GatewayRequestLog)
    if since is not None:
        stmt = stmt.where(GatewayRequestLog.started_at >= since)
    return stmt


def _build_request_log_summary_stats_stmt(*, since: datetime | None = None):
    ttft_count = func.sum(GatewayRequestHourlyStat.ttft_count)
    stmt = select(
        func.coalesce(func.sum(GatewayRequestHourlyStat.request_count), 0).label("total"),
        func.coalesce(func.sum(GatewayRequestHourlyStat.success_count), 0).label("successful"),
        func.coalesce(func.sum(GatewayRequestHourlyStat.failure_count), 0).label("failed"),
        func.coalesce(func.sum(GatewayRequestHourlyStat.streaming_count), 0).label("streaming"),
        (func.sum(GatewayRequestHourlyStat.ttft_ms_sum) / func.nullif(ttft_count, 0)).label("avg_ttft_ms"),
        func.coalesce(func.sum(GatewayRequestHourlyStat.input_tokens), 0).label("input_tokens"),
        func.coalesce(func.sum(GatewayRequestHourlyStat.output_tokens), 0).label("output_tokens"),
        func.coalesce(func.sum(GatewayRequestHourlyStat.total_tokens), 0).label("total_tokens"),
        func.coalesce(func.sum(GatewayRequestHourlyStat.estimated_cost_usd), 0).label("estimated_cost_usd"),
    ).select_from(GatewayRequestHourlyStat)
    if since is not None:
        stmt = stmt.where(GatewayRequestHourlyStat.bucket_start >= _hour_floor(since))
    return stmt


def _build_request_model_analytics_stmt(*, since: datetime, top_models: int):
    model_source = (
        select(
            _request_model_label_expr().label("model_name"),
            GatewayRequestLog.total_tokens.label("total_tokens"),
            GatewayRequestLog.estimated_cost_usd.label("estimated_cost_usd"),
        )
        .where(GatewayRequestLog.started_at >= since)
        .subquery()
    )
    return (
        select(
            model_source.c.model_name,
            func.count(),
            func.sum(model_source.c.total_tokens),
            func.sum(model_source.c.estimated_cost_usd),
        )
        .group_by(model_source.c.model_name)
        .order_by(
            func.sum(model_source.c.estimated_cost_usd).desc().nullslast(),
            func.sum(model_source.c.total_tokens).desc().nullslast(),
            func.count().desc(),
        )
        .limit(top_models)
    )


def _build_request_model_analytics_stats_stmt(*, since: datetime, top_models: int):
    model_source = (
        select(
            _request_hourly_model_label_expr().label("model_name"),
            GatewayRequestHourlyStat.request_count.label("request_count"),
            GatewayRequestHourlyStat.total_tokens.label("total_tokens"),
            GatewayRequestHourlyStat.estimated_cost_usd.label("estimated_cost_usd"),
        )
        .where(GatewayRequestHourlyStat.bucket_start >= _hour_floor(since))
        .subquery()
    )
    return (
        select(
            model_source.c.model_name,
            func.sum(model_source.c.request_count),
            func.sum(model_source.c.total_tokens),
            func.sum(model_source.c.estimated_cost_usd),
        )
        .group_by(model_source.c.model_name)
        .order_by(
            func.sum(model_source.c.estimated_cost_usd).desc().nullslast(),
            func.sum(model_source.c.total_tokens).desc().nullslast(),
            func.sum(model_source.c.request_count).desc(),
        )
        .limit(top_models)
    )


def _build_request_bucket_analytics_stmt(*, since: datetime, bucket_minutes: int):
    safe_bucket_minutes = max(1, int(bucket_minutes))
    bucket_start = func.date_bin(
        text(f"INTERVAL '{safe_bucket_minutes} minutes'"),
        GatewayRequestLog.started_at,
        text("TIMESTAMPTZ '1970-01-01 00:00:00+00'"),
    ).label("bucket_start")
    return (
        select(
            bucket_start,
            func.count().label("request_count"),
            func.sum(GatewayRequestLog.input_tokens).label("input_tokens"),
            func.sum(GatewayRequestLog.output_tokens).label("output_tokens"),
            func.sum(GatewayRequestLog.total_tokens).label("total_tokens"),
            func.sum(GatewayRequestLog.estimated_cost_usd).label("estimated_cost_usd"),
        )
        .where(GatewayRequestLog.started_at >= since)
        .group_by(bucket_start)
        .order_by(bucket_start.asc())
    )


def _build_request_bucket_analytics_stats_stmt(*, since: datetime, bucket_minutes: int):
    safe_bucket_minutes = max(1, int(bucket_minutes))
    bucket_start = func.date_bin(
        text(f"INTERVAL '{safe_bucket_minutes} minutes'"),
        GatewayRequestHourlyStat.bucket_start,
        text("TIMESTAMPTZ '1970-01-01 00:00:00+00'"),
    ).label("bucket_start")
    return (
        select(
            bucket_start,
            func.sum(GatewayRequestHourlyStat.request_count).label("request_count"),
            func.sum(GatewayRequestHourlyStat.input_tokens).label("input_tokens"),
            func.sum(GatewayRequestHourlyStat.output_tokens).label("output_tokens"),
            func.sum(GatewayRequestHourlyStat.total_tokens).label("total_tokens"),
            func.sum(GatewayRequestHourlyStat.estimated_cost_usd).label("estimated_cost_usd"),
        )
        .where(GatewayRequestHourlyStat.bucket_start >= _hour_floor(since))
        .group_by(bucket_start)
        .order_by(bucket_start.asc())
    )


async def create_request_log(
    *,
    request_id: str,
    endpoint: str,
    model: str | None,
    model_name: str | None = None,
    is_stream: bool,
    started_at: datetime | None = None,
    client_ip: str | None = None,
    user_agent: str | None = None,
    request_payload_hash: str | None = None,
    upstream_payload_hash: str | None = None,
    prompt_template_hash: str | None = None,
    prompt_dynamic_hash: str | None = None,
    prompt_cache_source: str | None = None,
    prompt_cache_key_hash: str | None = None,
    prompt_cache_retention_requested: str | None = None,
    prompt_cache_retention_sent: str | None = None,
    session_id_hash: str | None = None,
    session_id_source: str | None = None,
    previous_response_id_hash: str | None = None,
    upstream_response_id: str | None = None,
    cache_hit_ratio: float | None = None,
    cache_affinity_result: str | None = None,
    cache_affinity_lane_index: int | None = None,
    prompt_cache_trace: dict[str, Any] | None = None,
) -> GatewayRequestLog:
    started_at = started_at or utcnow()
    async with get_request_log_session() as session:
        async with session.begin():
            item = GatewayRequestLog(
                request_id=request_id,
                endpoint=endpoint,
                model=model,
                model_name=model_name or model,
                is_stream=is_stream,
                started_at=started_at,
                client_ip=client_ip,
                user_agent=user_agent,
                request_payload_hash=request_payload_hash,
                upstream_payload_hash=upstream_payload_hash,
                prompt_template_hash=prompt_template_hash,
                prompt_dynamic_hash=prompt_dynamic_hash,
                prompt_cache_source=prompt_cache_source,
                prompt_cache_key_hash=prompt_cache_key_hash,
                prompt_cache_retention_requested=prompt_cache_retention_requested,
                prompt_cache_retention_sent=prompt_cache_retention_sent,
                session_id_hash=session_id_hash,
                session_id_source=session_id_source,
                previous_response_id_hash=previous_response_id_hash,
                upstream_response_id=upstream_response_id,
                cache_hit_ratio=cache_hit_ratio,
                cache_affinity_result=cache_affinity_result,
                cache_affinity_lane_index=cache_affinity_lane_index,
                prompt_cache_trace=prompt_cache_trace,
            )
            session.add(item)
            await session.flush()
            return item


async def finalize_request_log(
    request_log_id: int,
    *,
    status_code: int,
    success: bool,
    attempt_count: int,
    finished_at: datetime | None = None,
    first_token_at: datetime | None = None,
    token_id: int | None = None,
    account_id: str | None = None,
    model_name: str | None = None,
    input_tokens: int | None = None,
    cached_input_tokens: int | None = None,
    output_tokens: int | None = None,
    total_tokens: int | None = None,
    estimated_cost_usd: float | None = None,
    request_payload_hash: str | None = None,
    upstream_payload_hash: str | None = None,
    prompt_template_hash: str | None = None,
    prompt_dynamic_hash: str | None = None,
    prompt_cache_source: str | None = None,
    prompt_cache_key_hash: str | None = None,
    prompt_cache_retention_requested: str | None = None,
    prompt_cache_retention_sent: str | None = None,
    session_id_hash: str | None = None,
    session_id_source: str | None = None,
    previous_response_id_hash: str | None = None,
    upstream_response_id: str | None = None,
    cache_hit_ratio: float | None = None,
    cache_affinity_result: str | None = None,
    cache_affinity_lane_index: int | None = None,
    prompt_cache_trace: dict[str, Any] | None = None,
    timing_spans: dict[str, Any] | None = None,
    error_message: str | None = None,
) -> dict[str, Any] | None:
    finalize_started = perf_counter()
    finished_at = finished_at or utcnow()
    resolved_timing_spans = _normalize_timing_spans(timing_spans)
    async with get_request_log_session() as session:
        async with session.begin():
            item = await session.get(GatewayRequestLog, request_log_id, with_for_update=True)
            if item is None:
                return None
            item.status_code = int(status_code)
            item.success = bool(success)
            item.attempt_count = max(0, int(attempt_count))
            item.finished_at = finished_at
            item.first_token_at = first_token_at
            item.ttft_ms = _ms_between(item.started_at, first_token_at)
            item.duration_ms = _ms_between(item.started_at, finished_at)
            if token_id is not None:
                try:
                    item.token_id = max(1, int(token_id))
                except (TypeError, ValueError):
                    pass
            item.account_id = account_id or item.account_id
            item.model_name = model_name or item.model_name or item.model
            item.input_tokens = _int_or_zero(input_tokens) if input_tokens is not None else item.input_tokens
            item.cached_input_tokens = (
                _int_or_zero(cached_input_tokens) if cached_input_tokens is not None else item.cached_input_tokens
            )
            item.output_tokens = _int_or_zero(output_tokens) if output_tokens is not None else item.output_tokens
            item.total_tokens = _int_or_zero(total_tokens) if total_tokens is not None else item.total_tokens
            item.estimated_cost_usd = (
                _round_cost(estimated_cost_usd) if estimated_cost_usd is not None else item.estimated_cost_usd
            )
            item.request_payload_hash = str(request_payload_hash or "").strip()[:64] or item.request_payload_hash
            item.upstream_payload_hash = str(upstream_payload_hash or "").strip()[:64] or item.upstream_payload_hash
            item.prompt_template_hash = str(prompt_template_hash or "").strip()[:64] or item.prompt_template_hash
            item.prompt_dynamic_hash = str(prompt_dynamic_hash or "").strip()[:64] or item.prompt_dynamic_hash
            item.prompt_cache_source = str(prompt_cache_source or "").strip()[:64] or item.prompt_cache_source
            item.prompt_cache_key_hash = (
                str(prompt_cache_key_hash or "").strip()[:64] or item.prompt_cache_key_hash
            )
            item.prompt_cache_retention_requested = (
                str(prompt_cache_retention_requested or "").strip()[:32] or item.prompt_cache_retention_requested
            )
            item.prompt_cache_retention_sent = (
                str(prompt_cache_retention_sent or "").strip()[:32] or item.prompt_cache_retention_sent
            )
            item.session_id_hash = str(session_id_hash or "").strip()[:64] or item.session_id_hash
            item.session_id_source = str(session_id_source or "").strip()[:32] or item.session_id_source
            item.previous_response_id_hash = (
                str(previous_response_id_hash or "").strip()[:64] or item.previous_response_id_hash
            )
            item.upstream_response_id = str(upstream_response_id or "").strip()[:128] or item.upstream_response_id
            if cache_hit_ratio is not None:
                try:
                    item.cache_hit_ratio = max(0.0, min(1.0, float(cache_hit_ratio)))
                except (TypeError, ValueError):
                    pass
            if item.cache_hit_ratio is None:
                item.cache_hit_ratio = _cache_hit_ratio(item.input_tokens, item.cached_input_tokens)
            item.cache_affinity_result = (
                str(cache_affinity_result or "").strip()[:64] or item.cache_affinity_result
            )
            if cache_affinity_lane_index is not None:
                item.cache_affinity_lane_index = _int_or_zero(cache_affinity_lane_index)
            if prompt_cache_trace is not None:
                item.prompt_cache_trace = prompt_cache_trace
            if resolved_timing_spans is not None:
                resolved_timing_spans["finalize_ms"] = max(0, int((perf_counter() - finalize_started) * 1000))
                item.timing_spans = resolved_timing_spans
            item.error_message = (error_message or "").strip()[:4000] or None
    return resolved_timing_spans


def _request_log_payload_values(payload: dict[str, Any]) -> dict[str, Any]:
    started_at = payload.get("started_at") or utcnow()
    finished_at = payload.get("finished_at")
    first_token_at = payload.get("first_token_at")
    model = payload.get("model")
    model_name = payload.get("model_name") or model
    timing_spans = _normalize_timing_spans(payload.get("timing_spans"))
    prompt_cache_trace = payload.get("prompt_cache_trace") if isinstance(payload.get("prompt_cache_trace"), dict) else None
    trace = prompt_cache_trace or {}
    prompt_trace = trace.get("prompt") if isinstance(trace.get("prompt"), dict) else {}
    route_trace = trace.get("route") if isinstance(trace.get("route"), dict) else {}
    usage_trace = trace.get("usage") if isinstance(trace.get("usage"), dict) else {}
    response_trace = trace.get("response") if isinstance(trace.get("response"), dict) else {}
    error_message = str(payload.get("error_message") or "").strip()[:4000] or None
    status_code = payload.get("status_code")
    try:
        resolved_status_code = int(status_code) if status_code is not None else None
    except (TypeError, ValueError):
        resolved_status_code = None
    try:
        attempt_count = max(0, int(payload.get("attempt_count") or 0))
    except (TypeError, ValueError):
        attempt_count = 0
    try:
        token_id = payload.get("token_id")
        resolved_token_id = max(1, int(token_id)) if token_id is not None else None
    except (TypeError, ValueError):
        resolved_token_id = None

    return {
        "request_id": str(payload["request_id"]),
        "endpoint": str(payload.get("endpoint") or ""),
        "model": model,
        "model_name": model_name,
        "is_stream": bool(payload.get("is_stream")),
        "status_code": resolved_status_code,
        "success": payload.get("success") if payload.get("success") is not None else None,
        "attempt_count": attempt_count,
        "token_id": resolved_token_id,
        "account_id": payload.get("account_id"),
        "client_ip": payload.get("client_ip"),
        "user_agent": payload.get("user_agent"),
        "started_at": started_at,
        "finished_at": finished_at,
        "first_token_at": first_token_at,
        "ttft_ms": _ms_between(started_at, first_token_at),
        "duration_ms": _ms_between(started_at, finished_at),
        "timing_spans": timing_spans,
        "input_tokens": _int_or_zero(payload.get("input_tokens")) if payload.get("input_tokens") is not None else None,
        "cached_input_tokens": (
            _int_or_zero(payload.get("cached_input_tokens")) if payload.get("cached_input_tokens") is not None else None
        ),
        "output_tokens": _int_or_zero(payload.get("output_tokens")) if payload.get("output_tokens") is not None else None,
        "total_tokens": _int_or_zero(payload.get("total_tokens")) if payload.get("total_tokens") is not None else None,
        "estimated_cost_usd": (
            _round_cost(payload.get("estimated_cost_usd")) if payload.get("estimated_cost_usd") is not None else None
        ),
        "request_payload_hash": str(payload.get("request_payload_hash") or trace.get("request_payload_hash") or "").strip()[:64] or None,
        "upstream_payload_hash": str(payload.get("upstream_payload_hash") or trace.get("upstream_payload_hash") or "").strip()[:64] or None,
        "prompt_template_hash": str(payload.get("prompt_template_hash") or prompt_trace.get("template_hash") or "").strip()[:64] or None,
        "prompt_dynamic_hash": str(payload.get("prompt_dynamic_hash") or prompt_trace.get("dynamic_hash") or "").strip()[:64] or None,
        "prompt_cache_source": str(payload.get("prompt_cache_source") or trace.get("prompt_cache_key_source") or "").strip()[:64] or None,
        "prompt_cache_key_hash": str(payload.get("prompt_cache_key_hash") or trace.get("prompt_cache_key_hash") or "").strip()[:64] or None,
        "prompt_cache_retention_requested": (
            str(payload.get("prompt_cache_retention_requested") or trace.get("prompt_cache_retention_requested") or "").strip()[:32] or None
        ),
        "prompt_cache_retention_sent": str(payload.get("prompt_cache_retention_sent") or trace.get("prompt_cache_retention_sent") or "").strip()[:32] or None,
        "session_id_hash": str(payload.get("session_id_hash") or trace.get("session_id_hash") or "").strip()[:64] or None,
        "session_id_source": str(payload.get("session_id_source") or trace.get("session_id_source") or "").strip()[:32] or None,
        "previous_response_id_hash": str(payload.get("previous_response_id_hash") or trace.get("previous_response_id_hash") or "").strip()[:64] or None,
        "upstream_response_id": str(payload.get("upstream_response_id") or response_trace.get("response_id") or "").strip()[:128] or None,
        "cache_hit_ratio": (
            _float_or_zero(payload.get("cache_hit_ratio"))
            if payload.get("cache_hit_ratio") is not None
            else (
                _float_or_zero(usage_trace.get("cache_hit_ratio"))
                if usage_trace.get("cache_hit_ratio") is not None
                else _cache_hit_ratio(payload.get("input_tokens"), payload.get("cached_input_tokens"))
            )
        ),
        "cache_affinity_result": str(payload.get("cache_affinity_result") or route_trace.get("cache_affinity_result") or "").strip()[:64] or None,
        "cache_affinity_lane_index": (
            _int_or_zero(payload.get("cache_affinity_lane_index"))
            if payload.get("cache_affinity_lane_index") is not None
            else (
                _int_or_zero(route_trace.get("cache_affinity_lane_index"))
                if route_trace.get("cache_affinity_lane_index") is not None
                else None
            )
        ),
        "prompt_cache_trace": prompt_cache_trace,
        "error_message": error_message,
    }


def _merge_request_log_payload(item: GatewayRequestLog, values: dict[str, Any]) -> None:
    item.endpoint = values["endpoint"] or item.endpoint
    item.model = values["model"] if values["model"] is not None else item.model
    item.model_name = values["model_name"] or item.model_name or item.model
    item.is_stream = bool(values["is_stream"])
    item.client_ip = values["client_ip"] or item.client_ip
    item.user_agent = values["user_agent"] or item.user_agent
    item.started_at = values["started_at"] or item.started_at
    if values["status_code"] is not None:
        item.status_code = values["status_code"]
        item.success = bool(values["success"])
        item.attempt_count = values["attempt_count"]
        item.finished_at = values["finished_at"]
        item.first_token_at = values["first_token_at"]
        item.ttft_ms = values["ttft_ms"]
        item.duration_ms = values["duration_ms"]
        if values["token_id"] is not None:
            item.token_id = values["token_id"]
        item.account_id = values["account_id"] or item.account_id
        item.model_name = values["model_name"] or item.model_name or item.model
        item.input_tokens = values["input_tokens"] if values["input_tokens"] is not None else item.input_tokens
        cached_input_tokens = values.get("cached_input_tokens")
        item.cached_input_tokens = cached_input_tokens if cached_input_tokens is not None else item.cached_input_tokens
        item.output_tokens = values["output_tokens"] if values["output_tokens"] is not None else item.output_tokens
        item.total_tokens = values["total_tokens"] if values["total_tokens"] is not None else item.total_tokens
        item.estimated_cost_usd = (
            values["estimated_cost_usd"] if values["estimated_cost_usd"] is not None else item.estimated_cost_usd
        )
        item.request_payload_hash = values.get("request_payload_hash") or item.request_payload_hash
        item.upstream_payload_hash = values.get("upstream_payload_hash") or item.upstream_payload_hash
        item.prompt_template_hash = values.get("prompt_template_hash") or item.prompt_template_hash
        item.prompt_dynamic_hash = values.get("prompt_dynamic_hash") or item.prompt_dynamic_hash
        item.prompt_cache_source = values.get("prompt_cache_source") or item.prompt_cache_source
        item.prompt_cache_key_hash = values.get("prompt_cache_key_hash") or item.prompt_cache_key_hash
        item.prompt_cache_retention_requested = (
            values.get("prompt_cache_retention_requested") or item.prompt_cache_retention_requested
        )
        item.prompt_cache_retention_sent = values.get("prompt_cache_retention_sent") or item.prompt_cache_retention_sent
        item.session_id_hash = values.get("session_id_hash") or item.session_id_hash
        item.session_id_source = values.get("session_id_source") or item.session_id_source
        item.previous_response_id_hash = values.get("previous_response_id_hash") or item.previous_response_id_hash
        item.upstream_response_id = values.get("upstream_response_id") or item.upstream_response_id
        cache_hit_ratio = values.get("cache_hit_ratio")
        item.cache_hit_ratio = cache_hit_ratio if cache_hit_ratio is not None else item.cache_hit_ratio
        item.cache_affinity_result = values.get("cache_affinity_result") or item.cache_affinity_result
        cache_affinity_lane_index = values.get("cache_affinity_lane_index")
        item.cache_affinity_lane_index = (
            cache_affinity_lane_index
            if cache_affinity_lane_index is not None
            else item.cache_affinity_lane_index
        )
        item.prompt_cache_trace = values.get("prompt_cache_trace") or item.prompt_cache_trace
        item.timing_spans = values["timing_spans"] or item.timing_spans
        item.error_message = values["error_message"]


def _request_hourly_stat_deltas(rows: list[Any]) -> list[dict[str, Any]]:
    grouped: dict[tuple[datetime, str], dict[str, Any]] = {}
    for row in rows:
        started_at = getattr(row, "started_at", None)
        if not isinstance(started_at, datetime):
            continue
        model_name = str(getattr(row, "model_name", None) or getattr(row, "model", None) or "未识别模型").strip()
        model_name = (model_name or "未识别模型")[:128]
        key = (_hour_bucket_start(started_at), model_name)
        item = grouped.setdefault(
            key,
            {
                "bucket_start": key[0],
                "model_name": key[1],
                "request_count": 0,
                "success_count": 0,
                "failure_count": 0,
                "streaming_count": 0,
                "input_tokens": 0,
                "output_tokens": 0,
                "total_tokens": 0,
                "estimated_cost_usd": 0.0,
                "ttft_ms_sum": 0,
                "ttft_count": 0,
                "duration_ms_sum": 0,
                "duration_count": 0,
                "updated_at": utcnow(),
            },
        )
        item["request_count"] += 1
        if bool(getattr(row, "success", False)):
            item["success_count"] += 1
        else:
            item["failure_count"] += 1
        if bool(getattr(row, "is_stream", False)):
            item["streaming_count"] += 1
        item["input_tokens"] += _int_or_zero(getattr(row, "input_tokens", None))
        item["output_tokens"] += _int_or_zero(getattr(row, "output_tokens", None))
        item["total_tokens"] += _int_or_zero(getattr(row, "total_tokens", None))
        item["estimated_cost_usd"] += _float_or_zero(getattr(row, "estimated_cost_usd", None))
        ttft_ms = getattr(row, "ttft_ms", None)
        if ttft_ms is not None:
            item["ttft_ms_sum"] += _int_or_zero(ttft_ms)
            item["ttft_count"] += 1
        duration_ms = getattr(row, "duration_ms", None)
        if duration_ms is not None:
            item["duration_ms_sum"] += _int_or_zero(duration_ms)
            item["duration_count"] += 1
    return list(grouped.values())


async def _upsert_request_hourly_stats(session, deltas: list[dict[str, Any]]) -> None:
    if not deltas:
        return
    insert_stmt = pg_insert(GatewayRequestHourlyStat).values(deltas)
    excluded = insert_stmt.excluded
    await session.execute(
        insert_stmt.on_conflict_do_update(
            index_elements=[GatewayRequestHourlyStat.bucket_start, GatewayRequestHourlyStat.model_name],
            set_={
                "request_count": GatewayRequestHourlyStat.request_count + excluded.request_count,
                "success_count": GatewayRequestHourlyStat.success_count + excluded.success_count,
                "failure_count": GatewayRequestHourlyStat.failure_count + excluded.failure_count,
                "streaming_count": GatewayRequestHourlyStat.streaming_count + excluded.streaming_count,
                "input_tokens": GatewayRequestHourlyStat.input_tokens + excluded.input_tokens,
                "output_tokens": GatewayRequestHourlyStat.output_tokens + excluded.output_tokens,
                "total_tokens": GatewayRequestHourlyStat.total_tokens + excluded.total_tokens,
                "estimated_cost_usd": GatewayRequestHourlyStat.estimated_cost_usd + excluded.estimated_cost_usd,
                "ttft_ms_sum": GatewayRequestHourlyStat.ttft_ms_sum + excluded.ttft_ms_sum,
                "ttft_count": GatewayRequestHourlyStat.ttft_count + excluded.ttft_count,
                "duration_ms_sum": GatewayRequestHourlyStat.duration_ms_sum + excluded.duration_ms_sum,
                "duration_count": GatewayRequestHourlyStat.duration_count + excluded.duration_count,
                "updated_at": excluded.updated_at,
            },
        )
    )


async def _record_request_hourly_stats_once(session, request_ids: list[str]) -> None:
    normalized_ids = sorted({str(value or "").strip() for value in request_ids if str(value or "").strip()})
    if not normalized_ids:
        return
    now = utcnow()
    result = await session.execute(
        update(GatewayRequestLog)
        .where(
            GatewayRequestLog.request_id.in_(normalized_ids),
            GatewayRequestLog.status_code.is_not(None),
            GatewayRequestLog.analytics_recorded_at.is_(None),
        )
        .values(analytics_recorded_at=now)
        .returning(
            GatewayRequestLog.started_at,
            GatewayRequestLog.model,
            GatewayRequestLog.model_name,
            GatewayRequestLog.is_stream,
            GatewayRequestLog.success,
            GatewayRequestLog.input_tokens,
            GatewayRequestLog.output_tokens,
            GatewayRequestLog.total_tokens,
            GatewayRequestLog.estimated_cost_usd,
            GatewayRequestLog.ttft_ms,
            GatewayRequestLog.duration_ms,
        )
    )
    rows = result.all()
    await _upsert_request_hourly_stats(session, _request_hourly_stat_deltas(rows))


async def _request_hourly_stats_cover_window(session, *, since: datetime | None) -> bool:
    if since is None:
        return False
    row = (
        await session.execute(
            select(
                func.min(GatewayRequestHourlyStat.bucket_start),
                func.coalesce(func.sum(GatewayRequestHourlyStat.request_count), 0),
            )
        )
    ).one()
    first_bucket, request_count = row
    return bool(first_bucket is not None and _int_or_zero(request_count) > 0 and first_bucket <= _hour_floor(since))


async def _upsert_request_logs_portable(values_list: list[dict[str, Any]]) -> list[dict[str, Any]]:
    results: list[tuple[str, GatewayRequestLog]] = []
    async with get_request_log_session() as session:
        async with session.begin():
            request_ids = [str(values["request_id"]) for values in values_list]
            existing_result = await session.execute(
                select(GatewayRequestLog).where(GatewayRequestLog.request_id.in_(request_ids))
            )
            existing_by_request_id = {item.request_id: item for item in existing_result.scalars().all()}
            for values in values_list:
                request_id = str(values["request_id"])
                item = existing_by_request_id.get(request_id)
                if item is None:
                    item = GatewayRequestLog(**values)
                    session.add(item)
                    existing_by_request_id[request_id] = item
                else:
                    _merge_request_log_payload(item, values)
                results.append((request_id, item))
            await session.flush()
    return [
        {
            "request_id": request_id,
            "id": item.id,
            "timing_spans": item.timing_spans,
        }
        for request_id, item in results
    ]


def _timing_snapshot(timing_recorder: Any | None) -> dict[str, Any] | None:
    snapshot = getattr(timing_recorder, "snapshot", None)
    if not callable(snapshot):
        return None
    try:
        return _normalize_timing_spans(snapshot())
    except Exception:
        return None


def _merge_payload_timing(values_list: list[dict[str, Any]], timing_spans: dict[str, Any] | None) -> None:
    resolved = _normalize_timing_spans(timing_spans)
    if not resolved:
        return
    for values in values_list:
        current = _normalize_timing_spans(values.get("timing_spans")) or {}
        merged = dict(current)
        for key, value in resolved.items():
            if isinstance(value, str):
                merged[key] = value
                continue
            previous = merged.get(key, 0)
            merged[key] = (previous if isinstance(previous, int) else 0) + value
        values["timing_spans"] = merged


async def upsert_request_logs(
    payloads: list[dict[str, Any]],
    *,
    timing_recorder: Any | None = None,
) -> list[dict[str, Any]]:
    values_list = [_request_log_payload_values(payload) for payload in payloads if payload.get("request_id")]
    if not values_list:
        return []

    timing_merged = False
    async with get_request_log_session() as session:
        try:
            async with session.begin():
                await session.connection()
                _merge_payload_timing(values_list, _timing_snapshot(timing_recorder))
                timing_merged = True
                insert_stmt = pg_insert(GatewayRequestLog).values(values_list)
                excluded = insert_stmt.excluded
                update_values = {
                    "endpoint": excluded.endpoint,
                    "model": func.coalesce(excluded.model, GatewayRequestLog.model),
                    "model_name": func.coalesce(excluded.model_name, GatewayRequestLog.model_name, GatewayRequestLog.model),
                    "is_stream": excluded.is_stream,
                    "client_ip": func.coalesce(excluded.client_ip, GatewayRequestLog.client_ip),
                    "user_agent": func.coalesce(excluded.user_agent, GatewayRequestLog.user_agent),
                    "started_at": func.coalesce(excluded.started_at, GatewayRequestLog.started_at),
                    "status_code": func.coalesce(excluded.status_code, GatewayRequestLog.status_code),
                    "success": func.coalesce(excluded.success, GatewayRequestLog.success),
                    "attempt_count": case(
                        (excluded.status_code.is_not(None), excluded.attempt_count),
                        else_=GatewayRequestLog.attempt_count,
                    ),
                    "finished_at": func.coalesce(excluded.finished_at, GatewayRequestLog.finished_at),
                    "first_token_at": func.coalesce(excluded.first_token_at, GatewayRequestLog.first_token_at),
                    "ttft_ms": func.coalesce(excluded.ttft_ms, GatewayRequestLog.ttft_ms),
                    "duration_ms": func.coalesce(excluded.duration_ms, GatewayRequestLog.duration_ms),
                    "token_id": func.coalesce(excluded.token_id, GatewayRequestLog.token_id),
                    "account_id": func.coalesce(excluded.account_id, GatewayRequestLog.account_id),
                    "input_tokens": func.coalesce(excluded.input_tokens, GatewayRequestLog.input_tokens),
                    "cached_input_tokens": func.coalesce(
                        excluded.cached_input_tokens,
                        GatewayRequestLog.cached_input_tokens,
                    ),
                    "output_tokens": func.coalesce(excluded.output_tokens, GatewayRequestLog.output_tokens),
                    "total_tokens": func.coalesce(excluded.total_tokens, GatewayRequestLog.total_tokens),
                    "estimated_cost_usd": func.coalesce(excluded.estimated_cost_usd, GatewayRequestLog.estimated_cost_usd),
                    "request_payload_hash": func.coalesce(
                        excluded.request_payload_hash,
                        GatewayRequestLog.request_payload_hash,
                    ),
                    "upstream_payload_hash": func.coalesce(
                        excluded.upstream_payload_hash,
                        GatewayRequestLog.upstream_payload_hash,
                    ),
                    "prompt_template_hash": func.coalesce(
                        excluded.prompt_template_hash,
                        GatewayRequestLog.prompt_template_hash,
                    ),
                    "prompt_dynamic_hash": func.coalesce(
                        excluded.prompt_dynamic_hash,
                        GatewayRequestLog.prompt_dynamic_hash,
                    ),
                    "prompt_cache_source": func.coalesce(
                        excluded.prompt_cache_source,
                        GatewayRequestLog.prompt_cache_source,
                    ),
                    "prompt_cache_key_hash": func.coalesce(
                        excluded.prompt_cache_key_hash,
                        GatewayRequestLog.prompt_cache_key_hash,
                    ),
                    "prompt_cache_retention_requested": func.coalesce(
                        excluded.prompt_cache_retention_requested,
                        GatewayRequestLog.prompt_cache_retention_requested,
                    ),
                    "prompt_cache_retention_sent": func.coalesce(
                        excluded.prompt_cache_retention_sent,
                        GatewayRequestLog.prompt_cache_retention_sent,
                    ),
                    "session_id_hash": func.coalesce(excluded.session_id_hash, GatewayRequestLog.session_id_hash),
                    "session_id_source": func.coalesce(excluded.session_id_source, GatewayRequestLog.session_id_source),
                    "previous_response_id_hash": func.coalesce(
                        excluded.previous_response_id_hash,
                        GatewayRequestLog.previous_response_id_hash,
                    ),
                    "upstream_response_id": func.coalesce(
                        excluded.upstream_response_id,
                        GatewayRequestLog.upstream_response_id,
                    ),
                    "cache_hit_ratio": func.coalesce(excluded.cache_hit_ratio, GatewayRequestLog.cache_hit_ratio),
                    "cache_affinity_result": func.coalesce(
                        excluded.cache_affinity_result,
                        GatewayRequestLog.cache_affinity_result,
                    ),
                    "cache_affinity_lane_index": func.coalesce(
                        excluded.cache_affinity_lane_index,
                        GatewayRequestLog.cache_affinity_lane_index,
                    ),
                    "prompt_cache_trace": func.coalesce(
                        excluded.prompt_cache_trace,
                        GatewayRequestLog.prompt_cache_trace,
                    ),
                    "timing_spans": func.coalesce(excluded.timing_spans, GatewayRequestLog.timing_spans),
                    "error_message": func.coalesce(excluded.error_message, GatewayRequestLog.error_message),
                }
                stmt = (
                    insert_stmt.on_conflict_do_update(
                        index_elements=[GatewayRequestLog.request_id],
                        set_=update_values,
                    )
                    .returning(GatewayRequestLog.request_id, GatewayRequestLog.id, GatewayRequestLog.timing_spans)
                )

                result = await session.execute(stmt)
                rows = result.all()
                try:
                    async with session.begin_nested():
                        await _record_request_hourly_stats_once(session, [request_id for request_id, _, _ in rows])
                except Exception:
                    pass
                return [
                    {
                        "request_id": request_id,
                        "id": request_log_id,
                        "timing_spans": timing_spans,
                    }
                    for request_id, request_log_id, timing_spans in rows
                ]
        except Exception:
            pass
    if not timing_merged:
        _merge_payload_timing(values_list, _timing_snapshot(timing_recorder))
    try:
        return await _upsert_request_logs_portable(values_list)
    except Exception:
        raise


async def get_request_log_summary(*, hours: int | None = None) -> RequestLogSummary:
    since = None
    if hours is not None:
        since = utcnow() - timedelta(hours=max(1, min(int(hours), 24 * 30)))
    async with get_read_session() as session:
        if await _request_hourly_stats_cover_window(session, since=since):
            stats_row = (await session.execute(_build_request_log_summary_stats_stmt(since=since))).one()
            return RequestLogSummary(
                total=_int_or_zero(stats_row.total),
                successful=_int_or_zero(stats_row.successful),
                failed=_int_or_zero(stats_row.failed),
                streaming=_int_or_zero(stats_row.streaming),
                input_tokens=_int_or_zero(stats_row.input_tokens),
                output_tokens=_int_or_zero(stats_row.output_tokens),
                total_tokens=_int_or_zero(stats_row.total_tokens),
                estimated_cost_usd=_round_cost(_float_or_zero(stats_row.estimated_cost_usd)),
                avg_ttft_ms=int(round(float(stats_row.avg_ttft_ms))) if stats_row.avg_ttft_ms is not None else None,
            )

        row = (await session.execute(_build_request_log_summary_stmt(since=since))).one()
        (
            total,
            successful,
            failed,
            streaming,
            avg_ttft_raw,
            input_tokens,
            output_tokens,
            total_tokens,
            estimated_cost_usd,
        ) = row
        return RequestLogSummary(
            total=_int_or_zero(total),
            successful=_int_or_zero(successful),
            failed=_int_or_zero(failed),
            streaming=_int_or_zero(streaming),
            input_tokens=_int_or_zero(input_tokens),
            output_tokens=_int_or_zero(output_tokens),
            total_tokens=_int_or_zero(total_tokens),
            estimated_cost_usd=_round_cost(_float_or_zero(estimated_cost_usd)),
            avg_ttft_ms=int(round(float(avg_ttft_raw))) if avg_ttft_raw is not None else None,
        )


async def list_request_logs(limit: int = 100) -> list[RequestLogItem]:
    async with get_read_session() as session:
        stmt = (
            select(
                GatewayRequestLog.id,
                GatewayRequestLog.request_id,
                GatewayRequestLog.endpoint,
                GatewayRequestLog.model,
                GatewayRequestLog.model_name,
                GatewayRequestLog.is_stream,
                GatewayRequestLog.status_code,
                GatewayRequestLog.success,
                GatewayRequestLog.attempt_count,
                GatewayRequestLog.token_id,
                GatewayRequestLog.account_id,
                GatewayRequestLog.started_at,
                GatewayRequestLog.finished_at,
                GatewayRequestLog.first_token_at,
                GatewayRequestLog.ttft_ms,
                GatewayRequestLog.duration_ms,
                GatewayRequestLog.timing_spans,
                GatewayRequestLog.input_tokens,
                GatewayRequestLog.cached_input_tokens,
                GatewayRequestLog.output_tokens,
                GatewayRequestLog.total_tokens,
                GatewayRequestLog.estimated_cost_usd,
                GatewayRequestLog.request_payload_hash,
                GatewayRequestLog.upstream_payload_hash,
                GatewayRequestLog.prompt_template_hash,
                GatewayRequestLog.prompt_dynamic_hash,
                GatewayRequestLog.prompt_cache_source,
                GatewayRequestLog.prompt_cache_key_hash,
                GatewayRequestLog.prompt_cache_retention_requested,
                GatewayRequestLog.prompt_cache_retention_sent,
                GatewayRequestLog.session_id_hash,
                GatewayRequestLog.session_id_source,
                GatewayRequestLog.previous_response_id_hash,
                GatewayRequestLog.upstream_response_id,
                GatewayRequestLog.cache_hit_ratio,
                GatewayRequestLog.cache_affinity_result,
                GatewayRequestLog.cache_affinity_lane_index,
                GatewayRequestLog.error_message,
            )
            .order_by(GatewayRequestLog.started_at.desc(), GatewayRequestLog.id.desc())
            .limit(max(1, min(limit, 500)))
        )
        result = await session.execute(stmt)
        items = result.all()
        return [
            RequestLogItem(
                id=item.id,
                request_id=item.request_id,
                endpoint=item.endpoint,
                model=item.model,
                model_name=item.model_name,
                is_stream=item.is_stream,
                status_code=item.status_code,
                success=item.success,
                attempt_count=item.attempt_count,
                token_id=item.token_id,
                account_id=item.account_id,
                client_ip=None,
                user_agent=None,
                started_at=item.started_at,
                finished_at=item.finished_at,
                first_token_at=item.first_token_at,
                ttft_ms=item.ttft_ms,
                duration_ms=item.duration_ms,
                timing_spans=item.timing_spans if isinstance(item.timing_spans, dict) else None,
                input_tokens=item.input_tokens,
                cached_input_tokens=item.cached_input_tokens,
                output_tokens=item.output_tokens,
                total_tokens=item.total_tokens,
                estimated_cost_usd=_round_cost(_float_or_zero(item.estimated_cost_usd))
                if item.estimated_cost_usd is not None
                else None,
                request_payload_hash=item.request_payload_hash,
                upstream_payload_hash=item.upstream_payload_hash,
                prompt_template_hash=item.prompt_template_hash,
                prompt_dynamic_hash=item.prompt_dynamic_hash,
                prompt_cache_source=item.prompt_cache_source,
                prompt_cache_key_hash=item.prompt_cache_key_hash,
                prompt_cache_retention_requested=item.prompt_cache_retention_requested,
                prompt_cache_retention_sent=item.prompt_cache_retention_sent,
                session_id_hash=item.session_id_hash,
                session_id_source=item.session_id_source,
                previous_response_id_hash=item.previous_response_id_hash,
                upstream_response_id=item.upstream_response_id,
                cache_hit_ratio=item.cache_hit_ratio,
                cache_affinity_result=item.cache_affinity_result,
                cache_affinity_lane_index=item.cache_affinity_lane_index,
                prompt_cache_trace=None,
                error_message=item.error_message,
            )
            for item in items
        ]


async def get_request_costs_by_account(account_ids: list[str] | set[str] | tuple[str, ...]) -> dict[str, float]:
    resolved_account_ids = _normalize_request_account_ids(account_ids)
    if not resolved_account_ids:
        return {}

    async with get_read_session() as session:
        result = await session.execute(_build_request_account_costs_stmt(account_ids=resolved_account_ids))
        rows = result.all()
    return {
        str(account_id): _round_cost(_float_or_zero(estimated_cost_usd))
        for account_id, estimated_cost_usd in rows
        if str(account_id or "").strip()
    }


async def get_request_costs_by_token(token_ids: list[int] | set[int] | tuple[int, ...]) -> dict[int, float]:
    resolved_token_ids = _normalize_request_token_ids(token_ids)
    if not resolved_token_ids:
        return {}
    resolved_token_id_set = set(resolved_token_ids)

    async with get_read_session() as session:
        token_rows = (
            await session.execute(
                select(CodexToken.id, CodexToken.merged_into_token_id).where(
                    or_(
                        CodexToken.id.in_(resolved_token_ids),
                        CodexToken.merged_into_token_id.in_(resolved_token_ids),
                    )
                )
            )
        ).all()

        canonical_by_log_token_id: dict[int, int] = {}
        for token_id, merged_into_token_id in token_rows:
            resolved_id = int(token_id)
            merged_id = int(merged_into_token_id) if merged_into_token_id is not None else None
            canonical_id = merged_id if merged_id in resolved_token_id_set else resolved_id
            if canonical_id in resolved_token_id_set:
                canonical_by_log_token_id[resolved_id] = canonical_id

        if not canonical_by_log_token_id:
            return {}

        result = await session.execute(
            select(
                GatewayRequestLog.token_id,
                func.sum(GatewayRequestLog.estimated_cost_usd),
            )
            .where(GatewayRequestLog.token_id.in_(tuple(canonical_by_log_token_id)))
            .group_by(GatewayRequestLog.token_id)
        )
        rows = result.all()

    costs_by_canonical_id: dict[int, float] = {}
    for token_id, estimated_cost_usd in rows:
        if token_id is None:
            continue
        canonical_id = canonical_by_log_token_id.get(int(token_id))
        if canonical_id is None:
            continue
        costs_by_canonical_id[canonical_id] = costs_by_canonical_id.get(canonical_id, 0.0) + _float_or_zero(
            estimated_cost_usd
        )
    return {
        token_id: _round_cost(estimated_cost_usd)
        for token_id, estimated_cost_usd in costs_by_canonical_id.items()
    }


async def get_request_log_analytics(*, hours: int = 24, bucket_minutes: int = 60, top_models: int = 6) -> RequestLogAnalytics:
    effective_hours = max(1, min(int(hours), 24 * 30))
    effective_bucket_minutes = max(5, min(int(bucket_minutes), 24 * 60))
    effective_top_models = max(1, min(int(top_models), 24))
    now = utcnow()
    since = now - timedelta(hours=effective_hours)
    first_bucket = _floor_bucket_start(since, bucket_minutes=effective_bucket_minutes)
    last_bucket = _floor_bucket_start(now, bucket_minutes=effective_bucket_minutes)

    async with get_read_session() as session:
        if await _request_hourly_stats_cover_window(session, since=since):
            bucket_rows = (
                await session.execute(
                    _build_request_bucket_analytics_stats_stmt(
                        since=since,
                        bucket_minutes=effective_bucket_minutes,
                    )
                )
            ).all()
            model_stmt = _build_request_model_analytics_stats_stmt(since=since, top_models=effective_top_models)
            model_rows = (await session.execute(model_stmt)).all()
        else:
            bucket_rows = (
                await session.execute(
                    _build_request_bucket_analytics_stmt(
                        since=since,
                        bucket_minutes=effective_bucket_minutes,
                    )
                )
            ).all()
            model_stmt = _build_request_model_analytics_stmt(since=since, top_models=effective_top_models)
            model_rows = (await session.execute(model_stmt)).all()

    bucket_map: dict[datetime, dict[str, float | int | datetime]] = {}
    cursor = first_bucket
    while cursor <= last_bucket:
        bucket_map[cursor] = {
            "bucket_start": cursor,
            "request_count": 0,
            "input_tokens": 0,
            "output_tokens": 0,
            "total_tokens": 0,
            "estimated_cost_usd": 0.0,
        }
        cursor += timedelta(minutes=effective_bucket_minutes)

    for bucket_start, request_count, input_tokens, output_tokens, total_tokens, estimated_cost_usd in bucket_rows:
        bucket = bucket_map.setdefault(
            bucket_start,
            {
                "bucket_start": bucket_start,
                "request_count": 0,
                "input_tokens": 0,
                "output_tokens": 0,
                "total_tokens": 0,
                "estimated_cost_usd": 0.0,
            },
        )
        bucket["request_count"] = _int_or_zero(bucket["request_count"]) + _int_or_zero(request_count)
        bucket["input_tokens"] = _int_or_zero(bucket["input_tokens"]) + _int_or_zero(input_tokens)
        bucket["output_tokens"] = _int_or_zero(bucket["output_tokens"]) + _int_or_zero(output_tokens)
        bucket["total_tokens"] = _int_or_zero(bucket["total_tokens"]) + _int_or_zero(total_tokens)
        bucket["estimated_cost_usd"] = _float_or_zero(bucket["estimated_cost_usd"]) + _float_or_zero(estimated_cost_usd)

    buckets = [
        RequestAnalyticsBucket(
            bucket_start=bucket["bucket_start"],
            request_count=_int_or_zero(bucket["request_count"]),
            input_tokens=_int_or_zero(bucket["input_tokens"]),
            output_tokens=_int_or_zero(bucket["output_tokens"]),
            total_tokens=_int_or_zero(bucket["total_tokens"]),
            estimated_cost_usd=_round_cost(_float_or_zero(bucket["estimated_cost_usd"])),
        )
        for _, bucket in sorted(bucket_map.items(), key=lambda item: item[0])
    ]

    models = [
        RequestAnalyticsModel(
            model_name=str(model_name or "未识别模型"),
            request_count=_int_or_zero(request_count),
            total_tokens=_int_or_zero(total_tokens),
            estimated_cost_usd=_round_cost(_float_or_zero(estimated_cost_usd)),
        )
        for model_name, request_count, total_tokens, estimated_cost_usd in model_rows
    ]

    return RequestLogAnalytics(
        period_hours=effective_hours,
        bucket_minutes=effective_bucket_minutes,
        buckets=buckets,
        models=models,
    )


async def delete_request_logs_older_than(cutoff: datetime) -> int:
    async with get_request_log_session() as session:
        async with session.begin():
            result = await session.execute(delete(GatewayRequestLog).where(GatewayRequestLog.started_at < cutoff))
            return int(result.rowcount or 0)
