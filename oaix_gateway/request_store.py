from dataclasses import dataclass
from datetime import datetime, timedelta
from time import perf_counter
from typing import Any

from sqlalchemy import case, func, select, text

from .database import CodexToken, GatewayRequestLog, get_read_session, get_session, utcnow


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
    output_tokens: int | None
    total_tokens: int | None
    estimated_cost_usd: float | None
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


def _normalize_timing_spans(timing_spans: dict[str, Any] | None) -> dict[str, int] | None:
    if not isinstance(timing_spans, dict):
        return None

    normalized: dict[str, int] = {}
    for key, value in timing_spans.items():
        name = str(key or "").strip()
        if not name or isinstance(value, bool):
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


def _request_model_label_expr():
    return func.coalesce(GatewayRequestLog.model_name, GatewayRequestLog.model, "未识别模型")


def _build_request_log_summary_stmt():
    return select(
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
) -> GatewayRequestLog:
    started_at = started_at or utcnow()
    async with get_session() as session:
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
    output_tokens: int | None = None,
    total_tokens: int | None = None,
    estimated_cost_usd: float | None = None,
    timing_spans: dict[str, Any] | None = None,
    error_message: str | None = None,
) -> dict[str, int] | None:
    finalize_started = perf_counter()
    finished_at = finished_at or utcnow()
    resolved_timing_spans = _normalize_timing_spans(timing_spans)
    async with get_session() as session:
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
            item.output_tokens = _int_or_zero(output_tokens) if output_tokens is not None else item.output_tokens
            item.total_tokens = _int_or_zero(total_tokens) if total_tokens is not None else item.total_tokens
            item.estimated_cost_usd = (
                _round_cost(estimated_cost_usd) if estimated_cost_usd is not None else item.estimated_cost_usd
            )
            if resolved_timing_spans is not None:
                resolved_timing_spans["finalize_ms"] = max(0, int((perf_counter() - finalize_started) * 1000))
                item.timing_spans = resolved_timing_spans
            item.error_message = (error_message or "").strip()[:4000] or None
    return resolved_timing_spans


async def get_request_log_summary() -> RequestLogSummary:
    async with get_read_session() as session:
        row = (await session.execute(_build_request_log_summary_stmt())).one()
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
            select(GatewayRequestLog)
            .order_by(GatewayRequestLog.started_at.desc(), GatewayRequestLog.id.desc())
            .limit(max(1, min(limit, 500)))
        )
        result = await session.execute(stmt)
        items = result.scalars().all()
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
                client_ip=item.client_ip,
                user_agent=item.user_agent,
                started_at=item.started_at,
                finished_at=item.finished_at,
                first_token_at=item.first_token_at,
                ttft_ms=item.ttft_ms,
                duration_ms=item.duration_ms,
                timing_spans=item.timing_spans if isinstance(item.timing_spans, dict) else None,
                input_tokens=item.input_tokens,
                output_tokens=item.output_tokens,
                total_tokens=item.total_tokens,
                estimated_cost_usd=_round_cost(_float_or_zero(item.estimated_cost_usd))
                if item.estimated_cost_usd is not None
                else None,
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

    async with get_read_session() as session:
        result = await session.execute(_build_request_token_costs_stmt(token_ids=resolved_token_ids))
        rows = result.all()
    return {
        int(token_id): _round_cost(_float_or_zero(estimated_cost_usd))
        for token_id, estimated_cost_usd in rows
        if token_id is not None
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
