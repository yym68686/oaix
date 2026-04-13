from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import func, select

from .database import GatewayRequestLog, get_session, utcnow


@dataclass(frozen=True)
class RequestLogItem:
    id: int
    request_id: str
    endpoint: str
    model: str | None
    is_stream: bool
    status_code: int | None
    success: bool | None
    attempt_count: int
    account_id: str | None
    client_ip: str | None
    user_agent: str | None
    started_at: datetime
    finished_at: datetime | None
    first_token_at: datetime | None
    ttft_ms: int | None
    duration_ms: int | None
    error_message: str | None


@dataclass(frozen=True)
class RequestLogSummary:
    total: int
    successful: int
    failed: int
    streaming: int
    avg_ttft_ms: int | None


def _ms_between(started_at: datetime | None, ended_at: datetime | None) -> int | None:
    if started_at is None or ended_at is None:
        return None
    return max(0, int((ended_at - started_at).total_seconds() * 1000))


async def create_request_log(
    *,
    request_id: str,
    endpoint: str,
    model: str | None,
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
    account_id: str | None = None,
    error_message: str | None = None,
) -> None:
    finished_at = finished_at or utcnow()
    async with get_session() as session:
        async with session.begin():
            item = await session.get(GatewayRequestLog, request_log_id, with_for_update=True)
            if item is None:
                return
            item.status_code = int(status_code)
            item.success = bool(success)
            item.attempt_count = max(0, int(attempt_count))
            item.finished_at = finished_at
            item.first_token_at = first_token_at
            item.ttft_ms = _ms_between(item.started_at, first_token_at)
            item.duration_ms = _ms_between(item.started_at, finished_at)
            item.account_id = account_id or item.account_id
            item.error_message = (error_message or "").strip()[:4000] or None


async def get_request_log_summary() -> RequestLogSummary:
    async with get_session() as session:
        total_result = await session.execute(select(func.count()).select_from(GatewayRequestLog))
        success_result = await session.execute(
            select(func.count()).select_from(GatewayRequestLog).where(GatewayRequestLog.success.is_(True))
        )
        failed_result = await session.execute(
            select(func.count()).select_from(GatewayRequestLog).where(GatewayRequestLog.success.is_(False))
        )
        streaming_result = await session.execute(
            select(func.count()).select_from(GatewayRequestLog).where(GatewayRequestLog.is_stream.is_(True))
        )
        avg_ttft_result = await session.execute(select(func.avg(GatewayRequestLog.ttft_ms)).select_from(GatewayRequestLog))
        avg_ttft_raw = avg_ttft_result.scalar_one_or_none()
        return RequestLogSummary(
            total=int(total_result.scalar_one() or 0),
            successful=int(success_result.scalar_one() or 0),
            failed=int(failed_result.scalar_one() or 0),
            streaming=int(streaming_result.scalar_one() or 0),
            avg_ttft_ms=int(round(float(avg_ttft_raw))) if avg_ttft_raw is not None else None,
        )


async def list_request_logs(limit: int = 100) -> list[RequestLogItem]:
    async with get_session() as session:
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
                is_stream=item.is_stream,
                status_code=item.status_code,
                success=item.success,
                attempt_count=item.attempt_count,
                account_id=item.account_id,
                client_ip=item.client_ip,
                user_agent=item.user_agent,
                started_at=item.started_at,
                finished_at=item.finished_at,
                first_token_at=item.first_token_at,
                ttft_ms=item.ttft_ms,
                duration_ms=item.duration_ms,
                error_message=item.error_message,
            )
            for item in items
        ]
