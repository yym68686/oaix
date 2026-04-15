from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Protocol

from sqlalchemy import and_, or_, select

from .database import TokenImportJob, get_session, utcnow
from .token_store import upsert_token_payload


logger = logging.getLogger("oaix.import_jobs")

IMPORT_JOB_STATUS_QUEUED = "queued"
IMPORT_JOB_STATUS_RUNNING = "running"
IMPORT_JOB_STATUS_COMPLETED = "completed"
IMPORT_JOB_STATUS_FAILED = "failed"


def _float_env(name: str, default: float, *, minimum: float = 0.0) -> float:
    raw = str(os.getenv(name, "") or "").strip()
    try:
        value = float(raw) if raw else float(default)
    except ValueError:
        value = float(default)
    return max(minimum, value)


def _import_job_poll_interval_seconds() -> float:
    return _float_env("IMPORT_JOB_POLL_INTERVAL_SECONDS", 1.0, minimum=0.25)


def _import_job_stale_after_seconds() -> float:
    return _float_env("IMPORT_JOB_STALE_AFTER_SECONDS", 600.0, minimum=30.0)


def _as_item_list(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def _as_payload_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return list(value)
    return []


def _summarize_error(value: Any) -> str | None:
    text = str(value or "").strip()
    return text[:4000] if text else None


@dataclass(frozen=True)
class TokenImportJobState:
    id: int
    status: str
    total_count: int
    processed_count: int
    created_count: int
    updated_count: int
    skipped_count: int
    failed_count: int
    yielded_to_response_traffic_count: int
    response_traffic_timeout_count: int
    created: list[dict[str, Any]]
    updated: list[dict[str, Any]]
    skipped: list[dict[str, Any]]
    failed: list[dict[str, Any]]
    submitted_at: datetime | None
    started_at: datetime | None
    heartbeat_at: datetime | None
    finished_at: datetime | None
    last_error: str | None


@dataclass(frozen=True)
class TokenImportJobLease(TokenImportJobState):
    payloads: list[Any]


def _serialize_job_state(job: TokenImportJob) -> TokenImportJobState:
    return TokenImportJobState(
        id=job.id,
        status=str(job.status or IMPORT_JOB_STATUS_QUEUED),
        total_count=int(job.total_count or 0),
        processed_count=int(job.processed_count or 0),
        created_count=int(job.created_count or 0),
        updated_count=int(job.updated_count or 0),
        skipped_count=int(job.skipped_count or 0),
        failed_count=int(job.failed_count or 0),
        yielded_to_response_traffic_count=int(job.yielded_to_response_traffic_count or 0),
        response_traffic_timeout_count=int(job.response_traffic_timeout_count or 0),
        created=_as_item_list(job.created_items),
        updated=_as_item_list(job.updated_items),
        skipped=_as_item_list(job.skipped_items),
        failed=_as_item_list(job.failed_items),
        submitted_at=job.submitted_at,
        started_at=job.started_at,
        heartbeat_at=job.heartbeat_at,
        finished_at=job.finished_at,
        last_error=job.last_error,
    )


def _serialize_job_lease(job: TokenImportJob) -> TokenImportJobLease:
    state = _serialize_job_state(job)
    return TokenImportJobLease(
        **state.__dict__,
        payloads=_as_payload_list(job.payloads),
    )


async def create_token_import_job(payloads: list[Any]) -> TokenImportJobState:
    normalized_payloads = list(payloads)
    async with get_session() as session:
        async with session.begin():
            job = TokenImportJob(
                status=IMPORT_JOB_STATUS_QUEUED,
                payloads=normalized_payloads,
                total_count=len(normalized_payloads),
                processed_count=0,
                created_count=0,
                updated_count=0,
                skipped_count=0,
                failed_count=0,
                yielded_to_response_traffic_count=0,
                response_traffic_timeout_count=0,
                created_items=[],
                updated_items=[],
                skipped_items=[],
                failed_items=[],
                last_error=None,
                started_at=None,
                heartbeat_at=None,
                finished_at=None,
            )
            session.add(job)
            await session.flush()
            return _serialize_job_state(job)


async def get_token_import_job(job_id: int) -> TokenImportJobState | None:
    async with get_session() as session:
        job = await session.get(TokenImportJob, int(job_id))
        if job is None:
            return None
        return _serialize_job_state(job)


async def _get_token_import_job_for_update(session, job_id: int) -> TokenImportJob | None:
    return await session.get(TokenImportJob, int(job_id), with_for_update=True)


async def requeue_token_import_job(job_id: int, *, last_error: str | None = None) -> TokenImportJobState | None:
    async with get_session() as session:
        async with session.begin():
            job = await _get_token_import_job_for_update(session, job_id)
            if job is None:
                return None
            now = utcnow()
            job.status = IMPORT_JOB_STATUS_QUEUED
            job.finished_at = None
            job.heartbeat_at = now
            job.last_error = _summarize_error(last_error) or job.last_error
            await session.flush()
            return _serialize_job_state(job)


async def requeue_stale_token_import_jobs(*, stale_after_seconds: float | None = None) -> int:
    resolved = _import_job_stale_after_seconds() if stale_after_seconds is None else max(30.0, float(stale_after_seconds))
    cutoff = utcnow() - timedelta(seconds=resolved)
    async with get_session() as session:
        async with session.begin():
            result = await session.execute(
                select(TokenImportJob)
                .where(
                    TokenImportJob.status == IMPORT_JOB_STATUS_RUNNING,
                    or_(
                        TokenImportJob.heartbeat_at <= cutoff,
                        and_(TokenImportJob.heartbeat_at.is_(None), TokenImportJob.started_at <= cutoff),
                    ),
                )
                .with_for_update(skip_locked=True)
            )
            jobs = result.scalars().all()
            if not jobs:
                return 0

            now = utcnow()
            for job in jobs:
                job.status = IMPORT_JOB_STATUS_QUEUED
                job.finished_at = None
                job.heartbeat_at = now
                job.last_error = "Import worker heartbeat expired; job requeued to resume."
            await session.flush()
            return len(jobs)


async def claim_next_token_import_job(*, stale_after_seconds: float | None = None) -> TokenImportJobLease | None:
    await requeue_stale_token_import_jobs(stale_after_seconds=stale_after_seconds)
    async with get_session() as session:
        async with session.begin():
            result = await session.execute(
                select(TokenImportJob)
                .where(TokenImportJob.status == IMPORT_JOB_STATUS_QUEUED)
                .order_by(TokenImportJob.submitted_at.asc(), TokenImportJob.id.asc())
                .limit(1)
                .with_for_update(skip_locked=True)
            )
            job = result.scalars().first()
            if job is None:
                return None

            now = utcnow()
            job.status = IMPORT_JOB_STATUS_RUNNING
            if job.started_at is None:
                job.started_at = now
            job.heartbeat_at = now
            job.finished_at = None
            job.last_error = None
            await session.flush()
            return _serialize_job_lease(job)


async def touch_token_import_job(job_id: int) -> TokenImportJobState | None:
    async with get_session() as session:
        async with session.begin():
            job = await _get_token_import_job_for_update(session, job_id)
            if job is None:
                return None
            job.heartbeat_at = utcnow()
            await session.flush()
            return _serialize_job_state(job)


async def update_token_import_job_progress(
    job_id: int,
    *,
    processed_count: int,
    created: list[dict[str, Any]],
    updated: list[dict[str, Any]],
    skipped: list[dict[str, Any]],
    failed: list[dict[str, Any]],
    yielded_to_response_traffic_count: int,
    response_traffic_timeout_count: int,
) -> TokenImportJobState | None:
    async with get_session() as session:
        async with session.begin():
            job = await _get_token_import_job_for_update(session, job_id)
            if job is None:
                return None

            job.status = IMPORT_JOB_STATUS_RUNNING
            job.processed_count = int(processed_count)
            job.created_count = len(created)
            job.updated_count = len(updated)
            job.skipped_count = len(skipped)
            job.failed_count = len(failed)
            job.yielded_to_response_traffic_count = int(yielded_to_response_traffic_count)
            job.response_traffic_timeout_count = int(response_traffic_timeout_count)
            job.created_items = list(created)
            job.updated_items = list(updated)
            job.skipped_items = list(skipped)
            job.failed_items = list(failed)
            job.heartbeat_at = utcnow()
            job.finished_at = None
            await session.flush()
            return _serialize_job_state(job)


async def complete_token_import_job(
    job_id: int,
    *,
    processed_count: int,
    created: list[dict[str, Any]],
    updated: list[dict[str, Any]],
    skipped: list[dict[str, Any]],
    failed: list[dict[str, Any]],
    yielded_to_response_traffic_count: int,
    response_traffic_timeout_count: int,
) -> TokenImportJobState | None:
    async with get_session() as session:
        async with session.begin():
            job = await _get_token_import_job_for_update(session, job_id)
            if job is None:
                return None

            now = utcnow()
            job.status = IMPORT_JOB_STATUS_COMPLETED
            job.processed_count = int(processed_count)
            job.created_count = len(created)
            job.updated_count = len(updated)
            job.skipped_count = len(skipped)
            job.failed_count = len(failed)
            job.yielded_to_response_traffic_count = int(yielded_to_response_traffic_count)
            job.response_traffic_timeout_count = int(response_traffic_timeout_count)
            job.created_items = list(created)
            job.updated_items = list(updated)
            job.skipped_items = list(skipped)
            job.failed_items = list(failed)
            job.heartbeat_at = now
            job.finished_at = now
            job.last_error = None
            await session.flush()
            return _serialize_job_state(job)


async def fail_token_import_job(job_id: int, *, last_error: str) -> TokenImportJobState | None:
    async with get_session() as session:
        async with session.begin():
            job = await _get_token_import_job_for_update(session, job_id)
            if job is None:
                return None

            now = utcnow()
            job.status = IMPORT_JOB_STATUS_FAILED
            job.heartbeat_at = now
            job.finished_at = now
            job.last_error = _summarize_error(last_error)
            await session.flush()
            return _serialize_job_state(job)


class ResponseTrafficLike(Protocol):
    active_responses: int

    async def wait_for_import_turn(self, *, timeout_seconds: float | None = None) -> bool:
        ...


async def process_token_import_job(
    job: TokenImportJobLease,
    *,
    response_traffic: ResponseTrafficLike,
) -> TokenImportJobState | None:
    payloads = list(job.payloads)
    created = list(job.created)
    updated = list(job.updated)
    skipped = list(job.skipped)
    failed = list(job.failed)
    yielded_to_response_traffic = int(job.yielded_to_response_traffic_count or 0)
    response_traffic_timeout_count = int(job.response_traffic_timeout_count or 0)
    processed_count = max(0, min(int(job.processed_count or 0), len(payloads)))

    try:
        for index in range(processed_count, len(payloads)):
            await touch_token_import_job(job.id)
            try:
                if await response_traffic.wait_for_import_turn():
                    yielded_to_response_traffic += 1
            except TimeoutError:
                response_traffic_timeout_count += 1
                logger.warning(
                    "Token import job wait timed out while /v1/responses traffic remained active; continuing import: job_id=%s index=%s total_payloads=%s active_responses=%s",
                    job.id,
                    index,
                    len(payloads),
                    response_traffic.active_responses,
                )
                await asyncio.sleep(0)

            payload = payloads[index]
            if not isinstance(payload, dict):
                failed.append({"index": index, "error": "Token payload must be a JSON object"})
            else:
                try:
                    result = await upsert_token_payload(payload)
                    item = {
                        "id": result.token.id,
                        "email": result.token.email,
                        "account_id": result.token.account_id,
                        "index": index,
                    }
                    if result.action == "created":
                        created.append(item)
                    elif result.action == "updated":
                        updated.append(item)
                    else:
                        skipped.append(
                            {
                                **item,
                                "reason": "duplicate_refresh_token_history",
                            }
                        )
                except Exception as exc:
                    failed.append({"index": index, "error": str(exc)})

            processed_count = index + 1
            await update_token_import_job_progress(
                job.id,
                processed_count=processed_count,
                created=created,
                updated=updated,
                skipped=skipped,
                failed=failed,
                yielded_to_response_traffic_count=yielded_to_response_traffic,
                response_traffic_timeout_count=response_traffic_timeout_count,
            )

        return await complete_token_import_job(
            job.id,
            processed_count=processed_count,
            created=created,
            updated=updated,
            skipped=skipped,
            failed=failed,
            yielded_to_response_traffic_count=yielded_to_response_traffic,
            response_traffic_timeout_count=response_traffic_timeout_count,
        )
    except asyncio.CancelledError:
        await requeue_token_import_job(
            job.id,
            last_error="Import worker interrupted before completion; job requeued to resume.",
        )
        raise
    except Exception as exc:
        logger.exception("Token import job failed unexpectedly: job_id=%s", job.id)
        return await fail_token_import_job(job.id, last_error=str(exc))


class TokenImportBackgroundWorker:
    def __init__(
        self,
        *,
        response_traffic: ResponseTrafficLike,
        poll_interval_seconds: float | None = None,
        stale_after_seconds: float | None = None,
    ) -> None:
        self._response_traffic = response_traffic
        self._poll_interval_seconds = (
            _import_job_poll_interval_seconds() if poll_interval_seconds is None else max(0.25, float(poll_interval_seconds))
        )
        self._stale_after_seconds = (
            _import_job_stale_after_seconds() if stale_after_seconds is None else max(30.0, float(stale_after_seconds))
        )
        self._wake_event = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._stopping = False

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._stopping = False
        requeued = await requeue_stale_token_import_jobs(stale_after_seconds=self._stale_after_seconds)
        if requeued:
            logger.warning("Requeued stale token import jobs on startup: count=%s", requeued)
        self._task = asyncio.create_task(self._run(), name="oaix-token-import-worker")

    async def stop(self) -> None:
        self._stopping = True
        self._wake_event.set()
        task = self._task
        self._task = None
        if task is None:
            return
        try:
            await asyncio.wait_for(task, timeout=1.0)
        except asyncio.TimeoutError:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    def notify(self) -> None:
        self._wake_event.set()

    async def _run(self) -> None:
        while not self._stopping:
            job = await claim_next_token_import_job(stale_after_seconds=self._stale_after_seconds)
            if job is None:
                try:
                    await asyncio.wait_for(self._wake_event.wait(), timeout=self._poll_interval_seconds)
                except asyncio.TimeoutError:
                    pass
                self._wake_event.clear()
                continue

            logger.info(
                "Processing token import job: job_id=%s processed=%s total=%s",
                job.id,
                job.processed_count,
                job.total_count,
            )
            await process_token_import_job(job, response_traffic=self._response_traffic)
