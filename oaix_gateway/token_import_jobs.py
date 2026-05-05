from __future__ import annotations

import asyncio
import hashlib
import inspect
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Awaitable, Callable, Protocol

import httpx
from sqlalchemy import and_, or_, select

from .database import (
    CodexToken,
    TokenImportItem,
    TokenImportJob,
    get_import_read_session,
    get_import_session,
    get_read_session,
    get_session,
    utcnow,
)
from .oauth import CodexOAuthManager, is_permanently_invalid_refresh_token_error
from .token_identity import normalize_refresh_token
from .token_store import (
    DEFAULT_TOKEN_IMPORT_QUEUE_POSITION,
    TokenSelectionSettings,
    apply_imported_token_queue_position,
    mark_token_error,
    mark_token_import_validation_pending,
    normalize_token_import_queue_position,
    upsert_token_payload,
)


logger = logging.getLogger("oaix.import_jobs")

IMPORT_JOB_STATUS_QUEUED = "queued"
IMPORT_JOB_STATUS_RUNNING = "running"
IMPORT_JOB_STATUS_COMPLETED = "completed"
IMPORT_JOB_STATUS_FAILED = "failed"
IMPORT_ITEM_STATUS_QUEUED = "queued"
IMPORT_ITEM_STATUS_VALIDATING = "validating"
IMPORT_ITEM_STATUS_VALIDATED = "validated"
IMPORT_ITEM_STATUS_PUBLISHING = "publishing"
IMPORT_ITEM_STATUS_PUBLISHED = "published"
IMPORT_ITEM_STATUS_FAILED = "failed"
IMPORT_ITEM_STATUS_DUPLICATE = "duplicate"


def _float_env(name: str, default: float, *, minimum: float = 0.0) -> float:
    raw = str(os.getenv(name, "") or "").strip()
    try:
        value = float(raw) if raw else float(default)
    except ValueError:
        value = float(default)
    return max(minimum, value)


def _int_env(name: str, default: int, *, minimum: int = 0) -> int:
    raw = str(os.getenv(name, "") or "").strip()
    try:
        value = int(raw) if raw else int(default)
    except ValueError:
        value = int(default)
    return max(minimum, value)


def _bool_env(name: str, default: bool) -> bool:
    raw = str(os.getenv(name, "") or "").strip().lower()
    if not raw:
        return bool(default)
    return raw in {"1", "true", "yes", "on"}


def _import_job_poll_interval_seconds() -> float:
    return _float_env("IMPORT_JOB_POLL_INTERVAL_SECONDS", 1.0, minimum=0.25)


def _import_job_stale_after_seconds() -> float:
    return _float_env("IMPORT_JOB_STALE_AFTER_SECONDS", 600.0, minimum=30.0)


def _import_job_max_concurrency() -> int:
    return _int_env("IMPORT_JOB_MAX_CONCURRENCY", 4, minimum=1)


def _import_job_progress_flush_every() -> int:
    return _int_env("IMPORT_JOB_PROGRESS_FLUSH_EVERY", 32, minimum=1)


def _import_job_progress_flush_interval_seconds() -> float:
    return _float_env("IMPORT_JOB_PROGRESS_FLUSH_INTERVAL_SECONDS", 2.0, minimum=0.0)


def _import_job_respect_response_traffic() -> bool:
    return _bool_env("IMPORT_JOB_RESPECT_RESPONSE_TRAFFIC", True)


def _import_token_validation_cooldown_seconds() -> int:
    return _int_env("IMPORT_TOKEN_VALIDATION_COOLDOWN_SECONDS", 300, minimum=1)


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
    import_queue_position: str
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


@dataclass(frozen=True)
class TokenImportBatchSummary:
    id: int
    status: str
    import_queue_position: str
    total_count: int
    processed_count: int
    created_count: int
    updated_count: int
    skipped_count: int
    failed_count: int
    token_count: int
    available: int
    cooling: int
    disabled: int
    missing: int
    token_ids: tuple[int, ...]
    submitted_at: datetime | None
    started_at: datetime | None
    finished_at: datetime | None


@dataclass(frozen=True)
class _TokenImportItemResult:
    index: int
    category: str
    item: dict[str, Any]
    yielded_to_response_traffic_count: int = 0
    response_traffic_timeout_count: int = 0
    validate_ms: int = 0
    publish_ms: int = 0


@dataclass(frozen=True)
class _TokenImportStagedItem:
    id: int
    job_id: int
    index: int
    status: str
    payload: dict[str, Any] | None
    validated_payload: dict[str, Any] | None
    token_id: int | None = None
    action: str | None = None
    error_message: str | None = None


def _serialize_job_state(job: TokenImportJob) -> TokenImportJobState:
    return TokenImportJobState(
        id=job.id,
        status=str(job.status or IMPORT_JOB_STATUS_QUEUED),
        import_queue_position=normalize_token_import_queue_position(
            getattr(job, "import_queue_position", DEFAULT_TOKEN_IMPORT_QUEUE_POSITION)
        ),
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


def _refresh_token_hash_from_payload(payload: Any) -> str | None:
    if not isinstance(payload, dict):
        return None
    refresh_token = normalize_refresh_token(payload.get("refresh_token"))
    if refresh_token is None:
        return None
    return hashlib.sha256(refresh_token.encode("utf-8")).hexdigest()


def _serialize_staged_item(item: TokenImportItem) -> _TokenImportStagedItem:
    payload = item.payload if isinstance(item.payload, dict) else None
    validated_payload = item.validated_payload if isinstance(item.validated_payload, dict) else None
    return _TokenImportStagedItem(
        id=int(item.id),
        job_id=int(item.job_id),
        index=int(item.item_index),
        status=str(item.status or IMPORT_ITEM_STATUS_QUEUED),
        payload=payload,
        validated_payload=validated_payload,
        token_id=item.token_id,
        action=item.action,
        error_message=item.error_message,
    )


def job_state_from_lease(job: TokenImportJobLease) -> TokenImportJobState:
    return TokenImportJobState(
        id=job.id,
        status=job.status,
        import_queue_position=job.import_queue_position,
        total_count=job.total_count,
        processed_count=job.processed_count,
        created_count=job.created_count,
        updated_count=job.updated_count,
        skipped_count=job.skipped_count,
        failed_count=job.failed_count,
        yielded_to_response_traffic_count=job.yielded_to_response_traffic_count,
        response_traffic_timeout_count=job.response_traffic_timeout_count,
        created=list(job.created),
        updated=list(job.updated),
        skipped=list(job.skipped),
        failed=list(job.failed),
        submitted_at=job.submitted_at,
        started_at=job.started_at,
        heartbeat_at=job.heartbeat_at,
        finished_at=job.finished_at,
        last_error=job.last_error,
    )


async def create_token_import_job(
    payloads: list[Any],
    *,
    start_immediately: bool = False,
    import_queue_position: str = DEFAULT_TOKEN_IMPORT_QUEUE_POSITION,
) -> TokenImportJobLease:
    normalized_payloads = list(payloads)
    resolved_import_queue_position = normalize_token_import_queue_position(import_queue_position)
    async with get_import_session() as session:
        async with session.begin():
            job = TokenImportJob(
                status=IMPORT_JOB_STATUS_QUEUED,
                import_queue_position=resolved_import_queue_position,
                payloads=[],
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
            for index, payload in enumerate(normalized_payloads):
                session.add(
                    TokenImportItem(
                        job_id=int(job.id),
                        item_index=index,
                        status=IMPORT_ITEM_STATUS_QUEUED,
                        refresh_token_hash=_refresh_token_hash_from_payload(payload),
                        payload=payload if isinstance(payload, dict) else {"_raw": payload},
                        validated_payload=None,
                        token_id=None,
                        action=None,
                        error_message=None,
                        validation_ms=None,
                        publish_ms=None,
                        validation_started_at=None,
                        validation_finished_at=None,
                        published_at=None,
                    )
                )
            await session.flush()
            state = _serialize_job_state(job)
            return TokenImportJobLease(**state.__dict__, payloads=normalized_payloads)


async def get_token_import_job(job_id: int) -> TokenImportJobState | None:
    async with get_import_read_session() as session:
        # AsyncSession.get() tries to autobegin even inside this read helper.
        # Keep read-only routes on explicit SELECT statements while autobegin is disabled.
        result = await session.execute(select(TokenImportJob).where(TokenImportJob.id == int(job_id)).limit(1))
        job = result.scalars().first()
        if job is None:
            return None
        return _serialize_job_state(job)


async def _get_token_import_job_for_update(session, job_id: int) -> TokenImportJob | None:
    return await session.get(TokenImportJob, int(job_id), with_for_update=True)


async def requeue_token_import_job(job_id: int, *, last_error: str | None = None) -> TokenImportJobState | None:
    async with get_import_session() as session:
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
    async with get_import_session() as session:
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


def _claim_job_lease(job: TokenImportJob) -> TokenImportJobLease:
    now = utcnow()
    job.status = IMPORT_JOB_STATUS_RUNNING
    if job.started_at is None:
        job.started_at = now
    job.heartbeat_at = now
    job.finished_at = None
    job.last_error = None
    return _serialize_job_lease(job)


async def claim_token_import_job(job_id: int) -> TokenImportJobLease | None:
    async with get_import_session() as session:
        async with session.begin():
            job = await _get_token_import_job_for_update(session, job_id)
            if job is None or str(job.status or "") != IMPORT_JOB_STATUS_QUEUED:
                return None

            claimed = _claim_job_lease(job)
            await session.flush()
            return claimed


async def claim_next_token_import_job(*, stale_after_seconds: float | None = None) -> TokenImportJobLease | None:
    await requeue_stale_token_import_jobs(stale_after_seconds=stale_after_seconds)
    async with get_import_session() as session:
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

            claimed = _claim_job_lease(job)
            await session.flush()
            return claimed


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
    async with get_import_session() as session:
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
    async with get_import_session() as session:
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
    async with get_import_session() as session:
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


async def _wait_for_response_traffic_turn(
    *,
    job_id: int,
    index: int,
    response_traffic: ResponseTrafficLike,
    respect_response_traffic: bool,
    phase: str,
) -> tuple[int, int]:
    if not respect_response_traffic:
        return 0, 0

    try:
        yielded = await response_traffic.wait_for_import_turn()
        return (1 if yielded else 0), 0
    except TimeoutError:
        logger.warning(
            "Token import job wait timed out while /v1/responses traffic remained active; continuing import: job_id=%s index=%s phase=%s active_responses=%s",
            job_id,
            index,
            phase,
            response_traffic.active_responses,
        )
        await asyncio.sleep(0)
        return 0, 1


def _sort_items_by_index(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    def sort_key(item: dict[str, Any]) -> tuple[int, int]:
        try:
            return (int(item.get("index")), 0)
        except (TypeError, ValueError):
            return (2**31 - 1, 1)

    return sorted(items, key=sort_key)


def _build_progress_snapshot(
    *,
    created: list[dict[str, Any]],
    updated: list[dict[str, Any]],
    skipped: list[dict[str, Any]],
    failed: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    return (
        _sort_items_by_index(list(created)),
        _sort_items_by_index(list(updated)),
        _sort_items_by_index(list(skipped)),
        _sort_items_by_index(list(failed)),
    )


def _imported_token_ids_from_job_state(job: TokenImportJobState) -> tuple[int, ...]:
    items = [*job.created, *job.updated]
    return _token_ids_from_import_items(items)


def _token_ids_from_import_items(items: list[dict[str, Any]]) -> tuple[int, ...]:
    def sort_key(item: dict[str, Any]) -> tuple[int, int]:
        try:
            return (int(item.get("index")), 0)
        except (TypeError, ValueError):
            return (2**31 - 1, 1)

    token_ids: list[int] = []
    seen: set[int] = set()
    for item in sorted(items, key=sort_key):
        try:
            token_id = int(item.get("id"))
        except (TypeError, ValueError):
            continue
        if token_id <= 0 or token_id in seen:
            continue
        seen.add(token_id)
        token_ids.append(token_id)
    return tuple(token_ids)


def _token_import_batch_status(token: CodexToken, *, now: datetime) -> str:
    if not bool(getattr(token, "is_active", False)):
        return "disabled"
    cooldown_until = getattr(token, "cooldown_until", None)
    if cooldown_until is not None and cooldown_until > now:
        return "cooling"
    return "available"


def _build_token_import_batch_summary(
    job: TokenImportJob,
    *,
    tokens_by_id: dict[int, CodexToken],
    now: datetime,
) -> TokenImportBatchSummary:
    state = _serialize_job_state(job)
    token_ids = _token_ids_from_import_items([*state.created, *state.updated, *state.skipped])
    available = 0
    cooling = 0
    disabled = 0

    for token_id in token_ids:
        token = tokens_by_id.get(token_id)
        if token is None:
            continue
        status = _token_import_batch_status(token, now=now)
        if status == "disabled":
            disabled += 1
        elif status == "cooling":
            cooling += 1
        else:
            available += 1

    present_count = available + cooling + disabled
    return TokenImportBatchSummary(
        id=state.id,
        status=state.status,
        import_queue_position=state.import_queue_position,
        total_count=state.total_count,
        processed_count=state.processed_count,
        created_count=state.created_count,
        updated_count=state.updated_count,
        skipped_count=state.skipped_count,
        failed_count=state.failed_count,
        token_count=present_count,
        available=available,
        cooling=cooling,
        disabled=disabled,
        missing=max(0, len(token_ids) - present_count),
        token_ids=token_ids,
        submitted_at=state.submitted_at,
        started_at=state.started_at,
        finished_at=state.finished_at,
    )


async def list_token_import_batch_summaries(limit: int = 30) -> list[TokenImportBatchSummary]:
    resolved_limit = max(1, min(int(limit or 30), 100))
    async with get_read_session() as session:
        result = await session.execute(
            select(TokenImportJob)
            .order_by(TokenImportJob.submitted_at.desc(), TokenImportJob.id.desc())
            .limit(resolved_limit)
        )
        jobs = list(result.scalars().all())
        if not jobs:
            return []

        token_ids = {
            token_id
            for job in jobs
            for token_id in _token_ids_from_import_items(
                [
                    *_as_item_list(job.created_items),
                    *_as_item_list(job.updated_items),
                    *_as_item_list(job.skipped_items),
                ]
            )
        }
        tokens_by_id: dict[int, CodexToken] = {}
        if token_ids:
            token_result = await session.execute(
                select(CodexToken).where(
                    CodexToken.merged_into_token_id.is_(None),
                    CodexToken.id.in_(token_ids),
                )
            )
            tokens_by_id = {int(token.id): token for token in token_result.scalars().all()}

        now = utcnow()
        return [
            _build_token_import_batch_summary(job, tokens_by_id=tokens_by_id, now=now)
            for job in jobs
        ]


async def _list_token_import_items(job_id: int) -> list[_TokenImportStagedItem]:
    async with get_import_read_session() as session:
        result = await session.execute(
            select(TokenImportItem)
            .where(TokenImportItem.job_id == int(job_id))
            .order_by(TokenImportItem.item_index.asc(), TokenImportItem.id.asc())
        )
        return [_serialize_staged_item(item) for item in result.scalars().all()]


async def _mark_import_item_validating(item_id: int) -> None:
    async with get_import_session() as session:
        async with session.begin():
            item = await session.get(TokenImportItem, int(item_id), with_for_update=True)
            if item is None:
                return
            item.status = IMPORT_ITEM_STATUS_VALIDATING
            item.validation_started_at = utcnow()
            item.error_message = None
            item.updated_at = utcnow()
            await session.flush()


async def _mark_import_item_validated(
    item_id: int,
    *,
    validated_payload: dict[str, Any],
    validation_ms: int,
) -> None:
    async with get_import_session() as session:
        async with session.begin():
            item = await session.get(TokenImportItem, int(item_id), with_for_update=True)
            if item is None:
                return
            now = utcnow()
            item.status = IMPORT_ITEM_STATUS_VALIDATED
            item.validated_payload = dict(validated_payload)
            item.validation_ms = max(0, int(validation_ms))
            item.validation_finished_at = now
            item.error_message = None
            item.updated_at = now
            await session.flush()


async def _mark_import_item_failed(
    item_id: int,
    *,
    error_message: str,
    validation_ms: int | None = None,
    publish_ms: int | None = None,
) -> None:
    async with get_import_session() as session:
        async with session.begin():
            item = await session.get(TokenImportItem, int(item_id), with_for_update=True)
            if item is None:
                return
            now = utcnow()
            item.status = IMPORT_ITEM_STATUS_FAILED
            item.error_message = _summarize_error(error_message)
            if validation_ms is not None:
                item.validation_ms = max(0, int(validation_ms))
                item.validation_finished_at = now
            if publish_ms is not None:
                item.publish_ms = max(0, int(publish_ms))
                item.published_at = now
            item.updated_at = now
            await session.flush()


async def _mark_import_item_publishing(item_id: int) -> None:
    async with get_import_session() as session:
        async with session.begin():
            item = await session.get(TokenImportItem, int(item_id), with_for_update=True)
            if item is None:
                return
            item.status = IMPORT_ITEM_STATUS_PUBLISHING
            item.updated_at = utcnow()
            await session.flush()


async def _mark_import_item_published(
    item_id: int,
    *,
    token_id: int,
    action: str,
    publish_ms: int,
) -> None:
    async with get_import_session() as session:
        async with session.begin():
            item = await session.get(TokenImportItem, int(item_id), with_for_update=True)
            if item is None:
                return
            now = utcnow()
            item.status = IMPORT_ITEM_STATUS_DUPLICATE if action == "duplicate" else IMPORT_ITEM_STATUS_PUBLISHED
            item.token_id = int(token_id)
            item.action = action
            item.publish_ms = max(0, int(publish_ms))
            item.published_at = now
            item.error_message = None
            item.updated_at = now
            await session.flush()


def _payload_from_staged_item(item: _TokenImportStagedItem) -> Any:
    if item.payload is None:
        return None
    if "_raw" in item.payload and len(item.payload) == 1:
        return item.payload.get("_raw")
    return dict(item.payload)


async def _process_single_token_import_payload(
    job_id: int,
    index: int,
    payload: Any,
    *,
    http_client: httpx.AsyncClient,
    oauth_manager: CodexOAuthManager,
    response_traffic: ResponseTrafficLike,
    respect_response_traffic: bool,
) -> _TokenImportItemResult:
    yielded_to_response_traffic_count = 0
    response_traffic_timeout_count = 0

    yielded, timed_out = await _wait_for_response_traffic_turn(
        job_id=job_id,
        index=index,
        response_traffic=response_traffic,
        respect_response_traffic=respect_response_traffic,
        phase="upsert",
    )
    yielded_to_response_traffic_count += yielded
    response_traffic_timeout_count += timed_out

    if not isinstance(payload, dict):
        return _TokenImportItemResult(
            index=index,
            category="failed",
            item={"index": index, "error": "Token payload must be a JSON object"},
            yielded_to_response_traffic_count=yielded_to_response_traffic_count,
            response_traffic_timeout_count=response_traffic_timeout_count,
        )

    try:
        result = await upsert_token_payload(payload)
        if result.action in {"created", "updated"} and not result.token.account_id:
            validation_cooldown_seconds = _import_token_validation_cooldown_seconds()
            await mark_token_import_validation_pending(
                result.token.id,
                "Import validation pending",
                cooldown_seconds=validation_cooldown_seconds,
            )
            yielded, timed_out = await _wait_for_response_traffic_turn(
                job_id=job_id,
                index=index,
                response_traffic=response_traffic,
                respect_response_traffic=respect_response_traffic,
                phase="oauth_refresh",
            )
            yielded_to_response_traffic_count += yielded
            response_traffic_timeout_count += timed_out
            try:
                await oauth_manager.get_access_token(result.token, http_client, force_refresh=True)
            except Exception as exc:
                permanent_refresh_failure = is_permanently_invalid_refresh_token_error(exc)
                await mark_token_error(
                    result.token.id,
                    str(getattr(exc, "detail", exc) or exc),
                    deactivate=permanent_refresh_failure,
                    cooldown_seconds=None if permanent_refresh_failure else validation_cooldown_seconds,
                    clear_access_token=True,
                )
                logger.warning(
                    "Token import could not enrich missing account_id: job_id=%s index=%s token_id=%s",
                    job_id,
                    index,
                    result.token.id,
                    exc_info=True,
                )
                return _TokenImportItemResult(
                    index=index,
                    category="failed",
                    item={
                        "id": result.token.id,
                        "email": result.token.email,
                        "account_id": result.token.account_id,
                        "index": index,
                        "error": str(getattr(exc, "detail", exc) or exc),
                    },
                    yielded_to_response_traffic_count=yielded_to_response_traffic_count,
                    response_traffic_timeout_count=response_traffic_timeout_count,
                )
        item = {
            "id": result.token.id,
            "email": result.token.email,
            "account_id": result.token.account_id,
            "index": index,
        }
        category = "created"
        if result.action == "updated":
            category = "updated"
        elif result.action != "created":
            category = "skipped"
            item = {
                **item,
                "reason": "duplicate_refresh_token_history",
            }
        return _TokenImportItemResult(
            index=index,
            category=category,
            item=item,
            yielded_to_response_traffic_count=yielded_to_response_traffic_count,
            response_traffic_timeout_count=response_traffic_timeout_count,
        )
    except Exception as exc:
        return _TokenImportItemResult(
            index=index,
            category="failed",
            item={"index": index, "error": str(exc)},
            yielded_to_response_traffic_count=yielded_to_response_traffic_count,
            response_traffic_timeout_count=response_traffic_timeout_count,
        )


def _build_validated_token_payload(
    payload: dict[str, Any],
    *,
    refreshed: dict[str, Any],
    refresh_token: str,
) -> dict[str, Any]:
    validated = dict(payload)
    next_refresh_token = refreshed.get("refresh_token") or refresh_token
    validated["refresh_token"] = next_refresh_token
    validated["access_token"] = refreshed["access_token"]
    if refreshed.get("id_token"):
        validated["id_token"] = refreshed["id_token"]
    if refreshed.get("account_id"):
        validated["account_id"] = refreshed["account_id"]
        validated["chatgpt_account_id"] = refreshed["account_id"]
    if refreshed.get("email"):
        validated["email"] = refreshed["email"]
    if refreshed.get("expires_at") is not None:
        validated["expired"] = refreshed["expires_at"].isoformat()
    validated["last_refresh"] = utcnow().isoformat()
    return validated


def _append_import_result(
    result: _TokenImportItemResult,
    *,
    created: list[dict[str, Any]],
    updated: list[dict[str, Any]],
    skipped: list[dict[str, Any]],
    failed: list[dict[str, Any]],
) -> None:
    if result.category == "created":
        created.append(result.item)
    elif result.category == "updated":
        updated.append(result.item)
    elif result.category == "skipped":
        skipped.append(result.item)
    elif result.category == "failed":
        failed.append(result.item)


async def _validate_single_staged_token_import_item(
    job_id: int,
    item: _TokenImportStagedItem,
    *,
    http_client: httpx.AsyncClient,
    oauth_manager: CodexOAuthManager,
) -> _TokenImportItemResult:
    payload = _payload_from_staged_item(item)
    started_at = time.monotonic()
    await _mark_import_item_validating(item.id)

    if not isinstance(payload, dict):
        error = "Token payload must be a JSON object"
        validation_ms = int((time.monotonic() - started_at) * 1000)
        await _mark_import_item_failed(item.id, error_message=error, validation_ms=validation_ms)
        return _TokenImportItemResult(
            index=item.index,
            category="failed",
            item={"index": item.index, "error": error},
            validate_ms=validation_ms,
        )

    refresh_token = normalize_refresh_token(payload.get("refresh_token"))
    if refresh_token is None:
        error = "Token payload missing refresh_token"
        validation_ms = int((time.monotonic() - started_at) * 1000)
        await _mark_import_item_failed(item.id, error_message=error, validation_ms=validation_ms)
        return _TokenImportItemResult(
            index=item.index,
            category="failed",
            item={"index": item.index, "error": error},
            validate_ms=validation_ms,
        )

    try:
        refreshed = await oauth_manager._refresh_codex_access_token(http_client, refresh_token)
        validated_payload = _build_validated_token_payload(payload, refreshed=refreshed, refresh_token=refresh_token)
        validation_ms = int((time.monotonic() - started_at) * 1000)
        await _mark_import_item_validated(
            item.id,
            validated_payload=validated_payload,
            validation_ms=validation_ms,
        )
        return _TokenImportItemResult(
            index=item.index,
            category="validated",
            item={"index": item.index},
            validate_ms=validation_ms,
        )
    except Exception as exc:
        error = str(getattr(exc, "detail", exc) or exc)
        validation_ms = int((time.monotonic() - started_at) * 1000)
        await _mark_import_item_failed(item.id, error_message=error, validation_ms=validation_ms)
        logger.warning(
            "Token import validation failed: job_id=%s index=%s item_id=%s",
            job_id,
            item.index,
            item.id,
            exc_info=True,
        )
        return _TokenImportItemResult(
            index=item.index,
            category="failed",
            item={"index": item.index, "error": error},
            validate_ms=validation_ms,
        )


async def _publish_single_staged_token_import_item(
    item: _TokenImportStagedItem,
) -> _TokenImportItemResult:
    payload = item.validated_payload
    started_at = time.monotonic()
    if not isinstance(payload, dict):
        error = "Validated token payload is missing"
        publish_ms = int((time.monotonic() - started_at) * 1000)
        await _mark_import_item_failed(item.id, error_message=error, publish_ms=publish_ms)
        return _TokenImportItemResult(
            index=item.index,
            category="failed",
            item={"index": item.index, "error": error},
            publish_ms=publish_ms,
        )

    await _mark_import_item_publishing(item.id)
    try:
        result = await upsert_token_payload(payload)
        publish_ms = int((time.monotonic() - started_at) * 1000)
        await _mark_import_item_published(
            item.id,
            token_id=result.token.id,
            action=result.action,
            publish_ms=publish_ms,
        )
        item_payload = {
            "id": result.token.id,
            "email": result.token.email,
            "account_id": result.token.account_id,
            "index": item.index,
        }
        if result.action == "created":
            category = "created"
        elif result.action == "updated":
            category = "updated"
        else:
            category = "skipped"
            item_payload = {
                **item_payload,
                "reason": "duplicate_refresh_token_history",
            }
        return _TokenImportItemResult(
            index=item.index,
            category=category,
            item=item_payload,
            publish_ms=publish_ms,
        )
    except Exception as exc:
        error = str(exc)
        publish_ms = int((time.monotonic() - started_at) * 1000)
        await _mark_import_item_failed(item.id, error_message=error, publish_ms=publish_ms)
        return _TokenImportItemResult(
            index=item.index,
            category="failed",
            item={"index": item.index, "error": error},
            publish_ms=publish_ms,
        )


async def _process_staged_token_import_job(
    job: TokenImportJobLease,
    *,
    response_traffic: ResponseTrafficLike,
    http_client: httpx.AsyncClient,
    oauth_manager: CodexOAuthManager,
) -> TokenImportJobState | None:
    staged_items = await _list_token_import_items(job.id)
    if not staged_items and not job.payloads:
        return await complete_token_import_job(
            job.id,
            processed_count=0,
            created=[],
            updated=[],
            skipped=[],
            failed=[],
            yielded_to_response_traffic_count=0,
            response_traffic_timeout_count=0,
        )

    created = _sort_items_by_index(list(job.created))
    updated = _sort_items_by_index(list(job.updated))
    skipped = _sort_items_by_index(list(job.skipped))
    failed = _sort_items_by_index(list(job.failed))
    yielded_to_response_traffic = int(job.yielded_to_response_traffic_count or 0)
    response_traffic_timeout_count = int(job.response_traffic_timeout_count or 0)

    if not staged_items and job.payloads:
        staged_items = [
            _TokenImportStagedItem(
                id=index + 1,
                job_id=job.id,
                index=index,
                status=IMPORT_ITEM_STATUS_QUEUED,
                payload=payload if isinstance(payload, dict) else {"_raw": payload},
                validated_payload=None,
            )
            for index, payload in enumerate(job.payloads)
        ]

    pending_validation = [
        item
        for item in staged_items
        if item.status in {IMPORT_ITEM_STATUS_QUEUED, IMPORT_ITEM_STATUS_VALIDATING}
        and item.validated_payload is None
        and item.status != IMPORT_ITEM_STATUS_FAILED
    ]
    pending_publish = [item for item in staged_items if item.status == IMPORT_ITEM_STATUS_VALIDATED and item.validated_payload]
    failed_items = [item for item in staged_items if item.status == IMPORT_ITEM_STATUS_FAILED]
    completed_indices = {
        int(item.get("index"))
        for item in [*created, *updated, *skipped]
        if isinstance(item, dict) and str(item.get("index", "")).lstrip("-").isdigit()
    }
    for item in staged_items:
        if item.status not in {IMPORT_ITEM_STATUS_PUBLISHED, IMPORT_ITEM_STATUS_DUPLICATE} or item.index in completed_indices:
            continue
        payload = {"id": item.token_id, "index": item.index}
        if item.action == "created":
            created.append(payload)
        elif item.action == "updated":
            updated.append(payload)
        else:
            skipped.append({**payload, "reason": "duplicate_refresh_token_history"})
        completed_indices.add(item.index)

    max_concurrency = max(1, min(_import_job_max_concurrency(), len(pending_validation) or 1))
    respect_response_traffic = _import_job_respect_response_traffic()

    if pending_validation:
        semaphore = asyncio.Semaphore(max_concurrency)

        async def run_validate(item: _TokenImportStagedItem) -> _TokenImportItemResult:
            async with semaphore:
                yielded, timed_out = await _wait_for_response_traffic_turn(
                    job_id=job.id,
                    index=item.index,
                    response_traffic=response_traffic,
                    respect_response_traffic=respect_response_traffic,
                    phase="validate",
                )
                result = await _validate_single_staged_token_import_item(
                    job.id,
                    item,
                    http_client=http_client,
                    oauth_manager=oauth_manager,
                )
                return _TokenImportItemResult(
                    index=result.index,
                    category=result.category,
                    item=result.item,
                    yielded_to_response_traffic_count=yielded + result.yielded_to_response_traffic_count,
                    response_traffic_timeout_count=timed_out + result.response_traffic_timeout_count,
                    validate_ms=result.validate_ms,
                    publish_ms=result.publish_ms,
                )

        validation_results = await asyncio.gather(*(run_validate(item) for item in pending_validation))
        for result in validation_results:
            yielded_to_response_traffic += result.yielded_to_response_traffic_count
            response_traffic_timeout_count += result.response_traffic_timeout_count
            _append_import_result(result, created=created, updated=updated, skipped=skipped, failed=failed)

        staged_items = await _list_token_import_items(job.id)
        pending_publish = [item for item in staged_items if item.status == IMPORT_ITEM_STATUS_VALIDATED and item.validated_payload]

    if pending_publish:
        publish_concurrency = max(1, min(4, len(pending_publish)))
        semaphore = asyncio.Semaphore(publish_concurrency)

        async def run_publish(item: _TokenImportStagedItem) -> _TokenImportItemResult:
            async with semaphore:
                result = await _publish_single_staged_token_import_item(item)
                return result

        publish_results = await asyncio.gather(*(run_publish(item) for item in pending_publish))
        for result in publish_results:
            _append_import_result(result, created=created, updated=updated, skipped=skipped, failed=failed)

    failed_indices = {
        int(item.get("index"))
        for item in failed
        if isinstance(item, dict) and str(item.get("index", "")).lstrip("-").isdigit()
    }
    for item in failed_items:
        if item.index in failed_indices:
            continue
        failed.append({"index": item.index, "error": item.error_message or "Token import item failed"})
    created, updated, skipped, failed = _build_progress_snapshot(
        created=created,
        updated=updated,
        skipped=skipped,
        failed=failed,
    )
    processed_count = len(created) + len(updated) + len(skipped) + len(failed)
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


async def process_token_import_job(
    job: TokenImportJobLease,
    *,
    response_traffic: ResponseTrafficLike,
    http_client: httpx.AsyncClient | None = None,
) -> TokenImportJobState | None:
    try:
        staged_items = await _list_token_import_items(job.id)
    except Exception:
        if not job.payloads:
            raise
        logger.exception("Falling back to legacy token import payload processing: job_id=%s", job.id)
        staged_items = []
    if staged_items or not job.payloads:
        oauth_manager = CodexOAuthManager()
        owns_http_client = False
        client = http_client
        if client is None:
            client = httpx.AsyncClient()
            owns_http_client = True
        try:
            assert client is not None
            return await _process_staged_token_import_job(
                job,
                response_traffic=response_traffic,
                http_client=client,
                oauth_manager=oauth_manager,
            )
        finally:
            if owns_http_client:
                await client.aclose()

    payloads = list(job.payloads)
    created = _sort_items_by_index(list(job.created))
    updated = _sort_items_by_index(list(job.updated))
    skipped = _sort_items_by_index(list(job.skipped))
    failed = _sort_items_by_index(list(job.failed))
    yielded_to_response_traffic = int(job.yielded_to_response_traffic_count or 0)
    response_traffic_timeout_count = int(job.response_traffic_timeout_count or 0)
    processed_count = max(0, min(int(job.processed_count or 0), len(payloads)))
    max_concurrency = max(1, min(_import_job_max_concurrency(), len(payloads) - processed_count or 1))
    progress_flush_every = _import_job_progress_flush_every()
    progress_flush_interval_seconds = _import_job_progress_flush_interval_seconds()
    respect_response_traffic = _import_job_respect_response_traffic()
    pending_tasks: list[asyncio.Task[_TokenImportItemResult]] = []
    oauth_manager = CodexOAuthManager()

    try:
        if processed_count < len(payloads):
            semaphore = asyncio.Semaphore(max_concurrency)
            last_progress_flush_at = time.monotonic()
            pending_results_since_flush = 0

            async with httpx.AsyncClient() as http_client:
                async def run_limited(index: int, payload: Any) -> _TokenImportItemResult:
                    async with semaphore:
                        result = await _process_single_token_import_payload(
                            job.id,
                            index,
                            payload,
                            http_client=http_client,
                            oauth_manager=oauth_manager,
                            response_traffic=response_traffic,
                            respect_response_traffic=respect_response_traffic,
                        )
                        if result.response_traffic_timeout_count:
                            logger.warning(
                                "Token import job wait timed out while /v1/responses traffic remained active; continuing import: job_id=%s index=%s total_payloads=%s active_responses=%s",
                                job.id,
                                index,
                                len(payloads),
                                response_traffic.active_responses,
                            )
                        return result

                pending_tasks = [
                    asyncio.create_task(
                        run_limited(index, payloads[index]),
                        name=f"oaix-token-import-{job.id}-{index}",
                    )
                    for index in range(processed_count, len(payloads))
                ]

                for task in asyncio.as_completed(pending_tasks):
                    result = await task
                    yielded_to_response_traffic += result.yielded_to_response_traffic_count
                    response_traffic_timeout_count += result.response_traffic_timeout_count
                    if result.category == "created":
                        created.append(result.item)
                    elif result.category == "updated":
                        updated.append(result.item)
                    elif result.category == "skipped":
                        skipped.append(result.item)
                    else:
                        failed.append(result.item)

                    processed_count += 1
                    pending_results_since_flush += 1

                    should_flush_progress = (
                        processed_count < len(payloads)
                        and pending_results_since_flush > 0
                        and (
                            pending_results_since_flush >= progress_flush_every
                            or time.monotonic() - last_progress_flush_at >= progress_flush_interval_seconds
                        )
                    )
                    if should_flush_progress:
                        progress_created, progress_updated, progress_skipped, progress_failed = _build_progress_snapshot(
                            created=created,
                            updated=updated,
                            skipped=skipped,
                            failed=failed,
                        )
                        await update_token_import_job_progress(
                            job.id,
                            processed_count=processed_count,
                            created=progress_created,
                            updated=progress_updated,
                            skipped=progress_skipped,
                            failed=progress_failed,
                            yielded_to_response_traffic_count=yielded_to_response_traffic,
                            response_traffic_timeout_count=response_traffic_timeout_count,
                        )
                        last_progress_flush_at = time.monotonic()
                        pending_results_since_flush = 0

        created, updated, skipped, failed = _build_progress_snapshot(
            created=created,
            updated=updated,
            skipped=skipped,
            failed=failed,
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
        for task in pending_tasks:
            task.cancel()
        if pending_tasks:
            await asyncio.gather(*pending_tasks, return_exceptions=True)
        await requeue_token_import_job(
            job.id,
            last_error="Import worker interrupted before completion; job requeued to resume.",
        )
        raise
    except Exception as exc:
        for task in pending_tasks:
            task.cancel()
        if pending_tasks:
            await asyncio.gather(*pending_tasks, return_exceptions=True)
        logger.exception("Token import job failed unexpectedly: job_id=%s", job.id)
        return await fail_token_import_job(job.id, last_error=str(exc))


class TokenImportBackgroundWorker:
    def __init__(
        self,
        *,
        response_traffic: ResponseTrafficLike,
        http_client: httpx.AsyncClient | None = None,
        poll_interval_seconds: float | None = None,
        stale_after_seconds: float | None = None,
        on_token_selection_settings_updated: Callable[[TokenSelectionSettings], Awaitable[None] | None] | None = None,
    ) -> None:
        self._response_traffic = response_traffic
        self._http_client = http_client
        self._on_token_selection_settings_updated = on_token_selection_settings_updated
        self._poll_interval_seconds = (
            _import_job_poll_interval_seconds() if poll_interval_seconds is None else max(0.25, float(poll_interval_seconds))
        )
        self._stale_after_seconds = (
            _import_job_stale_after_seconds() if stale_after_seconds is None else max(30.0, float(stale_after_seconds))
        )
        self._submitted_jobs: asyncio.Queue[int] = asyncio.Queue()
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
        self._task.add_done_callback(self._handle_task_done)

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

    def submit(self, job: TokenImportJobLease | int) -> None:
        job_id = int(job.id) if isinstance(job, TokenImportJobLease) else int(job)
        self._submitted_jobs.put_nowait(job_id)
        self._wake_event.set()

    def _handle_task_done(self, task: asyncio.Task[None]) -> None:
        if task.cancelled():
            return
        if self._stopping:
            task.exception()
            return
        exc = task.exception()
        if exc is None:
            logger.warning("Token import worker exited unexpectedly without an exception")
            return
        logger.exception("Token import worker task crashed", exc_info=exc)

    async def _publish_token_selection_settings(self, selection: TokenSelectionSettings | None) -> None:
        if selection is None or self._on_token_selection_settings_updated is None:
            return
        result = self._on_token_selection_settings_updated(selection)
        if inspect.isawaitable(result):
            await result

    async def _apply_imported_token_queue_position(self, job: TokenImportJobLease, result: TokenImportJobState | None) -> None:
        if result is None or result.status != IMPORT_JOB_STATUS_COMPLETED:
            return
        imported_token_ids = _imported_token_ids_from_job_state(result)
        if not imported_token_ids:
            return
        selection = await apply_imported_token_queue_position(
            imported_token_ids,
            position=job.import_queue_position,
        )
        await self._publish_token_selection_settings(selection)

    async def _run(self) -> None:
        while not self._stopping:
            try:
                try:
                    submitted_job_id = self._submitted_jobs.get_nowait()
                except asyncio.QueueEmpty:
                    submitted_job_id = None

                job: TokenImportJobLease | None = None
                if submitted_job_id is not None:
                    job = await claim_token_import_job(submitted_job_id)
                if job is None:
                    job = await claim_next_token_import_job(stale_after_seconds=self._stale_after_seconds)
                if job is None:
                    self._wake_event.clear()
                    if not self._submitted_jobs.empty():
                        self._wake_event.set()
                        continue
                    try:
                        await asyncio.wait_for(self._wake_event.wait(), timeout=self._poll_interval_seconds)
                    except asyncio.TimeoutError:
                        pass
                    continue

                logger.info(
                    "Processing token import job: job_id=%s processed=%s total=%s concurrency=%s respect_response_traffic=%s",
                    job.id,
                    job.processed_count,
                    job.total_count,
                    max(1, min(_import_job_max_concurrency(), job.total_count - job.processed_count or 1)),
                    _import_job_respect_response_traffic(),
                )
                result = await process_token_import_job(
                    job,
                    response_traffic=self._response_traffic,
                    http_client=self._http_client,
                )
                await self._apply_imported_token_queue_position(job, result)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Token import worker loop failed; retrying after backoff")
                if self._stopping:
                    break
                await asyncio.sleep(min(self._poll_interval_seconds, 1.0))
