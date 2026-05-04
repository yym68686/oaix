from __future__ import annotations

import asyncio
import inspect
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Awaitable, Callable, Protocol

import httpx
from sqlalchemy import and_, or_, select

from .database import CodexToken, TokenImportJob, get_read_session, get_session, utcnow
from .oauth import CodexOAuthManager
from .token_store import (
    DEFAULT_TOKEN_IMPORT_QUEUE_POSITION,
    TokenSelectionSettings,
    apply_imported_token_queue_position,
    normalize_token_import_queue_position,
    upsert_token_payload,
)


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
    return _int_env("IMPORT_JOB_MAX_CONCURRENCY", 16, minimum=1)


def _import_job_progress_flush_every() -> int:
    return _int_env("IMPORT_JOB_PROGRESS_FLUSH_EVERY", 32, minimum=1)


def _import_job_progress_flush_interval_seconds() -> float:
    return _float_env("IMPORT_JOB_PROGRESS_FLUSH_INTERVAL_SECONDS", 2.0, minimum=0.0)


def _import_job_respect_response_traffic() -> bool:
    return _bool_env("IMPORT_JOB_RESPECT_RESPONSE_TRAFFIC", False)


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
    async with get_session() as session:
        async with session.begin():
            job = TokenImportJob(
                status=IMPORT_JOB_STATUS_QUEUED,
                import_queue_position=resolved_import_queue_position,
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
            return _serialize_job_lease(job)


async def get_token_import_job(job_id: int) -> TokenImportJobState | None:
    async with get_read_session() as session:
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
    async with get_session() as session:
        async with session.begin():
            job = await _get_token_import_job_for_update(session, job_id)
            if job is None or str(job.status or "") != IMPORT_JOB_STATUS_QUEUED:
                return None

            claimed = _claim_job_lease(job)
            await session.flush()
            return claimed


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

    if respect_response_traffic:
        try:
            if await response_traffic.wait_for_import_turn():
                yielded_to_response_traffic_count = 1
        except TimeoutError:
            response_traffic_timeout_count = 1
            logger.warning(
                "Token import job wait timed out while /v1/responses traffic remained active; continuing import: job_id=%s index=%s active_responses=%s",
                job_id,
                index,
                response_traffic.active_responses,
            )
            await asyncio.sleep(0)

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
            try:
                await oauth_manager.get_access_token(result.token, http_client, force_refresh=True)
            except Exception:
                logger.warning(
                    "Token import could not enrich missing account_id: job_id=%s index=%s token_id=%s",
                    job_id,
                    index,
                    result.token.id,
                    exc_info=True,
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


async def process_token_import_job(
    job: TokenImportJobLease,
    *,
    response_traffic: ResponseTrafficLike,
) -> TokenImportJobState | None:
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
        poll_interval_seconds: float | None = None,
        stale_after_seconds: float | None = None,
        on_token_selection_settings_updated: Callable[[TokenSelectionSettings], Awaitable[None] | None] | None = None,
    ) -> None:
        self._response_traffic = response_traffic
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
                result = await process_token_import_job(job, response_traffic=self._response_traffic)
                await self._apply_imported_token_queue_position(job, result)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Token import worker loop failed; retrying after backoff")
                if self._stopping:
                    break
                await asyncio.sleep(min(self._poll_interval_seconds, 1.0))
