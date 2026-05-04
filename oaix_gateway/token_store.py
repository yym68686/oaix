import asyncio
import base64
import glob
import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, TypeAlias

from sqlalchemy import String, and_, case, cast, delete, func, nullsfirst, nullslast, or_, select, text, update
from sqlalchemy.dialects.postgresql import insert as postgresql_insert

from .database import (
    CodexToken,
    CodexTokenScopedCooldown,
    GatewaySetting,
    close_database,
    get_read_session,
    get_session,
    init_db,
    utcnow,
)
from .token_identity import (
    collect_refresh_token_aliases,
    group_rows_by_refresh_token_history,
    merge_refresh_token_aliases,
    normalize_refresh_token,
)

logger = logging.getLogger("oaix.gateway")

TOKEN_SELECTION_STRATEGY_LEAST_RECENTLY_USED = "least_recently_used"
TOKEN_SELECTION_STRATEGY_FILL_FIRST = "fill_first"
DEFAULT_TOKEN_SELECTION_STRATEGY = TOKEN_SELECTION_STRATEGY_LEAST_RECENTLY_USED
TOKEN_SELECTION_SETTING_KEY = "token_selection"
TOKEN_PLAN_TYPE_FREE = "free"
TOKEN_PLAN_TYPE_PLUS = "plus"
TOKEN_PLAN_TYPE_TEAM = "team"
TOKEN_PLAN_TYPE_PRO = "pro"
DEFAULT_TOKEN_PLAN_ORDER = (
    TOKEN_PLAN_TYPE_FREE,
    TOKEN_PLAN_TYPE_PLUS,
    TOKEN_PLAN_TYPE_TEAM,
    TOKEN_PLAN_TYPE_PRO,
)
TOKEN_LIST_STATUS_ALL = "all"
TOKEN_LIST_STATUS_AVAILABLE = "available"
TOKEN_LIST_STATUS_COOLING = "cooling"
TOKEN_LIST_STATUS_DISABLED = "disabled"
TOKEN_LIST_STATUSES = (
    TOKEN_LIST_STATUS_ALL,
    TOKEN_LIST_STATUS_AVAILABLE,
    TOKEN_LIST_STATUS_COOLING,
    TOKEN_LIST_STATUS_DISABLED,
)
TOKEN_LIST_PLAN_ALL = "all"
TOKEN_LIST_PLAN_UNKNOWN = "unknown"
TOKEN_LIST_SORT_NEWEST = "-created_at"
TOKEN_LIST_SORT_OLDEST = "created_at"
TOKEN_LIST_SORT_LAST_USED_DESC = "-last_used_at"
TOKEN_LIST_SORT_LAST_USED_ASC = "last_used_at"
TOKEN_LIST_SORT_STATUS = "status"
TOKEN_LIST_SORT_PLAN = "plan_type"
TOKEN_LIST_SORT_ACCOUNT = "account"
TOKEN_LIST_SORTS = (
    TOKEN_LIST_SORT_NEWEST,
    TOKEN_LIST_SORT_OLDEST,
    TOKEN_LIST_SORT_LAST_USED_DESC,
    TOKEN_LIST_SORT_LAST_USED_ASC,
    TOKEN_LIST_SORT_STATUS,
    TOKEN_LIST_SORT_PLAN,
    TOKEN_LIST_SORT_ACCOUNT,
)
TOKEN_IMPORT_QUEUE_POSITION_FRONT = "front"
TOKEN_IMPORT_QUEUE_POSITION_BACK = "back"
DEFAULT_TOKEN_IMPORT_QUEUE_POSITION = TOKEN_IMPORT_QUEUE_POSITION_FRONT
MERGED_DUPLICATE_TOKEN_ERROR_PREFIX = "Merged into token #"
TOKEN_IDENTITY_LOCK_NAMESPACE = "codex-token-identity"
TOKEN_HISTORY_REPAIR_LOCK_KEY = "codex-token-history:repair"
DEFAULT_FILL_FIRST_TOKEN_CACHE_TTL_MS = 250
DEFAULT_FILL_FIRST_TOKEN_STALE_TTL_SECONDS = 5.0
DEFAULT_TOKEN_STATUS_WRITE_CONCURRENCY = 1
DEFAULT_TOKEN_STATUS_WRITE_LOCK_TIMEOUT_MS = 250
MIN_TOKEN_ACTIVE_STREAM_CAP = 1
MAX_TOKEN_ACTIVE_STREAM_CAP = 10
DEFAULT_TOKEN_ACTIVE_STREAM_CAP = 10


@dataclass(frozen=True)
class StoredTokenSummary:
    id: int
    email: str | None
    account_id: str | None


@dataclass(frozen=True)
class TokenCounts:
    total: int
    active: int
    available: int
    cooling: int
    disabled: int


@dataclass(frozen=True)
class TokenPlanCounts:
    free: int = 0
    plus: int = 0
    team: int = 0
    pro: int = 0
    unknown: int = 0


@dataclass(frozen=True)
class TokenStatus:
    id: int
    email: str | None
    account_id: str | None
    is_active: bool
    cooldown_until: datetime | None
    last_refresh_at: datetime | None
    expires_at: datetime | None
    last_used_at: datetime | None
    last_error: str | None
    source_file: str | None
    updated_at: datetime


@dataclass(frozen=True)
class TokenUpsertResult:
    token: CodexToken
    action: str


@dataclass(frozen=True)
class TokenHistoryRepairSummary:
    duplicate_group_count: int
    merged_row_count: int
    canonical_ids: tuple[int, ...] = ()
    shadow_ids: tuple[int, ...] = ()


@dataclass(frozen=True)
class TokenFileImportSummary:
    created_count: int
    updated_count: int
    skipped_count: int
    failed_count: int


@dataclass(frozen=True)
class TokenSelectionSettings:
    strategy: str
    token_order: tuple[int, ...] = ()
    plan_order_enabled: bool = False
    plan_order: tuple[str, ...] = DEFAULT_TOKEN_PLAN_ORDER
    active_stream_cap: int = DEFAULT_TOKEN_ACTIVE_STREAM_CAP
    updated_at: datetime | None = None


@dataclass(frozen=True)
class TokenDeleteResult:
    canonical_id: int
    deleted_ids: tuple[int, ...]


@dataclass(frozen=True)
class _FillFirstTokenCacheEntry:
    expires_at: float
    stale_until: float
    tokens: tuple[CodexToken, ...]


_FillFirstCacheKey: TypeAlias = tuple[tuple[int, ...], bool, tuple[str, ...], str | None]
_FILL_FIRST_TOKEN_CACHE: dict[_FillFirstCacheKey, _FillFirstTokenCacheEntry] = {}
_FILL_FIRST_TOKEN_REFRESH_TASKS: dict[_FillFirstCacheKey, asyncio.Task[tuple[CodexToken, ...]]] = {}
_FILL_FIRST_TOKEN_REFRESH_GENERATIONS: dict[_FillFirstCacheKey, int] = {}
_FILL_FIRST_TOKEN_CACHE_GENERATION = 0


@dataclass(frozen=True)
class _TokenRuntimeState:
    is_active: bool | None = None
    cooldown_until: datetime | None = None
    last_error: str | None = None


@dataclass(frozen=True)
class _ScopedTokenRuntimeCooldown:
    cooldown_until: datetime
    last_error: str | None = None


@dataclass
class _TokenStatusWriteJob:
    token_id: int
    kind: str
    message: str | None = None
    deactivate: bool = False
    cooldown_until: datetime | None = None
    clear_access_token: bool = False
    scope: str | None = None
    attempts: int = 0


_TOKEN_RUNTIME_STATE: dict[int, _TokenRuntimeState] = {}
_TOKEN_SCOPED_RUNTIME_COOLDOWNS: dict[tuple[int, str], _ScopedTokenRuntimeCooldown] = {}
_TOKEN_STATUS_WRITE_QUEUE: asyncio.Queue[tuple[int, str]] | None = None
_TOKEN_STATUS_WRITE_QUEUE_LOOP: asyncio.AbstractEventLoop | None = None
_TOKEN_STATUS_WRITE_DRAINERS: set[asyncio.Task[None]] = set()
_TOKEN_STATUS_WRITE_RETRY_TASKS: set[asyncio.Task[None]] = set()
_TOKEN_STATUS_WRITE_PENDING: dict[tuple[int, str], _TokenStatusWriteJob] = {}


def _fill_first_token_cache_ttl_seconds() -> float:
    raw = str(os.getenv("FILL_FIRST_TOKEN_CACHE_TTL_MS") or "").strip()
    try:
        ttl_ms = int(raw) if raw else DEFAULT_FILL_FIRST_TOKEN_CACHE_TTL_MS
    except ValueError:
        ttl_ms = DEFAULT_FILL_FIRST_TOKEN_CACHE_TTL_MS
    return max(0.1, min(0.5, ttl_ms / 1000.0))


def _fill_first_token_stale_ttl_seconds() -> float:
    raw = str(os.getenv("FILL_FIRST_TOKEN_STALE_TTL_SECONDS") or "").strip()
    try:
        value = float(raw) if raw else DEFAULT_FILL_FIRST_TOKEN_STALE_TTL_SECONDS
    except ValueError:
        value = DEFAULT_FILL_FIRST_TOKEN_STALE_TTL_SECONDS
    return max(0.5, min(30.0, value))


def invalidate_fill_first_token_cache() -> None:
    global _FILL_FIRST_TOKEN_CACHE_GENERATION
    _FILL_FIRST_TOKEN_CACHE_GENERATION += 1
    _FILL_FIRST_TOKEN_CACHE.clear()


def _token_status_write_concurrency() -> int:
    raw = str(os.getenv("TOKEN_STATUS_WRITE_CONCURRENCY") or "").strip()
    try:
        value = int(raw) if raw else DEFAULT_TOKEN_STATUS_WRITE_CONCURRENCY
    except ValueError:
        value = DEFAULT_TOKEN_STATUS_WRITE_CONCURRENCY
    return max(1, min(8, value))


def _token_status_write_lock_timeout_ms() -> int:
    raw = str(os.getenv("TOKEN_STATUS_WRITE_LOCK_TIMEOUT_MS") or "").strip()
    try:
        value = int(raw) if raw else DEFAULT_TOKEN_STATUS_WRITE_LOCK_TIMEOUT_MS
    except ValueError:
        value = DEFAULT_TOKEN_STATUS_WRITE_LOCK_TIMEOUT_MS
    return max(50, min(5000, value))


def _token_is_runtime_available(
    token: CodexToken,
    *,
    now: datetime,
    scoped_cooldown_scope: str | None,
) -> bool:
    token_id = int(token.id)
    runtime_state = _TOKEN_RUNTIME_STATE.get(token_id)
    if runtime_state is not None:
        if runtime_state.is_active is False:
            return False
        if runtime_state.cooldown_until is not None:
            if runtime_state.cooldown_until > now:
                return False
            _TOKEN_RUNTIME_STATE.pop(token_id, None)

    if scoped_cooldown_scope:
        cooldown_key = (token_id, scoped_cooldown_scope)
        scoped_cooldown = _TOKEN_SCOPED_RUNTIME_COOLDOWNS.get(cooldown_key)
        if scoped_cooldown is not None:
            if scoped_cooldown.cooldown_until > now:
                return False
            _TOKEN_SCOPED_RUNTIME_COOLDOWNS.pop(cooldown_key, None)
    return True


def _runtime_unavailable_token_ids(*, now: datetime, scoped_cooldown_scope: str | None) -> tuple[int, ...]:
    unavailable: set[int] = set()
    for token_id, runtime_state in list(_TOKEN_RUNTIME_STATE.items()):
        if runtime_state.is_active is False:
            unavailable.add(token_id)
            continue
        if runtime_state.cooldown_until is not None:
            if runtime_state.cooldown_until > now:
                unavailable.add(token_id)
            else:
                _TOKEN_RUNTIME_STATE.pop(token_id, None)

    if scoped_cooldown_scope:
        for (token_id, scope), cooldown in list(_TOKEN_SCOPED_RUNTIME_COOLDOWNS.items()):
            if scope != scoped_cooldown_scope:
                continue
            if cooldown.cooldown_until > now:
                unavailable.add(token_id)
            else:
                _TOKEN_SCOPED_RUNTIME_COOLDOWNS.pop((token_id, scope), None)
    return tuple(sorted(unavailable))


def _apply_token_runtime_success_state(token_id: int) -> None:
    removed = _TOKEN_RUNTIME_STATE.pop(int(token_id), None)
    if removed is not None and (removed.is_active is False or removed.cooldown_until is not None):
        invalidate_fill_first_token_cache()


def _apply_token_runtime_error_state(
    token_id: int,
    *,
    message: str,
    deactivate: bool,
    cooldown_until: datetime | None,
) -> None:
    token_id = int(token_id)
    previous = _TOKEN_RUNTIME_STATE.get(token_id)
    if deactivate:
        next_state = _TokenRuntimeState(is_active=False, cooldown_until=None, last_error=message[:4000])
    elif cooldown_until is not None:
        next_state = _TokenRuntimeState(is_active=True, cooldown_until=cooldown_until, last_error=message[:4000])
    else:
        next_state = _TokenRuntimeState(
            is_active=previous.is_active if previous is not None else None,
            cooldown_until=previous.cooldown_until if previous is not None else None,
            last_error=message[:4000],
        )
    _TOKEN_RUNTIME_STATE[token_id] = next_state
    if deactivate or cooldown_until is not None:
        invalidate_fill_first_token_cache()


def _apply_token_runtime_scoped_cooldown(
    token_id: int,
    *,
    scope: str,
    message: str,
    cooldown_until: datetime,
) -> None:
    _TOKEN_SCOPED_RUNTIME_COOLDOWNS[(int(token_id), scope)] = _ScopedTokenRuntimeCooldown(
        cooldown_until=cooldown_until,
        last_error=message[:4000],
    )
    invalidate_fill_first_token_cache()


def _token_status_write_queue() -> asyncio.Queue[tuple[int, str]]:
    global _TOKEN_STATUS_WRITE_QUEUE, _TOKEN_STATUS_WRITE_QUEUE_LOOP, _TOKEN_STATUS_WRITE_DRAINERS
    loop = asyncio.get_running_loop()
    if _TOKEN_STATUS_WRITE_QUEUE is None or _TOKEN_STATUS_WRITE_QUEUE_LOOP is not loop:
        _TOKEN_STATUS_WRITE_QUEUE = asyncio.Queue()
        _TOKEN_STATUS_WRITE_QUEUE_LOOP = loop
        _TOKEN_STATUS_WRITE_DRAINERS = set()
    while len(_TOKEN_STATUS_WRITE_DRAINERS) < _token_status_write_concurrency():
        task = asyncio.create_task(_token_status_write_drainer(), name="oaix-token-status-write-drainer")
        _TOKEN_STATUS_WRITE_DRAINERS.add(task)
        task.add_done_callback(_TOKEN_STATUS_WRITE_DRAINERS.discard)
    return _TOKEN_STATUS_WRITE_QUEUE


def _merge_token_status_write_jobs(
    existing: _TokenStatusWriteJob,
    incoming: _TokenStatusWriteJob,
) -> _TokenStatusWriteJob:
    if incoming.kind == "success":
        return existing
    if existing.kind == "success":
        return incoming
    if incoming.kind == "scoped_cooldown":
        if existing.kind != "scoped_cooldown":
            return incoming
        cooldown_until = max(existing.cooldown_until or incoming.cooldown_until, incoming.cooldown_until)
        return _TokenStatusWriteJob(
            token_id=incoming.token_id,
            kind="scoped_cooldown",
            message=incoming.message or existing.message,
            cooldown_until=cooldown_until,
            scope=incoming.scope,
        )

    cooldown_until = existing.cooldown_until
    if incoming.cooldown_until is not None:
        cooldown_until = max(cooldown_until, incoming.cooldown_until) if cooldown_until is not None else incoming.cooldown_until
    return _TokenStatusWriteJob(
        token_id=incoming.token_id,
        kind="error",
        message=incoming.message or existing.message,
        deactivate=existing.deactivate or incoming.deactivate,
        cooldown_until=None if existing.deactivate or incoming.deactivate else cooldown_until,
        clear_access_token=existing.clear_access_token or incoming.clear_access_token,
    )


def _enqueue_token_status_write(job: _TokenStatusWriteJob) -> None:
    key = (int(job.token_id), f"scope:{job.scope}" if job.kind == "scoped_cooldown" else "token")
    pending = _TOKEN_STATUS_WRITE_PENDING.get(key)
    if pending is None:
        _TOKEN_STATUS_WRITE_PENDING[key] = job
        _token_status_write_queue().put_nowait(key)
        return
    _TOKEN_STATUS_WRITE_PENDING[key] = _merge_token_status_write_jobs(pending, job)


async def _set_low_priority_token_write_lock_timeout(session) -> None:
    bind = session.get_bind()
    if bind is None or bind.dialect.name != "postgresql":
        return
    timeout_ms = _token_status_write_lock_timeout_ms()
    await session.execute(text(f"SET LOCAL lock_timeout = '{timeout_ms}ms'"))


async def _write_token_success_job(session, job: _TokenStatusWriteJob) -> None:
    now = utcnow()
    await session.execute(
        update(CodexToken)
        .where(CodexToken.id == int(job.token_id))
        .where(CodexToken.merged_into_token_id.is_(None))
        .where(
            or_(
                CodexToken.is_active.is_not(True),
                CodexToken.cooldown_until.is_not(None),
                CodexToken.last_error.is_not(None),
            )
        )
        .values(
            is_active=True,
            cooldown_until=None,
            last_error=None,
            updated_at=now,
        )
    )


async def _write_token_error_job(session, job: _TokenStatusWriteJob) -> None:
    now = utcnow()
    values: dict[str, Any] = {
        "last_error": (job.message or "")[:4000],
        "updated_at": now,
    }
    if job.clear_access_token:
        values["access_token"] = None
        values["expires_at"] = None
    if job.deactivate:
        values["is_active"] = False
        values["cooldown_until"] = None
    elif job.cooldown_until is not None:
        values["is_active"] = True
        values["cooldown_until"] = job.cooldown_until
    await session.execute(
        update(CodexToken)
        .where(CodexToken.id == int(job.token_id))
        .where(CodexToken.merged_into_token_id.is_(None))
        .values(**values)
    )


async def _write_token_scoped_cooldown_job(session, job: _TokenStatusWriteJob) -> None:
    if not job.scope or job.cooldown_until is None:
        return
    now = utcnow()
    bind = session.get_bind()
    if bind is not None and bind.dialect.name == "postgresql":
        stmt = postgresql_insert(CodexTokenScopedCooldown).values(
            token_id=int(job.token_id),
            scope=job.scope,
            cooldown_until=job.cooldown_until,
            last_error=(job.message or "")[:4000],
            updated_at=now,
        )
        await session.execute(
            stmt.on_conflict_do_update(
                constraint="uq_codex_token_scoped_cooldowns_token_scope",
                set_={
                    "cooldown_until": job.cooldown_until,
                    "last_error": (job.message or "")[:4000],
                    "updated_at": now,
                },
            )
        )
        return

    result = await session.execute(
        select(CodexTokenScopedCooldown).where(
            CodexTokenScopedCooldown.token_id == int(job.token_id),
            CodexTokenScopedCooldown.scope == job.scope,
        )
    )
    cooldown = result.scalars().first()
    if cooldown is None:
        session.add(
            CodexTokenScopedCooldown(
                token_id=int(job.token_id),
                scope=job.scope,
                cooldown_until=job.cooldown_until,
                last_error=(job.message or "")[:4000],
                updated_at=now,
            )
        )
        return
    cooldown.cooldown_until = job.cooldown_until
    cooldown.last_error = (job.message or "")[:4000]
    cooldown.updated_at = now


async def _write_token_status_job(job: _TokenStatusWriteJob) -> None:
    async with get_session() as session:
        async with session.begin():
            await _set_low_priority_token_write_lock_timeout(session)
            if job.kind == "success":
                await _write_token_success_job(session, job)
            elif job.kind == "scoped_cooldown":
                await _write_token_scoped_cooldown_job(session, job)
            else:
                await _write_token_error_job(session, job)


async def _token_status_write_drainer() -> None:
    queue = _TOKEN_STATUS_WRITE_QUEUE
    if queue is None:
        return
    while True:
        key = await queue.get()
        job: _TokenStatusWriteJob | None = None
        try:
            job = _TOKEN_STATUS_WRITE_PENDING.pop(key, None)
            if job is None:
                continue
            await _write_token_status_job(job)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Failed to persist queued token status write")
            if job is not None and job.kind != "success" and job.attempts < 10:
                job.attempts += 1

                async def requeue_failed_write(failed_job: _TokenStatusWriteJob) -> None:
                    await asyncio.sleep(min(5.0, 0.25 * failed_job.attempts))
                    _enqueue_token_status_write(failed_job)

                retry_task = asyncio.create_task(requeue_failed_write(job), name="oaix-token-status-write-retry")
                _TOKEN_STATUS_WRITE_RETRY_TASKS.add(retry_task)
                retry_task.add_done_callback(_TOKEN_STATUS_WRITE_RETRY_TASKS.discard)
        finally:
            queue.task_done()


async def flush_token_status_write_queue() -> None:
    queue = _TOKEN_STATUS_WRITE_QUEUE
    if queue is None:
        return
    await queue.join()


async def stop_token_status_write_queue() -> None:
    await flush_token_status_write_queue()
    retry_tasks = list(_TOKEN_STATUS_WRITE_RETRY_TASKS)
    for task in retry_tasks:
        task.cancel()
    if retry_tasks:
        await asyncio.gather(*retry_tasks, return_exceptions=True)
    _TOKEN_STATUS_WRITE_RETRY_TASKS.clear()
    drainers = list(_TOKEN_STATUS_WRITE_DRAINERS)
    for task in drainers:
        task.cancel()
    if drainers:
        await asyncio.gather(*drainers, return_exceptions=True)
    _TOKEN_STATUS_WRITE_DRAINERS.clear()


def _canonical_token_filters() -> tuple[Any, ...]:
    return (CodexToken.merged_into_token_id.is_(None),)


def _datetime_sort_value(value: datetime | None) -> float:
    if value is None:
        return float("-inf")
    return value.timestamp()


def _non_empty_value(value: Any) -> Any | None:
    if isinstance(value, str):
        normalized = value.strip()
        return normalized or None
    return value if value is not None else None


def _normalize_plan_type(value: Any) -> str | None:
    normalized = str(value or "").strip().lower()
    if normalized.startswith("chatgpt_"):
        normalized = normalized[len("chatgpt_") :]
    return normalized or None


def _decode_base64_url(segment: str) -> bytes:
    value = segment.strip()
    padding = len(value) % 4
    if padding == 2:
        value += "=="
    elif padding == 3:
        value += "="
    elif padding == 1:
        value += "==="
    return base64.urlsafe_b64decode(value.encode("utf-8"))


def _parse_mapping_or_jwt(value: Any) -> dict[str, Any] | None:
    if isinstance(value, dict):
        return value

    raw = str(value or "").strip()
    if not raw:
        return None

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        payload = None
    if isinstance(payload, dict):
        return payload

    parts = raw.split(".")
    if len(parts) < 2:
        return None

    try:
        decoded = _decode_base64_url(parts[1])
        payload = json.loads(decoded)
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def extract_token_account_id_from_payload(payload: dict[str, Any]) -> str | None:
    def account_id_from_mapping(mapping: dict[str, Any]) -> str | None:
        direct = str(mapping.get("account_id") or mapping.get("chatgpt_account_id") or "").strip()
        if direct:
            return direct
        auth_payload = mapping.get("https://api.openai.com/auth")
        if isinstance(auth_payload, dict):
            nested = str(auth_payload.get("chatgpt_account_id") or auth_payload.get("account_id") or "").strip()
            if nested:
                return nested
        return None

    if not isinstance(payload, dict):
        return None

    direct = account_id_from_mapping(payload)
    if direct is not None:
        return direct

    id_token_payload = _parse_mapping_or_jwt(payload.get("id_token"))
    if isinstance(id_token_payload, dict):
        return account_id_from_mapping(id_token_payload)

    return None


def _extract_plan_type_from_payload(payload: dict[str, Any]) -> str | None:
    auth_payload = payload.get("https://api.openai.com/auth")
    if isinstance(auth_payload, dict):
        normalized = _normalize_plan_type(auth_payload.get("chatgpt_plan_type") or auth_payload.get("plan_type"))
        if normalized is not None:
            return normalized

    normalized = _normalize_plan_type(payload.get("chatgpt_plan_type") or payload.get("plan_type"))
    if normalized is not None:
        return normalized

    id_token_payload = _parse_mapping_or_jwt(payload.get("id_token"))
    if isinstance(id_token_payload, dict):
        auth_mapping = id_token_payload.get("https://api.openai.com/auth")
        if isinstance(auth_mapping, dict):
            normalized = _normalize_plan_type(
                auth_mapping.get("chatgpt_plan_type") or auth_mapping.get("plan_type")
            )
            if normalized is not None:
                return normalized
        return _normalize_plan_type(
            id_token_payload.get("chatgpt_plan_type") or id_token_payload.get("plan_type")
        )

    return None


def _is_merged_duplicate_error(message: str | None) -> bool:
    return str(message or "").startswith(MERGED_DUPLICATE_TOKEN_ERROR_PREFIX)


def _token_history_rank(token: CodexToken) -> tuple[float, ...]:
    return (
        _datetime_sort_value(token.last_refresh_at),
        _datetime_sort_value(token.updated_at),
        _datetime_sort_value(token.last_used_at),
        _datetime_sort_value(token.cooldown_until),
        1.0 if token.access_token else 0.0,
        _datetime_sort_value(token.expires_at),
        1.0 if token.is_active else 0.0,
        float(token.id),
    )


def _pick_canonical_token(tokens: list[CodexToken]) -> CodexToken:
    return max(tokens, key=_token_history_rank)


def _pick_latest_value(tokens: list[CodexToken], attribute: str) -> Any | None:
    for token in sorted(tokens, key=_token_history_rank, reverse=True):
        value = _non_empty_value(getattr(token, attribute, None))
        if value is not None:
            return value
    return None


def _pick_latest_non_merge_error(tokens: list[CodexToken]) -> str | None:
    best_error = None
    best_rank: tuple[float, float] | None = None
    for token in tokens:
        message = str(token.last_error or "").strip()
        if not message or _is_merged_duplicate_error(message):
            continue
        rank = (_datetime_sort_value(token.updated_at), float(token.id))
        if best_rank is None or rank > best_rank:
            best_rank = rank
            best_error = message
    return best_error


def _merge_shadow_error(canonical_id: int) -> str:
    return f"{MERGED_DUPLICATE_TOKEN_ERROR_PREFIX}{canonical_id} due to duplicate refresh token history"


def _merge_duplicate_token_rows(
    tokens: list[CodexToken],
    *,
    now: datetime | None = None,
) -> tuple[CodexToken, list[CodexToken]]:
    if not tokens:
        raise ValueError("Expected at least one token row to merge")

    merged_at = now or utcnow()
    canonical = _pick_canonical_token(tokens)
    shadows = [token for token in tokens if token.id != canonical.id]
    merged_aliases: list[str] = []
    for token in tokens:
        merged_aliases = merge_refresh_token_aliases(merged_aliases, *_token_aliases(token))

    canonical.refresh_token_aliases = merged_aliases
    canonical.merged_into_token_id = None
    canonical.email = canonical.email or _pick_latest_value(tokens, "email")
    canonical.account_id = canonical.account_id or _pick_latest_value(tokens, "account_id")
    canonical.id_token = canonical.id_token or _pick_latest_value(tokens, "id_token")
    canonical.source_file = canonical.source_file or _pick_latest_value(tokens, "source_file")
    canonical.recovery = canonical.recovery or _pick_latest_value(tokens, "recovery")
    canonical.raw_payload = canonical.raw_payload or _pick_latest_value(tokens, "raw_payload")
    canonical.token_type = canonical.token_type or _pick_latest_value(tokens, "token_type") or "codex"

    canonical.last_refresh_at = max(
        (token.last_refresh_at for token in tokens if token.last_refresh_at is not None),
        default=canonical.last_refresh_at,
    )
    canonical.last_used_at = max(
        (token.last_used_at for token in tokens if token.last_used_at is not None),
        default=canonical.last_used_at,
    )
    canonical.cooldown_until = (
        max((token.cooldown_until for token in tokens if token.cooldown_until is not None), default=None)
        if canonical.is_active
        else None
    )
    if canonical.access_token:
        canonical.expires_at = canonical.expires_at or max(
            (token.expires_at for token in tokens if token.expires_at is not None),
            default=canonical.expires_at,
        )
    else:
        canonical.expires_at = max(
            (token.expires_at for token in tokens if token.expires_at is not None),
            default=canonical.expires_at,
        )
    latest_error = _pick_latest_non_merge_error(tokens)
    if latest_error is not None or _is_merged_duplicate_error(canonical.last_error):
        canonical.last_error = latest_error
    canonical.updated_at = merged_at

    for shadow in shadows:
        shadow.merged_into_token_id = canonical.id
        shadow.is_active = False
        shadow.access_token = None
        shadow.expires_at = None
        shadow.cooldown_until = None
        shadow.last_error = _merge_shadow_error(canonical.id)
        shadow.updated_at = merged_at

    return canonical, shadows


def parse_rfc3339(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def parse_token_selection_strategy(value: Any) -> str:
    normalized = str(value or "").strip().lower().replace("-", "_")
    if normalized in {"oldest_unused", "least_recently_used", "lru"}:
        return TOKEN_SELECTION_STRATEGY_LEAST_RECENTLY_USED
    if normalized in {"fill_first", "fillfirst"}:
        return TOKEN_SELECTION_STRATEGY_FILL_FIRST
    raise ValueError(
        "Unsupported token selection strategy; expected one of: least_recently_used, fill_first"
    )


def normalize_token_selection_strategy(value: Any) -> str:
    try:
        return parse_token_selection_strategy(value)
    except ValueError:
        return DEFAULT_TOKEN_SELECTION_STRATEGY


def parse_token_active_stream_cap(value: Any) -> int:
    try:
        cap = int(value)
    except (TypeError, ValueError):
        raise ValueError(
            f"Unsupported token concurrency cap; expected an integer from {MIN_TOKEN_ACTIVE_STREAM_CAP} to {MAX_TOKEN_ACTIVE_STREAM_CAP}"
        ) from None
    if cap < MIN_TOKEN_ACTIVE_STREAM_CAP or cap > MAX_TOKEN_ACTIVE_STREAM_CAP:
        raise ValueError(
            f"Unsupported token concurrency cap; expected an integer from {MIN_TOKEN_ACTIVE_STREAM_CAP} to {MAX_TOKEN_ACTIVE_STREAM_CAP}"
        )
    return cap


def normalize_token_active_stream_cap(value: Any, *, default: int | None = None) -> int:
    try:
        return parse_token_active_stream_cap(value)
    except ValueError:
        return DEFAULT_TOKEN_ACTIVE_STREAM_CAP if default is None else default


def default_token_active_stream_cap() -> int:
    raw = str(os.getenv("FILL_FIRST_TOKEN_ACTIVE_STREAM_CAP") or "").strip()
    if not raw:
        return DEFAULT_TOKEN_ACTIVE_STREAM_CAP
    return normalize_token_active_stream_cap(raw)


def normalize_token_selection_order(value: Any) -> tuple[int, ...]:
    if not isinstance(value, (list, tuple)):
        return ()

    seen: set[int] = set()
    token_order: list[int] = []
    for item in value:
        try:
            token_id = int(item)
        except (TypeError, ValueError):
            continue
        if token_id <= 0 or token_id in seen:
            continue
        seen.add(token_id)
        token_order.append(token_id)
    return tuple(token_order)


def normalize_token_plan_order_enabled(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on", "enabled", "custom"}:
            return True
        if normalized in {"0", "false", "no", "off", "disabled", "default"}:
            return False
    return bool(value)


def normalize_token_plan_order(value: Any) -> tuple[str, ...]:
    plan_order: list[str] = []
    seen: set[str] = set()
    source = value if isinstance(value, (list, tuple)) else ()
    allowed = set(DEFAULT_TOKEN_PLAN_ORDER)

    for item in source:
        normalized = _normalize_plan_type(item)
        if normalized not in allowed or normalized in seen:
            continue
        seen.add(normalized)
        plan_order.append(normalized)

    for plan_type in DEFAULT_TOKEN_PLAN_ORDER:
        if plan_type in seen:
            continue
        plan_order.append(plan_type)

    return tuple(plan_order)


def normalize_token_list_status(value: Any) -> str:
    normalized = str(value or "").strip().lower().replace("-", "_")
    return normalized if normalized in TOKEN_LIST_STATUSES else TOKEN_LIST_STATUS_ALL


def normalize_token_list_plan_type(value: Any) -> str:
    normalized = str(value or "").strip().lower().replace("-", "_")
    if not normalized or normalized == TOKEN_LIST_PLAN_ALL:
        return TOKEN_LIST_PLAN_ALL
    if normalized in {TOKEN_LIST_PLAN_UNKNOWN, "unrecognized"}:
        return TOKEN_LIST_PLAN_UNKNOWN
    plan_type = _normalize_plan_type(normalized)
    return plan_type or TOKEN_LIST_PLAN_ALL


def normalize_token_list_sort(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    return normalized if normalized in TOKEN_LIST_SORTS else TOKEN_LIST_SORT_NEWEST


def parse_token_import_queue_position(value: Any) -> str:
    normalized = str(value or "").strip().lower().replace("-", "_")
    if normalized in {"front", "head", "start", "first", "prepend"}:
        return TOKEN_IMPORT_QUEUE_POSITION_FRONT
    if normalized in {"back", "tail", "end", "last", "append"}:
        return TOKEN_IMPORT_QUEUE_POSITION_BACK
    raise ValueError("Unsupported import queue position; expected one of: front, back")


def normalize_token_import_queue_position(value: Any) -> str:
    try:
        return parse_token_import_queue_position(value)
    except ValueError:
        return DEFAULT_TOKEN_IMPORT_QUEUE_POSITION


def merge_imported_token_order(
    current_token_ids: Iterable[int],
    imported_token_ids: Iterable[int],
    *,
    position: str,
) -> tuple[int, ...]:
    current_order = normalize_token_selection_order(list(current_token_ids))
    imported_order = normalize_token_selection_order(list(imported_token_ids))
    if not imported_order:
        return current_order

    imported_set = set(imported_order)
    remaining_order = [token_id for token_id in current_order if token_id not in imported_set]
    resolved_position = parse_token_import_queue_position(position)
    if resolved_position == TOKEN_IMPORT_QUEUE_POSITION_BACK:
        return tuple([*remaining_order, *imported_order])
    return tuple([*imported_order, *remaining_order])


def _load_token_payload(token_data_or_json: str | dict[str, Any]) -> dict[str, Any]:
    if isinstance(token_data_or_json, dict):
        payload = token_data_or_json
    else:
        payload = json.loads(token_data_or_json)
    if not isinstance(payload, dict):
        raise ValueError("Token payload must be a JSON object")
    refresh_token = str(payload.get("refresh_token") or "").strip()
    if not refresh_token:
        raise ValueError("Token payload missing refresh_token")
    return payload


def _available_token_filters(now: datetime, *, scoped_cooldown_scope: str | None = None) -> tuple[Any, ...]:
    filters: list[Any] = [
        *_canonical_token_filters(),
        CodexToken.is_active.is_(True),
        CodexToken.refresh_token.is_not(None),
        CodexToken.token_type == "codex",
        or_(CodexToken.cooldown_until.is_(None), CodexToken.cooldown_until <= now),
    ]
    if scoped_cooldown_scope:
        active_scoped_cooldown = (
            select(CodexTokenScopedCooldown.id)
            .where(
                CodexTokenScopedCooldown.token_id == CodexToken.id,
                CodexTokenScopedCooldown.scope == scoped_cooldown_scope,
                CodexTokenScopedCooldown.cooldown_until > now,
            )
            .exists()
        )
        filters.append(~active_scoped_cooldown)
    return tuple(filters)


def _token_available_condition(now: datetime) -> Any:
    return and_(
        CodexToken.is_active.is_(True),
        or_(CodexToken.cooldown_until.is_(None), CodexToken.cooldown_until <= now),
    )


def _token_cooling_condition(now: datetime) -> Any:
    return and_(
        CodexToken.is_active.is_(True),
        CodexToken.cooldown_until.is_not(None),
        CodexToken.cooldown_until > now,
    )


def _token_plan_value_expr() -> Any:
    return func.replace(func.lower(func.coalesce(CodexToken.plan_type, "")), "chatgpt_", "")


def _normalize_token_id_filter(token_ids: Iterable[int] | None) -> tuple[int, ...] | None:
    if token_ids is None:
        return None
    seen: set[int] = set()
    normalized: list[int] = []
    for value in token_ids:
        try:
            token_id = int(value)
        except (TypeError, ValueError):
            continue
        if token_id <= 0 or token_id in seen:
            continue
        seen.add(token_id)
        normalized.append(token_id)
    return tuple(normalized)


def _escape_sql_like(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _token_search_terms(search: str | None) -> tuple[str, ...]:
    return tuple(term for term in str(search or "").strip().lower().split() if term)


def _token_search_filters(search: str | None) -> tuple[Any, ...]:
    filters: list[Any] = []
    for term in _token_search_terms(search):
        pattern = f"%{_escape_sql_like(term)}%"
        haystacks = (
            cast(CodexToken.id, String),
            func.coalesce(CodexToken.email, ""),
            func.coalesce(CodexToken.account_id, ""),
            func.coalesce(CodexToken.source_file, ""),
            func.coalesce(CodexToken.plan_type, ""),
            func.coalesce(CodexToken.last_error, ""),
        )
        filters.append(
            or_(
                *(
                    func.lower(haystack).like(pattern, escape="\\")
                    for haystack in haystacks
                )
            )
        )
    return tuple(filters)


def _token_status_filter(status: str | None, now: datetime) -> Any | None:
    normalized = normalize_token_list_status(status)
    if normalized == TOKEN_LIST_STATUS_AVAILABLE:
        return _token_available_condition(now)
    if normalized == TOKEN_LIST_STATUS_COOLING:
        return _token_cooling_condition(now)
    if normalized == TOKEN_LIST_STATUS_DISABLED:
        return CodexToken.is_active.is_(False)
    return None


def _token_plan_filter(plan_type: str | None) -> Any | None:
    normalized = normalize_token_list_plan_type(plan_type)
    if normalized == TOKEN_LIST_PLAN_ALL:
        return None
    plan_value = _token_plan_value_expr()
    if normalized == TOKEN_LIST_PLAN_UNKNOWN:
        return or_(plan_value == "", ~plan_value.in_(DEFAULT_TOKEN_PLAN_ORDER))
    return plan_value == normalized


def _token_query_filters(
    now: datetime,
    *,
    search: str | None = None,
    status: str | None = TOKEN_LIST_STATUS_ALL,
    plan_type: str | None = TOKEN_LIST_PLAN_ALL,
    token_ids: Iterable[int] | None = None,
) -> tuple[Any, ...]:
    filters: list[Any] = [*_canonical_token_filters()]
    filters.extend(_token_search_filters(search))
    status_filter = _token_status_filter(status, now)
    if status_filter is not None:
        filters.append(status_filter)
    plan_filter = _token_plan_filter(plan_type)
    if plan_filter is not None:
        filters.append(plan_filter)

    resolved_token_ids = _normalize_token_id_filter(token_ids)
    if resolved_token_ids is not None:
        filters.append(CodexToken.id.in_(resolved_token_ids) if resolved_token_ids else text("false"))
    return tuple(filters)


def _token_list_order_clauses(sort: str | None, now: datetime) -> tuple[Any, ...]:
    normalized = normalize_token_list_sort(sort)
    plan_value = _token_plan_value_expr()

    if normalized == TOKEN_LIST_SORT_OLDEST:
        return (CodexToken.created_at.asc(), CodexToken.id.asc())
    if normalized == TOKEN_LIST_SORT_LAST_USED_DESC:
        return (nullslast(CodexToken.last_used_at.desc()), CodexToken.id.desc())
    if normalized == TOKEN_LIST_SORT_LAST_USED_ASC:
        return (nullsfirst(CodexToken.last_used_at.asc()), CodexToken.id.asc())
    if normalized == TOKEN_LIST_SORT_STATUS:
        return (
            case(
                (_token_available_condition(now), 0),
                (_token_cooling_condition(now), 1),
                (CodexToken.is_active.is_(False), 2),
                else_=3,
            ),
            CodexToken.id.desc(),
        )
    if normalized == TOKEN_LIST_SORT_PLAN:
        return (plan_value.asc(), CodexToken.id.desc())
    if normalized == TOKEN_LIST_SORT_ACCOUNT:
        return (
            func.lower(func.coalesce(CodexToken.email, CodexToken.account_id, "")).asc(),
            CodexToken.id.desc(),
        )
    return (CodexToken.created_at.desc(), CodexToken.id.desc())


def _token_selection_order_clauses(
    strategy: str | None,
    *,
    token_order: Iterable[int] | None = None,
    plan_order_enabled: bool = False,
    plan_order: Iterable[str] | None = None,
) -> tuple[Any, ...]:
    resolved = normalize_token_selection_strategy(strategy)
    plan_clauses: tuple[Any, ...] = ()
    if plan_order_enabled:
        resolved_plan_order = normalize_token_plan_order(list(plan_order or ()))
        plan_order_map = {plan_type: index for index, plan_type in enumerate(resolved_plan_order)}
        plan_value = func.replace(func.lower(func.coalesce(CodexToken.plan_type, "")), "chatgpt_", "")
        plan_clauses = (case(plan_order_map, value=plan_value, else_=len(plan_order_map)),)

    if resolved == TOKEN_SELECTION_STRATEGY_FILL_FIRST:
        ordered_token_ids = normalize_token_selection_order(list(token_order or ()))
        if ordered_token_ids:
            order_map = {token_id: index for index, token_id in enumerate(ordered_token_ids)}
            return (
                *plan_clauses,
                case(order_map, value=CodexToken.id, else_=len(order_map)),
                CodexToken.id.asc(),
            )
        return (*plan_clauses, CodexToken.id.asc())
    return (
        *plan_clauses,
        nullsfirst(CodexToken.last_used_at.asc()),
        CodexToken.updated_at.asc(),
        CodexToken.id.asc(),
    )


def _build_token_selection_settings(setting: GatewaySetting | None) -> TokenSelectionSettings:
    value = setting.value if setting is not None else None
    strategy = None
    token_order: tuple[int, ...] = ()
    if isinstance(value, dict):
        strategy = value.get("strategy")
        token_order = normalize_token_selection_order(value.get("token_order"))
        plan_order_enabled = normalize_token_plan_order_enabled(value.get("plan_order_enabled"))
        plan_order = normalize_token_plan_order(value.get("plan_order"))
        active_stream_cap = normalize_token_active_stream_cap(
            value.get("active_stream_cap"),
            default=default_token_active_stream_cap(),
        )
    else:
        plan_order_enabled = False
        plan_order = DEFAULT_TOKEN_PLAN_ORDER
        active_stream_cap = default_token_active_stream_cap()
    return TokenSelectionSettings(
        strategy=normalize_token_selection_strategy(strategy),
        token_order=token_order,
        plan_order_enabled=plan_order_enabled,
        plan_order=plan_order,
        active_stream_cap=active_stream_cap,
        updated_at=setting.updated_at if setting is not None else None,
    )


def _pick_preferred_token(tokens: list[CodexToken], *, account_id: str | None) -> CodexToken | None:
    if not tokens:
        return None
    if account_id:
        for token in tokens:
            if token.account_id == account_id:
                return token
    return max(tokens, key=lambda item: item.id)


def _token_aliases(token: CodexToken) -> list[str]:
    return collect_refresh_token_aliases(token.refresh_token_aliases, token.refresh_token, token.raw_payload)


def _normalize_token_account_ids(account_ids: list[str] | set[str] | tuple[str, ...]) -> list[str]:
    return sorted({str(value or "").strip() for value in account_ids if str(value or "").strip()})


async def _acquire_token_identity_locks(
    session,
    *,
    refresh_token: str,
    account_id: str | None,
) -> None:
    lock_keys = [f"{TOKEN_IDENTITY_LOCK_NAMESPACE}:refresh:{refresh_token}"]
    normalized_account_id = str(account_id or "").strip()
    if normalized_account_id:
        lock_keys.append(f"{TOKEN_IDENTITY_LOCK_NAMESPACE}:account:{normalized_account_id}")

    for lock_key in sorted(set(lock_keys)):
        await _acquire_postgres_transaction_lock(session, lock_key=lock_key)


async def _acquire_postgres_transaction_lock(session, *, lock_key: str) -> None:
    bind = session.get_bind()
    if bind is None or bind.dialect.name != "postgresql":
        return

    await session.execute(text("SELECT pg_advisory_xact_lock(hashtext(:key)::bigint)"), {"key": lock_key})


async def _acquire_token_history_repair_lock(session) -> None:
    await _acquire_postgres_transaction_lock(session, lock_key=TOKEN_HISTORY_REPAIR_LOCK_KEY)


async def _resolve_canonical_token_for_update(session, token_id: int) -> CodexToken | None:
    current_id: int | None = token_id
    visited: set[int] = set()
    while current_id is not None and current_id not in visited:
        visited.add(current_id)
        token = await session.get(CodexToken, current_id, with_for_update=True)
        if token is None:
            return None
        if token.merged_into_token_id is None:
            return token
        current_id = token.merged_into_token_id
    return None


async def _repair_duplicate_token_histories_in_session(session) -> TokenHistoryRepairSummary:
    await session.flush()
    result = await session.execute(
        select(CodexToken)
        .where(*_canonical_token_filters())
        .order_by(CodexToken.id.asc())
        .with_for_update()
    )
    tokens = result.scalars().all()
    duplicate_groups = [
        group
        for group in group_rows_by_refresh_token_history(tokens, alias_getter=_token_aliases)
        if len(group) > 1
    ]
    if not duplicate_groups:
        return TokenHistoryRepairSummary(duplicate_group_count=0, merged_row_count=0)

    canonical_ids: list[int] = []
    shadow_ids: list[int] = []
    merged_at = utcnow()
    for group in duplicate_groups:
        canonical, shadows = _merge_duplicate_token_rows(group, now=merged_at)
        canonical_ids.append(canonical.id)
        shadow_ids.extend(shadow.id for shadow in shadows)
    await session.flush()
    return TokenHistoryRepairSummary(
        duplicate_group_count=len(duplicate_groups),
        merged_row_count=len(shadow_ids),
        canonical_ids=tuple(canonical_ids),
        shadow_ids=tuple(sorted(shadow_ids)),
    )


async def _find_existing_token(
    session,
    *,
    refresh_token: str,
    account_id: str | None,
) -> CodexToken | None:
    exact_result = await session.execute(
        select(CodexToken)
        .where(*_canonical_token_filters(), CodexToken.refresh_token == refresh_token)
        .order_by(CodexToken.id.desc())
    )
    exact_matches = exact_result.scalars().all()
    token = _pick_preferred_token(exact_matches, account_id=account_id)
    if token is not None:
        return token

    canonical_result = await session.execute(
        select(CodexToken).where(*_canonical_token_filters()).order_by(CodexToken.id.desc())
    )
    canonical_tokens = canonical_result.scalars().all()
    matching_groups = [
        group
        for group in group_rows_by_refresh_token_history(canonical_tokens, alias_getter=_token_aliases)
        if refresh_token in {alias for token_row in group for alias in _token_aliases(token_row)}
    ]
    if not matching_groups:
        return None

    preferred_matches = [
        _pick_canonical_token(group)
        for group in matching_groups
        if account_id and any(token_row.account_id == account_id for token_row in group)
    ]
    if preferred_matches:
        return max(preferred_matches, key=lambda item: item.id)
    return max((_pick_canonical_token(group) for group in matching_groups), key=lambda item: item.id)


async def upsert_token_payload(
    token_data_or_json: str | dict[str, Any],
    *,
    source_file: str | None = None,
) -> TokenUpsertResult:
    payload = _load_token_payload(token_data_or_json)
    email = str(payload.get("email") or "").strip() or None
    account_id = extract_token_account_id_from_payload(payload)
    refresh_token = normalize_refresh_token(payload.get("refresh_token"))
    if refresh_token is None:
        raise ValueError("Token payload missing refresh_token")
    recovery = payload.get("recovery")
    if recovery is not None and not isinstance(recovery, dict):
        recovery = None

    async with get_session() as session:
        async with session.begin():
            await _acquire_token_identity_locks(session, refresh_token=refresh_token, account_id=account_id)
            token = await _find_existing_token(session, refresh_token=refresh_token, account_id=account_id)
            if token is None:
                token = CodexToken(
                    email=email,
                    account_id=account_id,
                    id_token=str(payload.get("id_token") or "").strip() or None,
                    access_token=str(payload.get("access_token") or "").strip() or None,
                    refresh_token=refresh_token,
                    refresh_token_aliases=[refresh_token],
                    merged_into_token_id=None,
                    token_type=str(payload.get("type") or "codex").strip() or "codex",
                    last_refresh_at=parse_rfc3339(payload.get("last_refresh")),
                    expires_at=parse_rfc3339(payload.get("expired")),
                    recovery=recovery,
                    raw_payload=payload,
                    plan_type=_extract_plan_type_from_payload(payload),
                    source_file=source_file,
                    is_active=True,
                    cooldown_until=None,
                    last_error=None,
                    updated_at=utcnow(),
                )
                session.add(token)
                await session.flush()
                invalidate_fill_first_token_cache()
                return TokenUpsertResult(token=token, action="created")

            token.email = token.email or email
            token.account_id = token.account_id or account_id
            token.token_type = str(payload.get("type") or token.token_type or "codex").strip() or token.token_type or "codex"
            token.source_file = source_file or token.source_file
            token.refresh_token_aliases = merge_refresh_token_aliases(_token_aliases(token), refresh_token)
            token.updated_at = utcnow()

            if refresh_token == normalize_refresh_token(token.refresh_token):
                token.id_token = str(payload.get("id_token") or "").strip() or None
                token.access_token = str(payload.get("access_token") or "").strip() or None
                token.last_refresh_at = parse_rfc3339(payload.get("last_refresh"))
                token.expires_at = parse_rfc3339(payload.get("expired"))
                token.recovery = recovery
                token.raw_payload = payload
                token.plan_type = _extract_plan_type_from_payload(payload) or token.plan_type
                token.is_active = True
                token.cooldown_until = None
                token.last_error = None
                await session.flush()
                invalidate_fill_first_token_cache()
                return TokenUpsertResult(token=token, action="updated")

            await session.flush()
            invalidate_fill_first_token_cache()
            return TokenUpsertResult(token=token, action="duplicate")


async def get_token_selection_settings() -> TokenSelectionSettings:
    async with get_read_session() as session:
        # Match the import-job read path and avoid AsyncSession.get() with autobegin disabled.
        result = await session.execute(
            select(GatewaySetting).where(GatewaySetting.key == TOKEN_SELECTION_SETTING_KEY).limit(1)
        )
        setting = result.scalars().first()
        return _build_token_selection_settings(setting)


async def update_token_selection_settings(*, strategy: str) -> TokenSelectionSettings:
    resolved_strategy = parse_token_selection_strategy(strategy)
    async with get_session() as session:
        async with session.begin():
            setting = await session.get(GatewaySetting, TOKEN_SELECTION_SETTING_KEY, with_for_update=True)
            if setting is None:
                setting = GatewaySetting(
                    key=TOKEN_SELECTION_SETTING_KEY,
                    value={
                        "strategy": resolved_strategy,
                        "token_order": [],
                        "plan_order_enabled": False,
                        "plan_order": list(DEFAULT_TOKEN_PLAN_ORDER),
                        "active_stream_cap": default_token_active_stream_cap(),
                    },
                    updated_at=utcnow(),
                )
                session.add(setting)
            else:
                payload = dict(setting.value) if isinstance(setting.value, dict) else {}
                payload["strategy"] = resolved_strategy
                payload["token_order"] = list(normalize_token_selection_order(payload.get("token_order")))
                payload["plan_order_enabled"] = normalize_token_plan_order_enabled(payload.get("plan_order_enabled"))
                payload["plan_order"] = list(normalize_token_plan_order(payload.get("plan_order")))
                payload["active_stream_cap"] = normalize_token_active_stream_cap(
                    payload.get("active_stream_cap"),
                    default=default_token_active_stream_cap(),
                )
                setting.value = payload
                setting.updated_at = utcnow()
            await session.flush()
            invalidate_fill_first_token_cache()
            return _build_token_selection_settings(setting)


async def update_token_order_settings(*, token_ids: Iterable[int]) -> TokenSelectionSettings:
    resolved_token_order = normalize_token_selection_order(list(token_ids))
    async with get_session() as session:
        async with session.begin():
            setting = await session.get(GatewaySetting, TOKEN_SELECTION_SETTING_KEY, with_for_update=True)
            if setting is None:
                setting = GatewaySetting(
                    key=TOKEN_SELECTION_SETTING_KEY,
                    value={
                        "strategy": DEFAULT_TOKEN_SELECTION_STRATEGY,
                        "token_order": list(resolved_token_order),
                        "plan_order_enabled": False,
                        "plan_order": list(DEFAULT_TOKEN_PLAN_ORDER),
                        "active_stream_cap": default_token_active_stream_cap(),
                    },
                    updated_at=utcnow(),
                )
                session.add(setting)
            else:
                payload = dict(setting.value) if isinstance(setting.value, dict) else {}
                payload["strategy"] = normalize_token_selection_strategy(payload.get("strategy"))
                payload["token_order"] = list(resolved_token_order)
                payload["plan_order_enabled"] = normalize_token_plan_order_enabled(payload.get("plan_order_enabled"))
                payload["plan_order"] = list(normalize_token_plan_order(payload.get("plan_order")))
                payload["active_stream_cap"] = normalize_token_active_stream_cap(
                    payload.get("active_stream_cap"),
                    default=default_token_active_stream_cap(),
                )
                setting.value = payload
                setting.updated_at = utcnow()
            await session.flush()
            invalidate_fill_first_token_cache()
            return _build_token_selection_settings(setting)


async def update_token_plan_order_settings(
    *,
    enabled: bool,
    plan_order: Iterable[str],
) -> TokenSelectionSettings:
    resolved_plan_order_enabled = bool(enabled)
    resolved_plan_order = normalize_token_plan_order(list(plan_order))
    async with get_session() as session:
        async with session.begin():
            setting = await session.get(GatewaySetting, TOKEN_SELECTION_SETTING_KEY, with_for_update=True)
            if setting is None:
                setting = GatewaySetting(
                    key=TOKEN_SELECTION_SETTING_KEY,
                    value={
                        "strategy": DEFAULT_TOKEN_SELECTION_STRATEGY,
                        "token_order": [],
                        "plan_order_enabled": resolved_plan_order_enabled,
                        "plan_order": list(resolved_plan_order),
                        "active_stream_cap": default_token_active_stream_cap(),
                    },
                    updated_at=utcnow(),
                )
                session.add(setting)
            else:
                payload = dict(setting.value) if isinstance(setting.value, dict) else {}
                payload["strategy"] = normalize_token_selection_strategy(payload.get("strategy"))
                payload["token_order"] = list(normalize_token_selection_order(payload.get("token_order")))
                payload["plan_order_enabled"] = resolved_plan_order_enabled
                payload["plan_order"] = list(resolved_plan_order)
                payload["active_stream_cap"] = normalize_token_active_stream_cap(
                    payload.get("active_stream_cap"),
                    default=default_token_active_stream_cap(),
                )
                setting.value = payload
                setting.updated_at = utcnow()
            await session.flush()
            invalidate_fill_first_token_cache()
            return _build_token_selection_settings(setting)


async def update_token_active_stream_cap_settings(*, active_stream_cap: int) -> TokenSelectionSettings:
    resolved_active_stream_cap = parse_token_active_stream_cap(active_stream_cap)
    async with get_session() as session:
        async with session.begin():
            setting = await session.get(GatewaySetting, TOKEN_SELECTION_SETTING_KEY, with_for_update=True)
            if setting is None:
                setting = GatewaySetting(
                    key=TOKEN_SELECTION_SETTING_KEY,
                    value={
                        "strategy": DEFAULT_TOKEN_SELECTION_STRATEGY,
                        "token_order": [],
                        "plan_order_enabled": False,
                        "plan_order": list(DEFAULT_TOKEN_PLAN_ORDER),
                        "active_stream_cap": resolved_active_stream_cap,
                    },
                    updated_at=utcnow(),
                )
                session.add(setting)
            else:
                payload = dict(setting.value) if isinstance(setting.value, dict) else {}
                payload["strategy"] = normalize_token_selection_strategy(payload.get("strategy"))
                payload["token_order"] = list(normalize_token_selection_order(payload.get("token_order")))
                payload["plan_order_enabled"] = normalize_token_plan_order_enabled(payload.get("plan_order_enabled"))
                payload["plan_order"] = list(normalize_token_plan_order(payload.get("plan_order")))
                payload["active_stream_cap"] = resolved_active_stream_cap
                setting.value = payload
                setting.updated_at = utcnow()
            await session.flush()
            invalidate_fill_first_token_cache()
            return _build_token_selection_settings(setting)


async def apply_imported_token_queue_position(
    token_ids: Iterable[int],
    *,
    position: str,
) -> TokenSelectionSettings:
    imported_token_ids = normalize_token_selection_order(list(token_ids))
    resolved_position = parse_token_import_queue_position(position)
    async with get_session() as session:
        async with session.begin():
            setting = await session.get(GatewaySetting, TOKEN_SELECTION_SETTING_KEY, with_for_update=True)
            existing_payload = dict(setting.value) if setting is not None and isinstance(setting.value, dict) else {}
            existing_order = normalize_token_selection_order(existing_payload.get("token_order"))
            current_result = await session.execute(
                select(CodexToken.id)
                .where(*_canonical_token_filters())
                .order_by(*_token_selection_order_clauses(TOKEN_SELECTION_STRATEGY_FILL_FIRST, token_order=existing_order))
            )
            current_token_ids = [int(token_id) for token_id in current_result.scalars().all()]
            next_order = merge_imported_token_order(
                current_token_ids,
                imported_token_ids,
                position=resolved_position,
            )
            if setting is None:
                setting = GatewaySetting(
                    key=TOKEN_SELECTION_SETTING_KEY,
                    value={
                        "strategy": DEFAULT_TOKEN_SELECTION_STRATEGY,
                        "token_order": list(next_order),
                        "plan_order_enabled": False,
                        "plan_order": list(DEFAULT_TOKEN_PLAN_ORDER),
                        "active_stream_cap": default_token_active_stream_cap(),
                    },
                    updated_at=utcnow(),
                )
                session.add(setting)
            else:
                existing_payload["strategy"] = normalize_token_selection_strategy(existing_payload.get("strategy"))
                existing_payload["token_order"] = list(next_order)
                existing_payload["plan_order_enabled"] = normalize_token_plan_order_enabled(
                    existing_payload.get("plan_order_enabled")
                )
                existing_payload["plan_order"] = list(normalize_token_plan_order(existing_payload.get("plan_order")))
                existing_payload["active_stream_cap"] = normalize_token_active_stream_cap(
                    existing_payload.get("active_stream_cap"),
                    default=default_token_active_stream_cap(),
                )
                setting.value = existing_payload
                setting.updated_at = utcnow()
            await session.flush()
            invalidate_fill_first_token_cache()
            return _build_token_selection_settings(setting)


async def repair_duplicate_token_histories() -> TokenHistoryRepairSummary:
    async with get_session() as session:
        async with session.begin():
            await _acquire_token_history_repair_lock(session)
            return await _repair_duplicate_token_histories_in_session(session)


async def _load_fill_first_available_tokens(
    *,
    token_order: tuple[int, ...],
    plan_order_enabled: bool,
    plan_order: tuple[str, ...],
    scoped_cooldown_scope: str | None,
) -> tuple[CodexToken, ...]:
    now = utcnow()
    stmt = (
        select(CodexToken)
        .where(*_available_token_filters(now, scoped_cooldown_scope=scoped_cooldown_scope))
        .order_by(
            *_token_selection_order_clauses(
                TOKEN_SELECTION_STRATEGY_FILL_FIRST,
                token_order=token_order,
                plan_order_enabled=plan_order_enabled,
                plan_order=plan_order,
            )
        )
    )
    async with get_read_session() as session:
        result = await session.execute(stmt)
        scalars = result.scalars()
        all_method = getattr(scalars, "all", None)
        if callable(all_method):
            return tuple(all_method())
        first = scalars.first()
        return (first,) if first is not None else ()


async def _refresh_fill_first_token_cache(
    cache_key: _FillFirstCacheKey,
    *,
    token_order: tuple[int, ...],
    plan_order_enabled: bool,
    plan_order: tuple[str, ...],
    scoped_cooldown_scope: str | None,
    generation: int,
) -> tuple[CodexToken, ...]:
    tokens = await _load_fill_first_available_tokens(
        token_order=token_order,
        plan_order_enabled=plan_order_enabled,
        plan_order=plan_order,
        scoped_cooldown_scope=scoped_cooldown_scope,
    )
    if generation == _FILL_FIRST_TOKEN_CACHE_GENERATION:
        now = time.monotonic()
        _FILL_FIRST_TOKEN_CACHE[cache_key] = _FillFirstTokenCacheEntry(
            expires_at=now + _fill_first_token_cache_ttl_seconds(),
            stale_until=now + _fill_first_token_stale_ttl_seconds(),
            tokens=tokens,
        )
    return tokens


def _start_fill_first_token_cache_refresh(
    cache_key: _FillFirstCacheKey,
    *,
    token_order: tuple[int, ...],
    plan_order_enabled: bool,
    plan_order: tuple[str, ...],
    scoped_cooldown_scope: str | None,
) -> asyncio.Task[tuple[CodexToken, ...]]:
    loop = asyncio.get_running_loop()
    task = _FILL_FIRST_TOKEN_REFRESH_TASKS.get(cache_key)
    if (
        task is not None
        and not task.done()
        and task.get_loop() is loop
        and _FILL_FIRST_TOKEN_REFRESH_GENERATIONS.get(cache_key) == _FILL_FIRST_TOKEN_CACHE_GENERATION
    ):
        return task

    generation = _FILL_FIRST_TOKEN_CACHE_GENERATION
    task = asyncio.create_task(
        _refresh_fill_first_token_cache(
            cache_key,
            token_order=token_order,
            plan_order_enabled=plan_order_enabled,
            plan_order=plan_order,
            scoped_cooldown_scope=scoped_cooldown_scope,
            generation=generation,
        ),
        name="oaix-fill-first-token-cache-refresh",
    )
    _FILL_FIRST_TOKEN_REFRESH_TASKS[cache_key] = task
    _FILL_FIRST_TOKEN_REFRESH_GENERATIONS[cache_key] = generation

    def _on_done(done_task: asyncio.Task[tuple[CodexToken, ...]]) -> None:
        if _FILL_FIRST_TOKEN_REFRESH_TASKS.get(cache_key) is done_task:
            _FILL_FIRST_TOKEN_REFRESH_TASKS.pop(cache_key, None)
            _FILL_FIRST_TOKEN_REFRESH_GENERATIONS.pop(cache_key, None)
        if done_task.cancelled():
            return
        try:
            done_task.result()
        except Exception:
            logger.exception("Failed to refresh fill_first token cache")

    task.add_done_callback(_on_done)
    return task


async def _await_fill_first_token_cache_refresh(
    cache_key: _FillFirstCacheKey,
    *,
    token_order: tuple[int, ...],
    plan_order_enabled: bool,
    plan_order: tuple[str, ...],
    scoped_cooldown_scope: str | None,
) -> tuple[CodexToken, ...]:
    task = _start_fill_first_token_cache_refresh(
        cache_key,
        token_order=token_order,
        plan_order_enabled=plan_order_enabled,
        plan_order=plan_order,
        scoped_cooldown_scope=scoped_cooldown_scope,
    )
    try:
        return await asyncio.shield(task)
    finally:
        if _FILL_FIRST_TOKEN_REFRESH_TASKS.get(cache_key) is task and task.done():
            _FILL_FIRST_TOKEN_REFRESH_TASKS.pop(cache_key, None)
            _FILL_FIRST_TOKEN_REFRESH_GENERATIONS.pop(cache_key, None)


def _select_fill_first_token_from_cached_entry(
    cached: _FillFirstTokenCacheEntry,
    *,
    exclude_token_ids: tuple[int, ...],
    scoped_cooldown_scope: str | None,
) -> CodexToken | None:
    excluded = set(exclude_token_ids)
    now = utcnow()
    for token in cached.tokens:
        if token.id in excluded:
            continue
        if not _token_is_runtime_available(token, now=now, scoped_cooldown_scope=scoped_cooldown_scope):
            continue
        return token
    return None


def _fill_first_token_cache_key(
    *,
    token_order: tuple[int, ...],
    plan_order_enabled: bool,
    plan_order: tuple[str, ...],
    scoped_cooldown_scope: str | None,
) -> _FillFirstCacheKey:
    return (
        token_order,
        bool(plan_order_enabled),
        normalize_token_plan_order(plan_order) if plan_order_enabled else (),
        scoped_cooldown_scope,
    )


async def _claim_fill_first_token_from_cache(
    *,
    exclude_token_ids: tuple[int, ...],
    token_order: tuple[int, ...],
    plan_order_enabled: bool,
    plan_order: tuple[str, ...],
    scoped_cooldown_scope: str | None,
) -> CodexToken | None:
    cache_key = _fill_first_token_cache_key(
        token_order=token_order,
        plan_order_enabled=plan_order_enabled,
        plan_order=plan_order,
        scoped_cooldown_scope=scoped_cooldown_scope,
    )
    now = time.monotonic()
    cached = _FILL_FIRST_TOKEN_CACHE.get(cache_key)
    if cached is not None and cached.expires_at > now:
        return _select_fill_first_token_from_cached_entry(
            cached,
            exclude_token_ids=exclude_token_ids,
            scoped_cooldown_scope=scoped_cooldown_scope,
        )

    if cached is not None:
        _start_fill_first_token_cache_refresh(
            cache_key,
            token_order=token_order,
            plan_order_enabled=plan_order_enabled,
            plan_order=plan_order,
            scoped_cooldown_scope=scoped_cooldown_scope,
        )
        return _select_fill_first_token_from_cached_entry(
            cached,
            exclude_token_ids=exclude_token_ids,
            scoped_cooldown_scope=scoped_cooldown_scope,
        )

    tokens = await _await_fill_first_token_cache_refresh(
        cache_key,
        token_order=token_order,
        plan_order_enabled=plan_order_enabled,
        plan_order=plan_order,
        scoped_cooldown_scope=scoped_cooldown_scope,
    )
    cached = _FILL_FIRST_TOKEN_CACHE.get(cache_key) or _FillFirstTokenCacheEntry(
        expires_at=0.0,
        stale_until=0.0,
        tokens=tokens,
    )
    return _select_fill_first_token_from_cached_entry(
        cached,
        exclude_token_ids=exclude_token_ids,
        scoped_cooldown_scope=scoped_cooldown_scope,
    )


async def prewarm_fill_first_token_cache(
    selection_settings: TokenSelectionSettings,
    *,
    scoped_cooldown_scope: str | None = None,
) -> None:
    if selection_settings.strategy != TOKEN_SELECTION_STRATEGY_FILL_FIRST:
        return
    resolved_token_order = tuple(selection_settings.token_order)
    resolved_plan_order_enabled = bool(selection_settings.plan_order_enabled)
    resolved_plan_order = normalize_token_plan_order(selection_settings.plan_order)
    await _await_fill_first_token_cache_refresh(
        _fill_first_token_cache_key(
            token_order=resolved_token_order,
            plan_order_enabled=resolved_plan_order_enabled,
            plan_order=resolved_plan_order,
            scoped_cooldown_scope=scoped_cooldown_scope,
        ),
        token_order=resolved_token_order,
        plan_order_enabled=resolved_plan_order_enabled,
        plan_order=resolved_plan_order,
        scoped_cooldown_scope=scoped_cooldown_scope,
    )


async def claim_next_active_token(
    *,
    selection_strategy: str | None = None,
    exclude_token_ids: Iterable[int] | None = None,
    token_order: Iterable[int] | None = None,
    plan_order_enabled: bool | None = None,
    plan_order: Iterable[str] | None = None,
    scoped_cooldown_scope: str | None = None,
) -> CodexToken | None:
    now = utcnow()
    excluded_ids = tuple(sorted({int(token_id) for token_id in (exclude_token_ids or ())}))
    resolved_strategy = normalize_token_selection_strategy(selection_strategy)
    resolved_plan_order_enabled = normalize_token_plan_order_enabled(plan_order_enabled)
    resolved_plan_order = normalize_token_plan_order(list(plan_order or ()))
    if resolved_strategy == TOKEN_SELECTION_STRATEGY_FILL_FIRST:
        resolved_token_order = tuple(token_order) if token_order is not None else None
        if resolved_token_order is None:
            async with get_read_session() as session:
                setting_result = await session.execute(
                    select(GatewaySetting).where(GatewaySetting.key == TOKEN_SELECTION_SETTING_KEY).limit(1)
                )
                settings = _build_token_selection_settings(setting_result.scalars().first())
                resolved_token_order = settings.token_order
                if plan_order_enabled is None:
                    resolved_plan_order_enabled = settings.plan_order_enabled
                if plan_order is None:
                    resolved_plan_order = settings.plan_order
        return await _claim_fill_first_token_from_cache(
            exclude_token_ids=excluded_ids,
            token_order=resolved_token_order,
            plan_order_enabled=resolved_plan_order_enabled,
            plan_order=resolved_plan_order,
            scoped_cooldown_scope=scoped_cooldown_scope,
        )

    async with get_session() as session:
        async with session.begin():
            stmt = (
                select(CodexToken)
                .where(*_available_token_filters(now, scoped_cooldown_scope=scoped_cooldown_scope))
                .order_by(
                    *_token_selection_order_clauses(
                        resolved_strategy,
                        plan_order_enabled=resolved_plan_order_enabled,
                        plan_order=resolved_plan_order,
                    )
                )
                .limit(1)
                .with_for_update(skip_locked=True)
            )
            if excluded_ids:
                stmt = stmt.where(CodexToken.id.notin_(excluded_ids))
            runtime_unavailable_ids = _runtime_unavailable_token_ids(
                now=now,
                scoped_cooldown_scope=scoped_cooldown_scope,
            )
            if runtime_unavailable_ids:
                stmt = stmt.where(CodexToken.id.notin_(runtime_unavailable_ids))
            result = await session.execute(stmt)
            token = result.scalars().first()
            if token is None:
                return None
            token.last_used_at = utcnow()
            token.updated_at = utcnow()
            await session.flush()
            return token


async def update_token_refresh_state(
    token_id: int,
    *,
    access_token: str,
    refresh_token: str | None,
    expires_at: datetime | None,
    id_token: str | None = None,
    account_id: str | None = None,
    email: str | None = None,
    plan_type: str | None = None,
    reactivate: bool = True,
) -> None:
    async with get_session() as session:
        async with session.begin():
            current_id: int | None = int(token_id)
            visited: set[int] = set()
            token: CodexToken | None = None
            while current_id is not None and current_id not in visited:
                visited.add(current_id)
                token = await session.get(CodexToken, current_id)
                if token is None:
                    return
                if token.merged_into_token_id is None:
                    break
                current_id = token.merged_into_token_id
            if token is None or token.merged_into_token_id is not None:
                return
            previous_refresh_token = token.refresh_token
            token.access_token = access_token
            normalized_id_token = str(id_token or "").strip() or None
            normalized_account_id = str(account_id or "").strip() or None
            normalized_email = str(email or "").strip() or None
            normalized_plan_type = _normalize_plan_type(plan_type)
            if normalized_id_token:
                token.id_token = normalized_id_token
            if normalized_account_id and not token.account_id:
                token.account_id = normalized_account_id
            if normalized_email and not token.email:
                token.email = normalized_email
            if normalized_plan_type:
                token.plan_type = normalized_plan_type
            if refresh_token:
                token.refresh_token = refresh_token
            token.refresh_token_aliases = merge_refresh_token_aliases(
                token.refresh_token_aliases,
                previous_refresh_token,
                refresh_token,
            )
            token.last_refresh_at = utcnow()
            token.expires_at = expires_at
            if reactivate:
                token.is_active = True
                token.cooldown_until = None
                token.last_error = None
                _TOKEN_STATUS_WRITE_PENDING.pop((int(token.id), "token"), None)
                _apply_token_runtime_success_state(token.id)
            token.updated_at = utcnow()
            invalidate_fill_first_token_cache()


async def update_token_plan_type(token_id: int, *, plan_type: str | None) -> None:
    normalized_plan_type = _normalize_plan_type(plan_type)
    if normalized_plan_type is None:
        return

    async with get_session() as session:
        async with session.begin():
            token = await _resolve_canonical_token_for_update(session, token_id)
            if token is None:
                return
            if token.plan_type == normalized_plan_type:
                return
            token.plan_type = normalized_plan_type
            token.updated_at = utcnow()
            invalidate_fill_first_token_cache()


async def mark_token_success(token_id: int) -> None:
    _apply_token_runtime_success_state(token_id)
    _enqueue_token_status_write(_TokenStatusWriteJob(token_id=int(token_id), kind="success"))


async def set_token_active_state(
    token_id: int,
    *,
    active: bool,
    clear_cooldown: bool = False,
) -> CodexToken | None:
    async with get_session() as session:
        async with session.begin():
            token = await _resolve_canonical_token_for_update(session, token_id)
            if token is None:
                return None
            token.is_active = bool(active)
            if not active or clear_cooldown:
                token.cooldown_until = None
            token.updated_at = utcnow()
            await session.flush()
            if active and clear_cooldown:
                _apply_token_runtime_success_state(token.id)
            elif not active:
                _apply_token_runtime_error_state(
                    token.id,
                    message="Manually disabled",
                    deactivate=True,
                    cooldown_until=None,
                )
            invalidate_fill_first_token_cache()
            return token


async def delete_token(token_id: int) -> TokenDeleteResult | None:
    async with get_session() as session:
        async with session.begin():
            token = await _resolve_canonical_token_for_update(session, token_id)
            if token is None:
                return None

            canonical_id = token.id
            result = await session.execute(
                select(CodexToken.id)
                .where(
                    or_(
                        CodexToken.id == canonical_id,
                        CodexToken.merged_into_token_id == canonical_id,
                    )
                )
                .order_by(CodexToken.id.asc())
                .with_for_update()
            )
            deleted_ids = tuple(int(row_id) for row_id in result.scalars().all())
            if not deleted_ids:
                return None

            await session.execute(delete(CodexToken).where(CodexToken.id.in_(deleted_ids)))
            await session.flush()
            invalidate_fill_first_token_cache()
            return TokenDeleteResult(canonical_id=canonical_id, deleted_ids=deleted_ids)


async def mark_token_error(
    token_id: int,
    message: str,
    *,
    deactivate: bool = False,
    cooldown_seconds: int | None = None,
    clear_access_token: bool = False,
) -> None:
    cooldown_until = (
        utcnow() + timedelta(seconds=max(0, int(cooldown_seconds)))
        if cooldown_seconds is not None
        else None
    )
    _apply_token_runtime_error_state(
        token_id,
        message=message,
        deactivate=deactivate,
        cooldown_until=cooldown_until,
    )
    _enqueue_token_status_write(
        _TokenStatusWriteJob(
            token_id=int(token_id),
            kind="error",
            message=message[:4000],
            deactivate=deactivate,
            cooldown_until=cooldown_until,
            clear_access_token=clear_access_token,
        )
    )


async def mark_token_scoped_cooldown(
    token_id: int,
    scope: str,
    message: str,
    *,
    cooldown_seconds: int,
) -> None:
    normalized_scope = str(scope or "").strip()
    if not normalized_scope:
        return

    cooldown_until = utcnow() + timedelta(seconds=max(0, int(cooldown_seconds)))
    _apply_token_runtime_scoped_cooldown(
        token_id,
        scope=normalized_scope,
        message=message,
        cooldown_until=cooldown_until,
    )
    _enqueue_token_status_write(
        _TokenStatusWriteJob(
            token_id=int(token_id),
            kind="scoped_cooldown",
            message=message[:4000],
            cooldown_until=cooldown_until,
            scope=normalized_scope,
        )
    )


def _build_token_counts_stmt(now: datetime):
    available_condition = and_(
        CodexToken.is_active.is_(True),
        CodexToken.refresh_token.is_not(None),
        CodexToken.token_type == "codex",
        or_(CodexToken.cooldown_until.is_(None), CodexToken.cooldown_until <= now),
    )
    cooling_condition = and_(
        CodexToken.is_active.is_(True),
        CodexToken.refresh_token.is_not(None),
        CodexToken.token_type == "codex",
        CodexToken.cooldown_until.is_not(None),
        CodexToken.cooldown_until > now,
    )
    return (
        select(
            func.count().label("total"),
            func.sum(case((CodexToken.is_active.is_(True), 1), else_=0)).label("active"),
            func.sum(case((available_condition, 1), else_=0)).label("available"),
            func.sum(case((cooling_condition, 1), else_=0)).label("cooling"),
            func.sum(case((CodexToken.is_active.is_(False), 1), else_=0)).label("disabled"),
        )
        .select_from(CodexToken)
        .where(*_canonical_token_filters())
    )


async def get_token_counts() -> TokenCounts:
    now = utcnow()
    async with get_read_session() as session:
        total, active, available, cooling, disabled = (await session.execute(_build_token_counts_stmt(now))).one()
        return TokenCounts(
            total=int(total or 0),
            active=int(active or 0),
            available=int(available or 0),
            cooling=int(cooling or 0),
            disabled=int(disabled or 0),
        )


async def get_token_status_counts(
    *,
    search: str | None = None,
    plan_type: str | None = TOKEN_LIST_PLAN_ALL,
    token_ids: Iterable[int] | None = None,
) -> TokenCounts:
    now = utcnow()
    filters = _token_query_filters(
        now,
        search=search,
        status=TOKEN_LIST_STATUS_ALL,
        plan_type=plan_type,
        token_ids=token_ids,
    )
    async with get_read_session() as session:
        total, active, available, cooling, disabled = (
            await session.execute(_build_token_counts_stmt(now).where(*filters))
        ).one()
        return TokenCounts(
            total=int(total or 0),
            active=int(active or 0),
            available=int(available or 0),
            cooling=int(cooling or 0),
            disabled=int(disabled or 0),
        )


async def get_token_plan_counts(
    *,
    search: str | None = None,
    status: str | None = TOKEN_LIST_STATUS_ALL,
    token_ids: Iterable[int] | None = None,
) -> TokenPlanCounts:
    now = utcnow()
    plan_value = _token_plan_value_expr()
    filters = _token_query_filters(
        now,
        search=search,
        status=status,
        plan_type=TOKEN_LIST_PLAN_ALL,
        token_ids=token_ids,
    )
    async with get_read_session() as session:
        result = await session.execute(
            select(plan_value.label("plan_type"), func.count().label("count"))
            .where(*filters)
            .group_by(plan_value)
        )
        rows = result.all()

    counts = {plan_type: 0 for plan_type in DEFAULT_TOKEN_PLAN_ORDER}
    counts[TOKEN_LIST_PLAN_UNKNOWN] = 0
    for raw_plan_type, raw_count in rows:
        plan_type = normalize_token_list_plan_type(raw_plan_type)
        if plan_type not in DEFAULT_TOKEN_PLAN_ORDER:
            plan_type = TOKEN_LIST_PLAN_UNKNOWN
        counts[plan_type] += int(raw_count or 0)

    return TokenPlanCounts(
        free=counts[TOKEN_PLAN_TYPE_FREE],
        plus=counts[TOKEN_PLAN_TYPE_PLUS],
        team=counts[TOKEN_PLAN_TYPE_TEAM],
        pro=counts[TOKEN_PLAN_TYPE_PRO],
        unknown=counts[TOKEN_LIST_PLAN_UNKNOWN],
    )


async def get_token_counts_by_account_ids(account_ids: list[str] | set[str] | tuple[str, ...]) -> dict[str, int]:
    resolved_account_ids = _normalize_token_account_ids(account_ids)
    if not resolved_account_ids:
        return {}

    async with get_read_session() as session:
        result = await session.execute(
            select(CodexToken.account_id, func.count())
            .where(
                *_canonical_token_filters(),
                CodexToken.account_id.in_(resolved_account_ids),
            )
            .group_by(CodexToken.account_id)
        )
        rows = result.all()
    return {
        str(account_id): int(count or 0)
        for account_id, count in rows
        if str(account_id or "").strip()
    }


async def count_token_rows(
    *,
    search: str | None = None,
    status: str | None = TOKEN_LIST_STATUS_ALL,
    plan_type: str | None = TOKEN_LIST_PLAN_ALL,
    token_ids: Iterable[int] | None = None,
) -> int:
    now = utcnow()
    async with get_read_session() as session:
        result = await session.execute(
            select(func.count())
            .select_from(CodexToken)
            .where(
                *_token_query_filters(
                    now,
                    search=search,
                    status=status,
                    plan_type=plan_type,
                    token_ids=token_ids,
                )
            )
        )
        return int(result.scalar_one() or 0)


async def list_token_rows(
    limit: int = 100,
    offset: int = 0,
    *,
    search: str | None = None,
    status: str | None = TOKEN_LIST_STATUS_ALL,
    plan_type: str | None = TOKEN_LIST_PLAN_ALL,
    sort: str | None = TOKEN_LIST_SORT_NEWEST,
    token_ids: Iterable[int] | None = None,
) -> list[CodexToken]:
    now = utcnow()
    async with get_read_session() as session:
        stmt = (
            select(CodexToken)
            .where(
                *_token_query_filters(
                    now,
                    search=search,
                    status=status,
                    plan_type=plan_type,
                    token_ids=token_ids,
                )
            )
            .order_by(*_token_list_order_clauses(sort, now))
            .limit(max(1, min(limit, 500)))
            .offset(max(0, int(offset)))
        )
        result = await session.execute(stmt)
        return result.scalars().all()


async def get_token_row(token_id: int) -> CodexToken | None:
    async with get_read_session() as session:
        current_id: int | None = int(token_id)
        visited: set[int] = set()
        while current_id is not None and current_id not in visited:
            visited.add(current_id)
            token = await session.get(CodexToken, current_id)
            if token is None:
                return None
            if token.merged_into_token_id is None:
                return token
            current_id = token.merged_into_token_id
    return None


async def list_tokens(limit: int = 100, offset: int = 0) -> list[TokenStatus]:
    tokens = await list_token_rows(limit=limit, offset=offset)
    return [
        TokenStatus(
            id=token.id,
            email=token.email,
            account_id=token.account_id,
            is_active=token.is_active,
            cooldown_until=token.cooldown_until,
            last_refresh_at=token.last_refresh_at,
            expires_at=token.expires_at,
            last_used_at=token.last_used_at,
            last_error=token.last_error,
            source_file=token.source_file,
            updated_at=token.updated_at,
        )
        for token in tokens
    ]


async def import_token_files(patterns: Iterable[str]) -> TokenFileImportSummary:
    await init_db()

    created = 0
    updated = 0
    skipped = 0
    failed = 0
    seen_paths: set[str] = set()
    for pattern in patterns:
        for matched in glob.glob(pattern):
            resolved = str(Path(matched).resolve())
            if resolved in seen_paths:
                continue
            seen_paths.add(resolved)
            try:
                with open(resolved, "r", encoding="utf-8") as f:
                    payload = json.load(f)
                result = await upsert_token_payload(payload, source_file=resolved)
                if result.action == "created":
                    created += 1
                elif result.action == "updated":
                    updated += 1
                else:
                    skipped += 1
            except Exception:
                failed += 1
    return TokenFileImportSummary(
        created_count=created,
        updated_count=updated,
        skipped_count=skipped,
        failed_count=failed,
    )


async def save_token_json(token_json: str, *, source_file: str | None = None) -> StoredTokenSummary:
    await init_db()
    result = await upsert_token_payload(token_json, source_file=source_file)
    token = result.token
    return StoredTokenSummary(id=token.id, email=token.email, account_id=token.account_id)


def save_token_json_sync(token_json: str, *, source_file: str | None = None) -> StoredTokenSummary:
    async def _runner() -> StoredTokenSummary:
        try:
            return await save_token_json(token_json, source_file=source_file)
        finally:
            await close_database()

    return asyncio.run(_runner())


def import_token_files_sync(patterns: Iterable[str]) -> TokenFileImportSummary:
    async def _runner() -> TokenFileImportSummary:
        try:
            return await import_token_files(patterns)
        finally:
            await close_database()

    return asyncio.run(_runner())
