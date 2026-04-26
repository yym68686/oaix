import asyncio
import base64
import glob
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

from sqlalchemy import and_, case, delete, func, nullsfirst, or_, select, text

from .database import CodexToken, GatewaySetting, close_database, get_read_session, get_session, init_db, utcnow
from .token_identity import (
    collect_refresh_token_aliases,
    group_rows_by_refresh_token_history,
    merge_refresh_token_aliases,
    normalize_refresh_token,
)

TOKEN_SELECTION_STRATEGY_LEAST_RECENTLY_USED = "least_recently_used"
TOKEN_SELECTION_STRATEGY_FILL_FIRST = "fill_first"
DEFAULT_TOKEN_SELECTION_STRATEGY = TOKEN_SELECTION_STRATEGY_LEAST_RECENTLY_USED
TOKEN_SELECTION_SETTING_KEY = "token_selection"
MERGED_DUPLICATE_TOKEN_ERROR_PREFIX = "Merged into token #"
TOKEN_IDENTITY_LOCK_NAMESPACE = "codex-token-identity"
TOKEN_HISTORY_REPAIR_LOCK_KEY = "codex-token-history:repair"


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
    updated_at: datetime | None = None


@dataclass(frozen=True)
class TokenDeleteResult:
    canonical_id: int
    deleted_ids: tuple[int, ...]


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


def _available_token_filters(now: datetime) -> tuple[Any, ...]:
    return (
        *_canonical_token_filters(),
        CodexToken.is_active.is_(True),
        CodexToken.refresh_token.is_not(None),
        CodexToken.token_type == "codex",
        or_(CodexToken.cooldown_until.is_(None), CodexToken.cooldown_until <= now),
    )


def _token_selection_order_clauses(
    strategy: str | None,
    *,
    token_order: Iterable[int] | None = None,
) -> tuple[Any, ...]:
    resolved = normalize_token_selection_strategy(strategy)
    if resolved == TOKEN_SELECTION_STRATEGY_FILL_FIRST:
        ordered_token_ids = normalize_token_selection_order(list(token_order or ()))
        if ordered_token_ids:
            order_map = {token_id: index for index, token_id in enumerate(ordered_token_ids)}
            return (
                case(order_map, value=CodexToken.id, else_=len(order_map)),
                CodexToken.id.asc(),
            )
        return (CodexToken.id.asc(),)
    return (
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
    return TokenSelectionSettings(
        strategy=normalize_token_selection_strategy(strategy),
        token_order=token_order,
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
    account_id = str(payload.get("account_id") or "").strip() or None
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
                return TokenUpsertResult(token=token, action="updated")

            await session.flush()
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
                    value={"strategy": resolved_strategy, "token_order": []},
                    updated_at=utcnow(),
                )
                session.add(setting)
            else:
                payload = dict(setting.value) if isinstance(setting.value, dict) else {}
                payload["strategy"] = resolved_strategy
                payload["token_order"] = list(normalize_token_selection_order(payload.get("token_order")))
                setting.value = payload
                setting.updated_at = utcnow()
            await session.flush()
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
                    },
                    updated_at=utcnow(),
                )
                session.add(setting)
            else:
                payload = dict(setting.value) if isinstance(setting.value, dict) else {}
                payload["strategy"] = normalize_token_selection_strategy(payload.get("strategy"))
                payload["token_order"] = list(resolved_token_order)
                setting.value = payload
                setting.updated_at = utcnow()
            await session.flush()
            return _build_token_selection_settings(setting)


async def repair_duplicate_token_histories() -> TokenHistoryRepairSummary:
    async with get_session() as session:
        async with session.begin():
            await _acquire_token_history_repair_lock(session)
            return await _repair_duplicate_token_histories_in_session(session)


async def claim_next_active_token(
    *,
    selection_strategy: str | None = None,
    exclude_token_ids: Iterable[int] | None = None,
) -> CodexToken | None:
    now = utcnow()
    excluded_ids = tuple(sorted({int(token_id) for token_id in (exclude_token_ids or ())}))
    resolved_strategy = normalize_token_selection_strategy(selection_strategy)
    async with get_session() as session:
        async with session.begin():
            token_order: tuple[int, ...] = ()
            if resolved_strategy == TOKEN_SELECTION_STRATEGY_FILL_FIRST:
                setting_result = await session.execute(
                    select(GatewaySetting).where(GatewaySetting.key == TOKEN_SELECTION_SETTING_KEY).limit(1)
                )
                token_order = _build_token_selection_settings(setting_result.scalars().first()).token_order
            stmt = (
                select(CodexToken)
                .where(*_available_token_filters(now))
                .order_by(*_token_selection_order_clauses(selection_strategy, token_order=token_order))
                .limit(1)
                .with_for_update(skip_locked=True)
            )
            if excluded_ids:
                stmt = stmt.where(CodexToken.id.notin_(excluded_ids))
            result = await session.execute(stmt)
            token = result.scalars().first()
            if token is None:
                return None
            if resolved_strategy == TOKEN_SELECTION_STRATEGY_FILL_FIRST:
                return token
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
    reactivate: bool = True,
) -> None:
    async with get_session() as session:
        async with session.begin():
            # Refresh writes may already hold a token row lock; serialize the
            # duplicate-history repair scan first so concurrent refreshes do not
            # deadlock while escalating to a full canonical-token FOR UPDATE pass.
            await _acquire_token_history_repair_lock(session)
            token = await _resolve_canonical_token_for_update(session, token_id)
            if token is None:
                return
            previous_refresh_token = token.refresh_token
            token.access_token = access_token
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
            token.updated_at = utcnow()
            await _repair_duplicate_token_histories_in_session(session)


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


async def mark_token_success(token_id: int) -> None:
    async with get_session() as session:
        async with session.begin():
            current = await session.get(CodexToken, token_id)
            if current is None:
                return
            if (
                current.merged_into_token_id is None
                and current.is_active is True
                and current.cooldown_until is None
                and not current.last_error
            ):
                return
            token = await _resolve_canonical_token_for_update(session, token_id)
            if token is None:
                return
            if token.is_active is True and token.cooldown_until is None and not token.last_error:
                return
            token.is_active = True
            token.cooldown_until = None
            token.last_error = None
            token.updated_at = utcnow()


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
            return TokenDeleteResult(canonical_id=canonical_id, deleted_ids=deleted_ids)


async def mark_token_error(
    token_id: int,
    message: str,
    *,
    deactivate: bool = False,
    cooldown_seconds: int | None = None,
    clear_access_token: bool = False,
) -> None:
    async with get_session() as session:
        async with session.begin():
            token = await _resolve_canonical_token_for_update(session, token_id)
            if token is None:
                return
            token.last_error = message[:4000]
            token.updated_at = utcnow()
            if clear_access_token:
                token.access_token = None
                token.expires_at = None
            if deactivate:
                token.is_active = False
                token.cooldown_until = None
            elif cooldown_seconds is not None:
                token.is_active = True
                token.cooldown_until = utcnow() + timedelta(seconds=max(0, int(cooldown_seconds)))


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


async def list_token_rows(limit: int = 100) -> list[CodexToken]:
    async with get_read_session() as session:
        stmt = (
            select(CodexToken)
            .where(*_canonical_token_filters())
            .order_by(CodexToken.id.desc())
            .limit(max(1, min(limit, 500)))
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


async def list_tokens(limit: int = 100) -> list[TokenStatus]:
    tokens = await list_token_rows(limit=limit)
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
