import asyncio
import glob
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

from sqlalchemy import func, nullsfirst, or_, select

from .database import CodexToken, GatewaySetting, close_database, get_session, init_db, utcnow
from .token_identity import collect_refresh_token_aliases, merge_refresh_token_aliases, normalize_refresh_token

TOKEN_SELECTION_STRATEGY_LEAST_RECENTLY_USED = "least_recently_used"
TOKEN_SELECTION_STRATEGY_FILL_FIRST = "fill_first"
DEFAULT_TOKEN_SELECTION_STRATEGY = TOKEN_SELECTION_STRATEGY_LEAST_RECENTLY_USED
TOKEN_SELECTION_SETTING_KEY = "token_selection"


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
class TokenFileImportSummary:
    created_count: int
    updated_count: int
    skipped_count: int
    failed_count: int


@dataclass(frozen=True)
class TokenSelectionSettings:
    strategy: str
    updated_at: datetime | None = None


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
        CodexToken.is_active.is_(True),
        CodexToken.refresh_token.is_not(None),
        CodexToken.token_type == "codex",
        or_(CodexToken.cooldown_until.is_(None), CodexToken.cooldown_until <= now),
    )


def _token_selection_order_clauses(strategy: str | None) -> tuple[Any, ...]:
    resolved = normalize_token_selection_strategy(strategy)
    if resolved == TOKEN_SELECTION_STRATEGY_FILL_FIRST:
        return (CodexToken.id.asc(),)
    return (
        nullsfirst(CodexToken.last_used_at.asc()),
        CodexToken.updated_at.asc(),
        CodexToken.id.asc(),
    )


def _build_token_selection_settings(setting: GatewaySetting | None) -> TokenSelectionSettings:
    value = setting.value if setting is not None else None
    strategy = None
    if isinstance(value, dict):
        strategy = value.get("strategy")
    return TokenSelectionSettings(
        strategy=normalize_token_selection_strategy(strategy),
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


async def _find_existing_token(
    session,
    *,
    refresh_token: str,
    account_id: str | None,
) -> CodexToken | None:
    exact_result = await session.execute(
        select(CodexToken).where(CodexToken.refresh_token == refresh_token).order_by(CodexToken.id.desc())
    )
    exact_matches = exact_result.scalars().all()
    token = _pick_preferred_token(exact_matches, account_id=account_id)
    if token is not None:
        return token

    account_candidates: list[CodexToken] = []
    if account_id:
        account_result = await session.execute(
            select(CodexToken).where(CodexToken.account_id == account_id).order_by(CodexToken.id.desc())
        )
        account_candidates = account_result.scalars().all()
        for candidate in account_candidates:
            if refresh_token in _token_aliases(candidate):
                return candidate

    all_result = await session.execute(select(CodexToken).order_by(CodexToken.id.desc()))
    all_tokens = all_result.scalars().all()
    account_candidate_ids = {item.id for item in account_candidates}
    for candidate in all_tokens:
        if account_candidate_ids and candidate.id in account_candidate_ids:
            continue
        if refresh_token in _token_aliases(candidate):
            return candidate
    return None


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
            token = await _find_existing_token(session, refresh_token=refresh_token, account_id=account_id)
            if token is None:
                token = CodexToken(
                    email=email,
                    account_id=account_id,
                    id_token=str(payload.get("id_token") or "").strip() or None,
                    access_token=str(payload.get("access_token") or "").strip() or None,
                    refresh_token=refresh_token,
                    refresh_token_aliases=[refresh_token],
                    token_type=str(payload.get("type") or "codex").strip() or "codex",
                    last_refresh_at=parse_rfc3339(payload.get("last_refresh")),
                    expires_at=parse_rfc3339(payload.get("expired")),
                    recovery=recovery,
                    raw_payload=payload,
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
                token.is_active = True
                token.cooldown_until = None
                token.last_error = None
                await session.flush()
                return TokenUpsertResult(token=token, action="updated")

            await session.flush()
            return TokenUpsertResult(token=token, action="duplicate")


async def get_token_selection_settings() -> TokenSelectionSettings:
    async with get_session() as session:
        setting = await session.get(GatewaySetting, TOKEN_SELECTION_SETTING_KEY)
        return _build_token_selection_settings(setting)


async def update_token_selection_settings(*, strategy: str) -> TokenSelectionSettings:
    resolved_strategy = parse_token_selection_strategy(strategy)
    async with get_session() as session:
        async with session.begin():
            setting = await session.get(GatewaySetting, TOKEN_SELECTION_SETTING_KEY, with_for_update=True)
            if setting is None:
                setting = GatewaySetting(
                    key=TOKEN_SELECTION_SETTING_KEY,
                    value={"strategy": resolved_strategy},
                    updated_at=utcnow(),
                )
                session.add(setting)
            else:
                payload = setting.value if isinstance(setting.value, dict) else {}
                payload["strategy"] = resolved_strategy
                setting.value = payload
                setting.updated_at = utcnow()
            await session.flush()
            return _build_token_selection_settings(setting)


async def claim_next_active_token(*, selection_strategy: str | None = None) -> CodexToken | None:
    now = utcnow()
    async with get_session() as session:
        async with session.begin():
            stmt = (
                select(CodexToken)
                .where(*_available_token_filters(now))
                .order_by(*_token_selection_order_clauses(selection_strategy))
                .limit(1)
                .with_for_update(skip_locked=True)
            )
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
) -> None:
    async with get_session() as session:
        async with session.begin():
            token = await session.get(CodexToken, token_id, with_for_update=True)
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
            token.is_active = True
            token.cooldown_until = None
            token.last_error = None
            token.updated_at = utcnow()


async def mark_token_success(token_id: int) -> None:
    async with get_session() as session:
        async with session.begin():
            token = await session.get(CodexToken, token_id, with_for_update=True)
            if token is None:
                return
            token.is_active = True
            token.cooldown_until = None
            token.last_error = None
            token.updated_at = utcnow()


async def set_token_active_state(token_id: int, *, active: bool) -> CodexToken | None:
    async with get_session() as session:
        async with session.begin():
            token = await session.get(CodexToken, token_id, with_for_update=True)
            if token is None:
                return None
            token.is_active = bool(active)
            token.updated_at = utcnow()
            await session.flush()
            return token


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
            token = await session.get(CodexToken, token_id, with_for_update=True)
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


async def get_token_counts() -> TokenCounts:
    now = utcnow()
    async with get_session() as session:
        total_result = await session.execute(select(func.count()).select_from(CodexToken))
        active_result = await session.execute(
            select(func.count()).select_from(CodexToken).where(CodexToken.is_active.is_(True))
        )
        available_result = await session.execute(
            select(func.count()).select_from(CodexToken).where(*_available_token_filters(now))
        )
        cooling_result = await session.execute(
            select(func.count()).select_from(CodexToken).where(
                CodexToken.is_active.is_(True),
                CodexToken.refresh_token.is_not(None),
                CodexToken.token_type == "codex",
                CodexToken.cooldown_until.is_not(None),
                CodexToken.cooldown_until > now,
            )
        )
        disabled_result = await session.execute(
            select(func.count()).select_from(CodexToken).where(CodexToken.is_active.is_(False))
        )
        return TokenCounts(
            total=int(total_result.scalar_one() or 0),
            active=int(active_result.scalar_one() or 0),
            available=int(available_result.scalar_one() or 0),
            cooling=int(cooling_result.scalar_one() or 0),
            disabled=int(disabled_result.scalar_one() or 0),
        )


async def list_token_rows(limit: int = 100) -> list[CodexToken]:
    async with get_session() as session:
        stmt = select(CodexToken).order_by(CodexToken.id.desc()).limit(max(1, min(limit, 500)))
        result = await session.execute(stmt)
        return result.scalars().all()


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
