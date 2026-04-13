import asyncio
import glob
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

from sqlalchemy import func, nullsfirst, or_, select

from .database import CodexToken, close_database, get_session, init_db, utcnow


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


async def _find_existing_token(session, *, account_id: str | None, email: str | None) -> CodexToken | None:
    if account_id:
        result = await session.execute(select(CodexToken).where(CodexToken.account_id == account_id))
        token = result.scalar_one_or_none()
        if token is not None:
            return token
    if email:
        result = await session.execute(select(CodexToken).where(CodexToken.email == email))
        token = result.scalar_one_or_none()
        if token is not None:
            return token
    return None


async def upsert_token_payload(
    token_data_or_json: str | dict[str, Any],
    *,
    source_file: str | None = None,
) -> CodexToken:
    payload = _load_token_payload(token_data_or_json)
    email = str(payload.get("email") or "").strip() or None
    account_id = str(payload.get("account_id") or "").strip() or None
    recovery = payload.get("recovery")
    if recovery is not None and not isinstance(recovery, dict):
        recovery = None

    async with get_session() as session:
        async with session.begin():
            token = await _find_existing_token(session, account_id=account_id, email=email)
            if token is None:
                token = CodexToken(
                    email=email,
                    account_id=account_id,
                    refresh_token=str(payload["refresh_token"]).strip(),
                )
                session.add(token)

            token.email = email or token.email
            token.account_id = account_id or token.account_id
            token.id_token = str(payload.get("id_token") or "").strip() or None
            token.access_token = str(payload.get("access_token") or "").strip() or None
            token.refresh_token = str(payload["refresh_token"]).strip()
            token.token_type = str(payload.get("type") or "codex").strip() or "codex"
            token.last_refresh_at = parse_rfc3339(payload.get("last_refresh"))
            token.expires_at = parse_rfc3339(payload.get("expired"))
            token.recovery = recovery
            token.raw_payload = payload
            token.source_file = source_file or token.source_file
            token.is_active = True
            token.cooldown_until = None
            token.last_error = None
            token.updated_at = utcnow()
            await session.flush()
            return token


async def claim_next_active_token() -> CodexToken | None:
    now = utcnow()
    async with get_session() as session:
        async with session.begin():
            stmt = (
                select(CodexToken)
                .where(*_available_token_filters(now))
                .order_by(
                    nullsfirst(CodexToken.last_used_at.asc()),
                    CodexToken.updated_at.asc(),
                    CodexToken.id.asc(),
                )
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
            token.access_token = access_token
            if refresh_token:
                token.refresh_token = refresh_token
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


async def list_tokens(limit: int = 100) -> list[TokenStatus]:
    async with get_session() as session:
        stmt = select(CodexToken).order_by(CodexToken.id.desc()).limit(max(1, min(limit, 500)))
        result = await session.execute(stmt)
        tokens = result.scalars().all()
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


async def import_token_files(patterns: Iterable[str]) -> tuple[int, int]:
    await init_db()

    imported = 0
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
                await upsert_token_payload(payload, source_file=resolved)
                imported += 1
            except Exception:
                failed += 1
    return imported, failed


async def save_token_json(token_json: str, *, source_file: str | None = None) -> StoredTokenSummary:
    await init_db()
    token = await upsert_token_payload(token_json, source_file=source_file)
    return StoredTokenSummary(id=token.id, email=token.email, account_id=token.account_id)


def save_token_json_sync(token_json: str, *, source_file: str | None = None) -> StoredTokenSummary:
    async def _runner() -> StoredTokenSummary:
        try:
            return await save_token_json(token_json, source_file=source_file)
        finally:
            await close_database()

    return asyncio.run(_runner())


def import_token_files_sync(patterns: Iterable[str]) -> tuple[int, int]:
    async def _runner() -> tuple[int, int]:
        try:
            return await import_token_files(patterns)
        finally:
            await close_database()

    return asyncio.run(_runner())
