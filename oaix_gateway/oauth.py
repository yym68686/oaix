import asyncio
import json
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx
from fastapi import HTTPException

from .database import CodexToken
from .token_store import update_token_refresh_state


CODEX_OAUTH_TOKEN_URL = "https://auth.openai.com/oauth/token"
CODEX_OAUTH_CLIENT_ID = "app_EMoamEEZ73f0CkXaXp7hrann"
CODEX_ACCESS_TOKEN_REFRESH_SKEW_SECONDS = 30
PERMANENT_REFRESH_TOKEN_ERROR_CODES = frozenset(
    {
        "invalid_grant",
        "invalid_refresh_token",
        "refresh_token_expired",
        "refresh_token_not_found",
        "refresh_token_reused",
        "refresh_token_revoked",
        "token_expired",
        "token_revoked",
    }
)
PERMANENT_REFRESH_TOKEN_MESSAGE_MARKERS = (
    "already been used to generate a new access token",
    "invalid refresh token",
    "please try signing in again",
    "refresh token expired",
    "refresh token is invalid",
    "refresh token not found",
    "refresh token revoked",
)


def _normalize_optional_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _parse_json_mapping(value: Any) -> dict[str, Any] | None:
    if isinstance(value, dict):
        return value

    raw = _normalize_optional_text(value)
    if raw is None:
        return None

    candidates = [raw]
    json_start = raw.find("{")
    if json_start >= 0:
        candidates.append(raw[json_start:])

    for candidate in candidates:
        try:
            payload = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            return payload
    return None


def _extract_oauth_error_source(payload: dict[str, Any]) -> dict[str, Any]:
    for key in ("error", "detail"):
        value = payload.get(key)
        if isinstance(value, dict):
            return value
    return payload


def is_permanently_invalid_refresh_token_error(error: Any) -> bool:
    status_code = getattr(error, "status_code", None)
    if status_code not in (None, 400, 401, 403):
        return False

    detail = getattr(error, "detail", error)
    payload = _parse_json_mapping(detail)

    source: dict[str, Any] | None = None
    if payload is not None:
        source = _extract_oauth_error_source(payload)
        error_code = _normalize_optional_text(
            source.get("code") or source.get("error") or source.get("type") or payload.get("error")
        )
        if error_code and error_code.lower() in PERMANENT_REFRESH_TOKEN_ERROR_CODES:
            return True

    haystack_parts: list[str] = []
    raw_detail = _normalize_optional_text(detail)
    if raw_detail is not None:
        haystack_parts.append(raw_detail)
    if source is not None:
        for value in (
            source.get("code"),
            source.get("type"),
            source.get("message"),
            source.get("detail"),
            payload.get("error_description") if payload is not None else None,
        ):
            text = _normalize_optional_text(value)
            if text is not None:
                haystack_parts.append(text)

    haystack = " ".join(haystack_parts).lower()
    if not haystack:
        return False
    if "refresh token" not in haystack and "invalid_grant" not in haystack:
        return False
    return any(marker in haystack for marker in PERMANENT_REFRESH_TOKEN_MESSAGE_MARKERS)


class CodexOAuthManager:
    def __init__(self) -> None:
        self._cache: dict[int, dict[str, Any]] = {}
        self._locks: dict[int, asyncio.Lock] = {}

    def invalidate(self, token_id: int) -> None:
        self._cache.pop(token_id, None)

    def _lock_for(self, token_id: int) -> asyncio.Lock:
        lock = self._locks.get(token_id)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[token_id] = lock
        return lock

    @staticmethod
    def _access_token_is_valid(access_token: str | None, expires_at: datetime | None) -> bool:
        if not access_token:
            return False
        if expires_at is None:
            return True
        now = datetime.now(timezone.utc)
        return now < (expires_at - timedelta(seconds=CODEX_ACCESS_TOKEN_REFRESH_SKEW_SECONDS))

    async def _refresh_codex_access_token(self, client: httpx.AsyncClient, refresh_token: str) -> dict[str, Any]:
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        }
        data = {
            "client_id": CODEX_OAUTH_CLIENT_ID,
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "scope": "openid profile email",
        }
        response = await client.post(CODEX_OAUTH_TOKEN_URL, data=data, headers=headers, timeout=30.0)
        if response.status_code != 200:
            raise HTTPException(
                status_code=response.status_code,
                detail=f"Codex token refresh failed: status {response.status_code}: {response.text}",
            )

        payload = response.json()
        access_token = str(payload.get("access_token") or "").strip()
        if not access_token:
            raise HTTPException(status_code=401, detail="Codex token refresh returned empty access_token")

        refresh_token_out = str(payload.get("refresh_token") or "").strip() or None
        expires_at = None
        expires_in = payload.get("expires_in")
        try:
            expires_in_int = int(expires_in)
            if expires_in_int > 0:
                expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in_int)
        except Exception:
            expires_at = None

        return {
            "access_token": access_token,
            "refresh_token": refresh_token_out,
            "expires_at": expires_at,
        }

    async def get_access_token(self, token_row: CodexToken, client: httpx.AsyncClient) -> tuple[str, bool]:
        lock = self._lock_for(token_row.id)
        async with lock:
            cache_entry = self._cache.get(token_row.id) or {}
            if self._access_token_is_valid(cache_entry.get("access_token"), cache_entry.get("expires_at")):
                return str(cache_entry["access_token"]), False

            if self._access_token_is_valid(token_row.access_token, token_row.expires_at):
                self._cache[token_row.id] = {
                    "access_token": token_row.access_token,
                    "refresh_token": token_row.refresh_token,
                    "expires_at": token_row.expires_at,
                }
                return str(token_row.access_token), False

            refreshed = await self._refresh_codex_access_token(client, token_row.refresh_token)
            next_refresh_token = refreshed.get("refresh_token") or token_row.refresh_token
            await update_token_refresh_state(
                token_row.id,
                access_token=refreshed["access_token"],
                refresh_token=next_refresh_token,
                expires_at=refreshed.get("expires_at"),
            )
            self._cache[token_row.id] = {
                "access_token": refreshed["access_token"],
                "refresh_token": next_refresh_token,
                "expires_at": refreshed.get("expires_at"),
            }
            token_row.access_token = refreshed["access_token"]
            token_row.refresh_token = next_refresh_token
            token_row.expires_at = refreshed.get("expires_at")
            return refreshed["access_token"], True
