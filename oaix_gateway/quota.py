import asyncio
import base64
import json
import logging
import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from .database import CodexToken, utcnow
from .oauth import CodexOAuthManager, is_permanently_invalid_refresh_token_error
from .token_store import mark_token_error, update_token_plan_type


logger = logging.getLogger("oaix.gateway")

WHAM_USAGE_URL = "https://chatgpt.com/backend-api/wham/usage"
WHAM_USER_AGENT = "codex_cli_rs/0.125.0 (Debian 13.0.0; x86_64) WindowsTerminal"
WINDOW_5H_SECONDS = 5 * 60 * 60
WINDOW_7D_SECONDS = 7 * 24 * 60 * 60


@dataclass(frozen=True)
class CodexPlanInfo:
    chatgpt_account_id: str | None
    plan_type: str | None
    subscription_active_start: datetime | None
    subscription_active_until: datetime | None


@dataclass(frozen=True)
class CodexQuotaWindow:
    id: str
    label: str
    limit_window_seconds: int | None
    used_percent: float | None
    remaining_percent: float | None
    reset_at: datetime | None
    exhausted: bool


@dataclass(frozen=True)
class ParsedCodexQuota:
    plan_type: str | None
    windows: list[CodexQuotaWindow]


@dataclass(frozen=True)
class CodexQuotaSnapshot:
    fetched_at: datetime
    error: str | None
    plan_type: str | None
    windows: list[CodexQuotaWindow]


@dataclass(frozen=True)
class _CachedQuotaSnapshot:
    snapshot: CodexQuotaSnapshot
    expires_at: datetime


def _normalize_optional_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def normalize_plan_type(value: Any) -> str | None:
    plan_type = _normalize_optional_text(value)
    if not plan_type:
        return None
    lowered = plan_type.lower()
    if lowered.startswith("chatgpt_"):
        return lowered[len("chatgpt_") :]
    return lowered


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

    raw = _normalize_optional_text(value)
    if raw is None:
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
    except Exception:
        return None

    try:
        payload = json.loads(decoded)
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def _extract_auth_mapping(payload: dict[str, Any]) -> dict[str, Any]:
    auth_info = payload.get("https://api.openai.com/auth")
    if isinstance(auth_info, dict):
        return auth_info
    return payload


def _parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None

    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    if isinstance(value, (int, float)):
        timestamp = float(value)
    else:
        raw = _normalize_optional_text(value)
        if raw is None:
            return None
        try:
            timestamp = float(raw)
        except ValueError:
            normalized = raw[:-1] + "+00:00" if raw.endswith("Z") else raw
            try:
                dt = datetime.fromisoformat(normalized)
            except ValueError:
                return None
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)

    if not math.isfinite(timestamp) or timestamp <= 0:
        return None
    if timestamp > 1_000_000_000_000:
        timestamp /= 1000
    try:
        return datetime.fromtimestamp(timestamp, tz=timezone.utc)
    except (OverflowError, OSError, ValueError):
        return None


def extract_codex_plan_info(
    id_token: Any,
    *,
    account_id: str | None = None,
    raw_payload: Any = None,
) -> CodexPlanInfo:
    chatgpt_account_id = _normalize_optional_text(account_id)
    plan_type = None
    subscription_active_start = None
    subscription_active_until = None

    if isinstance(raw_payload, dict):
        chatgpt_account_id = chatgpt_account_id or _normalize_optional_text(
            raw_payload.get("account_id") or raw_payload.get("chatgpt_account_id")
        )
        plan_type = normalize_plan_type(raw_payload.get("plan_type") or raw_payload.get("chatgpt_plan_type"))
        subscription_active_start = _parse_datetime(raw_payload.get("chatgpt_subscription_active_start"))
        subscription_active_until = _parse_datetime(raw_payload.get("chatgpt_subscription_active_until"))

    candidates: list[Any] = [id_token]
    if isinstance(raw_payload, dict):
        raw_id_token = raw_payload.get("id_token")
        if raw_id_token != id_token:
            candidates.append(raw_id_token)
        candidates.append(raw_payload)

    for candidate in candidates:
        payload = _parse_mapping_or_jwt(candidate)
        if not isinstance(payload, dict):
            continue

        auth_payload = _extract_auth_mapping(payload)
        chatgpt_account_id = chatgpt_account_id or _normalize_optional_text(
            auth_payload.get("chatgpt_account_id") or payload.get("chatgpt_account_id")
        )
        plan_type = plan_type or normalize_plan_type(
            auth_payload.get("chatgpt_plan_type")
            or auth_payload.get("plan_type")
            or payload.get("chatgpt_plan_type")
            or payload.get("plan_type")
        )
        subscription_active_start = subscription_active_start or _parse_datetime(
            auth_payload.get("chatgpt_subscription_active_start") or payload.get("chatgpt_subscription_active_start")
        )
        subscription_active_until = subscription_active_until or _parse_datetime(
            auth_payload.get("chatgpt_subscription_active_until") or payload.get("chatgpt_subscription_active_until")
        )

    return CodexPlanInfo(
        chatgpt_account_id=chatgpt_account_id,
        plan_type=plan_type,
        subscription_active_start=subscription_active_start,
        subscription_active_until=subscription_active_until,
    )


def _first_mapping(mapping: dict[str, Any] | None, *keys: str) -> dict[str, Any] | None:
    if not isinstance(mapping, dict):
        return None
    for key in keys:
        value = mapping.get(key)
        if isinstance(value, dict):
            return value
    return None


def _first_value(mapping: dict[str, Any] | None, *keys: str) -> Any | None:
    if not isinstance(mapping, dict):
        return None
    for key in keys:
        if key in mapping and mapping[key] is not None:
            return mapping[key]
    return None


def _coerce_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return number


def _coerce_int(value: Any) -> int | None:
    number = _coerce_float(value)
    if number is None:
        return None
    return int(number)


def _coerce_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return bool(value)
    text = _normalize_optional_text(value)
    if text is None:
        return None
    lowered = text.lower()
    if lowered in {"1", "true", "yes", "on"}:
        return True
    if lowered in {"0", "false", "no", "off"}:
        return False
    return None


def _clamp_percent(value: float) -> float:
    return max(0.0, min(float(value), 100.0))


def _find_codex_windows(rate_limit: dict[str, Any] | None) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    primary = _first_mapping(rate_limit, "primary_window", "primaryWindow")
    secondary = _first_mapping(rate_limit, "secondary_window", "secondaryWindow")

    five_hour = None
    weekly = None
    for candidate in (primary, secondary):
        if not isinstance(candidate, dict):
            continue
        duration = _coerce_int(_first_value(candidate, "limit_window_seconds", "limitWindowSeconds"))
        if duration == WINDOW_5H_SECONDS and five_hour is None:
            five_hour = candidate
        if duration == WINDOW_7D_SECONDS and weekly is None:
            weekly = candidate

    if five_hour is None and primary is not None:
        five_hour = primary
    if weekly is None and secondary is not None:
        weekly = secondary
    return five_hour, weekly


def _extract_reset_at(window: dict[str, Any], now: datetime) -> datetime | None:
    reset_at = _parse_datetime(_first_value(window, "reset_at", "resetAt"))
    if reset_at is not None:
        return reset_at

    reset_after_seconds = _coerce_float(_first_value(window, "reset_after_seconds", "resetAfterSeconds"))
    if reset_after_seconds is None or reset_after_seconds <= 0:
        return None
    return now + timedelta(seconds=reset_after_seconds)


def _deduce_used_percent(window: dict[str, Any], *, limit_reached: Any, allowed: Any, reset_at: datetime | None) -> float | None:
    used_percent = _coerce_float(_first_value(window, "used_percent", "usedPercent"))
    if used_percent is not None:
        return _clamp_percent(used_percent)

    exhausted_hint = _coerce_bool(limit_reached) is True or _coerce_bool(allowed) is False
    if exhausted_hint and reset_at is not None:
        return 100.0
    return None


def _build_codex_window(
    window_id: str,
    label: str,
    window: dict[str, Any] | None,
    *,
    limit_reached: Any,
    allowed: Any,
    now: datetime,
) -> CodexQuotaWindow | None:
    if not isinstance(window, dict):
        return None

    reset_at = _extract_reset_at(window, now)
    exhausted_hint = _coerce_bool(limit_reached) is True or _coerce_bool(allowed) is False
    used_percent = _deduce_used_percent(window, limit_reached=limit_reached, allowed=allowed, reset_at=reset_at)
    remaining_percent = None if used_percent is None else _clamp_percent(100.0 - used_percent)
    exhausted = bool(
        (used_percent is not None and used_percent >= 100.0)
        or (used_percent is None and exhausted_hint and (reset_at is not None or _coerce_bool(allowed) is False))
    )
    return CodexQuotaWindow(
        id=window_id,
        label=label,
        limit_window_seconds=_coerce_int(_first_value(window, "limit_window_seconds", "limitWindowSeconds")),
        used_percent=used_percent,
        remaining_percent=remaining_percent,
        reset_at=reset_at,
        exhausted=exhausted,
    )


def parse_codex_quota_payload(payload: Any, *, now: datetime | None = None) -> ParsedCodexQuota:
    if not isinstance(payload, dict):
        raise ValueError("Quota payload must be a JSON object")

    current_time = now or utcnow()
    rate_limit = _first_mapping(payload, "rate_limit", "rateLimit")
    five_hour, weekly = _find_codex_windows(rate_limit)
    limit_reached = _first_value(rate_limit, "limit_reached", "limitReached")
    allowed = _first_value(rate_limit, "allowed")

    windows: list[CodexQuotaWindow] = []
    for window in (
        _build_codex_window(
            "code-5h",
            "5h",
            five_hour,
            limit_reached=limit_reached,
            allowed=allowed,
            now=current_time,
        ),
        _build_codex_window(
            "code-7d",
            "7d",
            weekly,
            limit_reached=limit_reached,
            allowed=allowed,
            now=current_time,
        ),
    ):
        if window is not None:
            windows.append(window)

    return ParsedCodexQuota(
        plan_type=normalize_plan_type(payload.get("plan_type") or payload.get("planType")),
        windows=windows,
    )


def _shorten_error(value: str, *, limit: int = 220) -> str:
    compact = " ".join(str(value or "").split())
    if len(compact) <= limit:
        return compact
    return compact[: limit - 3].rstrip() + "..."


def _extract_response_error(response: httpx.Response) -> str:
    try:
        payload = response.json()
    except Exception:
        payload = None

    if isinstance(payload, dict):
        error = payload.get("error")
        if isinstance(error, dict):
            message = _normalize_optional_text(error.get("message") or error.get("type") or error.get("code"))
            if message:
                return f"HTTP {response.status_code}: {message}"
        detail = _normalize_optional_text(payload.get("detail") or payload.get("message"))
        if detail:
            return f"HTTP {response.status_code}: {detail}"

    body = _shorten_error(response.text)
    if body:
        return f"HTTP {response.status_code}: {body}"
    return f"HTTP {response.status_code}"


class CodexQuotaService:
    def __init__(
        self,
        *,
        ttl_seconds: int = 60,
        concurrency: int = 4,
        request_timeout_seconds: float = 30.0,
    ) -> None:
        self._ttl_seconds = max(1, int(ttl_seconds))
        self._request_timeout_seconds = max(1.0, float(request_timeout_seconds))
        self._cache: dict[int, _CachedQuotaSnapshot] = {}
        self._locks: dict[int, asyncio.Lock] = {}
        self._semaphore = asyncio.Semaphore(max(1, int(concurrency)))

    def _lock_for(self, token_id: int) -> asyncio.Lock:
        lock = self._locks.get(token_id)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[token_id] = lock
        return lock

    def _fresh_cached_snapshot(self, token_id: int) -> CodexQuotaSnapshot | None:
        cached = self._cache.get(token_id)
        if cached is None:
            return None
        if cached.expires_at <= utcnow():
            self._cache.pop(token_id, None)
            return None
        return cached.snapshot

    def get_cached_snapshot(self, token_id: int) -> CodexQuotaSnapshot | None:
        return self._fresh_cached_snapshot(token_id)

    async def get_snapshot(
        self,
        token_row: CodexToken,
        *,
        client: httpx.AsyncClient,
        oauth_manager: CodexOAuthManager,
        account_id: str | None,
    ) -> CodexQuotaSnapshot:
        cached = self._fresh_cached_snapshot(token_row.id)
        if cached is not None:
            return cached

        lock = self._lock_for(token_row.id)
        async with lock:
            cached = self._fresh_cached_snapshot(token_row.id)
            if cached is not None:
                return cached

            snapshot = await self._fetch_snapshot(
                token_row,
                client=client,
                oauth_manager=oauth_manager,
                account_id=account_id,
            )
            self._cache[token_row.id] = _CachedQuotaSnapshot(
                snapshot=snapshot,
                expires_at=snapshot.fetched_at + timedelta(seconds=self._ttl_seconds),
            )
            if snapshot.plan_type:
                await update_token_plan_type(token_row.id, plan_type=snapshot.plan_type)
            return snapshot

    async def get_many(
        self,
        token_rows: list[CodexToken],
        *,
        client: httpx.AsyncClient,
        oauth_manager: CodexOAuthManager,
        account_ids: dict[int, str | None],
    ) -> dict[int, CodexQuotaSnapshot]:
        async def load(token_row: CodexToken) -> tuple[int, CodexQuotaSnapshot]:
            snapshot = await self.get_snapshot(
                token_row,
                client=client,
                oauth_manager=oauth_manager,
                account_id=account_ids.get(token_row.id),
            )
            return token_row.id, snapshot

        results = await asyncio.gather(*(load(token_row) for token_row in token_rows))
        return {token_id: snapshot for token_id, snapshot in results}

    async def _fetch_snapshot(
        self,
        token_row: CodexToken,
        *,
        client: httpx.AsyncClient,
        oauth_manager: CodexOAuthManager,
        account_id: str | None,
    ) -> CodexQuotaSnapshot:
        fetched_at = utcnow()
        effective_account_id = _normalize_optional_text(account_id or token_row.account_id)
        if effective_account_id is None:
            return CodexQuotaSnapshot(
                fetched_at=fetched_at,
                error="missing account_id",
                plan_type=None,
                windows=[],
            )

        try:
            response = await self._request_usage(
                token_row,
                client=client,
                oauth_manager=oauth_manager,
                account_id=effective_account_id,
            )
        except Exception as exc:
            if is_permanently_invalid_refresh_token_error(exc):
                oauth_manager.invalidate(token_row.id)
                detail = getattr(exc, "detail", exc)
                await mark_token_error(
                    token_row.id,
                    json.dumps(detail, ensure_ascii=True) if isinstance(detail, dict) else str(detail),
                    deactivate=True,
                    clear_access_token=True,
                )
            logger.warning("Failed to query wham quota for token_id=%s: %s", token_row.id, exc)
            return CodexQuotaSnapshot(
                fetched_at=fetched_at,
                error=_shorten_error(str(exc)),
                plan_type=None,
                windows=[],
            )

        if response.status_code < 200 or response.status_code >= 300:
            return CodexQuotaSnapshot(
                fetched_at=fetched_at,
                error=_extract_response_error(response),
                plan_type=None,
                windows=[],
            )

        try:
            payload = response.json()
        except json.JSONDecodeError:
            return CodexQuotaSnapshot(
                fetched_at=fetched_at,
                error="Quota endpoint returned invalid JSON",
                plan_type=None,
                windows=[],
            )

        try:
            parsed = parse_codex_quota_payload(payload, now=fetched_at)
        except ValueError as exc:
            return CodexQuotaSnapshot(
                fetched_at=fetched_at,
                error=_shorten_error(str(exc)),
                plan_type=None,
                windows=[],
            )

        return CodexQuotaSnapshot(
            fetched_at=fetched_at,
            error=None,
            plan_type=parsed.plan_type,
            windows=parsed.windows,
        )

    async def _request_usage(
        self,
        token_row: CodexToken,
        *,
        client: httpx.AsyncClient,
        oauth_manager: CodexOAuthManager,
        account_id: str,
    ) -> httpx.Response:
        access_token, _ = await oauth_manager.get_access_token(token_row, client)
        response = await self._send_usage_request(client, access_token=access_token, account_id=account_id)
        if response.status_code not in (401, 403):
            return response

        oauth_manager.invalidate(token_row.id)
        token_row.access_token = None
        token_row.expires_at = None
        refreshed_access_token, _ = await oauth_manager.get_access_token(token_row, client)
        return await self._send_usage_request(client, access_token=refreshed_access_token, account_id=account_id)

    async def _send_usage_request(
        self,
        client: httpx.AsyncClient,
        *,
        access_token: str,
        account_id: str,
    ) -> httpx.Response:
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "User-Agent": WHAM_USER_AGENT,
            "Chatgpt-Account-Id": account_id,
        }
        async with self._semaphore:
            return await client.get(
                WHAM_USAGE_URL,
                headers=headers,
                timeout=self._request_timeout_seconds,
            )
