import asyncio
import base64
import json
from datetime import datetime, timedelta, timezone

from fastapi import HTTPException

from oaix_gateway.database import CodexToken
from oaix_gateway.quota import CodexQuotaService, WHAM_USER_AGENT, extract_codex_plan_info, parse_codex_quota_payload


def _jwt_like(payload: dict) -> str:
    header = {"alg": "none", "typ": "JWT"}

    def encode(value: dict) -> str:
        raw = json.dumps(value, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
        return base64.urlsafe_b64encode(raw).decode("utf-8").rstrip("=")

    return f"{encode(header)}.{encode(payload)}.signature"


def test_extract_codex_plan_info_reads_openai_auth_claims() -> None:
    token = _jwt_like(
        {
            "https://api.openai.com/auth": {
                "chatgpt_account_id": "acct_123",
                "chatgpt_plan_type": "plus",
                "chatgpt_subscription_active_start": "2026-04-01T00:00:00Z",
                "chatgpt_subscription_active_until": "2026-04-30T12:34:56Z",
            }
        }
    )

    info = extract_codex_plan_info(token)

    assert info.chatgpt_account_id == "acct_123"
    assert info.plan_type == "plus"
    assert info.subscription_active_start == datetime(2026, 4, 1, 0, 0, tzinfo=timezone.utc)
    assert info.subscription_active_until == datetime(2026, 4, 30, 12, 34, 56, tzinfo=timezone.utc)


def test_wham_user_agent_uses_current_codex_version() -> None:
    assert WHAM_USER_AGENT.startswith("codex_cli_rs/0.125.0 ")


def test_parse_codex_quota_payload_reads_5h_and_7d_windows() -> None:
    now = datetime(2026, 4, 14, 10, 0, tzinfo=timezone.utc)
    payload = {
        "plan_type": "plus",
        "rate_limit": {
            "allowed": True,
            "limit_reached": False,
            "primary_window": {
                "limit_window_seconds": 5 * 60 * 60,
                "used_percent": 25,
                "reset_at": int(datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc).timestamp()),
            },
            "secondary_window": {
                "limit_window_seconds": 7 * 24 * 60 * 60,
                "used_percent": 75,
                "reset_after_seconds": 7200,
            },
        },
    }

    parsed = parse_codex_quota_payload(payload, now=now)
    windows = {window.id: window for window in parsed.windows}

    assert parsed.plan_type == "plus"
    assert windows["code-5h"].used_percent == 25
    assert windows["code-5h"].remaining_percent == 75
    assert windows["code-5h"].reset_at == datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)
    assert windows["code-5h"].exhausted is False
    assert windows["code-7d"].used_percent == 75
    assert windows["code-7d"].remaining_percent == 25
    assert windows["code-7d"].reset_at == now + timedelta(seconds=7200)
    assert windows["code-7d"].exhausted is False


def test_parse_codex_quota_payload_marks_limit_reached_windows_exhausted() -> None:
    now = datetime(2026, 4, 14, 10, 0, tzinfo=timezone.utc)
    payload = {
        "rate_limit": {
            "allowed": False,
            "limit_reached": True,
            "primary_window": {
                "limit_window_seconds": 5 * 60 * 60,
                "reset_after_seconds": 600,
            },
            "secondary_window": {
                "limit_window_seconds": 7 * 24 * 60 * 60,
                "reset_after_seconds": 3600,
            },
        }
    }

    parsed = parse_codex_quota_payload(payload, now=now)
    windows = {window.id: window for window in parsed.windows}

    assert windows["code-5h"].used_percent == 100
    assert windows["code-5h"].remaining_percent == 0
    assert windows["code-5h"].exhausted is True
    assert windows["code-7d"].used_percent == 100
    assert windows["code-7d"].remaining_percent == 0
    assert windows["code-7d"].exhausted is True


def test_parse_codex_quota_payload_prefers_window_percent_over_global_limit_flags() -> None:
    now = datetime(2026, 4, 14, 10, 0, tzinfo=timezone.utc)
    payload = {
        "rate_limit": {
            "allowed": False,
            "limit_reached": True,
            "primary_window": {
                "limit_window_seconds": 5 * 60 * 60,
                "used_percent": 100,
                "reset_after_seconds": 600,
            },
            "secondary_window": {
                "limit_window_seconds": 7 * 24 * 60 * 60,
                "used_percent": 25,
                "reset_after_seconds": 3600,
            },
        }
    }

    parsed = parse_codex_quota_payload(payload, now=now)
    windows = {window.id: window for window in parsed.windows}

    assert windows["code-5h"].used_percent == 100
    assert windows["code-5h"].remaining_percent == 0
    assert windows["code-5h"].exhausted is True
    assert windows["code-7d"].used_percent == 25
    assert windows["code-7d"].remaining_percent == 75
    assert windows["code-7d"].exhausted is False


def test_quota_service_deactivates_permanently_invalid_refresh_tokens(monkeypatch) -> None:
    recorded: dict[str, object] = {}

    async def fake_mark_token_error(
        token_id: int,
        message: str,
        *,
        deactivate: bool = False,
        cooldown_seconds: int | None = None,
        clear_access_token: bool = False,
    ) -> None:
        recorded.update(
            {
                "token_id": token_id,
                "message": message,
                "deactivate": deactivate,
                "cooldown_seconds": cooldown_seconds,
                "clear_access_token": clear_access_token,
            }
        )

    monkeypatch.setattr("oaix_gateway.quota.mark_token_error", fake_mark_token_error)

    class DummyOAuthManager:
        def __init__(self) -> None:
            self.invalidated: list[int] = []

        def invalidate(self, token_id: int) -> None:
            self.invalidated.append(token_id)

        async def get_access_token(self, token_row: CodexToken, client) -> tuple[str, bool]:
            raise HTTPException(
                status_code=401,
                detail=(
                    'Codex token refresh failed: status 401: {"error":{"code":"refresh_token_reused",'
                    '"message":"This refresh token has already been used to generate a new access token. '
                    'Please try signing in again."}}'
                ),
            )

    token_row = CodexToken(
        id=7,
        account_id="acct_123",
        refresh_token="rt_123",
        token_type="codex",
        is_active=True,
    )
    service = CodexQuotaService(ttl_seconds=60)
    oauth_manager = DummyOAuthManager()

    async def runner():
        return await service.get_snapshot(
            token_row,
            client=object(),
            oauth_manager=oauth_manager,
            account_id="acct_123",
        )

    snapshot = asyncio.run(runner())

    assert snapshot.error is not None
    assert oauth_manager.invalidated == [7]
    assert recorded == {
        "token_id": 7,
        "message": (
            'Codex token refresh failed: status 401: {"error":{"code":"refresh_token_reused",'
            '"message":"This refresh token has already been used to generate a new access token. '
            'Please try signing in again."}}'
        ),
        "deactivate": True,
        "cooldown_seconds": None,
        "clear_access_token": True,
    }
