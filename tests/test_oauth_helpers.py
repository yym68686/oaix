import asyncio
from datetime import datetime, timedelta, timezone

from fastapi import HTTPException

from oaix_gateway.database import CodexToken
from oaix_gateway.oauth import CodexOAuthManager, is_permanently_invalid_refresh_token_error


def test_detects_refresh_token_reused_error() -> None:
    exc = HTTPException(
        status_code=401,
        detail=(
            'Codex token refresh failed: status 401: {"error":{"code":"refresh_token_reused",'
            '"message":"This refresh token has already been used to generate a new access token. '
            'Please try signing in again."}}'
        ),
    )

    assert is_permanently_invalid_refresh_token_error(exc) is True


def test_detects_invalid_grant_refresh_token_error_from_mapping() -> None:
    exc = HTTPException(
        status_code=400,
        detail={
            "error": "invalid_grant",
            "error_description": "Refresh token revoked, please sign in again.",
        },
    )

    assert is_permanently_invalid_refresh_token_error(exc) is True


def test_ignores_non_refresh_token_errors() -> None:
    exc = HTTPException(status_code=502, detail="Upstream request failed: ReadTimeout")

    assert is_permanently_invalid_refresh_token_error(exc) is False


def test_get_access_token_force_refresh_bypasses_cached_token(monkeypatch) -> None:
    captured: dict[str, object] = {}
    refreshed_expires_at = datetime.now(timezone.utc) + timedelta(minutes=30)

    async def fake_refresh_codex_access_token(self, client, refresh_token: str) -> dict[str, object]:
        captured["refresh_token"] = refresh_token
        return {
            "access_token": "fresh-access-token",
            "refresh_token": "fresh-refresh-token",
            "expires_at": refreshed_expires_at,
        }

    async def fake_update_token_refresh_state(
        token_id: int,
        *,
        access_token: str,
        refresh_token: str | None,
        expires_at,
        reactivate: bool = True,
    ) -> None:
        captured["update"] = {
            "token_id": token_id,
            "access_token": access_token,
            "refresh_token": refresh_token,
            "expires_at": expires_at,
            "reactivate": reactivate,
        }

    monkeypatch.setattr(CodexOAuthManager, "_refresh_codex_access_token", fake_refresh_codex_access_token)
    monkeypatch.setattr("oaix_gateway.oauth.update_token_refresh_state", fake_update_token_refresh_state)

    manager = CodexOAuthManager()
    token = CodexToken(
        id=17,
        refresh_token="refresh-token-old",
        access_token="cached-access-token",
        is_active=False,
    )
    token.expires_at = datetime.now(timezone.utc) + timedelta(minutes=20)

    access_token, refreshed = asyncio.run(
        manager.get_access_token(
            token,
            object(),
            force_refresh=True,
            reactivate_on_refresh=False,
        )
    )

    assert access_token == "fresh-access-token"
    assert refreshed is True
    assert token.access_token == "fresh-access-token"
    assert token.refresh_token == "fresh-refresh-token"
    assert token.expires_at == refreshed_expires_at
    assert captured["refresh_token"] == "refresh-token-old"
    assert captured["update"] == {
        "token_id": 17,
        "access_token": "fresh-access-token",
        "refresh_token": "fresh-refresh-token",
        "expires_at": refreshed_expires_at,
        "reactivate": False,
    }


def test_get_access_token_recovers_when_refresh_token_was_rotated_by_another_worker(monkeypatch) -> None:
    latest_expires_at = datetime.now(timezone.utc) + timedelta(minutes=30)

    async def fake_refresh_codex_access_token(self, client, refresh_token: str) -> dict[str, object]:
        raise HTTPException(
            status_code=401,
            detail=(
                'Codex token refresh failed: status 401: {"error":{"code":"refresh_token_reused",'
                '"message":"This refresh token has already been used to generate a new access token. '
                'Please try signing in again."}}'
            ),
        )

    async def fake_get_token_row(token_id: int):
        return CodexToken(
            id=token_id,
            refresh_token="refresh-token-new",
            access_token="access-token-new",
            token_type="codex",
            is_active=True,
            expires_at=latest_expires_at,
        )

    async def fake_update_token_refresh_state(*args, **kwargs) -> None:
        raise AssertionError("recovered refresh state should not be written again")

    monkeypatch.setattr(CodexOAuthManager, "_refresh_codex_access_token", fake_refresh_codex_access_token)
    monkeypatch.setattr("oaix_gateway.oauth.get_token_row", fake_get_token_row)
    monkeypatch.setattr("oaix_gateway.oauth.update_token_refresh_state", fake_update_token_refresh_state)

    manager = CodexOAuthManager()
    token = CodexToken(
        id=23,
        refresh_token="refresh-token-old",
        access_token=None,
        token_type="codex",
        is_active=True,
    )

    access_token, refreshed = asyncio.run(manager.get_access_token(token, object()))

    assert access_token == "access-token-new"
    assert refreshed is False
    assert token.refresh_token == "refresh-token-new"
    assert token.access_token == "access-token-new"
    assert token.expires_at == latest_expires_at
