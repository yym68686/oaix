import asyncio

import httpx
from fastapi.responses import JSONResponse

from oaix_gateway.api_server import create_app
from oaix_gateway.database import CodexToken
from oaix_gateway.token_store import TokenCounts


async def _request(
    app,
    method: str,
    path: str,
    *,
    headers: dict[str, str] | None = None,
    json: dict | None = None,
) -> httpx.Response:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        return await client.request(method, path, headers=headers, json=json)


def test_frontend_routes_are_registered() -> None:
    app = create_app()
    paths = {route.path for route in app.routes}

    assert "/" in paths
    assert "/healthz" in paths
    assert "/admin/tokens" in paths
    assert "/admin/token-selection" in paths
    assert "/admin/tokens/{token_id}/activation" in paths
    assert "/admin/tokens/{token_id}/probe" in paths
    assert "/admin/tokens/{token_id}" in paths
    assert "/admin/requests" in paths
    assert "/admin/tokens/import" in paths
    assert "/admin/tokens/import-jobs/{job_id}" in paths
    assert "/v1/chat/completions" in paths
    assert "/v1/responses" in paths
    assert "/v1/responses/compact" in paths
    assert "/v1/images/generations" in paths
    assert "/v1/images/edits" in paths


def test_assets_mount_is_registered() -> None:
    app = create_app()
    assert any(getattr(route, "path", None) == "/assets" for route in app.routes)


def test_frontend_index_includes_token_search_input() -> None:
    app = create_app()

    response = asyncio.run(_request(app, "GET", "/"))

    assert response.status_code == 200
    assert 'id="token-search-input"' in response.text
    assert 'id="token-search-summary"' in response.text


def test_chat_completions_preflight_is_handled_by_cors(monkeypatch) -> None:
    monkeypatch.delenv("SERVICE_API_KEYS", raising=False)
    monkeypatch.delenv("API_KEY", raising=False)
    monkeypatch.delenv("CORS_ALLOW_ORIGINS", raising=False)
    monkeypatch.delenv("CORS_ALLOW_ORIGIN_REGEX", raising=False)
    monkeypatch.delenv("CORS_ALLOW_CREDENTIALS", raising=False)
    app = create_app()

    response = asyncio.run(
        _request(
            app,
            "OPTIONS",
            "/v1/chat/completions",
            headers={
                "Origin": "https://app.example.com",
                "Access-Control-Request-Method": "POST",
                "Access-Control-Request-Headers": "authorization,content-type",
            },
        )
    )

    assert response.status_code == 200
    assert response.headers["access-control-allow-origin"] == "*"
    assert "POST" in response.headers["access-control-allow-methods"]
    assert response.headers["access-control-allow-headers"] == "authorization,content-type"


def test_chat_completions_post_includes_cors_headers(monkeypatch) -> None:
    monkeypatch.delenv("SERVICE_API_KEYS", raising=False)
    monkeypatch.delenv("API_KEY", raising=False)
    monkeypatch.delenv("CORS_ALLOW_ORIGINS", raising=False)
    monkeypatch.delenv("CORS_ALLOW_ORIGIN_REGEX", raising=False)
    monkeypatch.delenv("CORS_ALLOW_CREDENTIALS", raising=False)

    async def fake_execute_proxy_request_with_failover(*args, **kwargs):
        return JSONResponse({"ok": True})

    monkeypatch.setattr(
        "oaix_gateway.api_server._execute_proxy_request_with_failover",
        fake_execute_proxy_request_with_failover,
    )
    app = create_app()

    response = asyncio.run(
        _request(
            app,
            "POST",
            "/v1/chat/completions",
            headers={"Origin": "https://app.example.com"},
            json={
                "model": "gpt-5.4-mini",
                "messages": [{"role": "user", "content": "hi"}],
            },
        )
    )

    assert response.status_code == 200
    assert response.json() == {"ok": True}
    assert response.headers["access-control-allow-origin"] == "*"


def test_token_activation_route_forwards_clear_cooldown(monkeypatch) -> None:
    monkeypatch.delenv("SERVICE_API_KEYS", raising=False)
    monkeypatch.delenv("API_KEY", raising=False)
    app = create_app()
    captured: dict[str, object] = {}

    async def fake_set_token_active_state(
        token_id: int,
        *,
        active: bool,
        clear_cooldown: bool = False,
    ) -> CodexToken | None:
        captured["token_id"] = token_id
        captured["active"] = active
        captured["clear_cooldown"] = clear_cooldown
        return CodexToken(
            id=token_id,
            token_type="codex",
            is_active=active,
            cooldown_until=None,
        )

    async def fake_get_token_counts() -> TokenCounts:
        return TokenCounts(total=3, active=2, available=2, cooling=0, disabled=1)

    monkeypatch.setattr("oaix_gateway.api_server.set_token_active_state", fake_set_token_active_state)
    monkeypatch.setattr("oaix_gateway.api_server.get_token_counts", fake_get_token_counts)

    response = asyncio.run(
        _request(
            app,
            "POST",
            "/admin/tokens/9/activation",
            json={"active": True, "clear_cooldown": True},
        )
    )

    assert response.status_code == 200
    assert captured == {
        "token_id": 9,
        "active": True,
        "clear_cooldown": True,
    }
    assert response.json() == {
        "id": 9,
        "is_active": True,
        "cooldown_until": None,
        "counts": {
            "total": 3,
            "active": 2,
            "available": 2,
            "cooling": 0,
            "disabled": 1,
        },
    }
