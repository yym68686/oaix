import asyncio
import hashlib
from types import SimpleNamespace

import httpx
from fastapi.responses import JSONResponse

from oaix_gateway.api_server import WEB_DIR, create_app
from oaix_gateway.database import CodexToken
from oaix_gateway.token_store import TokenCounts, TokenSelectionSettings


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
    assert "/favicon.ico" in paths
    assert "/apple-touch-icon.png" in paths
    assert "/apple-touch-icon-precomposed.png" in paths
    assert "/healthz" in paths
    assert "/admin/tokens" in paths
    assert "/admin/token-selection" in paths
    assert "/admin/token-selection/order" in paths
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


def test_frontend_index_busts_asset_cache_with_content_hash() -> None:
    app = create_app()

    response = asyncio.run(_request(app, "GET", "/"))

    css_version = hashlib.sha256((WEB_DIR / "styles.css").read_bytes()).hexdigest()[:12]
    js_version = hashlib.sha256((WEB_DIR / "app.js").read_bytes()).hexdigest()[:12]

    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store, max-age=0"
    assert f'/assets/styles.css?v={css_version}' in response.text
    assert f'/assets/app.js?v={js_version}' in response.text


def test_browser_icon_routes_do_not_404() -> None:
    app = create_app()

    for path in ("/favicon.ico", "/apple-touch-icon.png", "/apple-touch-icon-precomposed.png"):
        response = asyncio.run(_request(app, "GET", path))
        assert response.status_code == 204


def test_frontend_token_cards_always_render_probe_button() -> None:
    app_js = (WEB_DIR / "app.js").read_text()
    action_renderer = app_js.split("function renderTokenActionButtons", 1)[1].split("function clampWidth", 1)[0]

    assert 'const PROBE_MODEL_OPTIONS = ["gpt-5.5", "gpt-5.4-mini"]' in app_js
    assert 'data-token-probe-model="true"' in action_renderer
    assert 'const probeButton = `' in action_renderer
    assert 'data-token-probe="true"' in action_renderer
    assert "${probeButton}" in action_renderer
    assert action_renderer.index("${probeButton}") < action_renderer.index('${actionButtons.join("")}')


def test_frontend_token_cards_support_fill_first_drag_ordering() -> None:
    app_js = (WEB_DIR / "app.js").read_text()

    assert 'data-token-draggable="${draggable ? "true" : "false"}"' in app_js
    assert 'draggable="${draggable ? "true" : "false"}"' in app_js
    assert 'fetchJson("/admin/token-selection/order"' in app_js
    assert 'elements.tokenList.addEventListener("dragstart", handleTokenCardDragStart)' in app_js
    assert 'state.tokenSelectionStrategy === "fill_first"' in app_js
    assert 'renderTokenFact("当前并发", activeStreamsValue, activeStreamsTone)' in app_js


def test_frontend_import_panel_supports_queue_position_switch() -> None:
    index_html = (WEB_DIR / "index.html").read_text()
    app_js = (WEB_DIR / "app.js").read_text()
    import_function = app_js.split("async function importTokens", 1)[1].split("function resetImportForm", 1)[0]

    assert 'id="import-queue-position-summary"' in index_html
    assert 'data-import-queue-position="front"' in index_html
    assert 'data-import-queue-position="back"' in index_html
    assert 'const DEFAULT_IMPORT_QUEUE_POSITION = "front"' in app_js
    assert "renderImportQueuePosition(state.importQueuePosition)" in app_js
    assert "tokens: payloads" in import_function
    assert "import_queue_position: state.importQueuePosition" in import_function


def test_frontend_dashboard_refresh_does_not_overlap_heavy_admin_requests() -> None:
    app_js = (WEB_DIR / "app.js").read_text()
    refresh_function = app_js.split("async function refreshDashboard", 1)[1].split("function splitTokenInputLines", 1)[0]

    assert "const REFRESH_INTERVAL_MS = 30000" in app_js
    assert "refreshing: false" in app_js
    assert "if (state.refreshing)" in refresh_function
    assert "state.refreshing = true" in refresh_function
    assert "await loadRequests();" in refresh_function
    assert "await loadTokens();" in refresh_function
    assert "Promise.all([loadTokens(), loadRequests()])" not in refresh_function


def test_frontend_probe_request_sends_selected_model() -> None:
    app_js = (WEB_DIR / "app.js").read_text()
    probe_function = app_js.split("async function probeToken", 1)[1].split("async function deleteToken", 1)[0]

    assert "const selectedModel = normalizeProbeModel(model)" in probe_function
    assert "headers: authHeaders(true)" in probe_function
    assert "body: JSON.stringify({ model: selectedModel })" in probe_function


def test_frontend_summarizes_html_error_pages_before_rendering() -> None:
    app_js = (WEB_DIR / "app.js").read_text()
    fetch_function = app_js.split("async function fetchJson", 1)[1].split("async function loadHealth", 1)[0]
    request_renderer = app_js.split("function renderRequestList", 1)[1].split("function escapeHtml", 1)[0]

    assert "function summarizeHtmlError" in app_js
    assert "524: \"源站响应超时，请稍后重试\"" in app_js
    assert "throw createFetchError(response, data)" in fetch_function
    assert "summarizeTokenError(rawErrorMessage)" in request_renderer
    assert "data?.detail || data?.message" not in fetch_function


def test_token_selection_order_route_forwards_token_ids(monkeypatch) -> None:
    monkeypatch.delenv("SERVICE_API_KEYS", raising=False)
    monkeypatch.delenv("API_KEY", raising=False)
    app = create_app()
    captured: dict[str, object] = {}

    async def fake_update_token_order_settings(*, token_ids):
        captured["token_ids"] = token_ids
        return TokenSelectionSettings(strategy="fill_first", token_order=(7, 3, 9))

    monkeypatch.setattr("oaix_gateway.api_server.update_token_order_settings", fake_update_token_order_settings)

    response = asyncio.run(
        _request(
            app,
            "POST",
            "/admin/token-selection/order",
            json={"token_ids": [7, 3, 7, 9]},
        )
    )

    assert response.status_code == 200
    assert captured == {"token_ids": [7, 3, 7, 9]}
    assert response.json()["strategy"] == "fill_first"
    assert response.json()["token_order"] == [7, 3, 9]


def test_admin_token_probe_route_accepts_model_payload(monkeypatch) -> None:
    monkeypatch.delenv("SERVICE_API_KEYS", raising=False)
    monkeypatch.delenv("API_KEY", raising=False)
    app = create_app()
    calls: dict[str, object] = {}

    async def fake_get_token_row(token_id: int):
        return SimpleNamespace(id=token_id)

    async def fake_probe_token_with_latest_access_token(app_arg, *, http_request, token_row, probe_model=None):
        calls["probe"] = {
            "app": app_arg,
            "path": http_request.url.path,
            "token_id": token_row.id,
            "probe_model": probe_model,
        }
        return {"id": token_row.id, "probe_model": probe_model}

    monkeypatch.setattr("oaix_gateway.api_server.get_token_row", fake_get_token_row)
    monkeypatch.setattr(
        "oaix_gateway.api_server._probe_token_with_latest_access_token",
        fake_probe_token_with_latest_access_token,
    )

    response = asyncio.run(_request(app, "POST", "/admin/tokens/12/probe", json={"model": "gpt-5.5"}))

    assert response.status_code == 200
    assert response.json() == {"id": 12, "probe_model": "gpt-5.5"}
    assert calls["probe"] == {
        "app": app,
        "path": "/admin/tokens/12/probe",
        "token_id": 12,
        "probe_model": "gpt-5.5",
    }

    response = asyncio.run(_request(app, "POST", "/admin/tokens/13/probe"))

    assert response.status_code == 200
    assert response.json() == {"id": 13, "probe_model": None}
    assert calls["probe"] == {
        "app": app,
        "path": "/admin/tokens/13/probe",
        "token_id": 13,
        "probe_model": None,
    }


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
