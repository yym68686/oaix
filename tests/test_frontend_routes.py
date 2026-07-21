import asyncio
import hashlib
import json as json_module
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import httpx
from fastapi.responses import JSONResponse

import oaix_gateway.api_server as api_server
from oaix_gateway.api_server import WEB_DIR, create_app
from oaix_gateway.database import CodexToken
from oaix_gateway.request_store import (
    RequestAnalyticsBucket,
    RequestLogAnalytics,
    RequestLogSummary,
    RequestAnalyticsModel,
)
from oaix_gateway.token_import_jobs import TokenImportBatchSummary
from oaix_gateway.token_store import TokenCounts, TokenPlanCounts, TokenSelectionSettings


REPO_ROOT = Path(__file__).resolve().parents[1]
FRONTEND_SRC = REPO_ROOT / "frontend" / "src"
FRONTEND_APP = FRONTEND_SRC / "App.tsx"
FRONTEND_API = FRONTEND_SRC / "lib" / "api.ts"
COSS_UI_DIR = FRONTEND_SRC / "registry" / "default" / "ui"


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
    assert "/livez" in paths
    assert "/healthz" in paths
    assert "/admin/tokens" in paths
    assert "/admin/tokens/quota" in paths
    assert "/admin/tokens/import-batches" in paths
    assert "/admin/token-selection" in paths
    assert "/admin/token-selection/order" in paths
    assert "/admin/token-selection/plan-order" in paths
    assert "/admin/tokens/{token_id}/activation" in paths
    assert "/admin/tokens/{token_id}/remark" in paths
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
    assert '<div id="root"></div>' in response.text
    assert '/assets/src/main.js?v=' in response.text
    assert '/assets/styles.css?v=' in response.text
    assert "oaix Key Console" in response.text
    assert "__OAIX_WEB_VERSION_HASH__" not in response.text
    assert "__OAIX_WEB_VERSION_TIME__" not in response.text


def test_frontend_uses_react_vite_and_coss_registry() -> None:
    app_tsx = FRONTEND_APP.read_text()
    main_tsx = (FRONTEND_SRC / "main.tsx").read_text()
    styles_css = (FRONTEND_SRC / "styles.css").read_text()

    assert 'from "@/registry/default/ui/button"' in app_tsx
    assert 'from "@/registry/default/ui/card"' in app_tsx
    assert 'from "@/registry/default/ui/tabs"' in app_tsx
    assert 'from "@/registry/default/ui/dialog"' in app_tsx
    assert 'createRoot(document.getElementById("root")' in main_tsx
    assert '@source "./**/*.{ts,tsx}"' in styles_css
    assert (COSS_UI_DIR / "button.tsx").exists()
    assert (COSS_UI_DIR / "card.tsx").exists()
    assert (COSS_UI_DIR / "select.tsx").exists()


def test_frontend_probe_result_distinguishes_local_failure_from_upstream_response() -> None:
    api_ts = FRONTEND_API.read_text()
    domain_ts = (FRONTEND_SRC / "shared" / "domain.ts").read_text()
    components_tsx = (FRONTEND_SRC / "shared" / "components.tsx").read_text()
    keys_tsx = (FRONTEND_SRC / "features" / "keys" / "KeysPage.tsx").read_text()

    assert "upstream_attempted?: boolean | null" in api_ts
    assert "probe_stage?:" in api_ts
    assert "error_code?: string | null" in api_ts
    assert "result.upstream_attempted === false" in domain_ts
    assert "具体原因：" in components_tsx
    assert "没有向模型上游发送请求" in components_tsx
    assert 'result.upstream_attempted === false ? "未执行"' in components_tsx
    assert "原因：${detail}" in keys_tsx


def test_livez_returns_without_token_count_query(monkeypatch) -> None:
    async def fail_get_token_counts():
        raise AssertionError("livez should not query token counts")

    monkeypatch.setattr("oaix_gateway.api_server.get_token_counts", fail_get_token_counts)
    app = create_app()

    response = asyncio.run(_request(app, "GET", "/livez"))

    assert response.status_code == 200
    assert response.json()["ok"] is True


def test_healthz_returns_degraded_when_token_count_query_times_out(monkeypatch) -> None:
    monkeypatch.setenv("HEALTHZ_DB_TIMEOUT_SECONDS", "0.01")

    async def stalled_get_token_counts():
        await asyncio.sleep(60)

    monkeypatch.setattr("oaix_gateway.api_server.get_token_counts", stalled_get_token_counts)
    app = create_app()

    response = asyncio.run(_request(app, "GET", "/healthz"))

    assert response.status_code == 503
    body = response.json()
    assert body["ok"] is False
    assert body["degraded"] is True
    assert body["error"] == "token count query timed out"


def test_frontend_index_busts_asset_cache_with_content_hash() -> None:
    app = create_app()

    response = asyncio.run(_request(app, "GET", "/"))

    css_version = hashlib.sha256((WEB_DIR / "styles.css").read_bytes()).hexdigest()[:12]
    js_version = hashlib.sha256((WEB_DIR / "src" / "main.js").read_bytes()).hexdigest()[:12]
    bundle_digest = hashlib.sha256()
    bundle_mtime = 0.0
    for path in (WEB_DIR / "index.html", WEB_DIR / "styles.css", WEB_DIR / "src" / "main.js"):
        bundle_digest.update(path.read_bytes())
        bundle_mtime = max(bundle_mtime, path.stat().st_mtime)
    bundle_version_hash = bundle_digest.hexdigest()[:12]
    bundle_version_time = (
        datetime.fromtimestamp(bundle_mtime, tz=timezone.utc)
        .astimezone(timezone(timedelta(hours=8), name="UTC+8"))
        .strftime("%Y-%m-%d %H:%M:%S UTC+8")
    )

    assert response.status_code == 200
    assert response.headers["cache-control"] == "public, max-age=60, stale-while-revalidate=300"
    assert f'/assets/styles.css?v={css_version}' in response.text
    assert f'/assets/src/main.js?v={js_version}' in response.text
    assert f'hash: "{bundle_version_hash}"' in response.text
    assert f'time: "{bundle_version_time}"' in response.text
    assert "__OAIX_WEB_VERSION_HASH__" not in response.text
    assert "__OAIX_WEB_VERSION_TIME__" not in response.text


def test_browser_icon_routes_do_not_404() -> None:
    app = create_app()

    for path in ("/favicon.ico", "/apple-touch-icon.png", "/apple-touch-icon-precomposed.png"):
        response = asyncio.run(_request(app, "GET", path))
        assert response.status_code == 204


def test_frontend_token_explorer_keeps_core_admin_actions() -> None:
    app_tsx = FRONTEND_APP.read_text()
    api_ts = FRONTEND_API.read_text()

    assert "const PAGE_SIZE = 100" in app_tsx
    assert 'type TokenStatus = "all" | "available" | "cooling" | "disabled"' in app_tsx
    assert 'type TokenMode = "keys" | "import_batches"' in app_tsx
    assert "api.listTokens(params)" in app_tsx
    assert "api.batchTokens" in app_tsx
    assert "api.updateActivation" in app_tsx
    assert "api.updateRemark" in app_tsx
    assert "api.deleteToken" in app_tsx
    assert "api.importJobs(50)" in app_tsx
    assert 'listTokens: (params: URLSearchParams)' in api_ts
    assert 'batchTokens: (payload: Record<string, unknown>)' in api_ts


def test_frontend_import_panel_supports_queue_position_and_token_formats() -> None:
    app_tsx = FRONTEND_APP.read_text()

    assert 'const [queuePosition, setQueuePosition] = useState<"front" | "back">("front")' in app_tsx
    assert "import_queue_position: queuePosition" in app_tsx
    assert 'tokens: entries' in app_tsx
    assert "collectImportEntries(tokenInput, fileInputRef.current?.files)" in app_tsx
    assert "parseTokenText(await file.text())" in app_tsx
    assert '"access_token"' in app_tsx
    assert '"refresh_token"' in app_tsx
    assert '"refreshToken"' in app_tsx
    assert "sub2api 导出 JSON" in app_tsx


def test_frontend_dashboard_refresh_loads_admin_panels_together() -> None:
    app_tsx = FRONTEND_APP.read_text()

    assert "await Promise.allSettled([loadTokenSelection(), loadTokens(), loadRequests(), loadSettings()])" in app_tsx
    assert "window.setInterval(() => void refreshAll(), 30_000)" in app_tsx
    assert "loading && !counts.total" in app_tsx
    assert "tokenLoading && !tokens.length" in app_tsx
    assert "requestLoading && !requests.length && !requestSummary.total" in app_tsx


def test_frontend_service_key_storage_keeps_legacy_compatibility() -> None:
    api_ts = FRONTEND_API.read_text()

    assert 'const KEY_STORAGE = "oaix.serviceApiKey";' in api_ts
    assert 'const LEGACY_KEY_STORAGE = "oaix.serviceKey";' in api_ts
    assert "window.localStorage.setItem(KEY_STORAGE, legacy)" in api_ts
    assert "window.localStorage.setItem(LEGACY_KEY_STORAGE, key)" in api_ts
    assert 'headers.set("Authorization", `Bearer ${key}`)' in api_ts


def test_frontend_request_settings_and_dispatch_panels_are_wired() -> None:
    app_tsx = FRONTEND_APP.read_text()
    api_ts = FRONTEND_API.read_text()

    assert "api.requests(80)" in app_tsx
    assert "api.analytics(24)" in app_tsx
    assert "api.settings()" in app_tsx
    assert "api.updateSetting(settingKey.trim(), value)" in app_tsx
    assert "api.updateTokenSelection({ active_stream_cap: streamCap })" in app_tsx
    assert 'requests: (limit = 80)' in api_ts
    assert 'analytics: (hours = 24)' in api_ts
    assert 'settings: ()' in api_ts
    assert 'updateSetting: (key: string, value: unknown)' in api_ts


def test_admin_requests_expired_cache_refreshes_on_request(monkeypatch) -> None:
    api_server._ADMIN_REQUESTS_CACHE.clear()
    monkeypatch.setattr(api_server, "_ADMIN_REQUESTS_CACHE_LOCK", None)
    api_server._ADMIN_REQUESTS_CACHE[80] = (0.0, b'{"items":[{"started_at":"old"}]}')

    async def fake_get_request_log_summary(*, hours):
        return RequestLogSummary(
            total=1,
            successful=1,
            failed=0,
            streaming=1,
            input_tokens=10,
            output_tokens=20,
            total_tokens=30,
            estimated_cost_usd=0.01,
            avg_ttft_ms=1200,
        )

    async def fake_get_request_log_analytics(*, hours, bucket_minutes, top_models):
        return RequestLogAnalytics(
            period_hours=hours,
            bucket_minutes=bucket_minutes,
            buckets=[
                RequestAnalyticsBucket(
                    bucket_start=datetime(2026, 4, 14, 12, tzinfo=timezone.utc),
                    request_count=1,
                    input_tokens=10,
                    output_tokens=20,
                    total_tokens=30,
                    estimated_cost_usd=0.01,
                )
            ],
            models=[
                RequestAnalyticsModel(
                    model_name="gpt-5.5",
                    request_count=1,
                    total_tokens=30,
                    estimated_cost_usd=0.01,
                )
            ],
        )

    async def fake_list_request_logs(*, limit):
        return [
            SimpleNamespace(
                endpoint="/v1/responses",
                model="gpt-5.5",
                model_name="gpt-5.5",
                is_stream=True,
                status_code=200,
                success=True,
                attempt_count=1,
                started_at=datetime(2026, 4, 14, 12, 1, tzinfo=timezone.utc),
                ttft_ms=1200,
                cache_hit_ratio=None,
                prompt_cache_source="explicit",
                cache_affinity_result="primary_hit",
                cache_affinity_lane_index=0,
                error_message=None,
            )
        ]

    monkeypatch.setattr(api_server, "get_request_log_summary", fake_get_request_log_summary)
    monkeypatch.setattr(api_server, "get_request_log_analytics", fake_get_request_log_analytics)
    monkeypatch.setattr(api_server, "list_request_logs", fake_list_request_logs)

    body, cache_status = asyncio.run(
        api_server._cached_admin_requests_payload(create_app(), limit=80),
    )

    payload = json_module.loads(body)
    assert cache_status == "miss"
    assert payload["items"][0]["started_at"] == "2026-04-14T12:01:00+00:00"
    assert b"old" not in body


def test_frontend_uses_state_driven_token_loading_without_stale_closure_requests() -> None:
    app_tsx = FRONTEND_APP.read_text()

    assert "useEffect(() => {" in app_tsx
    assert "window.setTimeout(" in app_tsx
    assert "tokenSearch.trim() ? 260 : 0" in app_tsx
    assert "setTokenPage(1)" in app_tsx
    assert "sortParam(tokenSort)" in app_tsx
    assert "params.set(\"status\", tokenStatus)" in app_tsx
    assert "params.set(\"q\", tokenSearch.trim())" in app_tsx


def test_frontend_token_cards_show_status_timestamps_and_coss_badges() -> None:
    app_tsx = FRONTEND_APP.read_text()

    assert "function tokenStatusOf" in app_tsx
    assert "item.disabled_at" in app_tsx
    assert "item.cooldown_until" in app_tsx
    assert "最近使用 {formatDate(item.last_used_at)}" in app_tsx
    assert "冷却至 {formatDate(item.cooldown_until)}" in app_tsx
    assert "statusBadge(status)" in app_tsx
    assert "<Badge" in app_tsx


def test_frontend_delete_and_remark_dialogs_use_coss_dialogs() -> None:
    app_tsx = FRONTEND_APP.read_text()

    assert "function DeleteDialog" in app_tsx
    assert "function RemarkDialog" in app_tsx
    assert "<Dialog open={Boolean(target)}" in app_tsx
    assert "<DialogPopup" in app_tsx
    assert "<DialogFooter>" in app_tsx
    assert "api.updateRemark(remarkTarget.id, remarkTarget.remark)" in app_tsx
    assert "api.batchTokens({ action: \"delete\", token_ids: deleteTarget.ids })" in app_tsx


def test_frontend_fetch_errors_preserve_backend_detail_messages() -> None:
    api_ts = FRONTEND_API.read_text()

    assert "payload?.detail?.message" in api_ts
    assert "payload?.detail" in api_ts
    assert "response.statusText" in api_ts
    assert "throw Object.assign(new Error(String(message))" in api_ts


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


def test_token_selection_plan_order_route_forwards_plan_order(monkeypatch) -> None:
    monkeypatch.delenv("SERVICE_API_KEYS", raising=False)
    monkeypatch.delenv("API_KEY", raising=False)
    app = create_app()
    captured: dict[str, object] = {}

    async def fake_update_token_plan_order_settings(*, enabled, plan_order):
        captured["enabled"] = enabled
        captured["plan_order"] = plan_order
        return TokenSelectionSettings(
            strategy="least_recently_used",
            plan_order_enabled=True,
            plan_order=("team", "plus", "pro", "free"),
        )

    monkeypatch.setattr(
        "oaix_gateway.api_server.update_token_plan_order_settings",
        fake_update_token_plan_order_settings,
    )

    response = asyncio.run(
        _request(
            app,
            "POST",
            "/admin/token-selection/plan-order",
            json={"enabled": True, "plan_order": ["team", "plus", "pro", "free"]},
        )
    )

    assert response.status_code == 200
    assert captured == {"enabled": True, "plan_order": ["team", "plus", "pro", "free"]}
    assert response.json()["plan_order_enabled"] is True
    assert response.json()["plan_order"] == ["team", "plus", "pro", "free"]


def test_token_selection_concurrency_route_forwards_active_stream_cap(monkeypatch) -> None:
    monkeypatch.delenv("SERVICE_API_KEYS", raising=False)
    monkeypatch.delenv("API_KEY", raising=False)
    app = create_app()
    captured: dict[str, object] = {}

    async def fake_update_token_active_stream_cap_settings(*, active_stream_cap):
        captured["active_stream_cap"] = active_stream_cap
        return TokenSelectionSettings(
            strategy="fill_first",
            token_order=(7,),
            active_stream_cap=4,
        )

    monkeypatch.setattr(
        "oaix_gateway.api_server.update_token_active_stream_cap_settings",
        fake_update_token_active_stream_cap_settings,
    )

    response = asyncio.run(
        _request(
            app,
            "POST",
            "/admin/token-selection/concurrency",
            json={"active_stream_cap": 4},
        )
    )

    assert response.status_code == 200
    assert captured == {"active_stream_cap": 4}
    assert response.json()["strategy"] == "fill_first"
    assert response.json()["active_stream_cap"] == 4


def test_admin_tokens_route_includes_import_batch_summaries(monkeypatch) -> None:
    monkeypatch.delenv("SERVICE_API_KEYS", raising=False)
    monkeypatch.delenv("API_KEY", raising=False)
    app = create_app()

    async def fake_get_token_counts():
        return TokenCounts(total=21, active=20, available=19, cooling=1, disabled=1)

    async def fake_get_token_status_counts(**kwargs):
        assert kwargs == {"search": "acct", "plan_type": "plus", "token_ids": None}
        return TokenCounts(total=21, active=20, available=19, cooling=1, disabled=1)

    async def fake_get_token_plan_counts(**kwargs):
        assert kwargs == {"search": "acct", "status": "available", "token_ids": None}
        return TokenPlanCounts(free=1, plus=20, team=0, pro=0, unknown=0)

    async def fake_count_token_rows(**kwargs):
        assert kwargs == {"search": "acct", "status": "available", "plan_type": "plus", "token_ids": None}
        return 21

    async def fake_list_token_rows(*, limit, offset, search, status, plan_type, sort, token_ids, include_credentials=True):
        assert limit == 10
        assert offset == 20
        assert search == "acct"
        assert status == "available"
        assert plan_type == "plus"
        assert sort == "account"
        assert token_ids is None
        assert include_credentials is False
        return [SimpleNamespace(id=7)]

    async def fake_build_admin_token_items(app, *, token_rows, include_quota, include_observed_cost=True):
        del app
        assert include_quota is False
        assert include_observed_cost is False
        assert [item.id for item in token_rows] == [7]
        return [{"id": 7, "email": "a@example.com"}]

    summary_calls: list[int] = []

    async def fake_list_token_import_batch_summaries(*, limit, include_observed_cost=False):
        assert limit == 30
        assert include_observed_cost is False
        summary_calls.append(limit)
        submitted_at = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
        return [
            TokenImportBatchSummary(
                id=42,
                status="completed",
                import_queue_position="front",
                total_count=3,
                processed_count=3,
                created_count=2,
                updated_count=1,
                skipped_count=0,
                failed_count=0,
                token_count=3,
                available=1,
                cooling=1,
                disabled=1,
                missing=0,
                token_ids=(7, 8, 9),
                submitted_at=submitted_at,
                started_at=submitted_at,
                finished_at=submitted_at,
            )
        ]

    monkeypatch.setattr("oaix_gateway.api_server.get_token_counts", fake_get_token_counts)
    monkeypatch.setattr("oaix_gateway.api_server.get_token_status_counts", fake_get_token_status_counts)
    monkeypatch.setattr("oaix_gateway.api_server.get_token_plan_counts", fake_get_token_plan_counts)
    monkeypatch.setattr("oaix_gateway.api_server.count_token_rows", fake_count_token_rows)
    monkeypatch.setattr("oaix_gateway.api_server.list_token_rows", fake_list_token_rows)
    monkeypatch.setattr("oaix_gateway.api_server._build_admin_token_items", fake_build_admin_token_items)
    monkeypatch.setattr(
        "oaix_gateway.api_server.list_token_import_batch_summaries",
        fake_list_token_import_batch_summaries,
    )

    response = asyncio.run(
        _request(
            app,
            "GET",
            "/admin/tokens?limit=10&offset=20&include_import_batches=1&q=acct&status=available&plan_type=plus&sort=account",
        )
    )

    assert response.status_code == 200
    body = response.json()
    assert body["items"] == [{"id": 7, "email": "a@example.com"}]
    assert body["pagination"] == {
        "limit": 10,
        "offset": 20,
        "returned": 1,
        "total": 21,
        "page": 3,
        "total_pages": 3,
        "has_previous": True,
        "has_next": False,
    }
    assert body["import_batches"][0]["id"] == 42
    assert body["import_batches"][0]["available"] == 1
    assert body["import_batches"][0]["cooling"] == 1
    assert body["import_batches"][0]["disabled"] == 1
    assert body["import_batches"][0]["token_ids"] == [7, 8, 9]
    assert body["import_batches"][0]["observed_cost_usd"] == 0.0
    assert body["import_batches"][0]["average_observed_cost_usd"] is None
    assert body["import_batches_loaded"] is True
    assert body["filtered_counts"]["available"] == 19
    assert body["plan_counts"]["plus"] == 20
    assert body["query"] == {
        "q": "acct",
        "status": "available",
        "plan_type": "plus",
        "sort": "account",
        "import_batch_id": None,
    }
    assert summary_calls == [30]

    response = asyncio.run(
        _request(
            app,
            "GET",
            "/admin/tokens?limit=10&offset=20&q=acct&status=available&plan_type=plus&sort=account",
        )
    )

    assert response.status_code == 200
    body = response.json()
    assert body["import_batches_loaded"] is False
    assert body["import_batches"] == []
    assert summary_calls == [30]


def test_admin_tokens_route_reuses_global_counts_for_default_filters(monkeypatch) -> None:
    monkeypatch.delenv("SERVICE_API_KEYS", raising=False)
    monkeypatch.delenv("API_KEY", raising=False)
    app = create_app()

    async def fake_get_token_counts():
        return TokenCounts(total=21, active=20, available=19, cooling=1, disabled=1)

    async def fail_get_token_status_counts(**kwargs):
        raise AssertionError(f"default filters should reuse global counts: {kwargs}")

    async def fake_get_token_plan_counts(**kwargs):
        assert kwargs == {"search": "", "status": "available", "token_ids": None}
        return TokenPlanCounts(free=1, plus=18, team=0, pro=1, unknown=0)

    async def fail_count_token_rows(**kwargs):
        raise AssertionError(f"default filters should derive total from global counts: {kwargs}")

    async def fake_list_token_rows(*, limit, offset, search, status, plan_type, sort, token_ids, include_credentials=True):
        assert limit == 10
        assert offset == 0
        assert search == ""
        assert status == "available"
        assert plan_type == "all"
        assert sort == "-created_at"
        assert token_ids is None
        assert include_credentials is False
        return [SimpleNamespace(id=7)]

    async def fake_build_admin_token_items(app, *, token_rows, include_quota, include_observed_cost=True):
        del app
        assert include_quota is False
        assert include_observed_cost is False
        assert [item.id for item in token_rows] == [7]
        return [{"id": 7, "email": "a@example.com"}]

    monkeypatch.setattr("oaix_gateway.api_server.get_token_counts", fake_get_token_counts)
    monkeypatch.setattr("oaix_gateway.api_server.get_token_status_counts", fail_get_token_status_counts)
    monkeypatch.setattr("oaix_gateway.api_server.get_token_plan_counts", fake_get_token_plan_counts)
    monkeypatch.setattr("oaix_gateway.api_server.count_token_rows", fail_count_token_rows)
    monkeypatch.setattr("oaix_gateway.api_server.list_token_rows", fake_list_token_rows)
    monkeypatch.setattr("oaix_gateway.api_server._build_admin_token_items", fake_build_admin_token_items)

    response = asyncio.run(_request(app, "GET", "/admin/tokens?limit=10&offset=0&status=available"))

    assert response.status_code == 200
    body = response.json()
    assert body["filtered_counts"] == {
        "total": 21,
        "active": 20,
        "available": 19,
        "cooling": 1,
        "disabled": 1,
    }
    assert body["pagination"]["total"] == 19
    assert body["pagination"]["total_pages"] == 2
    assert body["items"] == [{"id": 7, "email": "a@example.com"}]


def test_admin_token_import_batches_route_returns_summaries(monkeypatch) -> None:
    monkeypatch.delenv("SERVICE_API_KEYS", raising=False)
    monkeypatch.delenv("API_KEY", raising=False)
    app = create_app()

    async def fake_list_token_import_batch_summaries(*, limit, include_observed_cost=False):
        assert limit == 5
        assert include_observed_cost is False
        submitted_at = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
        return [
            TokenImportBatchSummary(
                id=77,
                status="completed",
                import_queue_position="back",
                total_count=2,
                processed_count=2,
                created_count=2,
                updated_count=0,
                skipped_count=0,
                failed_count=0,
                token_count=2,
                available=2,
                cooling=0,
                disabled=0,
                missing=0,
                token_ids=(11, 12),
                submitted_at=submitted_at,
                started_at=submitted_at,
                finished_at=submitted_at,
            )
        ]

    monkeypatch.setattr(
        "oaix_gateway.api_server.list_token_import_batch_summaries",
        fake_list_token_import_batch_summaries,
    )

    response = asyncio.run(_request(app, "GET", "/admin/tokens/import-batches?limit=5"))

    assert response.status_code == 200
    body = response.json()
    assert body["items"][0]["id"] == 77
    assert body["items"][0]["token_ids"] == [11, 12]
    assert body["items"][0]["average_observed_cost_usd"] is None


def test_admin_token_import_batch_costs_route_returns_summaries(monkeypatch) -> None:
    monkeypatch.delenv("SERVICE_API_KEYS", raising=False)
    monkeypatch.delenv("API_KEY", raising=False)
    app = create_app()

    async def fake_list_token_import_batch_summaries_by_ids(batch_ids, *, include_observed_cost=False):
        assert batch_ids == (77, 42)
        assert include_observed_cost is True
        submitted_at = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
        return [
            TokenImportBatchSummary(
                id=77,
                status="completed",
                import_queue_position="front",
                total_count=2,
                processed_count=2,
                created_count=2,
                updated_count=0,
                skipped_count=0,
                failed_count=0,
                token_count=2,
                available=2,
                cooling=0,
                disabled=0,
                missing=0,
                token_ids=(11, 12),
                submitted_at=submitted_at,
                started_at=submitted_at,
                finished_at=submitted_at,
                observed_cost_usd=4.5,
                average_observed_cost_usd=2.25,
            )
        ]

    monkeypatch.setattr(
        "oaix_gateway.api_server.list_token_import_batch_summaries_by_ids",
        fake_list_token_import_batch_summaries_by_ids,
    )

    response = asyncio.run(_request(app, "GET", "/admin/tokens/import-batches/costs?ids=77,42"))

    assert response.status_code == 200
    body = response.json()
    assert body["items"] == [
        {
            "id": 77,
            "status": "completed",
            "import_queue_position": "front",
            "total_count": 2,
            "processed_count": 2,
            "created_count": 2,
            "updated_count": 0,
            "skipped_count": 0,
            "failed_count": 0,
            "token_count": 2,
            "available": 2,
            "cooling": 0,
            "disabled": 0,
            "missing": 0,
            "token_ids": [11, 12],
            "submitted_at": "2026-05-01T12:00:00Z",
            "started_at": "2026-05-01T12:00:00Z",
            "finished_at": "2026-05-01T12:00:00Z",
            "observed_cost_usd": 4.5,
            "average_observed_cost_usd": 2.25,
        }
    ]


def test_admin_tokens_route_includes_selected_import_batch_failed_items(monkeypatch) -> None:
    monkeypatch.delenv("SERVICE_API_KEYS", raising=False)
    monkeypatch.delenv("API_KEY", raising=False)
    app = create_app()

    async def fake_get_token_counts():
        return TokenCounts(total=3, active=2, available=1, cooling=1, disabled=1)

    async def fake_get_token_status_counts(**kwargs):
        assert kwargs == {"search": "", "plan_type": "all", "token_ids": (7,)}
        return TokenCounts(total=3, active=2, available=1, cooling=1, disabled=1)

    async def fake_get_token_plan_counts(**kwargs):
        assert kwargs == {"search": "", "status": "available", "token_ids": (7,)}
        return TokenPlanCounts(free=1, plus=1, team=1, pro=0, unknown=0)

    async def fake_count_token_rows(**kwargs):
        assert kwargs == {"search": "", "status": "available", "plan_type": "all", "token_ids": (7,)}
        return 1

    async def fake_list_token_rows(*, limit, offset, search, status, plan_type, sort, token_ids, include_credentials=True):
        assert limit == 10
        assert offset == 0
        assert search == ""
        assert status == "available"
        assert plan_type == "all"
        assert sort == "-created_at"
        assert token_ids == (7,)
        assert include_credentials is False
        return [SimpleNamespace(id=7)]

    async def fake_build_admin_token_items(app, *, token_rows, include_quota, include_observed_cost=True):
        del app
        assert include_quota is False
        assert include_observed_cost is False
        assert [item.id for item in token_rows] == [7]
        return [{"id": 7, "email": "a@example.com"}]

    async def fake_list_token_import_batch_summaries(*, limit, include_observed_cost=False):
        assert limit == 30
        assert include_observed_cost is False
        submitted_at = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
        return [
            TokenImportBatchSummary(
                id=42,
                status="completed",
                import_queue_position="front",
                total_count=3,
                processed_count=3,
                created_count=1,
                updated_count=0,
                skipped_count=0,
                failed_count=2,
                token_count=1,
                available=1,
                cooling=0,
                disabled=0,
                missing=0,
                token_ids=(7,),
                submitted_at=submitted_at,
                started_at=submitted_at,
                finished_at=submitted_at,
            )
        ]

    async def fake_list_token_import_batch_failed_items(job_id):
        assert job_id == 42
        return [
            {
                "index": 1,
                "status": "failed",
                "error": "Token payload missing refresh_token",
                "account_id": "acct-1",
                "refresh_token_hash": "abcd1234abcd1234",
            },
            {
                "index": 2,
                "status": "failed",
                "error": "Quota request failed",
            },
        ]

    monkeypatch.setattr("oaix_gateway.api_server.get_token_counts", fake_get_token_counts)
    monkeypatch.setattr("oaix_gateway.api_server.get_token_status_counts", fake_get_token_status_counts)
    monkeypatch.setattr("oaix_gateway.api_server.get_token_plan_counts", fake_get_token_plan_counts)
    monkeypatch.setattr("oaix_gateway.api_server.count_token_rows", fake_count_token_rows)
    monkeypatch.setattr("oaix_gateway.api_server.list_token_rows", fake_list_token_rows)
    monkeypatch.setattr("oaix_gateway.api_server._build_admin_token_items", fake_build_admin_token_items)
    monkeypatch.setattr(
        "oaix_gateway.api_server.list_token_import_batch_summaries",
        fake_list_token_import_batch_summaries,
    )
    monkeypatch.setattr(
        "oaix_gateway.api_server.list_token_import_batch_failed_items",
        fake_list_token_import_batch_failed_items,
    )

    response = asyncio.run(_request(app, "GET", "/admin/tokens?limit=10&offset=0&status=available&import_batch_id=42"))

    assert response.status_code == 200
    body = response.json()
    assert body["selected_import_batch"]["id"] == 42
    assert body["selected_import_batch"]["failed_items"] == [
        {
            "index": 1,
            "status": "failed",
            "error": "Token payload missing refresh_token",
            "account_id": "acct-1",
            "refresh_token_hash": "abcd1234abcd1234",
        },
        {
            "index": 2,
            "status": "failed",
            "error": "Quota request failed",
        },
    ]
    assert body["query"]["import_batch_id"] == 42


def test_admin_token_quota_route_forwards_requested_ids(monkeypatch) -> None:
    monkeypatch.delenv("SERVICE_API_KEYS", raising=False)
    monkeypatch.delenv("API_KEY", raising=False)
    app = create_app()

    async def fake_list_token_rows(*, limit, offset, token_ids, **kwargs):
        assert limit == 2
        assert offset == 0
        assert token_ids == (7, 12)
        assert kwargs == {}
        return [SimpleNamespace(id=7), SimpleNamespace(id=12)]

    async def fake_build_admin_token_quota_items(app_arg, *, token_rows):
        assert app_arg is app
        assert [item.id for item in token_rows] == [7, 12]
        return [
            {"id": 7, "quota": {"windows": []}},
            {"id": 12, "quota": None},
        ]

    monkeypatch.setattr("oaix_gateway.api_server.list_token_rows", fake_list_token_rows)
    monkeypatch.setattr("oaix_gateway.api_server._build_admin_token_quota_items", fake_build_admin_token_quota_items)

    response = asyncio.run(_request(app, "GET", "/admin/tokens/quota?ids=7,12,7"))

    assert response.status_code == 200
    assert response.json() == {
        "items": [
            {"id": 7, "quota": {"windows": []}},
            {"id": 12, "quota": None},
        ]
    }

    response = asyncio.run(_request(app, "GET", "/admin/tokens/quota?ids=7,nope"))
    assert response.status_code == 400


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
        "disabled_at": None,
        "counts": {
            "total": 3,
            "active": 2,
            "available": 2,
            "cooling": 0,
            "disabled": 1,
        },
    }


def test_token_remark_route_forwards_remark(monkeypatch) -> None:
    monkeypatch.delenv("SERVICE_API_KEYS", raising=False)
    monkeypatch.delenv("API_KEY", raising=False)
    app = create_app()
    captured: dict[str, object] = {}
    updated_at = datetime(2026, 6, 17, 8, 30, tzinfo=timezone.utc)

    async def fake_update_token_remark(token_id: int, *, remark: str | None) -> CodexToken | None:
        captured["token_id"] = token_id
        captured["remark"] = remark
        token = CodexToken(
            id=token_id,
            token_type="codex",
            is_active=True,
            refresh_token="rt_12",
            remark=remark,
        )
        token.updated_at = updated_at
        return token

    monkeypatch.setattr("oaix_gateway.api_server.update_token_remark", fake_update_token_remark)

    response = asyncio.run(
        _request(
            app,
            "POST",
            "/admin/tokens/12/remark",
            json={"remark": "internal batch A"},
        )
    )

    assert response.status_code == 200
    assert captured == {"token_id": 12, "remark": "internal batch A"}
    assert response.json()["id"] == 12
    assert response.json()["remark"] == "internal batch A"
    assert response.json()["updated_at"] == "2026-06-17T08:30:00Z"
