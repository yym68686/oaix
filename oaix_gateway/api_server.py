import json
import logging
import os
import uuid
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict

from .database import close_database, init_db
from .oauth import CodexOAuthManager
from .token_store import (
    claim_next_active_token,
    get_token_counts,
    list_tokens,
    mark_token_error,
    mark_token_success,
    upsert_token_payload,
)


logger = logging.getLogger("oaix.gateway")
WEB_DIR = Path(__file__).resolve().parent / "web"


class ResponsesRequest(BaseModel):
    model: str
    input: Any
    stream: bool | None = None

    model_config = ConfigDict(extra="allow")


def _int_env(name: str, default: int, *, minimum: int = 0) -> int:
    raw = str(os.getenv(name, "") or "").strip()
    try:
        value = int(raw) if raw else int(default)
    except ValueError:
        value = int(default)
    return max(minimum, value)


def _max_request_account_retries() -> int:
    return _int_env("MAX_REQUEST_ACCOUNT_RETRIES", 100, minimum=1)


def _default_usage_limit_cooldown_seconds() -> int:
    return _int_env("DEFAULT_USAGE_LIMIT_COOLDOWN_SECONDS", 300, minimum=1)


def _compact_server_error_cooldown_seconds() -> int:
    return _int_env("COMPACT_SERVER_ERROR_COOLDOWN_SECONDS", 60, minimum=0)


def _get_service_api_keys() -> set[str]:
    raw = os.getenv("SERVICE_API_KEYS") or os.getenv("API_KEY") or ""
    return {item.strip() for item in raw.split(",") if item.strip()}


async def verify_service_api_key(http_request: Request) -> None:
    valid_keys = _get_service_api_keys()
    if not valid_keys:
        return

    header = (http_request.headers.get("Authorization") or "").strip()
    scheme, _, token = header.partition(" ")
    if scheme.lower() != "bearer" or token.strip() not in valid_keys:
        raise HTTPException(status_code=401, detail="Invalid or missing bearer token")


def _normalize_responses_upstream_url(base_url: str) -> str:
    base = (base_url or "").strip()
    if not base:
        return "https://chatgpt.com/backend-api/codex/responses"

    base = base.rstrip("/")
    if base.endswith("/v1/responses") or base.endswith("/responses"):
        return base
    return f"{base}/responses"


def _normalize_responses_compact_upstream_url(base_url: str) -> str:
    base = (base_url or "").strip()
    if not base:
        return "https://chatgpt.com/backend-api/codex/responses/compact"

    base = base.rstrip("/")
    if base.endswith("/v1/responses/compact") or base.endswith("/responses/compact"):
        return base

    base = _normalize_responses_upstream_url(base)
    if base.endswith("/v1/responses") or base.endswith("/responses"):
        return f"{base}/compact"
    if base.endswith("/compact"):
        return base
    return f"{base}/compact"


def _codex_responses_url(*, compact: bool = False) -> str:
    base = os.getenv("CODEX_BASE_URL") or ""
    if compact:
        return _normalize_responses_compact_upstream_url(base)
    return _normalize_responses_upstream_url(base)


def _build_upstream_headers(http_request: Request, access_token: str, account_id: str | None, stream: bool) -> dict[str, str]:
    session_id = (http_request.headers.get("Session_id") or "").strip() or str(uuid.uuid4())
    conversation_id = (http_request.headers.get("Conversation_id") or "").strip() or session_id

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Openai-Beta": (http_request.headers.get("Openai-Beta") or "responses=experimental").strip(),
        "Originator": (http_request.headers.get("Originator") or "codex_cli_rs").strip(),
        "Version": (http_request.headers.get("Version") or "0.21.0").strip(),
        "Session_id": session_id,
        "Conversation_id": conversation_id,
        "User-Agent": (http_request.headers.get("User-Agent") or "codex_cli_rs/0.50.0").strip(),
        "Connection": "Keep-Alive",
        "Accept": "text/event-stream" if stream else "application/json",
    }
    if account_id:
        headers["Chatgpt-Account-Id"] = account_id
    return headers


def _sanitize_codex_payload(payload: dict[str, Any], *, compact: bool = False) -> dict[str, Any]:
    payload.pop("max_output_tokens", None)
    payload.pop("response_format", None)
    payload.pop("previous_response_id", None)
    payload.pop("prompt_cache_retention", None)
    payload.pop("safety_identifier", None)
    if compact:
        payload.pop("store", None)
    payload.setdefault("instructions", "")
    return payload


def _decode_error_body(raw: bytes) -> str:
    try:
        return raw.decode("utf-8", errors="replace").strip()
    except Exception:
        return repr(raw)


def _extract_usage_limit_cooldown_seconds(status_code: int, error_text: str) -> int | None:
    if status_code != 429:
        return None

    try:
        payload = json.loads(error_text)
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None

    error = payload.get("error")
    if not isinstance(error, dict):
        return None

    error_type = str(error.get("type") or "").strip()
    error_message = str(error.get("message") or "").strip().lower()
    if error_type != "usage_limit_reached" and "usage limit" not in error_message:
        return None

    resets_in_seconds = error.get("resets_in_seconds")
    if resets_in_seconds is not None:
        try:
            return max(0, int(float(resets_in_seconds)))
        except Exception:
            pass

    resets_at = error.get("resets_at")
    if resets_at is not None:
        try:
            reset_at_epoch = int(float(resets_at))
            now_epoch = int(datetime.now(timezone.utc).timestamp())
            return max(0, reset_at_epoch - now_epoch)
        except Exception:
            pass

    return _default_usage_limit_cooldown_seconds()


def _is_permanent_account_disable_error(status_code: int, error_text: str) -> bool:
    if status_code not in (401, 402):
        return False

    try:
        payload = json.loads(error_text)
    except Exception:
        return False
    if not isinstance(payload, dict):
        return False

    if status_code == 402:
        detail = payload.get("detail")
        if isinstance(detail, dict):
            detail_code = str(detail.get("code") or "").strip()
            if detail_code == "deactivated_workspace":
                return True

    if status_code == 401:
        error = payload.get("error")
        if isinstance(error, dict):
            error_code = str(error.get("code") or "").strip()
            if error_code == "account_deactivated":
                return True

    return False


def _get_compact_codex_server_error_cooling_time(
    *,
    compact: bool,
    status_code: int,
    error_text: str,
) -> int:
    if not compact:
        return 0
    if status_code < 500:
        return 0
    if _extract_usage_limit_cooldown_seconds(status_code, error_text) is not None:
        return 0
    return _compact_server_error_cooldown_seconds()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper())
    await init_db()
    app.state.http_client = httpx.AsyncClient(
        follow_redirects=True,
        http2=False,
        limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
    )
    app.state.oauth_manager = CodexOAuthManager()
    try:
        yield
    finally:
        await app.state.http_client.aclose()
        await close_database()


def create_app() -> FastAPI:
    app = FastAPI(title="oaix Gateway", version="0.1.0", lifespan=lifespan)
    app.mount("/assets", StaticFiles(directory=str(WEB_DIR)), name="assets")

    @app.get("/")
    async def index() -> HTMLResponse:
        html = (WEB_DIR / "index.html").read_text(encoding="utf-8")
        css_version = int((WEB_DIR / "styles.css").stat().st_mtime)
        js_version = int((WEB_DIR / "app.js").stat().st_mtime)
        html = html.replace("/assets/styles.css", f"/assets/styles.css?v={css_version}")
        html = html.replace("/assets/app.js", f"/assets/app.js?v={js_version}")
        return HTMLResponse(
            content=html,
            headers={"Cache-Control": "no-store, max-age=0"},
        )

    @app.get("/healthz")
    async def healthz() -> JSONResponse:
        counts = await get_token_counts()
        ok = counts.available > 0
        return JSONResponse(
            status_code=200 if ok else 503,
            content={
                "ok": ok,
                "upstream": _codex_responses_url(),
                "counts": asdict(counts),
                "service_key_protected": bool(_get_service_api_keys()),
            },
        )

    @app.get("/admin/tokens")
    async def list_tokens_route(
        limit: int = Query(100, ge=1, le=500),
        _: None = Depends(verify_service_api_key),
    ) -> dict[str, Any]:
        counts = await get_token_counts()
        items = [asdict(token) for token in await list_tokens(limit=limit)]
        return {
            "counts": asdict(counts),
            "items": items,
        }

    @app.post("/admin/tokens/import")
    async def import_tokens_route(
        http_request: Request,
        _: None = Depends(verify_service_api_key),
    ) -> dict[str, Any]:
        try:
            body = await http_request.json()
        except json.JSONDecodeError as exc:
            raise HTTPException(status_code=400, detail="Request body must be valid JSON") from exc

        if isinstance(body, dict) and isinstance(body.get("tokens"), list):
            payloads = body["tokens"]
        elif isinstance(body, list):
            payloads = body
        elif isinstance(body, dict):
            payloads = [body]
        else:
            raise HTTPException(
                status_code=400,
                detail="Body must be a token object, an array of token objects, or {'tokens': [...]}",
            )

        imported: list[dict[str, Any]] = []
        failed: list[dict[str, Any]] = []
        for index, payload in enumerate(payloads):
            if not isinstance(payload, dict):
                failed.append({"index": index, "error": "Token payload must be a JSON object"})
                continue
            try:
                token = await upsert_token_payload(payload)
                imported.append(
                    {
                        "id": token.id,
                        "email": token.email,
                        "account_id": token.account_id,
                    }
                )
            except Exception as exc:
                failed.append({"index": index, "error": str(exc)})

        return {
            "imported_count": len(imported),
            "failed_count": len(failed),
            "imported": imported,
            "failed": failed,
        }

    @app.post("/v1/responses")
    async def responses_route(
        http_request: Request,
        request_data: ResponsesRequest,
        _: None = Depends(verify_service_api_key),
    ) -> Response:
        return await _responses_route_common(http_request, request_data, compact=False)

    @app.post("/v1/responses/compact")
    async def responses_compact_route(
        http_request: Request,
        request_data: ResponsesRequest,
        _: None = Depends(verify_service_api_key),
    ) -> Response:
        return await _responses_route_common(http_request, request_data, compact=True)

    async def _responses_route_common(
        http_request: Request,
        request_data: ResponsesRequest,
        *,
        compact: bool,
    ) -> Response:
        counts = await get_token_counts()
        if counts.available <= 0:
            raise HTTPException(status_code=503, detail="No available Codex token in database")

        max_attempts = max(1, min(counts.available, _max_request_account_retries()))
        client: httpx.AsyncClient = http_request.app.state.http_client
        oauth_manager: CodexOAuthManager = http_request.app.state.oauth_manager
        last_error: HTTPException | None = None

        for attempt in range(1, max_attempts + 1):
            token_row = await claim_next_active_token()
            if token_row is None:
                break

            try:
                access_token, access_token_refreshed = await oauth_manager.get_access_token(token_row, client)
            except HTTPException as exc:
                oauth_manager.invalidate(token_row.id)
                await mark_token_error(token_row.id, str(exc.detail), deactivate=exc.status_code in (401, 403))
                if exc.status_code in (401, 403):
                    last_error = exc
                    continue
                raise

            try:
                response = await _proxy_request_with_token(
                    client,
                    http_request,
                    request_data,
                    access_token=access_token,
                    account_id=token_row.account_id,
                    compact=compact,
                )
                await mark_token_success(token_row.id)
                return response
            except HTTPException as exc:
                status_code = getattr(exc, "status_code", 500)
                detail = str(getattr(exc, "detail", "") or exc)
                cooldown_seconds = _extract_usage_limit_cooldown_seconds(status_code, detail)
                permanently_disabled = _is_permanent_account_disable_error(status_code, detail)
                auth_failed_after_refresh = status_code in (401, 403) and access_token_refreshed

                if cooldown_seconds is not None:
                    await mark_token_error(
                        token_row.id,
                        detail,
                        cooldown_seconds=cooldown_seconds,
                    )
                    logger.warning(
                        "Codex account cooled down: token_id=%s account_id=%s cooldown_seconds=%s attempt=%s/%s",
                        token_row.id,
                        token_row.account_id,
                        cooldown_seconds,
                        attempt,
                        max_attempts,
                    )
                    last_error = exc
                    continue

                compact_server_error_cooling_time = _get_compact_codex_server_error_cooling_time(
                    compact=compact,
                    status_code=status_code,
                    error_text=detail,
                )
                if compact_server_error_cooling_time > 0 and attempt < max_attempts:
                    await mark_token_error(
                        token_row.id,
                        detail,
                        cooldown_seconds=compact_server_error_cooling_time,
                    )
                    logger.warning(
                        "Codex compact key cooled after upstream server error: token_id=%s account_id=%s status=%s cooldown_seconds=%s attempt=%s/%s",
                        token_row.id,
                        token_row.account_id,
                        status_code,
                        compact_server_error_cooling_time,
                        attempt,
                        max_attempts,
                    )
                    last_error = exc
                    continue

                if permanently_disabled or auth_failed_after_refresh:
                    oauth_manager.invalidate(token_row.id)
                    await mark_token_error(
                        token_row.id,
                        detail,
                        deactivate=True,
                        clear_access_token=True,
                    )
                    logger.warning(
                        "Codex account disabled after upstream auth failure: token_id=%s account_id=%s status=%s refreshed=%s attempt=%s/%s",
                        token_row.id,
                        token_row.account_id,
                        status_code,
                        access_token_refreshed,
                        attempt,
                        max_attempts,
                    )
                    last_error = exc
                    continue

                if status_code in (401, 403):
                    oauth_manager.invalidate(token_row.id)
                    await mark_token_error(token_row.id, detail, clear_access_token=True)
                    logger.warning(
                        "Codex upstream auth failed; invalidated access token for retry: token_id=%s account_id=%s status=%s attempt=%s/%s",
                        token_row.id,
                        token_row.account_id,
                        status_code,
                        attempt,
                        max_attempts,
                    )
                    last_error = exc
                    continue

                raise
            except httpx.HTTPError as exc:
                message = f"Upstream request failed: {type(exc).__name__}: {exc}"
                compact_server_error_cooling_time = _get_compact_codex_server_error_cooling_time(
                    compact=compact,
                    status_code=500,
                    error_text=message,
                )
                if compact_server_error_cooling_time > 0 and attempt < max_attempts:
                    await mark_token_error(
                        token_row.id,
                        message,
                        cooldown_seconds=compact_server_error_cooling_time,
                    )
                    logger.warning(
                        "Codex compact key cooled after upstream transport error: token_id=%s account_id=%s error_type=%s cooldown_seconds=%s attempt=%s/%s",
                        token_row.id,
                        token_row.account_id,
                        type(exc).__name__,
                        compact_server_error_cooling_time,
                        attempt,
                        max_attempts,
                    )
                    last_error = HTTPException(status_code=502, detail=message)
                    continue
                await mark_token_error(token_row.id, message)
                raise HTTPException(status_code=502, detail=message) from exc

        if last_error is not None:
            raise HTTPException(
                status_code=503,
                detail=f"All available Codex accounts are exhausted or cooling down. Last error: {last_error.detail}",
            )
        raise HTTPException(status_code=503, detail="No available Codex token could satisfy this request")

    return app


async def _stream_upstream_response(
    stream_cm,
    upstream_response: httpx.Response,
) -> AsyncGenerator[bytes, None]:
    try:
        async for chunk in upstream_response.aiter_raw():
            yield chunk
    finally:
        await stream_cm.__aexit__(None, None, None)


async def _proxy_request_with_token(
    client: httpx.AsyncClient,
    http_request: Request,
    request_data: ResponsesRequest,
    *,
    access_token: str,
    account_id: str | None,
    compact: bool = False,
) -> Response:
    payload = _sanitize_codex_payload(request_data.model_dump(exclude_unset=True), compact=compact)
    headers = _build_upstream_headers(
        http_request,
        access_token=access_token,
        account_id=account_id,
        stream=bool(request_data.stream),
    )
    upstream_url = _codex_responses_url(compact=compact)
    json_payload = json.dumps(payload, ensure_ascii=False)

    if request_data.stream:
        stream_cm = client.stream(
            "POST",
            upstream_url,
            headers=headers,
            content=json_payload,
            timeout=httpx.Timeout(connect=30.0, read=None, write=30.0, pool=30.0),
        )
        upstream_response = await stream_cm.__aenter__()
        if upstream_response.status_code < 200 or upstream_response.status_code >= 300:
            raw = await upstream_response.aread()
            await stream_cm.__aexit__(None, None, None)
            raise HTTPException(
                status_code=upstream_response.status_code,
                detail=_decode_error_body(raw),
            )

        return StreamingResponse(
            _stream_upstream_response(stream_cm, upstream_response),
            media_type="text/event-stream",
        )

    upstream_response = await client.post(
        upstream_url,
        headers=headers,
        content=json_payload,
        timeout=httpx.Timeout(connect=30.0, read=300.0, write=30.0, pool=30.0),
    )
    if upstream_response.status_code < 200 or upstream_response.status_code >= 300:
        raw = await upstream_response.aread()
        raise HTTPException(
            status_code=upstream_response.status_code,
            detail=_decode_error_body(raw),
        )

    try:
        data = upstream_response.json()
    except json.JSONDecodeError:
        return Response(
            content=upstream_response.content,
            status_code=upstream_response.status_code,
            media_type=upstream_response.headers.get("content-type", "application/octet-stream"),
        )
    return JSONResponse(status_code=upstream_response.status_code, content=data)


app = create_app()


def main() -> None:
    uvicorn.run(
        "oaix_gateway.api_server:app",
        host=os.getenv("HOST", "0.0.0.0"),
        port=int(os.getenv("PORT", "8000")),
    )
