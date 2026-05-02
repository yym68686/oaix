import asyncio
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

from sqlalchemy.dialects import postgresql

from oaix_gateway.database import CodexToken
from oaix_gateway.token_store import (
    TOKEN_SELECTION_STRATEGY_FILL_FIRST,
    TokenHistoryRepairSummary,
    TokenSelectionSettings,
    _FILL_FIRST_TOKEN_CACHE,
    _FILL_FIRST_TOKEN_REFRESH_TASKS,
    _FillFirstTokenCacheEntry,
    _TOKEN_RUNTIME_STATE,
    _apply_token_runtime_error_state,
    _build_token_counts_stmt,
    _merge_duplicate_token_rows,
    claim_next_active_token,
    invalidate_fill_first_token_cache,
    prewarm_fill_first_token_cache,
    repair_duplicate_token_histories,
    set_token_active_state,
    update_token_refresh_state,
)


class _FakeTransaction:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False


class _FakeSession:
    def begin(self) -> _FakeTransaction:
        return _FakeTransaction()

    async def flush(self) -> None:
        return None

    async def close(self) -> None:
        return None


class _FakeScalarResult:
    def __init__(self, value) -> None:
        self._value = value

    def first(self):
        return self._value


class _FakeResult:
    def __init__(self, value) -> None:
        self._value = value

    def scalars(self) -> _FakeScalarResult:
        return _FakeScalarResult(self._value)


class _FakeReadSession:
    def __init__(self, value) -> None:
        self._value = value
        self.statements: list[str] = []

    async def execute(self, stmt) -> _FakeResult:
        self.statements.append(
            str(
                stmt.compile(
                    dialect=postgresql.dialect(),
                    compile_kwargs={"literal_binds": True},
                )
            )
        )
        return _FakeResult(self._value)


def test_build_token_counts_stmt_uses_single_aggregate_query() -> None:
    stmt = _build_token_counts_stmt(datetime(2026, 4, 25, tzinfo=timezone.utc))

    sql = str(
        stmt.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )

    assert "count(*) AS total" in sql
    assert "sum(CASE WHEN (codex_tokens.is_active IS true)" in sql
    assert "codex_tokens.type = 'codex'" in sql
    assert sql.count("FROM codex_tokens") == 1


def test_claim_next_active_token_fill_first_is_read_only_and_uses_app_token_order(monkeypatch) -> None:
    invalidate_fill_first_token_cache()
    token = CodexToken(
        id=7,
        refresh_token="rt_7",
        token_type="codex",
        is_active=True,
        last_used_at=None,
    )
    read_session = _FakeReadSession(token)

    @asynccontextmanager
    async def fake_get_read_session():
        yield read_session

    @asynccontextmanager
    async def fake_get_session():
        raise AssertionError("fill_first claim should use the read-only session")
        yield

    monkeypatch.setattr("oaix_gateway.token_store.get_read_session", fake_get_read_session)
    monkeypatch.setattr("oaix_gateway.token_store.get_session", fake_get_session)

    result = asyncio.run(
        claim_next_active_token(
            selection_strategy=TOKEN_SELECTION_STRATEGY_FILL_FIRST,
            token_order=(9, 7, 5),
        )
    )

    assert result is token
    assert token.last_used_at is None
    assert len(read_session.statements) == 1
    sql = read_session.statements[0]
    assert "FROM codex_tokens" in sql
    assert "gateway_settings" not in sql
    assert "FOR UPDATE" not in sql
    assert "CASE codex_tokens.id" in sql
    invalidate_fill_first_token_cache()


def test_claim_next_active_token_filters_requested_scoped_cooldown(monkeypatch) -> None:
    invalidate_fill_first_token_cache()
    token = CodexToken(
        id=8,
        refresh_token="rt_8",
        token_type="codex",
        is_active=True,
        last_used_at=None,
    )
    read_session = _FakeReadSession(token)

    @asynccontextmanager
    async def fake_get_read_session():
        yield read_session

    monkeypatch.setattr("oaix_gateway.token_store.get_read_session", fake_get_read_session)

    result = asyncio.run(
        claim_next_active_token(
            selection_strategy=TOKEN_SELECTION_STRATEGY_FILL_FIRST,
            token_order=(8,),
            scoped_cooldown_scope="gpt-image-2:input-images",
        )
    )

    assert result is token
    sql = read_session.statements[0]
    assert "codex_token_scoped_cooldowns" in sql
    assert "gpt-image-2:input-images" in sql
    invalidate_fill_first_token_cache()


def test_claim_next_active_token_fill_first_uses_short_ttl_cache(monkeypatch) -> None:
    invalidate_fill_first_token_cache()
    token = CodexToken(
        id=9,
        refresh_token="rt_9",
        token_type="codex",
        is_active=True,
        last_used_at=None,
    )
    read_session = _FakeReadSession(token)

    @asynccontextmanager
    async def fake_get_read_session():
        yield read_session

    monkeypatch.setattr("oaix_gateway.token_store.get_read_session", fake_get_read_session)

    async def run() -> tuple[CodexToken | None, CodexToken | None]:
        first = await claim_next_active_token(
            selection_strategy=TOKEN_SELECTION_STRATEGY_FILL_FIRST,
            token_order=(9,),
        )
        second = await claim_next_active_token(
            selection_strategy=TOKEN_SELECTION_STRATEGY_FILL_FIRST,
            token_order=(9,),
        )
        return first, second

    first, second = asyncio.run(run())

    assert first is token
    assert second is token
    assert len(read_session.statements) == 1
    invalidate_fill_first_token_cache()


def test_claim_next_active_token_fill_first_singleflights_cache_miss(monkeypatch) -> None:
    invalidate_fill_first_token_cache()
    token = CodexToken(
        id=11,
        refresh_token="rt_11",
        token_type="codex",
        is_active=True,
    )
    load_count = 0

    async def fake_load(*, token_order, plan_order_enabled=False, plan_order=(), scoped_cooldown_scope):
        del plan_order_enabled, plan_order
        nonlocal load_count
        load_count += 1
        await asyncio.sleep(0.01)
        return (token,)

    monkeypatch.setattr("oaix_gateway.token_store._load_fill_first_available_tokens", fake_load)

    async def run() -> list[CodexToken | None]:
        return await asyncio.gather(
            *[
                claim_next_active_token(
                    selection_strategy=TOKEN_SELECTION_STRATEGY_FILL_FIRST,
                    token_order=(11,),
                )
                for _ in range(20)
            ]
        )

    results = asyncio.run(run())

    assert results == [token] * 20
    assert load_count == 1
    invalidate_fill_first_token_cache()


def test_claim_next_active_token_fill_first_serves_stale_while_refreshing(monkeypatch) -> None:
    invalidate_fill_first_token_cache()
    old_token = CodexToken(id=12, refresh_token="rt_12", token_type="codex", is_active=True)
    new_token = CodexToken(id=13, refresh_token="rt_13", token_type="codex", is_active=True)
    cache_key = ((12, 13), False, (), None)
    now = time.monotonic()
    _FILL_FIRST_TOKEN_CACHE[cache_key] = _FillFirstTokenCacheEntry(
        expires_at=now - 1,
        stale_until=now + 10,
        tokens=(old_token,),
    )
    refresh_released = asyncio.Event()
    load_count = 0

    async def fake_load(*, token_order, plan_order_enabled=False, plan_order=(), scoped_cooldown_scope):
        del token_order, plan_order_enabled, plan_order, scoped_cooldown_scope
        nonlocal load_count
        load_count += 1
        await refresh_released.wait()
        return (new_token,)

    monkeypatch.setattr("oaix_gateway.token_store._load_fill_first_available_tokens", fake_load)

    async def run() -> tuple[CodexToken | None, CodexToken | None]:
        first = await claim_next_active_token(
            selection_strategy=TOKEN_SELECTION_STRATEGY_FILL_FIRST,
            token_order=(12, 13),
        )
        assert first is old_token
        refresh_task = _FILL_FIRST_TOKEN_REFRESH_TASKS[cache_key]
        refresh_released.set()
        await refresh_task
        second = await claim_next_active_token(
            selection_strategy=TOKEN_SELECTION_STRATEGY_FILL_FIRST,
            token_order=(12, 13),
        )
        return first, second

    first, second = asyncio.run(run())

    assert first is old_token
    assert second is new_token
    assert load_count == 1
    invalidate_fill_first_token_cache()


def test_claim_next_active_token_fill_first_serves_expired_stale_cache_without_waiting(monkeypatch) -> None:
    invalidate_fill_first_token_cache()
    old_token = CodexToken(id=16, refresh_token="rt_16", token_type="codex", is_active=True)
    new_token = CodexToken(id=17, refresh_token="rt_17", token_type="codex", is_active=True)
    cache_key = ((16, 17), False, (), None)
    now = time.monotonic()
    _FILL_FIRST_TOKEN_CACHE[cache_key] = _FillFirstTokenCacheEntry(
        expires_at=now - 10,
        stale_until=now - 1,
        tokens=(old_token,),
    )
    refresh_released = asyncio.Event()
    load_count = 0

    async def fake_load(*, token_order, plan_order_enabled=False, plan_order=(), scoped_cooldown_scope):
        del token_order, plan_order_enabled, plan_order, scoped_cooldown_scope
        nonlocal load_count
        load_count += 1
        await refresh_released.wait()
        return (new_token,)

    monkeypatch.setattr("oaix_gateway.token_store._load_fill_first_available_tokens", fake_load)

    async def run() -> tuple[CodexToken | None, CodexToken | None]:
        first = await asyncio.wait_for(
            claim_next_active_token(
                selection_strategy=TOKEN_SELECTION_STRATEGY_FILL_FIRST,
                token_order=(16, 17),
            ),
            timeout=0.01,
        )
        assert first is old_token
        refresh_task = _FILL_FIRST_TOKEN_REFRESH_TASKS[cache_key]
        refresh_released.set()
        await refresh_task
        second = await claim_next_active_token(
            selection_strategy=TOKEN_SELECTION_STRATEGY_FILL_FIRST,
            token_order=(16, 17),
        )
        return first, second

    first, second = asyncio.run(run())

    assert first is old_token
    assert second is new_token
    assert load_count == 1
    invalidate_fill_first_token_cache()


def test_prewarm_fill_first_token_cache_loads_current_order(monkeypatch) -> None:
    invalidate_fill_first_token_cache()
    token = CodexToken(id=18, refresh_token="rt_18", token_type="codex", is_active=True)
    load_calls: list[tuple[tuple[int, ...], bool, tuple[str, ...], str | None]] = []

    async def fake_load(*, token_order, plan_order_enabled=False, plan_order=(), scoped_cooldown_scope):
        load_calls.append((tuple(token_order), bool(plan_order_enabled), tuple(plan_order), scoped_cooldown_scope))
        return (token,)

    monkeypatch.setattr("oaix_gateway.token_store._load_fill_first_available_tokens", fake_load)

    asyncio.run(
        prewarm_fill_first_token_cache(
            TokenSelectionSettings(strategy=TOKEN_SELECTION_STRATEGY_FILL_FIRST, token_order=(18,))
        )
    )

    assert load_calls == [((18,), False, ("free", "plus", "team", "pro"), None)]
    assert _FILL_FIRST_TOKEN_CACHE[((18,), False, (), None)].tokens == (token,)
    invalidate_fill_first_token_cache()


def test_claim_next_active_token_fill_first_respects_runtime_cooldown() -> None:
    invalidate_fill_first_token_cache()
    first_token = CodexToken(id=14, refresh_token="rt_14", token_type="codex", is_active=True)
    second_token = CodexToken(id=15, refresh_token="rt_15", token_type="codex", is_active=True)
    cache_key = ((14, 15), False, (), None)
    _apply_token_runtime_error_state(
        14,
        message="rate limited",
        deactivate=False,
        cooldown_until=datetime.now(timezone.utc) + timedelta(seconds=60),
    )
    now = time.monotonic()
    _FILL_FIRST_TOKEN_CACHE[cache_key] = _FillFirstTokenCacheEntry(
        expires_at=now + 10,
        stale_until=now + 10,
        tokens=(first_token, second_token),
    )

    result = asyncio.run(
        claim_next_active_token(
            selection_strategy=TOKEN_SELECTION_STRATEGY_FILL_FIRST,
            token_order=(14, 15),
        )
    )

    assert result is second_token
    _TOKEN_RUNTIME_STATE.clear()
    invalidate_fill_first_token_cache()


def test_merge_duplicate_token_rows_marks_shadow_and_preserves_canonical_history() -> None:
    now = datetime(2026, 4, 15, 10, 0, tzinfo=timezone.utc)
    older = CodexToken(
        id=24,
        account_id="acc_shared",
        refresh_token="rt_old",
        refresh_token_aliases=["rt_seed"],
        token_type="codex",
        is_active=True,
        access_token="access_old",
        expires_at=now + timedelta(hours=1),
        cooldown_until=now + timedelta(hours=2),
        last_error="额度已用尽，等待窗口重置",
        last_refresh_at=now - timedelta(hours=3),
        last_used_at=now - timedelta(minutes=15),
        updated_at=now - timedelta(hours=2),
        source_file="/tmp/older.json",
    )
    newer = CodexToken(
        id=25,
        email="shared@example.com",
        account_id="acc_shared",
        refresh_token="rt_new",
        refresh_token_aliases=["rt_seed", "rt_new"],
        token_type="codex",
        is_active=True,
        access_token=None,
        expires_at=None,
        cooldown_until=None,
        last_error=None,
        last_refresh_at=now - timedelta(minutes=30),
        last_used_at=now - timedelta(minutes=5),
        updated_at=now - timedelta(minutes=1),
    )

    canonical, shadows = _merge_duplicate_token_rows([older, newer], now=now)

    assert canonical.id == 25
    assert canonical.merged_into_token_id is None
    assert canonical.email == "shared@example.com"
    assert canonical.source_file == "/tmp/older.json"
    assert canonical.last_error == "额度已用尽，等待窗口重置"
    assert canonical.cooldown_until == now + timedelta(hours=2)
    assert canonical.last_used_at == now - timedelta(minutes=5)
    assert canonical.refresh_token_aliases == ["rt_seed", "rt_old", "rt_new"]

    assert [shadow.id for shadow in shadows] == [24]
    shadow = shadows[0]
    assert shadow.merged_into_token_id == 25
    assert shadow.is_active is False
    assert shadow.access_token is None
    assert shadow.expires_at is None
    assert shadow.cooldown_until is None
    assert shadow.last_error == "Merged into token #25 due to duplicate refresh token history"


def test_merge_duplicate_token_rows_prefers_latest_state_even_if_it_is_inactive() -> None:
    now = datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc)
    older = CodexToken(
        id=10,
        account_id="acc_shared",
        refresh_token="rt_old",
        refresh_token_aliases=["rt_seed"],
        token_type="codex",
        is_active=True,
        access_token="access_old",
        expires_at=now + timedelta(hours=1),
        last_error=None,
        last_refresh_at=now - timedelta(hours=3),
        updated_at=now - timedelta(hours=3),
    )
    newer = CodexToken(
        id=11,
        account_id="acc_shared",
        refresh_token="rt_new",
        refresh_token_aliases=["rt_seed", "rt_new"],
        token_type="codex",
        is_active=False,
        access_token=None,
        expires_at=None,
        cooldown_until=now + timedelta(minutes=30),
        last_error="refresh_token_reused",
        last_refresh_at=now - timedelta(minutes=10),
        updated_at=now - timedelta(minutes=1),
    )

    canonical, shadows = _merge_duplicate_token_rows([older, newer], now=now)

    assert canonical.id == 11
    assert canonical.is_active is False
    assert canonical.cooldown_until is None
    assert canonical.last_error == "refresh_token_reused"
    assert [shadow.id for shadow in shadows] == [10]


def test_update_token_refresh_state_skips_duplicate_repair_hot_path(monkeypatch) -> None:
    token = CodexToken(
        id=7,
        refresh_token="rt_old",
        refresh_token_aliases=["rt_old"],
        token_type="codex",
        is_active=True,
    )
    calls: list[str] = []

    class _FakeRefreshSession(_FakeSession):
        async def get(self, model, token_id: int):
            assert model is CodexToken
            calls.append(f"get:{token_id}")
            return token if token_id == 7 else None

    fake_session = _FakeRefreshSession()

    @asynccontextmanager
    async def fake_get_session():
        yield fake_session

    async def fake_acquire_history_lock(session) -> None:
        raise AssertionError("refresh state should not take the duplicate-history repair lock")

    async def fake_repair(session) -> TokenHistoryRepairSummary:
        raise AssertionError("refresh state should not run duplicate-history repair")

    monkeypatch.setattr("oaix_gateway.token_store.get_session", fake_get_session)
    monkeypatch.setattr("oaix_gateway.token_store._acquire_token_history_repair_lock", fake_acquire_history_lock)
    monkeypatch.setattr("oaix_gateway.token_store._repair_duplicate_token_histories_in_session", fake_repair)

    asyncio.run(
        update_token_refresh_state(
            7,
            access_token="access_new",
            refresh_token="rt_new",
            expires_at=None,
        )
    )

    assert calls == ["get:7"]
    assert token.access_token == "access_new"
    assert token.refresh_token == "rt_new"
    assert token.refresh_token_aliases == ["rt_old", "rt_new"]


def test_repair_duplicate_token_histories_acquires_history_lock(monkeypatch) -> None:
    fake_session = _FakeSession()
    calls: list[str] = []
    expected = TokenHistoryRepairSummary(
        duplicate_group_count=1,
        merged_row_count=2,
        canonical_ids=(11,),
        shadow_ids=(9, 10),
    )

    @asynccontextmanager
    async def fake_get_session():
        yield fake_session

    async def fake_acquire_history_lock(session) -> None:
        assert session is fake_session
        calls.append("lock")

    async def fake_repair(session) -> TokenHistoryRepairSummary:
        assert session is fake_session
        assert calls == ["lock"]
        calls.append("repair")
        return expected

    monkeypatch.setattr("oaix_gateway.token_store.get_session", fake_get_session)
    monkeypatch.setattr("oaix_gateway.token_store._acquire_token_history_repair_lock", fake_acquire_history_lock)
    monkeypatch.setattr("oaix_gateway.token_store._repair_duplicate_token_histories_in_session", fake_repair)

    summary = asyncio.run(repair_duplicate_token_histories())

    assert calls == ["lock", "repair"]
    assert summary == expected


def test_set_token_active_state_can_clear_cooldown_when_reactivating(monkeypatch) -> None:
    token = CodexToken(
        id=7,
        token_type="codex",
        is_active=True,
        cooldown_until=datetime(2026, 4, 15, 15, 0, tzinfo=timezone.utc),
    )
    fake_session = _FakeSession()

    @asynccontextmanager
    async def fake_get_session():
        yield fake_session

    async def fake_resolve(session, token_id: int) -> CodexToken:
        assert session is fake_session
        assert token_id == 7
        return token

    monkeypatch.setattr("oaix_gateway.token_store.get_session", fake_get_session)
    monkeypatch.setattr("oaix_gateway.token_store._resolve_canonical_token_for_update", fake_resolve)

    result = asyncio.run(set_token_active_state(7, active=True, clear_cooldown=True))

    assert result is token
    assert token.is_active is True
    assert token.cooldown_until is None


def test_set_token_active_state_clears_cooldown_when_deactivating(monkeypatch) -> None:
    token = CodexToken(
        id=8,
        token_type="codex",
        is_active=True,
        cooldown_until=datetime(2026, 4, 15, 15, 30, tzinfo=timezone.utc),
    )
    fake_session = _FakeSession()

    @asynccontextmanager
    async def fake_get_session():
        yield fake_session

    async def fake_resolve(session, token_id: int) -> CodexToken:
        assert session is fake_session
        assert token_id == 8
        return token

    monkeypatch.setattr("oaix_gateway.token_store.get_session", fake_get_session)
    monkeypatch.setattr("oaix_gateway.token_store._resolve_canonical_token_for_update", fake_resolve)

    result = asyncio.run(set_token_active_state(8, active=False))

    assert result is token
    assert token.is_active is False
    assert token.cooldown_until is None
