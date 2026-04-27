import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

from sqlalchemy.dialects import postgresql

from oaix_gateway.database import CodexToken
from oaix_gateway.token_store import (
    TOKEN_SELECTION_STRATEGY_FILL_FIRST,
    TokenHistoryRepairSummary,
    _build_token_counts_stmt,
    _merge_duplicate_token_rows,
    claim_next_active_token,
    invalidate_fill_first_token_cache,
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


def test_claim_next_active_token_filters_requested_scoped_cooldown(monkeypatch) -> None:
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


def test_update_token_refresh_state_acquires_history_lock_before_row_locks(monkeypatch) -> None:
    token = CodexToken(
        id=7,
        refresh_token="rt_old",
        refresh_token_aliases=["rt_old"],
        token_type="codex",
        is_active=True,
    )
    fake_session = _FakeSession()
    calls: list[str] = []

    @asynccontextmanager
    async def fake_get_session():
        yield fake_session

    async def fake_acquire_history_lock(session) -> None:
        assert session is fake_session
        calls.append("lock")

    async def fake_resolve(session, token_id: int) -> CodexToken:
        assert session is fake_session
        assert calls == ["lock"]
        calls.append(f"resolve:{token_id}")
        return token

    async def fake_repair(session) -> TokenHistoryRepairSummary:
        assert session is fake_session
        assert calls == ["lock", "resolve:7"]
        calls.append("repair")
        return TokenHistoryRepairSummary(duplicate_group_count=0, merged_row_count=0)

    monkeypatch.setattr("oaix_gateway.token_store.get_session", fake_get_session)
    monkeypatch.setattr("oaix_gateway.token_store._acquire_token_history_repair_lock", fake_acquire_history_lock)
    monkeypatch.setattr("oaix_gateway.token_store._resolve_canonical_token_for_update", fake_resolve)
    monkeypatch.setattr("oaix_gateway.token_store._repair_duplicate_token_histories_in_session", fake_repair)

    asyncio.run(
        update_token_refresh_state(
            7,
            access_token="access_new",
            refresh_token="rt_new",
            expires_at=None,
        )
    )

    assert calls == ["lock", "resolve:7", "repair"]
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
