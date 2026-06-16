import asyncio

import anyio
import pytest
from sqlalchemy import create_engine, text

from oaix_gateway import database


class _FakeTransaction:
    def __init__(self, session: "_FakeSession") -> None:
        self._session = session

    async def __aenter__(self) -> None:
        self._session.begin_entered += 1

    async def __aexit__(self, exc_type, exc, tb) -> None:
        self._session.begin_exited += 1


class _FakeSession:
    def __init__(self) -> None:
        self.begin_entered = 0
        self.begin_exited = 0
        self.close_started = asyncio.Event()
        self.close_release = asyncio.Event()
        self.close_finished = False

    def begin(self) -> _FakeTransaction:
        return _FakeTransaction(self)

    async def close(self) -> None:
        self.close_started.set()
        await self.close_release.wait()
        self.close_finished = True


def test_get_read_session_wraps_explicit_transaction(monkeypatch) -> None:
    fake_session = _FakeSession()
    fake_session.close_release.set()
    monkeypatch.setattr(database, "get_session_factory", lambda: lambda: fake_session)

    async def run() -> None:
        async with database.get_read_session() as session:
            assert session is fake_session
            assert fake_session.begin_entered == 1
            assert fake_session.begin_exited == 0

    asyncio.run(run())

    assert fake_session.begin_entered == 1
    assert fake_session.begin_exited == 1
    assert fake_session.close_finished is True


def test_get_session_waits_for_close_during_cancellation(monkeypatch) -> None:
    fake_session = _FakeSession()
    monkeypatch.setattr(database, "get_session_factory", lambda: lambda: fake_session)

    async def worker() -> None:
        async with database.get_session():
            return

    async def run() -> None:
        task = asyncio.create_task(worker())
        await fake_session.close_started.wait()
        task.cancel()
        await asyncio.sleep(0)
        assert fake_session.close_finished is False
        fake_session.close_release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert fake_session.close_finished is True

    asyncio.run(run())


def test_get_session_close_survives_anyio_cancel_scope(monkeypatch) -> None:
    fake_session = _FakeSession()
    monkeypatch.setattr(database, "get_session_factory", lambda: lambda: fake_session)

    async def worker() -> None:
        with anyio.CancelScope() as scope:
            async with database.get_session():
                scope.cancel()

    async def run() -> None:
        task = asyncio.create_task(worker())
        await fake_session.close_started.wait()
        await asyncio.sleep(0)
        assert fake_session.close_finished is False
        fake_session.close_release.set()
        await task
        assert fake_session.close_finished is True

    asyncio.run(run())


def test_schema_migrations_use_physical_codex_token_type_column_for_raw_indexes() -> None:
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE codex_tokens (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    email VARCHAR(320),
                    account_id VARCHAR(128),
                    id_token TEXT,
                    access_token TEXT,
                    refresh_token TEXT NOT NULL,
                    refresh_token_aliases JSON,
                    merged_into_token_id INTEGER,
                    type VARCHAR(32) NOT NULL DEFAULT 'codex',
                    last_refresh TIMESTAMP,
                    expired TIMESTAMP,
                    recovery JSON,
                    raw_payload JSON,
                    plan_type VARCHAR(32),
                    source_file VARCHAR(512),
                    is_active BOOLEAN NOT NULL DEFAULT 1,
                    cooldown_until TIMESTAMP,
                    last_used_at TIMESTAMP,
                    last_error TEXT,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO codex_tokens (
                    email, account_id, refresh_token, type, is_active, created_at, updated_at
                )
                VALUES (
                    'disabled@example.com', 'acct_disabled', 'rt_disabled', 'codex', 0,
                    '2026-04-15 09:00:00', '2026-04-15 10:30:00'
                )
                """
            )
        )

        database._run_schema_migrations(conn)
        index_rows = conn.execute(text("select name, sql from sqlite_master where type = 'index'")).mappings().all()
        column_rows = conn.execute(text("pragma table_info(codex_tokens)")).mappings().all()
        disabled_at = conn.execute(
            text("select disabled_at from codex_tokens where account_id = 'acct_disabled'")
        ).scalar_one()

    index_sql = {row["name"]: row["sql"] for row in index_rows}
    columns = {row["name"] for row in column_rows}
    assert "disabled_at" in columns
    assert "remark" in columns
    assert disabled_at == "2026-04-15 10:30:00"
    assert "ix_codex_tokens_pool_snapshot" in index_sql
    assert "ix_codex_tokens_lru_available" in index_sql
    assert "ix_codex_tokens_disabled_at" in index_sql
    assert "ix_codex_tokens_newest_canonical" in index_sql
    assert "created_at DESC" in (index_sql["ix_codex_tokens_newest_canonical"] or "")
    assert "merged_into_token_id IS NULL" in (index_sql["ix_codex_tokens_newest_canonical"] or "")
    assert "id DESC" in (index_sql["ix_codex_tokens_newest_canonical"] or "")
    assert "token_type" not in (index_sql["ix_codex_tokens_pool_snapshot"] or "")
    assert "token_type" not in (index_sql["ix_codex_tokens_lru_available"] or "")
