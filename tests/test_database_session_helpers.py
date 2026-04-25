import asyncio

import anyio
import pytest

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
