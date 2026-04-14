import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from sqlalchemy import Boolean, DateTime, Float, Integer, JSON, String, Text, func, inspect, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from .token_identity import collect_refresh_token_aliases


DEFAULT_DATABASE_URL = "postgresql+asyncpg://postgres:postgres@127.0.0.1:5432/oaix_gateway"


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def normalize_database_url(database_url: str | None = None) -> str:
    raw = (database_url or os.getenv("DATABASE_URL") or DEFAULT_DATABASE_URL).strip()
    if raw.startswith("postgres://"):
        return "postgresql+asyncpg://" + raw[len("postgres://") :]
    if raw.startswith("postgresql://"):
        return "postgresql+asyncpg://" + raw[len("postgresql://") :]
    return raw


class Base(DeclarativeBase):
    pass


class CodexToken(Base):
    __tablename__ = "codex_tokens"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    email: Mapped[str | None] = mapped_column(String(320), index=True, nullable=True)
    account_id: Mapped[str | None] = mapped_column(String(128), index=True, nullable=True)
    id_token: Mapped[str | None] = mapped_column(Text, nullable=True)
    access_token: Mapped[str | None] = mapped_column(Text, nullable=True)
    refresh_token: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    refresh_token_aliases: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)
    token_type: Mapped[str] = mapped_column("type", String(32), nullable=False, default="codex", index=True)
    last_refresh_at: Mapped[datetime | None] = mapped_column("last_refresh", DateTime(timezone=True), nullable=True, index=True)
    expires_at: Mapped[datetime | None] = mapped_column("expired", DateTime(timezone=True), nullable=True, index=True)
    recovery: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    raw_payload: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    source_file: Mapped[str | None] = mapped_column(String(512), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, index=True)
    cooldown_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    last_used_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())


class GatewayRequestLog(Base):
    __tablename__ = "gateway_request_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    request_id: Mapped[str] = mapped_column(String(64), nullable=False, unique=True, index=True)
    endpoint: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    model: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    model_name: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    is_stream: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, index=True)
    status_code: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    success: Mapped[bool | None] = mapped_column(Boolean, nullable=True, index=True)
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    account_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    client_ip: Mapped[str | None] = mapped_column(String(64), nullable=True)
    user_agent: Mapped[str | None] = mapped_column(String(512), nullable=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now(), index=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    first_token_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    ttft_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    duration_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    input_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    output_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    total_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    estimated_cost_usd: Mapped[float | None] = mapped_column(Float, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)


class GatewaySetting(Base):
    __tablename__ = "gateway_settings"

    key: Mapped[str] = mapped_column(String(128), primary_key=True)
    value: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())


_engine = None
_session_factory = None


def get_engine():
    global _engine
    if _engine is None:
        _engine = create_async_engine(
            normalize_database_url(),
            pool_pre_ping=True,
        )
    return _engine


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    global _session_factory
    if _session_factory is None:
        _session_factory = async_sessionmaker(
            get_engine(),
            class_=AsyncSession,
            expire_on_commit=False,
        )
    return _session_factory


@asynccontextmanager
async def get_session() -> AsyncIterator[AsyncSession]:
    session = get_session_factory()()
    try:
        yield session
    finally:
        await session.close()


async def init_db() -> None:
    async with get_engine().begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.run_sync(_run_schema_migrations)
    await _backfill_token_refresh_aliases()


def _drop_single_column_uniques(sync_conn, table_name: str, column_name: str) -> None:
    inspector = inspect(sync_conn)
    for constraint in inspector.get_unique_constraints(table_name):
        if constraint.get("column_names") == [column_name] and constraint.get("name"):
            sync_conn.execute(text(f'ALTER TABLE "{table_name}" DROP CONSTRAINT IF EXISTS "{constraint["name"]}"'))

    inspector = inspect(sync_conn)
    for index in inspector.get_indexes(table_name):
        if index.get("unique") and index.get("column_names") == [column_name] and index.get("name"):
            sync_conn.execute(text(f'DROP INDEX IF EXISTS "{index["name"]}"'))

    sync_conn.execute(text(f'CREATE INDEX IF NOT EXISTS ix_{table_name}_{column_name} ON "{table_name}" ("{column_name}")'))


def _run_schema_migrations(sync_conn) -> None:
    inspector = inspect(sync_conn)
    table_names = set(inspector.get_table_names())

    if "codex_tokens" in table_names:
        token_columns = {column["name"] for column in inspector.get_columns("codex_tokens")}
        if "cooldown_until" not in token_columns:
            sync_conn.execute(text("ALTER TABLE codex_tokens ADD COLUMN cooldown_until TIMESTAMPTZ"))
        if "refresh_token_aliases" not in token_columns:
            sync_conn.execute(text("ALTER TABLE codex_tokens ADD COLUMN refresh_token_aliases JSON"))
        sync_conn.execute(text("CREATE INDEX IF NOT EXISTS ix_codex_tokens_cooldown_until ON codex_tokens (cooldown_until)"))
        sync_conn.execute(text("CREATE INDEX IF NOT EXISTS ix_codex_tokens_refresh_token ON codex_tokens (refresh_token)"))
        _drop_single_column_uniques(sync_conn, "codex_tokens", "account_id")
        _drop_single_column_uniques(sync_conn, "codex_tokens", "email")

    if "gateway_request_logs" in table_names:
        request_columns = {column["name"] for column in inspector.get_columns("gateway_request_logs")}
        if "model" not in request_columns:
            sync_conn.execute(text("ALTER TABLE gateway_request_logs ADD COLUMN model VARCHAR(128)"))
        if "model_name" not in request_columns:
            sync_conn.execute(text("ALTER TABLE gateway_request_logs ADD COLUMN model_name VARCHAR(128)"))
        if "input_tokens" not in request_columns:
            sync_conn.execute(text("ALTER TABLE gateway_request_logs ADD COLUMN input_tokens INTEGER"))
        if "output_tokens" not in request_columns:
            sync_conn.execute(text("ALTER TABLE gateway_request_logs ADD COLUMN output_tokens INTEGER"))
        if "total_tokens" not in request_columns:
            sync_conn.execute(text("ALTER TABLE gateway_request_logs ADD COLUMN total_tokens INTEGER"))
        if "estimated_cost_usd" not in request_columns:
            sync_conn.execute(text("ALTER TABLE gateway_request_logs ADD COLUMN estimated_cost_usd DOUBLE PRECISION"))
        sync_conn.execute(text("CREATE INDEX IF NOT EXISTS ix_gateway_request_logs_model ON gateway_request_logs (model)"))
        sync_conn.execute(text("CREATE INDEX IF NOT EXISTS ix_gateway_request_logs_model_name ON gateway_request_logs (model_name)"))


async def _backfill_token_refresh_aliases() -> None:
    async with get_session() as session:
        async with session.begin():
            result = await session.execute(select(CodexToken))
            tokens = result.scalars().all()
            for token in tokens:
                aliases = collect_refresh_token_aliases(
                    token.refresh_token_aliases,
                    token.refresh_token,
                    token.raw_payload,
                )
                if aliases != (token.refresh_token_aliases or []):
                    token.refresh_token_aliases = aliases
                    token.updated_at = utcnow()


async def close_database() -> None:
    global _engine, _session_factory
    if _engine is not None:
        await _engine.dispose()
    _engine = None
    _session_factory = None
