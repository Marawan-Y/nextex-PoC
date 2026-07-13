"""
cloud_common.db
=================
Persistence layer. Uses SQLAlchemy 2.0 async ORM over Postgres.

Design notes:
- A single `events` table holds both event types with nullable
  type-specific columns (frame_path, threshold_used) — appropriate here
  because both event types share the vast majority of fields and query
  patterns (metrics need to group/filter across both uniformly). If the
  two event types diverged further in the future, splitting into separate
  tables (or a JSONB payload column) would be the next evolution.
- `processed_at` is set by the consumer, not the API, so it doubles as
  proof the event actually made it through the queue rather than just
  being accepted at the door.
- Indexes are chosen to directly serve the metrics endpoint's queries:
  event_type (for distribution-by-type), captured_at (for time-window
  rates), factory_id/machine_id (for per-tenant/per-machine breakdowns).
"""
from __future__ import annotations

import asyncio
from datetime import datetime
from typing import AsyncGenerator, Optional

from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    Index,
    String,
    select,
    func,
)
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from .config import settings


class Base(DeclarativeBase):
    pass


class Event(Base):
    __tablename__ = "events"

    event_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    device_id: Mapped[str] = mapped_column(String(128), index=True)
    machine_id: Mapped[str] = mapped_column(String(128), index=True)
    factory_id: Mapped[str] = mapped_column(String(128), index=True)
    event_type: Mapped[str] = mapped_column(String(32), index=True)

    anomaly_class: Mapped[str] = mapped_column(String(128), index=True)
    confidence: Mapped[float] = mapped_column(Float)
    threshold_used: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    frame_id: Mapped[str] = mapped_column(String(128))
    has_frame: Mapped[bool] = mapped_column(Boolean, default=False)
    frame_path: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)

    captured_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    sent_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    processed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        Index("ix_events_type_captured", "event_type", "captured_at"),
        Index("ix_events_factory_machine", "factory_id", "machine_id"),
    )


_engine = create_async_engine(settings.DATABASE_URL, pool_pre_ping=True)
_SessionLocal = async_sessionmaker(_engine, expire_on_commit=False, class_=AsyncSession)


async def init_db(retries: int = 5, delay_seconds: float = 0.5) -> None:
    """
    Create tables if they don't exist yet.

    Both cloud-api and cloud-consumer call this on startup (each service
    owns making sure its own dependencies exist rather than relying on
    startup ordering). On a cold start they can race: both see "table
    doesn't exist" and both issue CREATE TABLE concurrently, and Postgres
    raises a duplicate-key error on the underlying pg_type entry for
    whichever one loses the race — not because anything is actually
    wrong, just because SQLAlchemy's checkfirst check-then-create isn't
    atomic across two separate connections.

    In production this is exactly what a dedicated migration step
    (Alembic, run once before any service starts) would eliminate. For
    this take-home, the pragmatic fix is to retry: the loser's retry will
    see the table already exists (created by the winner) and become a
    no-op, which is safe because CREATE TABLE IF NOT EXISTS semantics are
    idempotent once the race window has passed.
    """
    from sqlalchemy.exc import IntegrityError

    for attempt in range(retries):
        try:
            async with _engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            return
        except IntegrityError:
            if attempt == retries - 1:
                raise
            await asyncio.sleep(delay_seconds)


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    async with _SessionLocal() as session:
        yield session


async def insert_event(session: AsyncSession, **fields) -> Event:
    event = Event(**fields)
    session.add(event)
    await session.commit()
    return event


async def total_processed(session: AsyncSession) -> int:
    result = await session.execute(select(func.count()).select_from(Event))
    return result.scalar_one()


async def distribution_by_type(session: AsyncSession) -> dict[str, int]:
    result = await session.execute(
        select(Event.event_type, func.count()).group_by(Event.event_type)
    )
    return {row[0]: row[1] for row in result.all()}


async def distribution_by_anomaly_class(session: AsyncSession, limit: int = 10) -> dict[str, int]:
    result = await session.execute(
        select(Event.anomaly_class, func.count())
        .group_by(Event.anomaly_class)
        .order_by(func.count().desc())
        .limit(limit)
    )
    return {row[0]: row[1] for row in result.all()}


async def distribution_by_factory(session: AsyncSession) -> dict[str, int]:
    result = await session.execute(
        select(Event.factory_id, func.count()).group_by(Event.factory_id)
    )
    return {row[0]: row[1] for row in result.all()}


async def events_in_last_minutes(session: AsyncSession, minutes: int = 5) -> int:
    from datetime import timedelta, timezone
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    result = await session.execute(
        select(func.count()).select_from(Event).where(Event.processed_at >= cutoff)
    )
    return result.scalar_one()


async def most_recent_events(session: AsyncSession, limit: int = 20) -> list[Event]:
    result = await session.execute(
        select(Event).order_by(Event.processed_at.desc()).limit(limit)
    )
    return list(result.scalars().all())


async def last_event_timestamp(session: AsyncSession) -> Optional[datetime]:
    result = await session.execute(select(func.max(Event.processed_at)))
    return result.scalar_one_or_none()
