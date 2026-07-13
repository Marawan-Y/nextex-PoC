"""
cloud_common.queue
=====================
Redis Streams wrapper.

WHY REDIS STREAMS (see README for the full comparison against
RabbitMQ / SQS-LocalStack / Kafka):
  - Single extra container instead of two-plus (broker + management UI
    for RabbitMQ), which matters for a "runs on docker-compose on a
    laptop" take-home and for a real pre-MVP startup's ops budget alike.
  - Consumer groups give at-least-once delivery and per-consumer
    pending-entry tracking out of the box (XREADGROUP / XACK / XPENDING)
    — exactly the primitives needed for "queue backlog" and "processing
    lag" metrics without hand-rolling them.
  - XLEN gives an O(1) queue-depth read for the metrics endpoint; XPENDING
    gives "how many entries were delivered to a consumer but not yet
    acked," which is the more honest definition of backlog than raw
    stream length once you have multiple consumers.
  - Persistence (AOF) is enough durability for this use case; we are not
    trying to replace an event-sourcing system of record, Postgres is
    that system of record — Redis here is purely the transport buffer.
"""
from __future__ import annotations

import json
from typing import Any, Optional

import redis.asyncio as redis

from .config import settings

_redis_client: Optional[redis.Redis] = None


def get_redis() -> redis.Redis:
    global _redis_client
    if _redis_client is None:
        _redis_client = redis.from_url(settings.REDIS_URL, decode_responses=True)
    return _redis_client


async def ensure_group() -> None:
    """Create the consumer group if it doesn't exist yet (idempotent)."""
    r = get_redis()
    try:
        await r.xgroup_create(
            name=settings.STREAM_NAME, groupname=settings.CONSUMER_GROUP, id="0", mkstream=True
        )
    except redis.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


async def publish_event(payload: dict[str, Any]) -> str:
    """Publish one event onto the stream. Returns the Redis-assigned entry ID."""
    r = get_redis()
    entry_id = await r.xadd(
        settings.STREAM_NAME,
        {"data": json.dumps(payload, default=str)},
        maxlen=settings.STREAM_MAXLEN,
        approximate=True,
    )
    return entry_id


async def queue_depth() -> int:
    """Total entries currently in the stream (processed + unprocessed).
    Combined with pending_count(), this is what the metrics endpoint
    reports as 'queue backlog'."""
    r = get_redis()
    return await r.xlen(settings.STREAM_NAME)


async def pending_count() -> int:
    """Entries delivered to a consumer via XREADGROUP but not yet XACKed —
    i.e. actually in-flight/backlogged work, not just raw stream length."""
    r = get_redis()
    try:
        summary = await r.xpending(settings.STREAM_NAME, settings.CONSUMER_GROUP)
    except redis.ResponseError:
        return 0
    if not summary:
        return 0
    return summary.get("pending", 0) if isinstance(summary, dict) else summary[0]


async def read_group(count: int, block_ms: int) -> list[tuple[str, dict]]:
    r = get_redis()
    resp = await r.xreadgroup(
        groupname=settings.CONSUMER_GROUP,
        consumername=settings.CONSUMER_NAME,
        streams={settings.STREAM_NAME: ">"},
        count=count,
        block=block_ms,
    )
    entries: list[tuple[str, dict]] = []
    for _stream_name, messages in resp or []:
        for entry_id, fields in messages:
            entries.append((entry_id, json.loads(fields["data"])))
    return entries


async def ack(entry_id: str) -> None:
    r = get_redis()
    await r.xack(settings.STREAM_NAME, settings.CONSUMER_GROUP, entry_id)


async def claim_stale_entries(idle_ms: int, count: int = 50) -> list[tuple[str, dict]]:
    """Reclaim entries that were delivered to a (possibly crashed) consumer
    and never acked within `idle_ms` — basic redelivery/self-healing so a
    dead consumer doesn't permanently strand messages."""
    r = get_redis()
    try:
        result = await r.xautoclaim(
            name=settings.STREAM_NAME,
            groupname=settings.CONSUMER_GROUP,
            consumername=settings.CONSUMER_NAME,
            min_idle_time=idle_ms,
            start_id="0-0",
            count=count,
        )
    except redis.ResponseError:
        return []
    _next_cursor, messages, _deleted = result
    entries: list[tuple[str, dict]] = []
    for entry_id, fields in messages:
        if fields and "data" in fields:
            entries.append((entry_id, json.loads(fields["data"])))
    return entries
