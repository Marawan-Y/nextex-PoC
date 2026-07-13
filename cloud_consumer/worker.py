"""
cloud-consumer / worker.py
=============================
Standalone consumer process. Deliberately a *separate* container/process
from cloud-api (not a background task inside the API process) so that:
  - Ingestion throughput is never limited by persistence throughput —
    if Postgres gets slow, the API keeps accepting and queueing events;
    only the backlog grows, which is visible in /metrics as
    queue_backlog/queue_pending_unacked climbing, rather than API
    latency degrading for the Jetson devices trying to report.
  - The consumer can be scaled independently (multiple replicas, each
    with a distinct CONSUMER_NAME) purely by adding containers, since
    Redis consumer groups handle partitioning delivery across consumers
    automatically.
  - A crash in persistence logic (e.g. a bad DB migration) doesn't take
    the ingestion API down with it.

Processing loop:
  1. XREADGROUP for up to BATCH_SIZE new entries (blocking up to BLOCK_MS
     if the stream is empty, to avoid a hot busy-loop).
  2. For each entry: validate against the Pydantic schema, decode+persist
     the frame (if present) to disk, insert a row into Postgres, XACK.
  3. Periodically (each loop iteration) attempt to reclaim any stale
     pending entries via XAUTOCLAIM, so a consumer that crashed mid-batch
     doesn't strand those events forever (basic self-healing).

A failure to persist an individual event does NOT ack it — it stays
pending and will be reclaimed and retried by claim_stale_entries() on a
future iteration (or by another consumer instance), which is the
at-least-once semantics Redis Streams consumer groups are designed for.
"""
from __future__ import annotations

import asyncio
import base64
import logging
import sys
from datetime import datetime
from pathlib import Path
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from cloud_common.config import settings
from cloud_common.db import get_session, init_db, insert_event
from cloud_common.queue import ensure_group, read_group, ack, claim_stale_entries

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("nextex-consumer")


def _save_frame(event_id: str, frame_jpeg_b64: str) -> str:
    raw = base64.b64decode(frame_jpeg_b64)
    filename = f"{event_id}.jpg"
    path = settings.FRAME_STORAGE_DIR / filename
    path.write_bytes(raw)
    return str(path)


async def _process_entry(entry_id: str, payload: dict) -> bool:
    """Returns True if the event was successfully persisted (safe to ack)."""
    try:
        event_type = payload["event_type"]
        detection = payload["detection"]
        frame_b64 = payload.get("frame_jpeg_b64")

        has_frame = bool(frame_b64)
        frame_path = None
        if has_frame:
            frame_path = _save_frame(payload.get("event_id", str(uuid4())), frame_b64)

        async for session in get_session():
            await insert_event(
                session,
                event_id=payload["event_id"],
                device_id=payload["device_id"],
                machine_id=payload["machine_id"],
                factory_id=payload["factory_id"],
                event_type=event_type,
                anomaly_class=detection["anomaly_class"],
                confidence=detection["confidence"],
                threshold_used=payload.get("threshold_used"),
                frame_id=payload["frame_id"],
                has_frame=has_frame,
                frame_path=frame_path,
                captured_at=datetime.fromisoformat(payload["captured_at"].replace("Z", "+00:00")),
                sent_at=datetime.fromisoformat(payload["sent_at"].replace("Z", "+00:00")),
            )
            break

        log.info(
            "persisted event_id=%s type=%s class=%s confidence=%.3f device=%s",
            payload["event_id"], event_type, detection["anomaly_class"],
            detection["confidence"], payload["device_id"],
        )
        return True

    except Exception:
        log.exception("failed to process entry_id=%s — leaving unacked for retry", entry_id)
        return False


async def run_forever() -> None:
    await init_db()
    await ensure_group()
    log.info(
        "consumer '%s' started, group='%s', stream='%s'",
        settings.CONSUMER_NAME, settings.CONSUMER_GROUP, settings.STREAM_NAME,
    )

    while True:
        # 1. Reclaim anything stale from a previous crashed consumer first.
        stale = await claim_stale_entries(idle_ms=settings.PENDING_CLAIM_IDLE_MS)
        for entry_id, payload in stale:
            log.warning("reclaimed stale entry_id=%s", entry_id)
            if await _process_entry(entry_id, payload):
                await ack(entry_id)

        # 2. Normal read of new entries.
        entries = await read_group(count=settings.BATCH_SIZE, block_ms=settings.BLOCK_MS)
        for entry_id, payload in entries:
            if await _process_entry(entry_id, payload):
                await ack(entry_id)
            # if processing failed, entry stays pending -> retried via
            # claim_stale_entries() on a future loop iteration


if __name__ == "__main__":
    asyncio.run(run_forever())
