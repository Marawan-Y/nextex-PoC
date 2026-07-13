"""
cloud-api / main.py
=====================
Ingestion + metrics/health API. This is the "producer" side of the
queue/consumer architecture: it does the minimum work necessary to
validate and enqueue an event, then returns immediately — all actual
persistence happens asynchronously in cloud-consumer. This separation is
the whole point of the assignment (decouple ingestion rate from
processing rate) and is what lets the API stay fast under a burst of
Jetson devices reporting simultaneously.
"""
from __future__ import annotations

import base64
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from cloud_common.config import settings
from cloud_common.db import (
    get_session,
    init_db,
    total_processed,
    distribution_by_type,
    distribution_by_anomaly_class,
    distribution_by_factory,
    events_in_last_minutes,
    most_recent_events,
    last_event_timestamp,
)
from cloud_common.queue import ensure_group, publish_event, queue_depth, pending_count
from cloud_common.schemas import NewAnomalyClassEvent, ThresholdExceededEvent, IngestAck

app = FastAPI(title="NexTex Cloud Ingestion API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # take-home simplicity; would be locked down per-environment in prod
    allow_methods=["*"],
    allow_headers=["*"],
)

_startup_time = time.time()


@app.on_event("startup")
async def on_startup():
    await init_db()
    await ensure_group()


# ---------------------------------------------------------------------------
# Ingestion endpoints — one per event type (see cloud_common.schemas for why
# these are kept as distinct models rather than one generic endpoint).
# ---------------------------------------------------------------------------

@app.post("/events/new-anomaly-class", response_model=IngestAck)
async def ingest_new_anomaly_class(event: NewAnomalyClassEvent):
    if not event.frame_jpeg_b64:
        raise HTTPException(400, "new_anomaly_class events must include a frame")
    try:
        base64.b64decode(event.frame_jpeg_b64, validate=True)
    except Exception:
        raise HTTPException(400, "frame_jpeg_b64 is not valid base64")

    await publish_event(event.model_dump(mode="json"))
    return IngestAck(event_id=event.event_id, accepted=True)


@app.post("/events/threshold-exceeded", response_model=IngestAck)
async def ingest_threshold_exceeded(event: ThresholdExceededEvent):
    if event.frame_jpeg_b64:
        try:
            base64.b64decode(event.frame_jpeg_b64, validate=True)
        except Exception:
            raise HTTPException(400, "frame_jpeg_b64 is not valid base64")

    await publish_event(event.model_dump(mode="json"))
    return IngestAck(event_id=event.event_id, accepted=True)


# ---------------------------------------------------------------------------
# Health & metrics
# ---------------------------------------------------------------------------

@app.get("/health")
async def health():
    """Liveness/readiness signal. Checks the two hard dependencies
    (Redis reachable, Postgres reachable) rather than just returning 200
    unconditionally — a health check that can't fail isn't useful."""
    status = {"status": "ok", "uptime_seconds": round(time.time() - _startup_time, 1)}
    try:
        depth = await queue_depth()
        status["redis"] = "ok"
        status["queue_depth"] = depth
    except Exception as e:
        status["redis"] = f"error: {e}"
        status["status"] = "degraded"

    try:
        async for session in get_session():
            await total_processed(session)
            status["postgres"] = "ok"
            break
    except Exception as e:
        status["postgres"] = f"error: {e}"
        status["status"] = "degraded"

    return status


@app.get("/metrics")
async def metrics():
    """
    Single JSON metrics endpoint covering everything the assignment asks
    for plus a few extras that would matter operationally:
      - total_processed_events        (from Postgres — durable count)
      - queue_backlog                 (from Redis — real-time queue state)
      - event_distribution_by_type
      - event_distribution_by_anomaly_class (top 10 — extra, useful for
        spotting which defect classes are driving alarm volume)
      - event_distribution_by_factory       (extra — per-tenant volume)
      - events_last_5min / events_per_minute (extra — throughput/rate,
        not just a cumulative total, which is what you actually want on
        an operations dashboard)
      - last_event_at / seconds_since_last_event (extra — a stalled
        pipeline shows 0 errors but a growing "seconds since last event";
        this is usually the first signal an ops person actually watches)
    """
    backlog = await queue_depth()
    pending = await pending_count()

    async for session in get_session():
        processed = await total_processed(session)
        by_type = await distribution_by_type(session)
        by_class = await distribution_by_anomaly_class(session)
        by_factory = await distribution_by_factory(session)
        last_5min = await events_in_last_minutes(session, minutes=5)
        last_ts = await last_event_timestamp(session)
        break

    seconds_since_last = None
    if last_ts is not None:
        now = datetime.now(timezone.utc)
        seconds_since_last = round((now - last_ts).total_seconds(), 1)

    return {
        "total_processed_events": processed,
        "queue_backlog": backlog,
        "queue_pending_unacked": pending,
        "event_distribution_by_type": by_type,
        "event_distribution_by_anomaly_class": by_class,
        "event_distribution_by_factory": by_factory,
        "events_last_5min": last_5min,
        "events_per_minute_last_5min": round(last_5min / 5, 2),
        "last_event_at": last_ts.isoformat() if last_ts else None,
        "seconds_since_last_event": seconds_since_last,
    }


@app.get("/events/recent")
async def recent_events(limit: int = 20):
    """Convenience endpoint for the monitoring UI's cloud-status panel —
    not required by the assignment but makes the UI feel alive rather than
    just showing counters."""
    async for session in get_session():
        events = await most_recent_events(session, limit=min(limit, 100))
        return [
            {
                "event_id": e.event_id,
                "device_id": e.device_id,
                "machine_id": e.machine_id,
                "factory_id": e.factory_id,
                "event_type": e.event_type,
                "anomaly_class": e.anomaly_class,
                "confidence": e.confidence,
                "has_frame": e.has_frame,
                "processed_at": e.processed_at.isoformat(),
            }
            for e in events
        ]
