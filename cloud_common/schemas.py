"""
cloud_common.schemas
=====================
Shared event schemas for the NexTex AI cloud ingestion pipeline.

Both event types described in the assignment are modeled explicitly rather
than collapsed into one generic "event" blob, because they have genuinely
different payloads and different downstream handling:

  - NEW_ANOMALY_CLASS: a frame is attached (base64-encoded JPEG in this
    take-home; in production this would be a pre-signed upload URL to
    object storage, with the event carrying only the resulting object key).
    These events feed the retraining data pipeline.

  - THRESHOLD_EXCEEDED: a lightweight alarm/event with no frame required
    (the frame may optionally be attached for audit purposes, but the
    event's primary job is low-latency alerting, not data collection).

Keeping these as distinct Pydantic models (rather than one big optional-
field blob) means invalid combinations are rejected at the API boundary
instead of silently accepted and mishandled downstream.
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Optional
from uuid import uuid4

from pydantic import BaseModel, Field, field_validator


class EventType(str, Enum):
    NEW_ANOMALY_CLASS = "new_anomaly_class"
    THRESHOLD_EXCEEDED = "threshold_exceeded"


class AnomalyResult(BaseModel):
    """The mocked/real anomaly-detection model output attached to a frame."""
    anomaly_class: str = Field(..., description="e.g. 'needle_line', 'oil_stain', 'hole'")
    confidence: float = Field(..., ge=0.0, le=1.0)


class BaseEdgeEvent(BaseModel):
    """Fields common to every event a Jetson device sends to the cloud."""
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    device_id: str = Field(..., description="Jetson device identifier, e.g. 'jetson-terrot-de-01-m03'")
    machine_id: str = Field(..., description="Knitting machine identifier")
    factory_id: str = Field(..., description="Pilot factory / tenant identifier")
    event_type: EventType
    detection: AnomalyResult
    frame_id: str = Field(..., description="Unique identifier for the source frame")
    captured_at: datetime = Field(..., description="When the frame was captured on-device")
    sent_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @field_validator("captured_at", "sent_at")
    @classmethod
    def _ensure_tz(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            return v.replace(tzinfo=timezone.utc)
        return v


class NewAnomalyClassEvent(BaseEdgeEvent):
    """
    Sent the FIRST time a device observes an anomaly class it hasn't
    reported before (or hasn't reported recently — see README for the
    de-dup / cooldown discussion). Frame image is required: this event
    exists specifically to feed future retraining, so the pixel data is
    the whole point.
    """
    event_type: EventType = EventType.NEW_ANOMALY_CLASS
    frame_jpeg_b64: str = Field(..., description="Base64-encoded JPEG frame")


class ThresholdExceededEvent(BaseEdgeEvent):
    """
    Sent whenever detection confidence exceeds the configured alarm
    threshold, regardless of whether the class itself is new. This is the
    "operator alarm" path and is optimized for low latency, not data
    volume — frame is optional and omitted by default.
    """
    event_type: EventType = EventType.THRESHOLD_EXCEEDED
    frame_jpeg_b64: Optional[str] = None
    threshold_used: float = Field(..., ge=0.0, le=1.0)


class IngestAck(BaseModel):
    event_id: str
    accepted: bool
    queued_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
