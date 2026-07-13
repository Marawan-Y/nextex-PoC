"""
edge_simulator / main.py
===========================
Simulates a Jetson Orin device end-to-end:
  - Streams frames sequentially over a WebSocket (as if from a live
    camera) to the browser UI.
  - Runs the mocked anomaly detection model per frame.
  - Applies the two event-emission rules from the assignment:
      * first time a class is seen (per device, with a cooldown — see
        NEW_CLASS_COOLDOWN_SECONDS below) -> new_anomaly_class event
      * confidence > ALARM_THRESHOLD -> threshold_exceeded event
  - Posts qualifying events to the cloud ingestion API (cloud_api),
    store-and-forward buffered locally if the cloud is unreachable.
  - Publishes Jetson/machine telemetry via the hybrid local-disk+MQTT
    publisher (see telemetry.py).
  - Serves the static monitoring UI and a small REST surface the UI polls.
"""
from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from .dataset import FrameSource
from .mock_detector import MockDetector
from .telemetry import TelemetryGenerator, HybridTelemetryPublisher

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("edge-simulator")

# --- Config (env-overridable, matching docker-compose.yml) ---
DEVICE_ID = os.environ.get("DEVICE_ID", "jetson-terrot-de-01-m03")
MACHINE_ID = os.environ.get("MACHINE_ID", "M03")
FACTORY_ID = os.environ.get("FACTORY_ID", "terrot-de-01")
CLOUD_API_BASE = os.environ.get("CLOUD_API_BASE", "http://localhost:8001")
MQTT_HOST = os.environ.get("MQTT_HOST", "localhost")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
ALARM_THRESHOLD = float(os.environ.get("ALARM_THRESHOLD", "0.85"))
STREAM_FPS = float(os.environ.get("STREAM_FPS", "2.0"))
NEW_CLASS_COOLDOWN_SECONDS = int(os.environ.get("NEW_CLASS_COOLDOWN_SECONDS", "120"))

app = FastAPI(title="NexTex Edge Simulator")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

STATIC_DIR = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

frame_source = FrameSource(seed=42)
detector = MockDetector(seed=7)
telemetry_gen = TelemetryGenerator(device_id=DEVICE_ID, machine_id=MACHINE_ID)
telemetry_publisher: HybridTelemetryPublisher | None = None

# Per-device "classes seen recently" for the new_anomaly_class cooldown —
# without a cooldown, a persistently-defective machine would fire a
# "new class" event on literally every frame, which defeats the purpose
# (that event type exists to feed retraining, not to duplicate the alarm
# stream).
_seen_classes: dict[str, float] = {}  # class -> last_seen_ts

# Local store-and-forward outbox for cloud events, in case the cloud API
# is briefly unreachable (mirrors the telemetry hybrid pattern).
_outbox: deque = deque()
_stats = {
    "frames_streamed": 0,
    "events_sent_to_cloud": 0,
    "events_buffered_locally": 0,
    "cloud_send_failures": 0,
    "started_at": time.time(),
}


@app.on_event("startup")
async def startup():
    global telemetry_publisher
    telemetry_publisher = HybridTelemetryPublisher(mqtt_host=MQTT_HOST, mqtt_port=MQTT_PORT)
    asyncio.create_task(_telemetry_loop())
    asyncio.create_task(_outbox_drain_loop())
    log.info("edge simulator started, device_id=%s, frame source: %s", DEVICE_ID, frame_source.source_description)


async def _telemetry_loop():
    while True:
        point = telemetry_gen.next_point()
        telemetry_publisher.publish(point)
        await asyncio.sleep(2.0)


async def _post_event(endpoint: str, payload: dict) -> bool:
    url = f"{CLOUD_API_BASE}{endpoint}"
    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            resp = await client.post(url, json=payload)
            resp.raise_for_status()
            _stats["events_sent_to_cloud"] += 1
            return True
    except Exception as e:
        log.warning("cloud POST failed (%s) — buffering locally: %s", endpoint, e)
        _stats["cloud_send_failures"] += 1
        return False


async def _outbox_drain_loop():
    """Retries buffered events against the cloud API periodically — the
    store-and-forward safety net for the event-ingestion path, mirroring
    the telemetry hybrid publisher's flush loop."""
    while True:
        await asyncio.sleep(4.0)
        if not _outbox:
            continue
        remaining = deque()
        while _outbox:
            endpoint, payload = _outbox.popleft()
            ok = await _post_event(endpoint, payload)
            if not ok:
                remaining.append((endpoint, payload))
        for item in remaining:
            _outbox.append(item)


def _maybe_build_events(frame_index: int, frame_jpeg: bytes, detection) -> list[tuple[str, dict]]:
    """Given one frame's mocked detection, decide which cloud event(s) (if
    any) should be emitted, per the assignment's two trigger rules."""
    events: list[tuple[str, dict]] = []
    now = time.time()
    now_iso = datetime.now(timezone.utc).isoformat()
    frame_id = f"{DEVICE_ID}-{frame_index}"

    is_new_class = (
        detection.anomaly_class != "no_defect"
        and (
            detection.anomaly_class not in _seen_classes
            or (now - _seen_classes[detection.anomaly_class]) > NEW_CLASS_COOLDOWN_SECONDS
        )
    )
    if is_new_class:
        _seen_classes[detection.anomaly_class] = now
        events.append((
            "/events/new-anomaly-class",
            {
                "device_id": DEVICE_ID,
                "machine_id": MACHINE_ID,
                "factory_id": FACTORY_ID,
                "detection": {"anomaly_class": detection.anomaly_class, "confidence": detection.confidence},
                "frame_id": frame_id,
                "captured_at": now_iso,
                "frame_jpeg_b64": base64.b64encode(frame_jpeg).decode(),
            },
        ))

    if detection.confidence > ALARM_THRESHOLD and detection.anomaly_class != "no_defect":
        events.append((
            "/events/threshold-exceeded",
            {
                "device_id": DEVICE_ID,
                "machine_id": MACHINE_ID,
                "factory_id": FACTORY_ID,
                "detection": {"anomaly_class": detection.anomaly_class, "confidence": detection.confidence},
                "frame_id": frame_id,
                "captured_at": now_iso,
                "threshold_used": ALARM_THRESHOLD,
                # frame omitted by design for alarm events — see schemas.py docstring
            },
        ))

    return events


@app.get("/config")
async def config():
    """Lets the static UI discover runtime config without hardcoding it
    into the JS bundle."""
    return {
        "device_id": DEVICE_ID,
        "machine_id": MACHINE_ID,
        "factory_id": FACTORY_ID,
        "cloud_api_base": CLOUD_API_BASE,
        "alarm_threshold": ALARM_THRESHOLD,
        "stream_fps": STREAM_FPS,
        "frame_source": frame_source.source_description,
    }


@app.get("/stats")
async def stats():
    return {
        **_stats,
        "uptime_seconds": round(time.time() - _stats["started_at"], 1),
        "outbox_depth": len(_outbox),
        "known_classes_seen": list(_seen_classes.keys()),
    }


@app.get("/")
async def index():
    return FileResponse(str(STATIC_DIR / "index.html"))


@app.websocket("/ws/camera-feed")
async def camera_feed(websocket: WebSocket):
    await websocket.accept()
    log.info("UI client connected to camera feed")
    interval = 1.0 / STREAM_FPS
    try:
        while True:
            frame = frame_source.next_frame()
            detection = detector.detect(frame.label)
            _stats["frames_streamed"] += 1

            events = _maybe_build_events(frame.index, frame.image_bytes, detection)
            for endpoint, payload in events:
                ok = await _post_event(endpoint, payload)
                if not ok:
                    _outbox.append((endpoint, payload))
                    _stats["events_buffered_locally"] += 1

            await websocket.send_text(json.dumps({
                "frame_index": frame.index,
                "image_b64": base64.b64encode(frame.image_bytes).decode(),
                "detection": {
                    "anomaly_class": detection.anomaly_class,
                    "confidence": detection.confidence,
                },
                "alarm": detection.confidence > ALARM_THRESHOLD and detection.anomaly_class != "no_defect",
                "events_emitted": [e[0] for e in events],
                "ts": datetime.now(timezone.utc).isoformat(),
            }))
            await asyncio.sleep(interval)
    except WebSocketDisconnect:
        log.info("UI client disconnected from camera feed")
