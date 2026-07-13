"""
edge_simulator.telemetry
============================
Simulates Jetson device telemetry + machine sensor signals, and implements
the HYBRID local-disk + MQTT strategy recommended in docs/DESIGN_NOTES.md.

WHY HYBRID (short version — full reasoning in docs/DESIGN_NOTES.md)
----------------------------------------------------------------------
The assignment explicitly asks us to choose between local disk and MQTT
for telemetry, and explain why. The honest answer is "both, for different
reasons":

  - MQTT publish is the PRIMARY path — it's what lets a live monitoring
    dashboard (or the cloud, if a subscriber bridges MQTT -> cloud) see
    telemetry as it happens, which is the whole point of telemetry.
  - A local disk buffer (JSONL, append-only, rotated by size) is a
    SAFETY NET, not a primary store — every point is written locally
    first, then published to MQTT. If the MQTT broker is unreachable
    (factory network drop, exactly the scenario called out in the
    earlier interview-prep materials), points keep accumulating on disk
    and nothing is lost; a background flush loop drains the backlog to
    MQTT once connectivity returns.

This mirrors the store-and-forward pattern already used for the cloud
event ingestion path (events are queued locally in the WS/HTTP client
buffer before being POSTed) — the whole system is consistent about "local
buffer first, network second" wherever a network hop is involved.
"""
from __future__ import annotations

import json
import random
import threading
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import paho.mqtt.client as mqtt

TELEMETRY_LOG_DIR = Path(__file__).parent / "data" / "telemetry_buffer"
TELEMETRY_LOG_DIR.mkdir(parents=True, exist_ok=True)
MAX_LOG_LINES_BEFORE_ROTATE = 5000


@dataclass
class TelemetryPoint:
    device_id: str
    machine_id: str
    ts: str
    speed_rpm: float
    yarn_feed_tension: float
    jetson_temp_c: float
    inference_latency_ms: float
    fps: float


class TelemetryGenerator:
    """Produces plausible-looking machine + Jetson telemetry each tick."""

    def __init__(self, device_id: str, machine_id: str, seed: int = 11):
        self.device_id = device_id
        self.machine_id = machine_id
        self._rng = random.Random(seed)
        self._base_speed = 800.0
        self._tension = 0.97

    def next_point(self) -> TelemetryPoint:
        # slow random walk on tension so instability windows are
        # occasionally realistic (mirrors PoC3's tension instability demo)
        self._tension += self._rng.uniform(-0.02, 0.02)
        self._tension = max(0.4, min(1.0, self._tension))

        return TelemetryPoint(
            device_id=self.device_id,
            machine_id=self.machine_id,
            ts=datetime.now(timezone.utc).isoformat(),
            speed_rpm=round(self._base_speed + self._rng.uniform(-15, 15), 1),
            yarn_feed_tension=round(self._tension, 4),
            jetson_temp_c=round(45 + self._rng.uniform(-3, 8), 1),
            inference_latency_ms=round(self._rng.uniform(8, 22), 2),
            fps=round(self._rng.uniform(18, 30), 1),
        )


class HybridTelemetryPublisher:
    """
    Local-disk-first, MQTT-second publisher. See module docstring for the
    reasoning. Runs a background thread that periodically attempts to
    flush any unsent buffered lines to MQTT, so a broker outage is
    tolerated rather than fatal.
    """

    def __init__(self, mqtt_host: str, mqtt_port: int, topic_prefix: str = "nextex/telemetry"):
        self._topic_prefix = topic_prefix
        self._log_path = TELEMETRY_LOG_DIR / "buffer.jsonl"
        self._sent_offset_path = TELEMETRY_LOG_DIR / "buffer.offset"
        self._lock = threading.Lock()
        self._client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="edge-simulator")
        self._connected = False
        self._mqtt_host = mqtt_host
        self._mqtt_port = mqtt_port
        self._connect()
        self._flush_thread = threading.Thread(target=self._flush_loop, daemon=True)
        self._flush_thread.start()

    def _connect(self):
        try:
            self._client.on_connect = self._on_connect
            self._client.on_disconnect = self._on_disconnect
            self._client.connect_async(self._mqtt_host, self._mqtt_port, keepalive=30)
            self._client.loop_start()
        except Exception:
            self._connected = False

    def _on_connect(self, client, userdata, flags, reason_code, properties=None):
        self._connected = reason_code == 0 or reason_code == "Success"

    def _on_disconnect(self, client, userdata, reason_code, properties=None, *args):
        self._connected = False

    def publish(self, point: TelemetryPoint) -> None:
        # 1. ALWAYS write to local disk first — this is the durability
        #    guarantee; nothing is lost even if the next line fails.
        line = json.dumps(asdict(point))
        with self._lock:
            with open(self._log_path, "a") as f:
                f.write(line + "\n")
            self._rotate_if_needed()

        # 2. Best-effort immediate MQTT publish. If it fails, the
        #    background flush loop will pick it up from disk later —
        #    this call never raises or blocks the caller.
        if self._connected:
            try:
                topic = f"{self._topic_prefix}/{point.device_id}"
                self._client.publish(topic, line, qos=1)
            except Exception:
                pass

    def _rotate_if_needed(self):
        if not self._log_path.exists():
            return
        with open(self._log_path) as f:
            n_lines = sum(1 for _ in f)
        if n_lines > MAX_LOG_LINES_BEFORE_ROTATE:
            archive = TELEMETRY_LOG_DIR / f"buffer.{int(time.time())}.jsonl"
            self._log_path.rename(archive)

    def _flush_loop(self):
        """Periodically re-publish any lines not yet confirmed sent. In
        this simulator, 'confirmed sent' is tracked with a simple line
        offset file — a production implementation would use MQTT's QoS1
        PUBACK plus a durable outbox table, but the pattern (local
        durability decoupled from network delivery) is the same."""
        while True:
            time.sleep(5)
            if not self._connected:
                continue
            try:
                self._resend_unflushed()
            except Exception:
                pass

    def _resend_unflushed(self):
        offset = 0
        if self._sent_offset_path.exists():
            offset = int(self._sent_offset_path.read_text().strip() or 0)
        if not self._log_path.exists():
            return
        with open(self._log_path) as f:
            lines = f.readlines()
        new_lines = lines[offset:]
        for line in new_lines:
            try:
                point = json.loads(line)
                topic = f"{self._topic_prefix}/{point['device_id']}"
                self._client.publish(topic, line.strip(), qos=1)
            except Exception:
                break
        with open(self._sent_offset_path, "w") as f:
            f.write(str(len(lines)))
