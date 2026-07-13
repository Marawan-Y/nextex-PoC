# NexTex AI — Proof of Concept

**Anomaly-detection event pipeline: cloud queue/consumer service + Jetson edge simulator with live monitoring UI.**

Everything runs locally via `docker compose up` — no cloud infrastructure required.

```
docker compose up --build
```

Then open:
- **Monitoring UI**: http://localhost:8002
- **Cloud API docs (Swagger)**: http://localhost:8001/docs
- **Cloud health**: http://localhost:8001/health
- **Cloud metrics**: http://localhost:8001/metrics

First boot takes a few seconds while Postgres/Redis become healthy and the consumer creates its consumer group. The UI will show "disconnected" for a moment, then start streaming.

---

## 1. What this is

Two cooperating systems, matching the two parts of the assignment:

| Part | Service(s) | Role |
|---|---|---|
| **1. Cloud side** | `cloud-api`, `cloud-consumer`, `redis`, `postgres` | Receives `new_anomaly_class` and `threshold_exceeded` events, queues them, persists them, exposes health/metrics |
| **2. Edge + UI** | `edge-simulator`, `mosquitto` | Simulates a Jetson device streaming frames, runs mocked detection, emits events to the cloud, publishes telemetry, serves the monitoring UI |

See `diagrams/architecture.png` for the full data-flow diagram and `diagrams/event_sequence.png` for a single event's lifecycle from frame capture to persisted metric.

---

## 2. Architecture

![architecture](diagrams/architecture.png)

**Why a separate `cloud-api` and `cloud-consumer` process, not one service?**
This is the central design decision of Part 1, so it's worth stating explicitly rather than leaving it implicit in the code: ingestion throughput and processing throughput are decoupled on purpose. If Postgres slows down or the consumer briefly falls behind, `cloud-api` keeps accepting events at full speed — the *queue backlog* grows (visible immediately in `/metrics`), but Jetson devices never see elevated latency or rejected requests. A single combined process would couple those two concerns together, which is exactly the failure mode a queue is supposed to prevent.

**Why the local outbox / hybrid telemetry pattern on the edge side?**
Factory networks drop. Every network hop in this system (event POSTs to the cloud, telemetry publishes to MQTT) is therefore "local-buffer-first, network-second": write it durably where it's produced, then best-effort push it out, with a background retry loop that drains the backlog once connectivity returns. See `docs/DESIGN_NOTES.md` for the full reasoning, especially around the disk-vs-MQTT choice for telemetry specifically.

---

## 3. Part 1 — Cloud queue/consumer service

### 3.1 Why Redis Streams (vs RabbitMQ / SQS-LocalStack / Kafka)

| Option | Verdict | Reasoning |
|---|---|---|
| **Redis Streams (chosen)** | Best fit | One extra container (Redis is likely already useful elsewhere — caching, rate limiting). Consumer groups give at-least-once delivery, XPENDING/XAUTOCLAIM give backlog + stale-consumer recovery for free, XLEN gives an O(1) queue-depth read for /metrics. Minimal ops overhead — the right trade-off for a pre-MVP, small-team startup context. |
| RabbitMQ | Reasonable, more ops | Powerful routing (exchanges/bindings) this use case doesn't need yet. A second distinct system to run, monitor, and reason about, for capability we're not using. Would reconsider if we needed complex routing (e.g. priority queues per factory) later. |
| SQS via LocalStack | Weakest fit for this exercise | LocalStack SQS is a simulation of a specific cloud vendor's API; it doesn't give you anything extra locally and the assignment explicitly says no cloud infra is required. |
| Kafka | Overkill | Built for a different scale/durability profile (long retention, many independent consumer groups replaying history). Postgres is the system of record here, not the stream — we don't need log replay semantics. |

### 3.2 Event types & endpoints

Two distinct Pydantic models, not one generic "event" blob (see `cloud_common/schemas.py`) — they have different required fields (a frame is *mandatory* for `new_anomaly_class`, *optional and omitted by default* for `threshold_exceeded`) and different purposes (retraining data vs. low-latency alarms).

```
POST /events/new-anomaly-class     — frame required, feeds retraining
POST /events/threshold-exceeded    — frame optional, feeds alarms
GET  /health                       — liveness + dependency checks (Redis, Postgres)
GET  /metrics                      — see below
GET  /events/recent?limit=20       — convenience endpoint for the UI's event feed
```

### 3.3 Metrics endpoint

`GET /metrics` returns, beyond what the assignment explicitly asked for:

```json
{
  "total_processed_events": 142,
  "queue_backlog": 3,
  "queue_pending_unacked": 1,
  "event_distribution_by_type": {"threshold_exceeded": 98, "new_anomaly_class": 44},
  "event_distribution_by_anomaly_class": {"needle_line": 51, "oil_stain": 30},
  "event_distribution_by_factory": {"terrot-de-01": 142},
  "events_last_5min": 40,
  "events_per_minute_last_5min": 8.0,
  "last_event_at": "2026-07-11T00:12:03.221Z",
  "seconds_since_last_event": 4.2
}
```

`queue_backlog` (XLEN) and `queue_pending_unacked` (XPENDING) are reported separately on purpose: backlog is "everything ever queued," pending is "in-flight right now, not yet acknowledged" — a growing `queue_pending_unacked` with a flat `queue_backlog` means a consumer is stuck, a different failure mode than ingestion simply outrunning processing.

`seconds_since_last_event` was added because it's usually the first thing an on-call person actually looks at — a pipeline can report zero errors while being completely stalled, and a cumulative total count won't show that.

### 3.4 Persistence

Single Postgres `events` table (`cloud_common/db.py`) with both event types sharing nullable type-specific columns (`threshold_used`, `frame_path`) rather than two separate tables or a schemaless JSONB blob — both event types share the overwhelming majority of fields and query patterns, so one table with clear columns is the right level of structure at this scale. Frames are written to a disk volume (`frame_storage`) as a stand-in for object storage (S3/GCS/Blob) — the event row stores the path, not the bytes, exactly as it would with a real object store and a pre-signed URL.

### 3.5 Reliability behavior (tested, not just claimed)

- If `cloud-consumer` is down or crashes mid-batch, events accepted by `cloud-api` sit safely in the Redis stream (`queue_backlog` rises).
- Entries delivered to a consumer but not acked within `PENDING_CLAIM_IDLE_MS` (default 60s) are reclaimed via XAUTOCLAIM and retried automatically — no message is silently dropped by a crashed consumer.
- A DB insert failure for one event does not ack that event; it is retried on the next reclaim pass rather than lost.
- Scaling: `docker compose up --scale cloud-consumer=3` adds more consumers; Redis consumer groups partition delivery across them automatically with no other code change.

---

## 4. Part 2 — Jetson simulation + monitoring UI

### 4.1 Dataset

The assignment suggests a public Kaggle fabric/textile defect dataset. This repo does not vendor one directly — pulling from Kaggle requires authenticated API credentials, and shipping a third party's dataset inside a take-home repo isn't good practice regardless. Instead (`edge_simulator/dataset.py`):

1. If real images exist at `edge_simulator/data/fabric_images/`, they're used (label = parent folder name).
2. Otherwise, a deterministic synthetic fabric-defect generator produces frames on the fly — periodic knit texture + injected defects (needle line, horizontal distortion, oil stain, stitch irregularity, hole), so the entire system runs end-to-end with zero setup.

See `docs/DATASET.md` for the exact `kaggle datasets download` command to drop a real dataset in — no code changes needed, just populate the folder and restart.

### 4.2 Mocked detection

`edge_simulator/mock_detector.py` doesn't just return random noise — it simulates plausible model behavior (high-confidence correct classification most of the time, occasional low-confidence false positives on clean frames, occasional misclassification on genuine defects) so the demo's alert/new-class logic is exercised against a realistic confidence trace rather than either a perfect oracle or pure noise.

### 4.3 Event-emission rules

Implemented exactly as specified, plus one addition flagged below:

- New anomaly class detected -> `POST /events/new-anomaly-class` (frame attached)
- Confidence > threshold (default 0.85, configurable via `ALARM_THRESHOLD`) -> `POST /events/threshold-exceeded`

**Addition — cooldown on "new class" events (`NEW_CLASS_COOLDOWN_SECONDS`, default 120s):** without this, a machine with a persistent, ongoing defect would fire a "new class" event on every single frame, which defeats the purpose of that event type (it exists to feed retraining with fresh examples, not to duplicate the alarm stream at full frame rate). This is exactly the kind of "requirement I'd modify and explain why" the assignment invites — happy to discuss trade-offs (shorter/longer cooldown, per-class cooldowns) in the follow-up interview.

### 4.4 Monitoring UI

Single-page app (`edge_simulator/static/index.html`, vanilla JS, no build step) showing:
- Live camera feed over WebSocket (`/ws/camera-feed`), frame-by-frame
- Detection overlay (class + confidence) on each frame
- A pulsing red border + banner when the alarm threshold is exceeded
- Edge-device stats (frames streamed, events sent/buffered, outbox depth)
- Cloud ingestion health/metrics panel — polls `cloud-api`'s `/health` and `/metrics` directly (the endpoints built in Part 1), including live bar charts of event distribution by type and by anomaly class

### 4.5 Telemetry: local disk vs MQTT — the choice made, briefly

Chosen: hybrid — local disk first, MQTT second (store-and-forward). Full reasoning in `docs/DESIGN_NOTES.md`; short version: MQTT is the right transport for telemetry (lightweight pub/sub, standard for IIoT, lets any interested subscriber consume live data), but treating it as the only copy means a broker/network outage silently drops telemetry. Every point is durably appended to a local JSONL buffer first; a background thread publishes to MQTT and retries anything unconfirmed. This is implemented, not just described — see `edge_simulator/telemetry.py`.

---

## 5. Repository structure

```
.
├── docker-compose.yml
├── mosquitto/mosquitto.conf
├── cloud_common/              # shared by cloud-api & cloud-consumer
│   ├── schemas.py             # Pydantic event models
│   ├── db.py                  # SQLAlchemy async models + queries
│   ├── queue.py               # Redis Streams wrapper
│   └── config.py
├── cloud_api/
│   ├── main.py                # FastAPI: ingestion + health + metrics
│   └── Dockerfile
├── cloud_consumer/
│   ├── worker.py               # consumer loop, retry/reclaim logic
│   └── Dockerfile
├── edge_simulator/
│   ├── main.py                 # FastAPI + WebSocket streaming server
│   ├── dataset.py              # real-or-synthetic frame source
│   ├── mock_detector.py        # realistic mocked model output
│   ├── telemetry.py            # hybrid disk+MQTT publisher
│   ├── static/index.html       # monitoring UI (vanilla JS)
│   └── Dockerfile
├── diagrams/
│   ├── architecture.png
│   └── event_sequence.png
├── docs/
│   ├── DESIGN_NOTES.md          # disk vs MQTT + production considerations
│   └── DATASET.md               # how to plug in a real Kaggle dataset
└── notebooks/
    └── walkthrough.ipynb        # end-to-end runnable demo notebook
```

---

## 6. Environment variables (all optional, sensible defaults for docker-compose)

| Variable | Service | Default | Purpose |
|---|---|---|---|
| `REDIS_URL` | cloud-api, cloud-consumer | redis://redis:6379/0 | Queue connection |
| `DATABASE_URL` | cloud-api, cloud-consumer | postgresql+asyncpg://... | Persistence |
| `FRAME_STORAGE_DIR` | cloud-api, cloud-consumer | /data/frames | Frame storage stand-in for object storage |
| `CONSUMER_NAME` | cloud-consumer | consumer-1 | Distinguishes replicas when scaled |
| `ALARM_THRESHOLD` | edge-simulator | 0.85 | Confidence threshold for threshold_exceeded |
| `NEW_CLASS_COOLDOWN_SECONDS` | edge-simulator | 120 | Cooldown before re-firing new_anomaly_class for the same class |
| `STREAM_FPS` | edge-simulator | 3.0 | Simulated camera frame rate |
| `MQTT_HOST` / `MQTT_PORT` | edge-simulator | mosquitto / 1883 | Telemetry broker |
| `CLOUD_API_BASE` | edge-simulator | http://cloud-api:8001 | Where to POST events |

---

## 7. What was tested

Every component in this repo was run and exercised directly (not just written and assumed correct) during development, outside Docker (Redis/Postgres/Mosquitto installed locally in the dev sandbox, equivalent to what docker-compose provisions) — see `notebooks/walkthrough.ipynb` for a live, **already-executed** record (outputs included, not just source) of:
- Both event types being ingested, queued, persisted, and reflected in `/metrics`
- Two real bugs found and fixed via this exact testing loop:
  1. A datetime-deserialization bug in the consumer (events silently stuck in `pending`, not lost — proving the retry design actually works)
  2. A concurrent-schema-creation race between `cloud-api` and `cloud-consumer` on a cold start (both racing to `CREATE TABLE`), fixed with a retry in `init_db()`
- A full from-scratch restart (dropped database, flushed queue) completing cleanly after both fixes
- The WebSocket stream producing frames, mocked detections, a `new_anomaly_class` event, a cooldown correctly suppressing a duplicate, and a `threshold_exceeded` alarm
- Telemetry points being durably written to disk and published over MQTT

## 8. Where I'd take this next (production considerations)

Covered in half a page in `docs/DESIGN_NOTES.md` Section 2 — short version: object storage instead of a disk volume for frames, TLS + per-device auth instead of open CORS/anonymous MQTT, Alembic migrations instead of `create_all`, structured logging + real metrics export (Prometheus) instead of a JSON endpoint, and horizontal scaling validation for the consumer under real load.
