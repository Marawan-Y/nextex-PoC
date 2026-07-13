# Design Notes

## 1. Local disk vs. MQTT for Jetson telemetry & machine sensor data

**Decision: both — local disk as the durability layer, MQTT as the transport layer, in that order.**

The two options aren't really competing on the same axis. MQTT answers *"how does telemetry get from the device to anyone who wants to consume it live"* — it's the correct choice there: lightweight, pub/sub, the de facto standard in industrial IoT, and it lets multiple subscribers (a local dashboard, a cloud bridge, a future analytics service) consume the same stream without the publisher knowing or caring who's listening. Local disk answers a different question: *"what happens to a telemetry point if nobody is listening right now"* — and on a factory floor, "nobody is listening right now" (broker unreachable, network segment down, Jetson temporarily isolated) is a normal operating condition, not an edge case.

Treating MQTT as the only copy means a broker outage silently and permanently drops telemetry for its duration — invisible data loss is the worst kind, because nothing errors, nothing alerts, the gap is just missing from the historical record. So the implementation (`edge_simulator/telemetry.py`) writes every point to a local append-only JSONL buffer *first*, unconditionally, and only then makes a best-effort MQTT publish. A background thread periodically re-publishes anything not yet confirmed sent, so a temporary outage self-heals once connectivity returns rather than requiring manual recovery.

This is the same "local-buffer-first, network-second" pattern applied to the cloud event path too (`edge_simulator/main.py`'s outbox) — one consistent principle applied at every network boundary in the system, rather than a special case invented just for telemetry.

If I had to pick *only one* for a resource-constrained device that truly couldn't run both: local disk, because durability beats liveness for sensor/telemetry data specifically — a KPI computed an hour late from a replayed buffer is still correct; a KPI computed from data that was silently dropped is wrong and nobody knows it's wrong.

## 2. Production considerations (~half page)

This take-home intentionally keeps scope tight (Docker Compose, no cloud). Moving it to production, in rough priority order:

- **Object storage for frames, not a disk volume.** The current `frame_storage` Docker volume works for a demo but doesn't survive node loss, isn't horizontally accessible, and has no lifecycle policy. Swap for S3/GCS/Azure Blob with the event carrying an object key instead of a local path — a one-line change in `cloud_consumer/worker.py`'s `_save_frame`.

- **Security.** CORS is wide open (`allow_origins=["*"]`) and Mosquitto allows anonymous connections — both fine for a local demo, both wrong in production. Production needs per-device MQTT credentials (or mutual TLS with per-device certificates, as discussed in the earlier system-design materials for this exact platform), API authentication for `cloud-api` (device-scoped API keys or mTLS), and TLS everywhere in transit.

- **Schema migrations.** `db.py` uses `create_all()` for simplicity here; production needs Alembic-managed migrations so schema changes are versioned, reviewable, and reversible instead of implicit.

- **Multi-tenancy enforcement at the DB layer.** `factory_id` exists as a column today but isn't yet enforced via row-level security — fine with one simulated factory, not fine once multiple real pilot factories' data lives in the same table. This was already scoped as a pattern in the earlier system-design prep (pooled schema + Postgres RLS) and would be the first thing added before onboarding a second real tenant.

- **Observability.** `/metrics` as hand-rolled JSON is enough to satisfy this assignment; production wants a real Prometheus `/metrics` endpoint (or push to an existing observability stack), structured JSON logs with correlation IDs, and alerting rules on the same signals already being computed here (`seconds_since_last_event`, `queue_pending_unacked` climbing).

- **Load-tested consumer scaling.** Horizontal scaling via `--scale cloud-consumer=N` is wired up (Redis consumer groups handle partitioning automatically), but hasn't been validated under realistic multi-device, multi-factory load — that validation, plus tuning `BATCH_SIZE`/`BLOCK_MS`, would be the first real production-readiness task.

- **Retraining pipeline consuming `new_anomaly_class` frames.** Today those frames just land in storage; production needs the actual downstream consumer (labeling queue, dataset versioning, retraining trigger) that this event type was designed to feed — this take-home stops at "the frame arrives safely," which is the right boundary for the assignment as scoped.
