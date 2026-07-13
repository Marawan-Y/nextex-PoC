# NexTex AI — Proof of Concept

An end-to-end textile anomaly-detection pipeline with a simulated Jetson edge device, live monitoring UI, Redis Streams, an independent cloud consumer, PostgreSQL persistence, and MQTT telemetry.

The project runs locally with Docker Compose. No cloud account, Node.js installation, Kaggle account, physical Jetson device, or external dataset is required for the default demo.

---

## Table of contents

1. [Project overview](#project-overview)
2. [Architecture](#architecture)
3. [Services and URLs](#services-and-urls)
4. [Quick start](#quick-start)
5. [Approach A — Docker on Windows 11](#approach-a--docker-on-windows-11)
6. [Running after the initial setup](#running-after-the-initial-setup)
7. [Meeting and demo runbook](#meeting-and-demo-runbook)
8. [Approach B — Manual setup in Ubuntu WSL](#approach-b--manual-setup-in-ubuntu-wsl)
9. [Dataset options](#dataset-options)
10. [Environment variables](#environment-variables)
11. [Development workflow](#development-workflow)
12. [Logs and diagnostics](#logs-and-diagnostics)
13. [Stopping, resetting, and preserving data](#stopping-resetting-and-preserving-data)
14. [Troubleshooting](#troubleshooting)
15. [Repository structure](#repository-structure)
16. [Design decisions](#design-decisions)
17. [Production considerations](#production-considerations)

---

## Project overview

NexTex AI contains two cooperating systems.

| Area | Services | Responsibility |
|---|---|---|
| Cloud pipeline | `cloud-api`, `cloud-consumer`, `redis`, `postgres` | Validate events, queue them, persist them, and expose health and metrics |
| Edge simulation | `edge-simulator`, `mosquitto` | Generate frames, run mocked detection, emit events, publish telemetry, and serve the monitoring UI |

The Edge Simulator emits two event types:

| Event | Trigger | Frame |
|---|---|---|
| `new_anomaly_class` | A defect class is observed for the first time or after its cooldown | Required |
| `threshold_exceeded` | Defect confidence exceeds `ALARM_THRESHOLD` | Omitted by default |

The default frame source is a deterministic synthetic textile generator. If real images exist under `edge_simulator/data/fabric_images/`, they are used automatically.

---

## Architecture

![NexTex architecture](diagrams/architecture.png)

```text
Synthetic or real images
          |
          v
Edge Simulator + Mock Detector
          |
          +---- WebSocket ----> Monitoring UI
          |
          +---- HTTP events --> Cloud API
          |                        |
          |                        v
          |                  Redis Stream
          |                        |
          |                        v
          |                  Cloud Consumer
          |                        |
          |                        v
          |                    PostgreSQL
          |
          +---- MQTT telemetry --> Mosquitto
          |
          +---- local buffering for temporary network failure
```

The Cloud API and Cloud Consumer are separate processes. The API can continue accepting events while persistence is temporarily slow. Redis consumer groups provide at-least-once delivery and stale-message recovery.

---

## Services and URLs

| Service | Host port | Purpose |
|---|---:|---|
| `edge-simulator` | `8002` | Dashboard, camera WebSocket, edge statistics |
| `cloud-api` | `8001` | Event ingestion, health, metrics, recent events |
| `postgres` | `5432` | Durable event storage |
| `redis` | `6379` | Redis Streams queue |
| `mosquitto` | `1883` | MQTT telemetry broker |
| `cloud-consumer` | none | Reads Redis and persists events |

| Page or endpoint | URL |
|---|---|
| Monitoring dashboard | http://localhost:8002 |
| Edge configuration | http://localhost:8002/config |
| Edge statistics | http://localhost:8002/stats |
| Swagger API documentation | http://localhost:8001/docs |
| Cloud health | http://localhost:8001/health |
| Cloud metrics | http://localhost:8001/metrics |
| Recent persisted events | http://localhost:8001/events/recent?limit=20 |

---

## Quick start

For a machine that has already completed the initial Docker setup:

```powershell
Set-Location "C:\path\to\nextex-PoC"

docker compose up -d
Start-Sleep -Seconds 10

docker compose ps
Invoke-RestMethod http://localhost:8001/health | ConvertTo-Json

Start-Process "http://localhost:8002"
Start-Process "http://localhost:8001/docs"
Start-Process "http://localhost:8001/metrics"
```

Stop the project while preserving data:

```powershell
docker compose down
```

Use `docker compose up -d --build` only after code, Dockerfile, dependency, or Compose changes.

---

# Approach A — Docker on Windows 11

This is the recommended and most reproducible approach.

Docker provides PostgreSQL, Redis, Mosquitto, Python 3.12, all Python dependencies, and all application processes.

## A1. Prerequisites

Required:

- Windows 11 64-bit
- Hardware virtualization enabled
- WSL 2
- Ubuntu under WSL
- Docker Desktop using the WSL 2 backend
- Git
- A modern browser

Not required:

- Node.js or npm
- Local PostgreSQL, Redis, or Mosquitto installations
- Kaggle credentials
- A cloud account
- A physical Jetson device

## A2. Install or verify WSL 2

Open **PowerShell as Administrator**:

```powershell
wsl --update
wsl --status
wsl --version
wsl -l -v
```

Ubuntu must show `VERSION 2`.

If WSL or Ubuntu is missing:

```powershell
wsl --install -d Ubuntu
```

Restart Windows if requested, then open Ubuntu once and create a Linux username and password.

Do not run `wsl --set-version Ubuntu 2` when `wsl -l -v` already shows version `2`.

## A3. Install and configure Docker Desktop

Install Docker Desktop for Windows and use the WSL 2 backend.

In Docker Desktop:

1. Open **Settings → General**.
2. Enable **Use the WSL 2 based engine**.
3. Open **Settings → Resources → WSL Integration**.
4. Enable the Ubuntu distribution.
5. Select **Apply & restart**.
6. Wait until Docker reports that the engine is running.

Verify from PowerShell:

```powershell
docker version
docker compose version
docker run --rm hello-world
```

Verify from Ubuntu WSL:

```bash
docker version
docker compose version
docker run --rm hello-world
```

Do not install Ubuntu's `docker.io` package while using Docker Desktop WSL integration. Two competing Docker installations create unnecessary daemon, socket, and PATH problems.

## A4. Install Git

Inside Ubuntu WSL:

```bash
sudo apt update
sudo apt install -y git curl
git --version
```

Git for Windows also works when the repository is managed from PowerShell.

## A5. Clone the repository

### Recommended: WSL filesystem

```bash
mkdir -p ~/projects
cd ~/projects

git clone https://github.com/Marawan-Y/nextex-PoC.git
cd nextex-PoC
```

The WSL filesystem normally performs better for Linux builds than a checkout under `/mnt/c`.

### Alternative: Windows filesystem

```powershell
Set-Location "C:\path\to\parent-folder"
git clone https://github.com/Marawan-Y/nextex-PoC.git
Set-Location ".\nextex-PoC"
```

A Windows checkout works, including paths containing spaces, when the full path is quoted.

## A6. Validate the Cloud API addresses

The Edge Simulator needs two Cloud API addresses:

```yaml
CLOUD_API_BASE: http://cloud-api:8001
CLOUD_API_PUBLIC_BASE: http://localhost:8001
```

| Variable | Used by | Correct Docker value |
|---|---|---|
| `CLOUD_API_BASE` | Edge container posting events to the Cloud API container | `http://cloud-api:8001` |
| `CLOUD_API_PUBLIC_BASE` | Browser dashboard polling through the Windows host | `http://localhost:8001` |

The browser cannot resolve the Docker-only hostname `cloud-api`.

Validate Compose:

```powershell
docker compose config | Select-String "CLOUD_API"
```

Expected:

```text
CLOUD_API_BASE: http://cloud-api:8001
CLOUD_API_PUBLIC_BASE: http://localhost:8001
```

When running, validate `/config`:

```powershell
Invoke-RestMethod http://localhost:8002/config | ConvertTo-Json
```

Expected field:

```json
"cloud_api_base": "http://localhost:8001"
```

## A7. First build and startup

From the repository root:

```powershell
docker compose config
docker compose up --build
```

The first build can take several minutes because Docker downloads images and Python packages.

A successful startup includes:

- PostgreSQL becomes healthy.
- Redis becomes healthy.
- Mosquitto accepts the Edge Simulator MQTT connection.
- Cloud API starts on port `8001`.
- Cloud Consumer joins the Redis consumer group.
- Edge Simulator starts on port `8002`.

Open:

```text
http://localhost:8002
```

The dashboard may show `disconnected` briefly during initialization.

## A8. First-run validation

Open a second PowerShell window in the repository.

```powershell
docker compose ps
```

Health:

```powershell
Invoke-RestMethod http://localhost:8001/health | ConvertTo-Json
```

Expected structure:

```json
{
  "status": "ok",
  "redis": "ok",
  "postgres": "ok"
}
```

Configuration:

```powershell
Invoke-RestMethod http://localhost:8002/config | ConvertTo-Json
```

Metrics:

```powershell
Invoke-RestMethod http://localhost:8001/metrics | ConvertTo-Json -Depth 5
```

Recent events:

```powershell
Invoke-RestMethod "http://localhost:8001/events/recent?limit=20" |
    ConvertTo-Json -Depth 5
```

Keep the dashboard open for several seconds. The WebSocket connection drives frame processing, and defects are intentionally less frequent than normal frames.

---

# Running after the initial setup

Do not rebuild on every launch.

## Normal startup

1. Start Docker Desktop.
2. Wait until the Docker engine is running.
3. Open PowerShell.
4. Move to the repository.
5. Start all services in detached mode.

```powershell
Set-Location "C:\path\to\nextex-PoC"

docker compose up -d
Start-Sleep -Seconds 10
docker compose ps
```

Health check:

```powershell
Invoke-RestMethod http://localhost:8001/health | ConvertTo-Json
```

Open the dashboard:

```powershell
Start-Process "http://localhost:8002"
```

## Normal shutdown

```powershell
docker compose down
```

This preserves PostgreSQL data, Redis data, frames, and Docker images.

Do not add `-v` during normal shutdown.

## Restart without rebuilding

```powershell
docker compose restart
```

Restart one service:

```powershell
docker compose restart edge-simulator
```

## Start after code or configuration changes

```powershell
docker compose down
docker compose up -d --build
```

## Force a fresh build

```powershell
docker compose down --remove-orphans
docker compose build --no-cache
docker compose up -d --force-recreate
```

Use this only when a normal rebuild does not pick up a confirmed source change.

---

# Meeting and demo runbook

Run the project once before the meeting. Do not make untested changes immediately before presenting.

## One-minute startup

```powershell
Set-Location "C:\path\to\nextex-PoC"

docker compose up -d
Start-Sleep -Seconds 10

docker compose ps

Invoke-RestMethod http://localhost:8001/health |
    ConvertTo-Json

Invoke-RestMethod http://localhost:8002/config |
    ConvertTo-Json

Start-Process "http://localhost:8002"
Start-Process "http://localhost:8001/docs"
Start-Process "http://localhost:8001/metrics"
```

Confirm:

- all six services are running,
- PostgreSQL and Redis are healthy,
- Cloud health is `ok`,
- `/config` returns `http://localhost:8001`,
- the camera feed is moving,
- event counters begin changing.

## Suggested presentation order

1. Dashboard: `http://localhost:8002`
2. Metrics: `http://localhost:8001/metrics`
3. Persisted events: `http://localhost:8001/events/recent?limit=20`
4. API documentation: `http://localhost:8001/docs`
5. Explain Redis Streams, the independent consumer, and local buffering.

## Optional `start-demo.ps1`

Create this file in the repository root:

```powershell
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

Write-Host "Checking Docker..." -ForegroundColor Cyan

docker info | Out-Null
if ($LASTEXITCODE -ne 0) {
    Write-Host "Docker is unavailable. Start Docker Desktop and retry." `
        -ForegroundColor Red
    exit 1
}

Write-Host "Starting NexTex services..." -ForegroundColor Cyan
docker compose up -d

if ($LASTEXITCODE -ne 0) {
    Write-Host "Docker Compose failed." -ForegroundColor Red
    exit 1
}

$deadline = (Get-Date).AddSeconds(60)
$health = $null

while ((Get-Date) -lt $deadline) {
    try {
        $health = Invoke-RestMethod `
            -Uri "http://localhost:8001/health" `
            -TimeoutSec 5

        if ($health.status -eq "ok") {
            break
        }
    }
    catch {
        Start-Sleep -Seconds 2
    }
}

docker compose ps

if ($null -eq $health -or $health.status -ne "ok") {
    Write-Host "Cloud API did not become healthy." -ForegroundColor Red
    docker compose logs --tail=100 cloud-api postgres redis
    exit 1
}

$config = Invoke-RestMethod `
    -Uri "http://localhost:8002/config" `
    -TimeoutSec 10

if ($config.cloud_api_base -ne "http://localhost:8001") {
    Write-Host "Incorrect public Cloud API URL: $($config.cloud_api_base)" `
        -ForegroundColor Red
    exit 1
}

Write-Host "NexTex is healthy. Opening demo pages..." `
    -ForegroundColor Green

Start-Process "http://localhost:8002"
Start-Process "http://localhost:8001/docs"
Start-Process "http://localhost:8001/metrics"
```

Run:

```powershell
.\start-demo.ps1
```

If PowerShell blocks local scripts:

```powershell
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
```

## Optional `stop-demo.ps1`

```powershell
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

docker compose down

if ($LASTEXITCODE -ne 0) {
    Write-Host "Shutdown returned an error." -ForegroundColor Red
    exit 1
}

Write-Host "NexTex stopped. Persistent data was preserved." `
    -ForegroundColor Green
```

---

# Approach B — Manual setup in Ubuntu WSL

Manual mode is useful for debugging individual services and understanding each dependency. It is less convenient than Docker.

Run all manual-mode components inside the same Ubuntu WSL distribution. Do not casually mix Windows Python, WSL services, and unrelated Windows database services.

## B1. Install system packages

```bash
sudo apt update

sudo apt install -y \
  git \
  curl \
  build-essential \
  libpq-dev \
  postgresql \
  postgresql-contrib \
  redis-server \
  mosquitto \
  mosquitto-clients
```

## B2. Use Python 3.12

The Docker images use Python 3.12. Manual mode should match it because the project pins binary packages.

Install `uv`:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
source ~/.bashrc
uv --version
```

Install Python 3.12:

```bash
uv python install 3.12
uv python list
```

Clone the repository if required:

```bash
mkdir -p ~/projects
cd ~/projects

git clone https://github.com/Marawan-Y/nextex-PoC.git
cd nextex-PoC
```

Create a virtual environment:

```bash
uv venv --python 3.12 .venv
source .venv/bin/activate
```

Install dependencies:

```bash
uv pip install \
  -r cloud_common/requirements.txt \
  -r edge_simulator/requirements.txt
```

Verify imports:

```bash
python -c \
'import fastapi, redis, sqlalchemy, asyncpg, httpx, paho.mqtt.client, PIL, numpy; print("Python dependencies OK")'
```

## B3. Start PostgreSQL, Redis, and Mosquitto

```bash
sudo service postgresql start
sudo service redis-server start
sudo service mosquitto start
```

Check status:

```bash
sudo service postgresql status
sudo service redis-server status
sudo service mosquitto status
```

## B4. Configure PostgreSQL

Set the expected password:

```bash
sudo -u postgres psql \
  -c "ALTER USER postgres WITH PASSWORD 'postgres';"
```

Create the database if missing:

```bash
sudo -u postgres psql -tAc \
  "SELECT 1 FROM pg_database WHERE datname='nextex'" |
  grep -q 1 ||
  sudo -u postgres createdb nextex
```

Check readiness:

```bash
pg_isready -h localhost -p 5432
```

Test credentials:

```bash
PGPASSWORD=postgres psql \
  -h localhost \
  -U postgres \
  -d nextex \
  -c "SELECT current_database();"
```

## B5. Verify Redis

```bash
redis-server --version
redis-cli ping
```

Expected:

```text
PONG
```

Redis 6.2 or newer is required for `XAUTOCLAIM` stale-entry recovery.

## B6. Verify Mosquitto

Terminal 1:

```bash
mosquitto_sub -h localhost -t nextex/test -C 1
```

Terminal 2:

```bash
mosquitto_pub -h localhost -t nextex/test -m "mqtt-ok"
```

Terminal 1 should print `mqtt-ok`.

## B7. Create manual configuration

```bash
mkdir -p .runtime/frames
```

Create `.env.manual`:

```bash
cat > .env.manual <<'ENVEOF'
export REDIS_URL="redis://localhost:6379/0"
export DATABASE_URL="postgresql+asyncpg://postgres:postgres@localhost:5432/nextex"
export FRAME_STORAGE_DIR="$PWD/.runtime/frames"

export STREAM_NAME="nextex:events"
export CONSUMER_GROUP="nextex-consumers"
export CONSUMER_NAME="consumer-1"
export STREAM_MAXLEN="100000"
export BLOCK_MS="5000"
export BATCH_SIZE="10"
export PENDING_CLAIM_IDLE_MS="60000"

export DEVICE_ID="jetson-terrot-de-01-m03"
export MACHINE_ID="M03"
export FACTORY_ID="terrot-de-01"

export CLOUD_API_BASE="http://localhost:8001"
export CLOUD_API_PUBLIC_BASE="http://localhost:8001"

export MQTT_HOST="localhost"
export MQTT_PORT="1883"

export ALARM_THRESHOLD="0.85"
export STREAM_FPS="3.0"
export NEW_CLASS_COOLDOWN_SECONDS="120"
ENVEOF
```

Load this in every application terminal:

```bash
source .venv/bin/activate
source .env.manual
```

The `FRAME_STORAGE_DIR` override avoids permission problems with the default `/data/frames` path.

## B8. Start the Cloud API

Terminal 1:

```bash
cd ~/projects/nextex-PoC
source .venv/bin/activate
source .env.manual

uvicorn cloud_api.main:app \
  --host 0.0.0.0 \
  --port 8001
```

## B9. Start the Cloud Consumer

Terminal 2:

```bash
cd ~/projects/nextex-PoC
source .venv/bin/activate
source .env.manual

python cloud_consumer/worker.py
```

## B10. Start the Edge Simulator

Terminal 3:

```bash
cd ~/projects/nextex-PoC
source .venv/bin/activate
source .env.manual

uvicorn edge_simulator.main:app \
  --host 0.0.0.0 \
  --port 8002
```

Open `http://localhost:8002`.

## B11. Validate manual mode

```bash
curl http://localhost:8001/health
curl http://localhost:8002/config
curl http://localhost:8001/metrics
curl "http://localhost:8001/events/recent?limit=20"
```

`/config` must contain:

```json
"cloud_api_base": "http://localhost:8001"
```

## B12. Stop manual mode

Stop the Python processes with `Ctrl+C`.

Optionally stop infrastructure:

```bash
sudo service mosquitto stop
sudo service redis-server stop
sudo service postgresql stop
```

---

# Dataset options

No external dataset is required.

The simulator checks:

```text
edge_simulator/data/fabric_images/
```

If the directory contains no images, synthetic frames are generated.

To use real labeled images:

```text
edge_simulator/data/fabric_images/
├── no_defect/
├── needle_line/
├── horizontal_distortion/
├── oil_stain/
├── stitch_irregularity/
└── hole/
```

The parent directory becomes the ground-truth class.

Supported extensions:

- `.jpg`
- `.jpeg`
- `.png`
- `.bmp`

After adding images under the mounted data directory:

```powershell
docker compose restart edge-simulator
```

See `docs/DATASET.md` for more details.

---

# Environment variables

## Cloud services

| Variable | Default outside Docker | Docker value | Purpose |
|---|---|---|---|
| `REDIS_URL` | `redis://localhost:6379/0` | `redis://redis:6379/0` | Redis connection |
| `STREAM_NAME` | `nextex:events` | same | Redis stream |
| `CONSUMER_GROUP` | `nextex-consumers` | same | Consumer group |
| `STREAM_MAXLEN` | `100000` | same | Approximate stream retention |
| `DATABASE_URL` | PostgreSQL on localhost | PostgreSQL container URL | SQLAlchemy connection |
| `FRAME_STORAGE_DIR` | `/data/frames` | `/data/frames` volume | Saved event frames |
| `CONSUMER_NAME` | `consumer-1` | same | Consumer identity |
| `BLOCK_MS` | `5000` | same | Blocking read duration |
| `BATCH_SIZE` | `10` | same | Events per batch |
| `PENDING_CLAIM_IDLE_MS` | `60000` | same | Stale-message reclaim threshold |

## Edge Simulator

| Variable | Default outside Docker | Docker value | Purpose |
|---|---|---|---|
| `DEVICE_ID` | `jetson-terrot-de-01-m03` | same | Device identifier |
| `MACHINE_ID` | `M03` | same | Machine identifier |
| `FACTORY_ID` | `terrot-de-01` | same | Factory identifier |
| `CLOUD_API_BASE` | `http://localhost:8001` | `http://cloud-api:8001` | Internal event POST target |
| `CLOUD_API_PUBLIC_BASE` | `http://localhost:8001` | same | Browser polling target |
| `MQTT_HOST` | `localhost` | `mosquitto` | MQTT broker |
| `MQTT_PORT` | `1883` | same | MQTT port |
| `ALARM_THRESHOLD` | `0.85` | same | Alarm threshold |
| `STREAM_FPS` | `2.0` in Python | `3.0` in Compose | Browser stream rate |
| `NEW_CLASS_COOLDOWN_SECONDS` | `120` | same | New-class deduplication window |

---

# Development workflow

## Check changes

```powershell
git status
git diff
```

## Validate Python syntax

```powershell
python -m py_compile .\edge_simulator\main.py
python -m py_compile .\cloud_api\main.py
python -m py_compile .\cloud_consumer\worker.py
```

## Rebuild one service

```powershell
docker compose up -d --build edge-simulator
docker compose up -d --build cloud-api
docker compose up -d --build cloud-consumer
```

## Rebuild everything

```powershell
docker compose up -d --build
```

## Inspect running container code

```powershell
docker compose exec edge-simulator sh -lc `
  "grep -n 'CLOUD_API' /app/edge_simulator/main.py"
```

Inspect environment variables:

```powershell
docker compose exec edge-simulator sh -lc `
  "env | grep CLOUD_API"
```

## Commit tested changes

```powershell
git status
git add .
git commit -m "Describe the change"
git push
```

Test the built containers before committing. A host source file can be correct while an old running image still contains previous code.

---

# Logs and diagnostics

## Container status

```powershell
docker compose ps
```

## All logs

```powershell
docker compose logs -f --tail=100
```

## Individual logs

```powershell
docker compose logs -f --tail=100 edge-simulator
docker compose logs -f --tail=100 cloud-api
docker compose logs -f --tail=100 cloud-consumer
docker compose logs -f --tail=100 postgres
docker compose logs -f --tail=100 redis
docker compose logs -f --tail=100 mosquitto
```

## Metrics

```powershell
Invoke-RestMethod http://localhost:8001/metrics |
    ConvertTo-Json -Depth 5
```

Important fields:

- `total_processed_events`
- `queue_backlog`
- `queue_pending_unacked`
- `event_distribution_by_type`
- `event_distribution_by_anomaly_class`
- `event_distribution_by_factory`
- `events_last_5min`
- `events_per_minute_last_5min`
- `last_event_at`
- `seconds_since_last_event`

`queue_backlog` is the Redis stream length, including retained acknowledged entries. `queue_pending_unacked` is the direct measure of delivered but unacknowledged events.

## Find port owners on Windows

```powershell
Get-NetTCPConnection -LocalPort 8001 -ErrorAction SilentlyContinue
Get-NetTCPConnection -LocalPort 8002 -ErrorAction SilentlyContinue
Get-NetTCPConnection -LocalPort 5432 -ErrorAction SilentlyContinue
Get-NetTCPConnection -LocalPort 6379 -ErrorAction SilentlyContinue
Get-NetTCPConnection -LocalPort 1883 -ErrorAction SilentlyContinue
```

---

# Stopping, resetting, and preserving data

## Stop and preserve data

```powershell
docker compose down
```

Preserved:

- `postgres_data`
- `redis_data`
- `frame_storage`
- built images
- host-mounted Edge data

## Stop without removing containers

```powershell
docker compose stop
```

Resume:

```powershell
docker compose start
```

## Recreate containers while preserving volumes

```powershell
docker compose down --remove-orphans
docker compose up -d --force-recreate
```

## Delete all persistent Docker data

```powershell
docker compose down -v --remove-orphans
```

This deletes PostgreSQL data, Redis data, and stored frames.

---

# Troubleshooting

## `docker` is not recognized

Start Docker Desktop, wait for the engine, open a new PowerShell window, and run:

```powershell
docker version
docker compose version
```

## `/usr/bin/docker: Input/output error` in WSL

PowerShell as Administrator:

```powershell
wsl --shutdown
```

Then restart Docker Desktop, verify Ubuntu is enabled under WSL Integration, reopen Ubuntu, and run:

```bash
docker version
docker compose version
docker run --rm hello-world
```

Do not install a second Docker daemon as a reaction to this error.

## Dashboard Cloud panel says `unreachable`

```powershell
Invoke-RestMethod http://localhost:8002/config | ConvertTo-Json
```

Correct:

```json
"cloud_api_base": "http://localhost:8001"
```

Validate Compose:

```powershell
docker compose config | Select-String "CLOUD_API"
```

Expected:

```text
CLOUD_API_BASE: http://cloud-api:8001
CLOUD_API_PUBLIC_BASE: http://localhost:8001
```

Rebuild the Edge Simulator if needed:

```powershell
docker compose down
docker compose build --no-cache edge-simulator
docker compose up -d --force-recreate
```

## Port is already allocated

```powershell
docker ps --format "table {{.Names}}\t{{.Ports}}"
Get-NetTCPConnection -LocalPort 8001 -ErrorAction SilentlyContinue
```

Stop the conflicting service or change the host-side port mapping.

## PostgreSQL port `5432` is occupied

```powershell
$connections = Get-NetTCPConnection -LocalPort 5432 -State Listen
$connections | Format-Table LocalAddress, LocalPort, OwningProcess

Get-Process -Id (
    $connections.OwningProcess |
    Select-Object -Unique
)
```

Only one service should own the host port used by this project.

## `PermissionError: /data/frames` in manual mode

```bash
source .env.manual
echo "$FRAME_STORAGE_DIR"
```

It should point to `<repo>/.runtime/frames`.

## PostgreSQL authentication failed

```bash
sudo -u postgres psql \
  -c "ALTER USER postgres WITH PASSWORD 'postgres';"

sudo service postgresql restart
```

## `ModuleNotFoundError` in manual mode

```bash
cd ~/projects/nextex-PoC
source .venv/bin/activate
source .env.manual

uv pip install \
  -r cloud_common/requirements.txt \
  -r edge_simulator/requirements.txt
```

## Camera works but metrics do not change

Check the Cloud Consumer and Edge logs:

```powershell
docker compose logs --tail=100 edge-simulator
docker compose logs --tail=100 cloud-consumer
```

Defects are intentionally a minority of frames, so counters do not change on every frame.

## Compose warns that `version` is obsolete

Modern Docker Compose ignores:

```yaml
version: "3.9"
```

The warning is harmless. The line can be removed.

## Docker runs old code after editing

```powershell
docker compose down
docker compose up -d --build
```

Verify container code:

```powershell
docker compose exec edge-simulator sh -lc `
  "grep -n 'CLOUD_API' /app/edge_simulator/main.py"
```

## Full emergency reset

Use only when persistent data can be deleted:

```powershell
docker compose down -v --remove-orphans
docker compose build --no-cache
docker compose up -d --force-recreate
```

---

# Repository structure

```text
.
├── README.md
├── docker-compose.yml
├── mosquitto/
│   └── mosquitto.conf
├── cloud_common/
│   ├── config.py
│   ├── schemas.py
│   ├── db.py
│   ├── queue.py
│   └── requirements.txt
├── cloud_api/
│   ├── main.py
│   └── Dockerfile
├── cloud_consumer/
│   ├── worker.py
│   └── Dockerfile
├── edge_simulator/
│   ├── main.py
│   ├── dataset.py
│   ├── mock_detector.py
│   ├── telemetry.py
│   ├── requirements.txt
│   ├── static/index.html
│   ├── data/
│   └── Dockerfile
├── diagrams/
├── docs/
└── notebooks/
    └── walkthrough.ipynb
```

---

# Design decisions

## Redis Streams

Redis Streams provides consumer groups, at-least-once delivery, pending-entry tracking, stale-message claiming, and low operational overhead. PostgreSQL remains the system of record.

## Independent Cloud Consumer

If PostgreSQL becomes slow, the Cloud API can continue accepting events while Redis absorbs the backlog. Persistence failures do not take ingestion down.

## Explicit event models

`new_anomaly_class` and `threshold_exceeded` use separate Pydantic models because their required fields and purposes differ.

## Local-buffer-first networking

- Edge Cloud events are held temporarily if the Cloud API is unreachable.
- Telemetry is appended to local JSONL before MQTT publication.
- The Cloud API places events in Redis before persistence.

## Synthetic-by-default dataset

The project starts without third-party credentials or licensed data. Real data can be added without changing code.

## New-class cooldown

Persistent defects continue generating operator alarms but do not upload nearly identical retraining frames on every frame.

---

# Production considerations

This repository is a proof of concept, not a production deployment.

Production work should include:

- object storage instead of a local frame volume,
- TLS for HTTP and MQTT,
- authenticated devices and per-device credentials,
- restricted CORS,
- non-anonymous Mosquitto configuration,
- Alembic migrations,
- PostgreSQL row-level security for multi-tenancy,
- structured logging and Prometheus metrics,
- alerting on pipeline staleness and pending messages,
- multi-device and multi-consumer load testing,
- a real labeling and retraining pipeline,
- a durable Edge event outbox instead of an in-memory deque,
- MQTT PUBACK-aware delivery tracking,
- graceful task shutdown and lifecycle management.

See `docs/DESIGN_NOTES.md`.

---

# Official installation references

- WSL: https://learn.microsoft.com/en-us/windows/wsl/install
- Docker Desktop for Windows: https://docs.docker.com/desktop/setup/install/windows-install/
- PostgreSQL on Ubuntu: https://www.postgresql.org/download/linux/ubuntu/
- Eclipse Mosquitto: https://mosquitto.org/download/
- `uv` installation: https://docs.astral.sh/uv/getting-started/installation/
- Python installation with `uv`: https://docs.astral.sh/uv/guides/install-python/
