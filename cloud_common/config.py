"""
cloud_common.config
=====================
Centralized environment-driven configuration, shared by cloud-api and
cloud-consumer so both services always agree on stream names, DB URLs, etc.
"""
import os
from pathlib import Path


class Settings:
    # --- Redis / queue ---
    REDIS_URL: str = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
    STREAM_NAME: str = os.environ.get("STREAM_NAME", "nextex:events")
    CONSUMER_GROUP: str = os.environ.get("CONSUMER_GROUP", "nextex-consumers")
    STREAM_MAXLEN: int = int(os.environ.get("STREAM_MAXLEN", "100000"))

    # --- Postgres ---
    DATABASE_URL: str = os.environ.get(
        "DATABASE_URL",
        "postgresql+asyncpg://postgres:postgres@localhost:5432/nextex",
    )

    # --- Frame storage (stand-in for S3/object storage in this take-home) ---
    FRAME_STORAGE_DIR: Path = Path(os.environ.get("FRAME_STORAGE_DIR", "/data/frames"))

    # --- Consumer behavior ---
    CONSUMER_NAME: str = os.environ.get("CONSUMER_NAME", "consumer-1")
    BLOCK_MS: int = int(os.environ.get("BLOCK_MS", "5000"))
    BATCH_SIZE: int = int(os.environ.get("BATCH_SIZE", "10"))
    PENDING_CLAIM_IDLE_MS: int = int(os.environ.get("PENDING_CLAIM_IDLE_MS", "60000"))


settings = Settings()
settings.FRAME_STORAGE_DIR.mkdir(parents=True, exist_ok=True)
