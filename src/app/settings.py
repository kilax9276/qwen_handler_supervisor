#copyright "Kilax @kilax9276"
from __future__ import annotations

import os
from dataclasses import dataclass


def _env_bool(name: str, default: bool) -> bool:
    v = os.environ.get(name)
    if v is None:
        return default
    s = v.strip().lower()
    if s in ("1", "true", "yes", "y", "on"):
        return True
    if s in ("0", "false", "no", "n", "off"):
        return False
    return default


def _env_int(name: str, default: int) -> int:
    v = os.environ.get(name)
    if v is None:
        return default
    try:
        return int(v.strip())
    except Exception:
        return default


@dataclass
class Settings:
    # Storage
    SQLITE_PATH: str = os.environ.get("SQLITE_PATH", "./data/orchestrator.sqlite")

    # Multi-container mode
    CONFIG_PATH: str | None = os.environ.get("CONFIG_PATH") or None

    # Optional per-container IO logging overrides (telemetry/debug)
    ORCH_CONTAINER_IO_LOG_ENABLED: bool = _env_bool("ORCH_CONTAINER_IO_LOG_ENABLED", False)
    ORCH_CONTAINER_IO_LOG_DIR: str = os.environ.get("ORCH_CONTAINER_IO_LOG_DIR", "./logs/container-io")
    ORCH_CONTAINER_IO_LOG_MAX_BYTES: int = _env_int("ORCH_CONTAINER_IO_LOG_MAX_BYTES", 10_000_000)
    ORCH_CONTAINER_IO_LOG_BACKUP_COUNT: int = _env_int("ORCH_CONTAINER_IO_LOG_BACKUP_COUNT", 5)
    ORCH_CONTAINER_IO_LOG_INCLUDE_BODIES: bool = _env_bool("ORCH_CONTAINER_IO_LOG_INCLUDE_BODIES", True)
    ORCH_CONTAINER_IO_LOG_REDACT_SECRETS: bool = _env_bool("ORCH_CONTAINER_IO_LOG_REDACT_SECRETS", True)
    ORCH_CONTAINER_IO_LOG_MAX_FIELD_CHARS: int = _env_int("ORCH_CONTAINER_IO_LOG_MAX_FIELD_CHARS", 8000)
    ORCH_CONTAINER_IO_LOG_LEVEL: str = os.environ.get("ORCH_CONTAINER_IO_LOG_LEVEL", "INFO")


settings = Settings()
