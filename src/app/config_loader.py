from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Optional

from pydantic import BaseModel, Field


class ContainerTimeouts(BaseModel):
    connect_seconds: float = 10.0
    read_seconds: float = 120.0


class ContainerConfig(BaseModel):
    id: str
    base_url: str
    enabled: bool = True
    weight: int = 1
    timeouts: ContainerTimeouts = Field(default_factory=ContainerTimeouts)
    analyze_retries: int = 1


class SocksConfig(BaseModel):
    socks_id: str
    url: str


class ProfileConfig(BaseModel):
    profile_id: str
    profile_value: str
    socks_id: Optional[str] = None
    allowed_containers: list[str] = Field(default_factory=list)
    max_uses: Optional[int] = None
    pending_replace: bool = False


class PromptConfig(BaseModel):
    prompt_id: str
    file: str
    default_max_chat_uses: int = 50


class ContainerIOLogConfig(BaseModel):
    """Логирование запросов/ответов к каждому контейнеру (JSONL по контейнеру)."""

    enabled: bool = False
    dir: str = "./logs/container-io"
    max_bytes: int = 10_000_000
    backup_count: int = 5
    include_bodies: bool = True
    redact_secrets: bool = True
    max_field_chars: int = 8000
    level: str = "INFO"


class AppConfig(BaseModel):
    containers: list[ContainerConfig] = Field(default_factory=list)
    socks: list[SocksConfig] = Field(default_factory=list)
    profiles: list[ProfileConfig] = Field(default_factory=list)
    prompts: list[PromptConfig] = Field(default_factory=list)

    # Whether /v1/solve may override socks via options.socks_override.
    allow_socks_override: bool = True

    # Optional: per-container IO logging settings.
    container_io_log: ContainerIOLogConfig = Field(default_factory=ContainerIOLogConfig)


def _resolve_relative_path(base_dir: Path, value: str) -> str:
    v = (value or "").strip()
    if not v:
        return value
    p = Path(v)
    if p.is_absolute():
        return str(p)
    # Resolve relative paths against the directory of CONFIG_PATH YAML.
    return str((base_dir / p).resolve())


def load_config(path: str) -> AppConfig:
    """Loads YAML config.

    CONFIG_PATH points to YAML.
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"CONFIG_PATH does not exist: {path}")

    try:
        import yaml  # type: ignore
    except Exception as e:  # pragma: no cover
        raise RuntimeError("PyYAML is required for CONFIG_PATH mode. Install pyyaml.") from e

    data: Any
    with p.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    if not isinstance(data, dict):
        raise ValueError("config YAML root must be a mapping")

    # Support nested key "config" if user wraps it.
    if "config" in data and isinstance(data["config"], dict):
        data = data["config"]

    cfg = AppConfig.model_validate(data)

    # Make paths predictable: resolve relative paths against config directory.
    base_dir = p.resolve().parent
    try:
        cfg.container_io_log.dir = _resolve_relative_path(base_dir, cfg.container_io_log.dir)
    except Exception:
        pass

    try:
        for pr in cfg.prompts:
            pr.file = _resolve_relative_path(base_dir, pr.file)
    except Exception:
        pass

    return cfg


def get_config_path_from_env() -> Optional[str]:
    v = os.environ.get("CONFIG_PATH")
    if v and v.strip():
        return v.strip()
    return None
