#copyright "Kilax @kilax9276"
from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Optional


_SENSITIVE_URL_RE = re.compile(r"://([^:@/]+):([^@/]+)@")


def _parse_bool(value: Optional[str], *, default: bool = False) -> bool:
    if value is None:
        return default
    v = str(value).strip().lower()
    if v in {"1", "true", "yes", "y", "on"}:
        return True
    if v in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _parse_int(value: Optional[str], *, default: int) -> int:
    if value is None:
        return default
    try:
        return int(str(value).strip())
    except Exception:
        return default


def _ts_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sanitize_container_id(container_id: str) -> str:
    s = (container_id or "unknown").strip()
    if not s:
        s = "unknown"
    # allow only [A-Za-z0-9._-]
    s = re.sub(r"[^A-Za-z0-9._-]", "_", s)
    return s[:128]


def _redact_secrets_in_str(s: str) -> str:
    # socks5://user:pass@host:port -> socks5://user:***@host:port
    try:
        return _SENSITIVE_URL_RE.sub(r"://\1:***@", s)
    except Exception:
        return s


def _truncate_str(s: str, *, max_chars: int) -> Any:
    if max_chars <= 0:
        return s
    if len(s) <= max_chars:
        return s
    return {"__truncated__": True, "len": len(s), "head": s[:max_chars]}


def _sanitize_obj(
    obj: Any,
    *,
    redact_secrets: bool,
    include_bodies: bool,
    max_field_chars: int,
) -> Any:
    """Best-effort санитаризация для логов.

    - редактирует пароли в socks URL;
    - не пишет целиком image_b64 (логирует len + head);
    - режет слишком длинные строки (max_field_chars).

    Возвращаемый объект может немного отличаться по типам (например, str -> dict при truncation),
    это сделано сознательно для удобной отладки и защиты от гигантских логов.
    """

    if obj is None:
        return None

    if isinstance(obj, (int, float, bool)):
        return obj

    if isinstance(obj, bytes):
        return {"__bytes__": True, "len": len(obj)}

    if isinstance(obj, str):
        s = obj
        if redact_secrets and ("socks" in s or "://" in s):
            s = _redact_secrets_in_str(s)
        return _truncate_str(s, max_chars=max_field_chars)

    if isinstance(obj, (list, tuple)):
        if not include_bodies:
            return {"__list__": True, "len": len(obj)}
        return [
            _sanitize_obj(
                x,
                redact_secrets=redact_secrets,
                include_bodies=include_bodies,
                max_field_chars=max_field_chars,
            )
            for x in obj
        ]

    if isinstance(obj, dict):
        if not include_bodies:
            return {"__dict__": True, "keys": list(obj.keys())[:50], "len": len(obj)}

        out: dict[str, Any] = {}
        for k, v in obj.items():
            ks = str(k)
            kl = ks.lower()

            # Special cases
            if kl in {"image_b64", "img_b64", "b64", "base64"} and isinstance(v, str):
                out[ks] = {"__b64__": True, "len": len(v), "head": v[:120]}
                continue

            if redact_secrets and kl in {"socks", "socks_override", "proxy", "proxy_url", "authorization", "cookie"}:
                if isinstance(v, str):
                    out[ks] = _truncate_str(_redact_secrets_in_str(v), max_chars=max_field_chars)
                    continue

            out[ks] = _sanitize_obj(
                v,
                redact_secrets=redact_secrets,
                include_bodies=include_bodies,
                max_field_chars=max_field_chars,
            )

        return out

    # Fallback
    try:
        s = json.dumps(obj, ensure_ascii=False)
    except Exception:
        s = str(obj)

    if redact_secrets:
        s = _redact_secrets_in_str(s)
    return _truncate_str(s, max_chars=max_field_chars)


class ContainerIOLLogger:
    """Логирование request/response к каждому upstream-контейнеру в отдельный файл.

    Формат: JSONL (1 строка = 1 обмен request/response).

    Включение/выключение:
      - в config.yaml через секцию container_io_log
      - или через ENV (имеет приоритет):
          ORCH_CONTAINER_IO_LOG_ENABLED=1
          ORCH_CONTAINER_IO_LOG_DIR=./logs/container-io
          ORCH_CONTAINER_IO_LOG_MAX_BYTES=10000000
          ORCH_CONTAINER_IO_LOG_BACKUP_COUNT=5
          ORCH_CONTAINER_IO_LOG_INCLUDE_BODIES=1
          ORCH_CONTAINER_IO_LOG_REDACT_SECRETS=1
          ORCH_CONTAINER_IO_LOG_MAX_FIELD_CHARS=8000
          ORCH_CONTAINER_IO_LOG_LEVEL=INFO
    """

    def __init__(
        self,
        *,
        enabled: bool,
        log_dir: str,
        max_bytes: int,
        backup_count: int,
        include_bodies: bool,
        redact_secrets: bool,
        max_field_chars: int,
        level_name: str,
    ) -> None:
        self._enabled = bool(enabled)
        self._log_dir = Path(log_dir)
        self._max_bytes = max(1024, int(max_bytes))
        self._backup_count = max(0, int(backup_count))
        self._include_bodies = bool(include_bodies)
        self._redact_secrets = bool(redact_secrets)
        self._max_field_chars = max(256, int(max_field_chars))

        ln = (level_name or "INFO").upper()
        self._level = getattr(logging, ln, logging.INFO)

        self._loggers: dict[str, logging.Logger] = {}

        if self._enabled:
            self._log_dir.mkdir(parents=True, exist_ok=True)

    @property
    def enabled(self) -> bool:
        return self._enabled

    @classmethod
    def from_sources(cls, *, yaml_config: Any = None) -> "ContainerIOLLogger":
        """Создаёт инстанс с учётом config.yaml и ENV (ENV имеет приоритет).

        yaml_config может быть:
          - pydantic model (attrs)
          - dict
          - None
        """

        def yget(name: str, default: Any) -> Any:
            if yaml_config is None:
                return default
            if isinstance(yaml_config, dict):
                return yaml_config.get(name, default)
            return getattr(yaml_config, name, default)

        # База из YAML (или дефолтов)
        enabled = bool(yget("enabled", False))
        log_dir = str(yget("dir", "./logs/container-io"))
        max_bytes = int(yget("max_bytes", 10_000_000))
        backup_count = int(yget("backup_count", 5))
        include_bodies = bool(yget("include_bodies", True))
        redact_secrets = bool(yget("redact_secrets", True))
        max_field_chars = int(yget("max_field_chars", 8000))
        level_name = str(yget("level", "INFO"))

        # ENV overrides (если переменная задана — даже пустая строка — считаем override)
        if os.getenv("ORCH_CONTAINER_IO_LOG_ENABLED") is not None:
            enabled = _parse_bool(os.getenv("ORCH_CONTAINER_IO_LOG_ENABLED"), default=enabled)
        if os.getenv("ORCH_CONTAINER_IO_LOG_DIR") is not None:
            v = (os.getenv("ORCH_CONTAINER_IO_LOG_DIR") or "").strip()
            if v:
                log_dir = v
        if os.getenv("ORCH_CONTAINER_IO_LOG_MAX_BYTES") is not None:
            max_bytes = _parse_int(os.getenv("ORCH_CONTAINER_IO_LOG_MAX_BYTES"), default=max_bytes)
        if os.getenv("ORCH_CONTAINER_IO_LOG_BACKUP_COUNT") is not None:
            backup_count = _parse_int(os.getenv("ORCH_CONTAINER_IO_LOG_BACKUP_COUNT"), default=backup_count)
        if os.getenv("ORCH_CONTAINER_IO_LOG_INCLUDE_BODIES") is not None:
            include_bodies = _parse_bool(os.getenv("ORCH_CONTAINER_IO_LOG_INCLUDE_BODIES"), default=include_bodies)
        if os.getenv("ORCH_CONTAINER_IO_LOG_REDACT_SECRETS") is not None:
            redact_secrets = _parse_bool(os.getenv("ORCH_CONTAINER_IO_LOG_REDACT_SECRETS"), default=redact_secrets)
        if os.getenv("ORCH_CONTAINER_IO_LOG_MAX_FIELD_CHARS") is not None:
            max_field_chars = _parse_int(os.getenv("ORCH_CONTAINER_IO_LOG_MAX_FIELD_CHARS"), default=max_field_chars)
        if os.getenv("ORCH_CONTAINER_IO_LOG_LEVEL") is not None:
            v = (os.getenv("ORCH_CONTAINER_IO_LOG_LEVEL") or "").strip()
            if v:
                level_name = v

        return cls(
            enabled=enabled,
            log_dir=log_dir,
            max_bytes=max_bytes,
            backup_count=backup_count,
            include_bodies=include_bodies,
            redact_secrets=redact_secrets,
            max_field_chars=max_field_chars,
            level_name=level_name,
        )

    @classmethod
    def from_settings(
        cls,
        *,
        enabled: bool,
        log_dir: str,
        max_bytes: int,
        backup_count: int,
        include_bodies: bool,
        redact_secrets: bool,
        max_field_chars: int,
        level: str,
    ) -> "ContainerIOLLogger":
        """Backwards-compatible factory.

        Старый wiring (main.py) создаёт логгер через from_settings(...).
        В новом коде основной фабрикой является from_sources(yaml_config=...).
        Оставляем алиас, чтобы не ломать прод/тесты.
        """
        return cls(
            enabled=bool(enabled),
            log_dir=str(log_dir),
            max_bytes=int(max_bytes),
            backup_count=int(backup_count),
            include_bodies=bool(include_bodies),
            redact_secrets=bool(redact_secrets),
            max_field_chars=int(max_field_chars),
            level_name=str(level or "INFO"),
        )

    def log_exchange(
        self,
        *,
        container_id: str,
        request_id: Optional[str],
        method: str,
        path: str,
        url: str,
        request_json: Optional[dict[str, Any]],
        status_code: Optional[int],
        response: Any,
        duration_ms: Optional[int],
        error: Optional[str] = None,
        attempt: Optional[int] = None,
        max_attempts: Optional[int] = None,
    ) -> None:
        if not self._enabled:
            return

        cid = _sanitize_container_id(container_id)
        lg = self._logger_for(cid)

        record: dict[str, Any] = {
            "ts": _ts_utc_iso(),
            "container_id": cid,
            "request_id": request_id,
            "method": method,
            "path": path,
            "url": url,
            "status_code": status_code,
            "duration_ms": duration_ms,
            "error": error,
        }
        if attempt is not None:
            record["attempt"] = int(attempt)
        if max_attempts is not None:
            record["max_attempts"] = int(max_attempts)

        record["request"] = _sanitize_obj(
            request_json,
            redact_secrets=self._redact_secrets,
            include_bodies=self._include_bodies,
            max_field_chars=self._max_field_chars,
        )
        record["response"] = _sanitize_obj(
            response,
            redact_secrets=self._redact_secrets,
            include_bodies=self._include_bodies,
            max_field_chars=self._max_field_chars,
        )

        try:
            lg.log(self._level, json.dumps(record, ensure_ascii=False))
        except Exception:
            lg.log(self._level, str(record))

    def _logger_for(self, container_id_sanitized: str) -> logging.Logger:
        lg = self._loggers.get(container_id_sanitized)
        if lg is not None:
            return lg

        name = f"orchestrator.container_io.{container_id_sanitized}"
        lg = logging.getLogger(name)
        lg.setLevel(self._level)
        lg.propagate = False

        if not lg.handlers:
            path = self._log_dir / f"{container_id_sanitized}.jsonl"
            h = RotatingFileHandler(
                path,
                maxBytes=self._max_bytes,
                backupCount=self._backup_count,
                encoding="utf-8",
            )
            h.setLevel(self._level)
            h.setFormatter(logging.Formatter("%(message)s"))
            lg.addHandler(h)

        self._loggers[container_id_sanitized] = lg
        return lg
