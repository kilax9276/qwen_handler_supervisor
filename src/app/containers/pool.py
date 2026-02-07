from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from ..config_loader import ContainerConfig
from ..upstream_client import UpstreamClient


def _get_timeout(c: Any, kind: str, default: float) -> float:
    """Достаёт таймаут из ContainerConfig с учётом старых/новых схем.

    Поддерживает:
      - legacy: c.connect_timeout_seconds / c.read_timeout_seconds
      - new: c.timeouts.connect_seconds / c.timeouts.read_seconds
    """
    legacy = getattr(c, f"{kind}_timeout_seconds", None)
    if legacy is not None:
        try:
            return float(legacy)
        except Exception:
            return float(default)

    timeouts = getattr(c, "timeouts", None)
    if timeouts is not None:
        nested = getattr(timeouts, f"{kind}_seconds", None)
        if nested is not None:
            try:
                return float(nested)
            except Exception:
                return float(default)

    return float(default)


@dataclass
class UpstreamClientPool:
    """Пул UpstreamClient по container_id.

    MVP/single-container ветки и env-based CONTAINER_IO_LOG_* удалены.
    Вся трассировка I/O настраивается через orchestrator (ContainerIOLLogger) и
    передаётся в пул как io_logger.
    """

    _clients: Dict[str, UpstreamClient]
    _enabled: set[str]

    def __init__(self, containers: List[ContainerConfig], *, io_logger: Optional[Any] = None) -> None:
        clients: Dict[str, UpstreamClient] = {}
        enabled: set[str] = set()

        for c in containers:
            cid = str(getattr(c, "container_id", None) or getattr(c, "id", None) or "").strip()
            if not cid:
                raise ValueError("ContainerConfig must have container_id (or id)")

            base_url = str(getattr(c, "base_url", None) or "").strip()
            if not base_url:
                raise ValueError(f"ContainerConfig {cid} must have base_url")

            clients[cid] = UpstreamClient(
                base_url,
                connect_timeout_seconds=_get_timeout(c, "connect", 10.0),
                read_timeout_seconds=_get_timeout(c, "read", 120.0),
                analyze_retries=int(getattr(c, "analyze_retries", 1) or 1),
                container_id=cid,
                io_logger=io_logger,
            )

            if bool(getattr(c, "enabled", True)):
                enabled.add(cid)

        self._clients = clients
        self._enabled = enabled

    def list_enabled(self) -> List[str]:
        return sorted(self._enabled)

    def is_enabled(self, container_id: str) -> bool:
        return str(container_id) in self._enabled

    def enable(self, container_id: str) -> None:
        cid = str(container_id)
        if cid not in self._clients:
            raise KeyError(f"Unknown container_id: {cid}")
        self._enabled.add(cid)

    def disable(self, container_id: str) -> None:
        self._enabled.discard(str(container_id))

    def get(self, container_id: str) -> UpstreamClient:
        cid = str(container_id)
        if cid not in self._clients:
            raise KeyError(f"Unknown container_id: {cid}")
        return self._clients[cid]

    async def aclose(self) -> None:
        for c in self._clients.values():
            await c.aclose()
