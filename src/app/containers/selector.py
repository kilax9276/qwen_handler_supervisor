#copyright "Kilax @kilax9276"

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from ..storage import Storage
from .pool import UpstreamClientPool

logger = logging.getLogger("orchestrator")


class NotEnoughContainersError(RuntimeError):
    def __init__(self, *, requested: int, available: int, details: dict[str, Any] | None = None) -> None:
        super().__init__(f"not enough containers: requested={requested} available={available}")
        self.requested = requested
        self.available = available
        self.details = details or {}


@dataclass(frozen=True)
class ContainerStatus:
    container_id: str
    payload: Dict[str, Any]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _is_busy(status_payload: dict[str, Any]) -> bool:
    # Поддерживаем разные форматы upstream status.
    if status_payload.get("status") == "busy":
        return True
    if bool(status_payload.get("busy")):
        return True
    return False


def _same_url(a: Optional[str], b: Optional[str]) -> bool:
    if not a or not b:
        return False
    return a.strip() == b.strip()


class ContainerSelector:
    """
    Совместимый селектор контейнеров.

    В старых версиях был select_container(), в новых executor вызывает select_containers().
    Здесь гарантируем наличие обоих методов.
    """

    def __init__(
        self,
        *,
        storage: Storage,
        upstream_pool: Optional[UpstreamClientPool] = None,
        pool: Optional[UpstreamClientPool] = None,
        strict_fanout: bool = True,
    ) -> None:
        # принимаем оба варианта именования (pool vs upstream_pool)
        resolved_pool = upstream_pool or pool
        if resolved_pool is None:
            raise TypeError("ContainerSelector requires upstream_pool=... (or pool=...)")

        self._pool = resolved_pool
        self._storage = storage
        self._strict_fanout = strict_fanout
        self._rr_cursor = 0

    @property
    def pool(self) -> UpstreamClientPool:
        return self._pool

    async def select_container(
        self,
        prompt_id: str,
        profile_id: Optional[str],
        *,
        socks_id: Optional[str] = None,
        chat_url: Optional[str] = None,
        allowed_containers: Optional[List[str]] = None,
        request_id: Optional[str] = None,
    ) -> str:
        """
        Backwards-compat (singular). Возвращает ровно 1 container_id либо кидает NotEnoughContainersError.
        """
        ids = await self.select_containers(
            prompt_id=prompt_id,
            profile_id=profile_id,
            fanout_requested=1,
            socks_id=socks_id,
            chat_url=chat_url,
            allowed_containers=allowed_containers,
            request_id=request_id,
        )
        if not ids:
            raise NotEnoughContainersError(requested=1, available=0, details={"reason": "empty_selection"})
        return ids[0]

    async def select_containers(
        self,
        prompt_id: str,
        profile_id: Optional[str],
        fanout_requested: Optional[int] = None,
        *,
        # Backwards-compat: некоторые старые вызовы использовали `fanout=`
        fanout: Optional[int] = None,
        socks_id: Optional[str] = None,
        chat_url: Optional[str] = None,
        allowed_containers: Optional[List[str]] = None,
        request_id: Optional[str] = None,
    ) -> List[str]:
        # Fanout в полной версии больше не обязателен, но параметры поддерживаем ради совместимости.
        if (fanout_requested is not None) and (fanout is not None) and (int(fanout_requested) != int(fanout)):
            try:
                logger.warning(
                    json.dumps(
                        {
                            "event": "container_selector_fanout_conflict",
                            "request_id": request_id,
                            "fanout_requested": fanout_requested,
                            "fanout": fanout,
                        },
                        ensure_ascii=False,
                    )
                )
            except Exception:
                pass

        want_src = fanout_requested if fanout_requested is not None else fanout
        want = int(want_src or 1)
        if want <= 0:
            want = 1

        enabled = list(self._pool.list_enabled())
        if allowed_containers:
            allowed_set = {c for c in allowed_containers if c in enabled}
            candidates = [c for c in enabled if c in allowed_set]
        else:
            candidates = enabled

        if not candidates:
            raise NotEnoughContainersError(requested=want, available=0, details={"reason": "no_enabled_containers"})

        # Контейнеры, заблокированные ручными chat-lock’ами
        now_iso = _utc_now().isoformat()
        blocked: set[str] = set()
        try:
            blocked = set(self._storage.list_locked_containers())
        except TypeError:
            # fallback: некоторые реализации принимали now_iso как позиционный аргумент
            try:
                blocked = set(self._storage.list_locked_containers(now_iso))
            except Exception as e2:
                logger.exception("storage_list_locked_containers_failed", extra={"error": str(e2)})
                blocked = set()
        except Exception as e:
            logger.exception("storage_list_locked_containers_failed", extra={"error": str(e)})
            blocked = set()

        try:
            logger.info(
                json.dumps(
                    {
                        "event": "container_selector_candidates",
                        "request_id": request_id,
                        "want": want,
                        "prompt_id": prompt_id,
                        "profile_id": profile_id,
                        "socks_id": socks_id,
                        "chat_url": chat_url,
                        "candidates": candidates,
                        "blocked": sorted(blocked),
                    },
                    ensure_ascii=False,
                )
            )
        except Exception:
            pass

        # Если задан chat_url — обязаны использовать контейнер, где этот чат зарегистрирован
        if chat_url:
            sess = self._storage.get_full_chat_session_by_url(chat_url)
            if sess is None:
                raise NotEnoughContainersError(
                    requested=1,
                    available=0,
                    details={"reason": "chat_url_not_registered", "chat_url": chat_url},
                )

            cid = sess.container_id
            if cid not in candidates or cid in blocked:
                raise NotEnoughContainersError(
                    requested=1,
                    available=0,
                    details={"reason": "chat_url_container_unavailable", "chat_url": chat_url, "container_id": cid},
                )

            st = await self._pool.get(cid).status(request_id=request_id)
            st = dict(st)
            if _is_busy(st) or not _same_url(st.get("page_url"), chat_url):
                raise NotEnoughContainersError(
                    requested=1,
                    available=0,
                    details={"reason": "chat_url_container_busy_or_mismatch", "chat_url": chat_url, "container_id": cid},
                )
            return [cid]

        # Иначе — собираем статусы параллельно
        statuses = await self._fetch_statuses(candidates, request_id=request_id)

        # оставляем только не busy и не blocked
        available = [s for s in statuses if (s.container_id not in blocked) and (not _is_busy(s.payload))]
        if not available:
            raise NotEnoughContainersError(
                requested=want,
                available=0,
                details={"reason": "all_busy_or_locked", "blocked": sorted(blocked)},
            )

        # Round-robin по доступным (простая и стабильная стратегия)
        ordered_ids = [s.container_id for s in available]
        if not ordered_ids:
            raise NotEnoughContainersError(requested=want, available=0, details={"reason": "no_available_after_filter"})

        start = self._rr_cursor % len(ordered_ids)
        self._rr_cursor = (self._rr_cursor + 1) % len(ordered_ids)

        rotated = ordered_ids[start:] + ordered_ids[:start]

        # В полной версии fanout обычно 1, но для совместимости вернем до want уникальных.
        uniq: List[str] = []
        for cid in rotated:
            if cid not in uniq:
                uniq.append(cid)
            if len(uniq) >= min(want, len(rotated)):
                break

        if self._strict_fanout and len(uniq) < want:
            raise NotEnoughContainersError(
                requested=want,
                available=len(uniq),
                details={"reason": "strict_fanout_not_satisfied", "selected": uniq},
            )

        return uniq

    async def _fetch_statuses(self, container_ids: List[str], *, request_id: Optional[str]) -> List[ContainerStatus]:
        async def _one(cid: str) -> ContainerStatus:
            try:
                st = await self._pool.get(cid).status(request_id=request_id)
                return ContainerStatus(container_id=cid, payload=dict(st))
            except Exception as e:
                return ContainerStatus(container_id=cid, payload={"status": "error", "message": str(e), "busy": True})

        tasks = [_one(cid) for cid in container_ids]
        return await asyncio.gather(*tasks)
