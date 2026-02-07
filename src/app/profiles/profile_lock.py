from __future__ import annotations

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional


logger = logging.getLogger("orchestrator")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _age_seconds(now_ts: float, started_ts: Optional[float]) -> Optional[float]:
    if started_ts is None:
        return None
    age = now_ts - started_ts
    if age < 0:
        age = 0.0
    return round(age, 3)


class ProfileBusyError(RuntimeError):
    """Profile is already locked by another in-flight request."""

    def __init__(
        self,
        profile_id: str,
        *,
        locked_by: Optional[str],
        locked_at: Optional[str],
        age_seconds: Optional[float],
        state: str,
    ) -> None:
        msg = f"profile '{profile_id}' is busy"
        if locked_by:
            msg += f" (locked_by={locked_by})"
        if locked_at:
            msg += f" (locked_at={locked_at})"
        if age_seconds is not None:
            msg += f" (age_seconds={age_seconds})"
        msg += f" (state={state})"
        super().__init__(msg)

        self.profile_id = profile_id
        self.locked_by = locked_by
        self.locked_at = locked_at
        self.age_seconds = age_seconds
        self.state = state

    @property
    def details(self) -> Dict[str, Any]:
        return {
            "profile_id": self.profile_id,
            "locked_by": self.locked_by,
            "locked_at": self.locked_at,
            "age_seconds": self.age_seconds,
            "state": self.state,
        }


@dataclass
class _LockEntry:
    lock: asyncio.Lock
    holders: int = 0

    # Reservation makes try_lock effectively non-blocking (no waiting on acquire).
    reserved: bool = False
    reserved_by: Optional[str] = None
    reserved_at_iso: Optional[str] = None
    reserved_at_ts: Optional[float] = None

    # Current owner info (best-effort diagnostics only).
    locked_by: Optional[str] = None
    locked_at_iso: Optional[str] = None
    locked_at_ts: Optional[float] = None


class ProfileLock:
    """Process-local exclusive lock per profile_id.

    IMPORTANT:
    - This lock is *process-local*. If you run uvicorn with multiple workers,
      each worker will have its own locks.
    - This lock is designed for orchestrator safety: a single browser profile
      directory must not be used concurrently.

    In diagnostics we track:
    - locked_by: owner id (usually request_id)
    - locked_at: ISO timestamp
    - age_seconds: how long the lock has been held
    """

    def __init__(self) -> None:
        self._locks: Dict[str, _LockEntry] = {}
        self._global = asyncio.Lock()

    def snapshot(self) -> list[dict[str, Any]]:
        """Non-async snapshot for admin/debug endpoints."""
        now_ts = time.time()
        out: list[dict[str, Any]] = []
        for profile_id, entry in list(self._locks.items()):
            if entry.lock.locked():
                out.append(
                    {
                        "profile_id": profile_id,
                        "state": "locked",
                        "locked_by": entry.locked_by,
                        "locked_at": entry.locked_at_iso,
                        "age_seconds": _age_seconds(now_ts, entry.locked_at_ts),
                    }
                )
            elif entry.reserved:
                out.append(
                    {
                        "profile_id": profile_id,
                        "state": "reserved",
                        "locked_by": entry.reserved_by,
                        "locked_at": entry.reserved_at_iso,
                        "age_seconds": _age_seconds(now_ts, entry.reserved_at_ts),
                    }
                )
        return out

    @asynccontextmanager
    async def lock(self, profile_id: str, *, owner: Optional[str] = None) -> None:
        """Blocking lock (legacy)."""
        profile_id = (profile_id or "").strip()
        owner = owner or "unknown"

        if not profile_id:
            yield
            return

        async with self._global:
            entry = self._locks.get(profile_id)
            if entry is None:
                entry = _LockEntry(lock=asyncio.Lock())
                self._locks[profile_id] = entry
            entry.holders += 1
            lock = entry.lock

        acquired = False
        try:
            await lock.acquire()
            acquired = True

            async with self._global:
                entry2 = self._locks.get(profile_id)
                if entry2 is not None:
                    entry2.locked_by = owner
                    entry2.locked_at_iso = _utc_now_iso()
                    entry2.locked_at_ts = time.time()

            logger.info(
                "profile_lock_acquired profile_id=%s owner=%s",
                profile_id,
                owner,
            )

            yield

        finally:
            if not acquired:
                # ✅ ВАЖНО: если нас отменили/упали ДО acquire — откатываем holders.
                async with self._global:
                    entry3 = self._locks.get(profile_id)
                    if entry3 is not None:
                        entry3.holders -= 1
                        if entry3.holders <= 0 and (not entry3.lock.locked()) and (not entry3.reserved):
                            self._locks.pop(profile_id, None)
                return

            lock.release()

            async with self._global:
                entry4 = self._locks.get(profile_id)
                if entry4 is not None:
                    entry4.locked_by = None
                    entry4.locked_at_iso = None
                    entry4.locked_at_ts = None
                    entry4.holders -= 1
                    if entry4.holders <= 0 and (not entry4.lock.locked()) and (not entry4.reserved):
                        self._locks.pop(profile_id, None)

            logger.info(
                "profile_lock_released profile_id=%s owner=%s",
                profile_id,
                owner,
            )

    @asynccontextmanager
    async def try_lock(self, profile_id: str, *, owner: str) -> None:
        """Non-blocking lock; raises ProfileBusyError if already locked.

        asyncio.Lock has no atomic try-acquire. We use a reservation flag under a global lock
        so that only one try_lock call can pass at a time.

        ✅ FIX: if task is cancelled/errored while awaiting lock.acquire(),
        we MUST clear reservation; otherwise profile becomes stuck in PROFILE_BUSY.
        """
        profile_id = (profile_id or "").strip()
        owner = (owner or "unknown").strip() or "unknown"

        if not profile_id:
            yield
            return

        now_ts = time.time()

        async with self._global:
            entry = self._locks.get(profile_id)
            if entry is None:
                entry = _LockEntry(lock=asyncio.Lock())
                self._locks[profile_id] = entry

            # If locked or being acquired right now -> fail fast.
            if entry.lock.locked():
                logger.warning(
                    "profile_lock_busy profile_id=%s state=locked owner=%s locked_by=%s locked_at=%s",
                    profile_id,
                    owner,
                    entry.locked_by,
                    entry.locked_at_iso,
                )
                raise ProfileBusyError(
                    profile_id,
                    locked_by=entry.locked_by,
                    locked_at=entry.locked_at_iso,
                    age_seconds=_age_seconds(now_ts, entry.locked_at_ts),
                    state="locked",
                )
            if entry.reserved:
                logger.warning(
                    "profile_lock_busy profile_id=%s state=reserved owner=%s reserved_by=%s reserved_at=%s",
                    profile_id,
                    owner,
                    entry.reserved_by,
                    entry.reserved_at_iso,
                )
                raise ProfileBusyError(
                    profile_id,
                    locked_by=entry.reserved_by,
                    locked_at=entry.reserved_at_iso,
                    age_seconds=_age_seconds(now_ts, entry.reserved_at_ts),
                    state="reserved",
                )

            entry.reserved = True
            entry.reserved_by = owner
            entry.reserved_at_iso = _utc_now_iso()
            entry.reserved_at_ts = now_ts
            entry.holders += 1
            lock = entry.lock

        acquired = False
        try:
            await lock.acquire()
            acquired = True

            async with self._global:
                entry2 = self._locks.get(profile_id)
                if entry2 is not None:
                    entry2.reserved = False
                    entry2.reserved_by = None
                    entry2.reserved_at_iso = None
                    entry2.reserved_at_ts = None
                    entry2.locked_by = owner
                    entry2.locked_at_iso = _utc_now_iso()
                    entry2.locked_at_ts = time.time()

            logger.info(
                "profile_lock_acquired profile_id=%s owner=%s",
                profile_id,
                owner,
            )

            yield

        finally:
            if not acquired:
                # ✅ ВАЖНО: если отменили/упали на await lock.acquire(),
                # нужно снять reservation и откатить holders, иначе stuck PROFILE_BUSY.
                async with self._global:
                    entry3 = self._locks.get(profile_id)
                    if entry3 is not None:
                        entry3.reserved = False
                        entry3.reserved_by = None
                        entry3.reserved_at_iso = None
                        entry3.reserved_at_ts = None
                        entry3.holders -= 1
                        if entry3.holders <= 0 and (not entry3.lock.locked()) and (not entry3.reserved):
                            self._locks.pop(profile_id, None)
                return

            lock.release()

            async with self._global:
                entry4 = self._locks.get(profile_id)
                if entry4 is not None:
                    entry4.locked_by = None
                    entry4.locked_at_iso = None
                    entry4.locked_at_ts = None
                    entry4.holders -= 1
                    if entry4.holders <= 0 and (not entry4.lock.locked()) and (not entry4.reserved):
                        self._locks.pop(profile_id, None)

            logger.info(
                "profile_lock_released profile_id=%s owner=%s",
                profile_id,
                owner,
            )


# Singleton used by the app
PROFILE_LOCK = ProfileLock()
