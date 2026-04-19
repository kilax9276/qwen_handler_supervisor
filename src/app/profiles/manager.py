#copyright "Kilax @kilax9276"
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Optional

from ..config_loader import AppConfig
from ..storage import ProfileRow, Storage


@dataclass(frozen=True)
class ResolvedProfile:
    profile_id: str
    profile_value: str
    socks_id: Optional[str]
    socks_url: Optional[str]
    allowed_containers: list[str]
    max_uses: Optional[int]
    pending_replace: bool
    manual_only: bool


class ProfileManager:
    def __init__(self, *, storage: Storage, config: AppConfig) -> None:
        self._storage = storage
        self._config = config

    def seed_from_config(self, *, prune_missing: bool = True) -> dict[str, int]:
        """Sync socks+profiles from YAML into SQLite.

        manual_only intentionally stays in YAML only and does not require DB schema changes.
        By default SQLite mirrors the current YAML so runtime reload reflects file edits fully.
        """
        current_socks: list[str] = []
        current_profiles: list[str] = []

        for s in self._config.socks:
            self._storage.upsert_socks(s.socks_id, s.url)
            current_socks.append(s.socks_id)

        for p in self._config.profiles:
            self._storage.upsert_profile(
                profile_id=p.profile_id,
                profile_value=p.profile_value,
                socks_id=p.socks_id,
                allowed_containers=list(p.allowed_containers or []),
                max_uses=p.max_uses,
                pending_replace=bool(p.pending_replace),
                preserve_existing_socks=False,
            )
            current_profiles.append(p.profile_id)

        deleted_profiles = 0
        deleted_socks = 0
        if prune_missing:
            deleted_profiles = int(self._storage.delete_profiles_except(current_profiles) or 0)
            deleted_socks = int(self._storage.delete_socks_except(current_socks) or 0)

        return {
            "profiles_upserted": len(current_profiles),
            "socks_upserted": len(current_socks),
            "profiles_deleted": deleted_profiles,
            "socks_deleted": deleted_socks,
        }

    def get_profile(self, profile_id: str) -> Optional[ProfileRow]:
        return self._storage.get_profile(profile_id)

    def is_manual_only(self, profile_id: str) -> bool:
        pid = (profile_id or "").strip()
        if not pid:
            return False
        for p in self._config.profiles:
            if (p.profile_id or "").strip() == pid:
                return bool(getattr(p, "manual_only", False))
        return False

    def resolve_for_request(
        self,
        profile_id: str,
        socks_override: Optional[str] = None,
        *,
        allow_socks_override: bool = True,
    ) -> ResolvedProfile:
        prof = self._storage.get_profile(profile_id)
        if prof is None:
            raise KeyError(f"Unknown profile_id: {profile_id}")

        socks_id_used = getattr(prof, "socks_id", None)
        socks_url_used: Optional[str] = None

        if socks_override and allow_socks_override:
            so = socks_override.strip()
            if so.startswith(("socks5://", "socks4://", "socks://")):
                socks_id_used = None
                socks_url_used = so
            else:
                socks_id_used = so

        if socks_url_used is None and socks_id_used:
            row = self._storage.get_socks(socks_id_used)
            if row is None:
                raise KeyError(f"Unknown socks_id: {socks_id_used}")
            socks_url_used = row.url

        if hasattr(prof, "allowed_containers"):
            allowed = list(getattr(prof, "allowed_containers") or [])
        else:
            acj = getattr(prof, "allowed_containers_json", None)
            try:
                allowed = list(json.loads(acj)) if acj else []
            except Exception:
                allowed = []

        return ResolvedProfile(
            profile_id=prof.profile_id,
            profile_value=prof.profile_value,
            socks_id=socks_id_used,
            socks_url=socks_url_used,
            allowed_containers=allowed,
            max_uses=getattr(prof, "max_uses", None),
            pending_replace=bool(getattr(prof, "pending_replace", False)),
            manual_only=self.is_manual_only(prof.profile_id),
        )

    def increment_use(self, profile_id: str) -> None:
        self._storage.increment_profile_use(profile_id)

    def set_pending_replace(self, profile_id: str, pending_replace: bool) -> None:
        self._storage.set_profile_pending_replace(profile_id, pending_replace)
