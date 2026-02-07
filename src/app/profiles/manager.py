#copyright "Kilax @kilax9276"
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple

from ..config_loader import AppConfig
from ..storage import ProfileRow, SocksRow, Storage


@dataclass(frozen=True)
class ResolvedProfile:
    profile_id: str
    profile_value: str
    socks_id: Optional[str]
    socks_url: Optional[str]
    allowed_containers: list[str]
    max_uses: Optional[int]
    pending_replace: bool


class ProfileManager:
    def __init__(self, *, storage: Storage, config: AppConfig) -> None:
        self._storage = storage
        self._config = config

    def seed_from_config(self) -> None:
        """Upserts socks+profiles from YAML into SQLite.

        This is idempotent and safe to call on start.
        """
        for s in self._config.socks:
            self._storage.upsert_socks(s.socks_id, s.url)

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

    def get_profile(self, profile_id: str) -> Optional[ProfileRow]:
        return self._storage.get_profile(profile_id)

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

        # совместимость: allowed_containers или allowed_containers_json
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
        )


    def increment_use(self, profile_id: str) -> None:
        self._storage.increment_profile_use(profile_id)

    def set_pending_replace(self, profile_id: str, pending_replace: bool) -> None:
        self._storage.set_profile_pending_replace(profile_id, pending_replace)
