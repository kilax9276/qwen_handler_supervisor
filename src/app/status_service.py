#copyright "Kilax @kilax9276"
from __future__ import annotations

from dataclasses import asdict, is_dataclass
from typing import Any, Dict, Optional, Set


def _as_plain_dict(obj: Any) -> Optional[Dict[str, Any]]:
    if obj is None:
        return None
    if is_dataclass(obj):
        return asdict(obj)
    if isinstance(obj, dict):
        return obj
    if hasattr(obj, "__dict__"):
        try:
            return dict(obj.__dict__)
        except Exception:
            return None
    return None


def _blocked_profile_ids(items: Any) -> Set[str]:
    """
    Поддержка разных форматов Storage.list_blocked_profiles():
      - ["p1","p2"]
      - [{"profile_id":"p1","guest_chats":1}, ...]
    """
    out: Set[str] = set()
    if isinstance(items, list):
        for it in items:
            if isinstance(it, str) and it.strip():
                out.add(it.strip())
            elif isinstance(it, dict):
                pid = it.get("profile_id")
                if isinstance(pid, str) and pid.strip():
                    out.add(pid.strip())
    return out


def _looks_like_guest_page_url(page_url: Optional[str]) -> bool:
    if not page_url:
        return False
    u = page_url.lower()
    return ("/c/guest" in u) or u.endswith("/guest") or ("/c/guets" in u) or u.endswith("/guets")


def _is_guest_chat(chat_id: Optional[str], tag: Optional[str], page_url: Optional[str]) -> bool:
    if isinstance(chat_id, str) and chat_id.strip().lower() in ("guest", "guets"):
        return True
    if isinstance(tag, str) and tag.strip().lower() == "guest":
        return True
    return _looks_like_guest_page_url(page_url)


def _is_archive_chat(chat_id: Optional[str], tag: Optional[str]) -> bool:
    if isinstance(chat_id, str) and chat_id.strip().lower() == "archive":
        return True
    if isinstance(tag, str) and tag.strip().lower() == "archive":
        return True
    return False


async def build_status_all(*, storage: Any, pool: Any) -> Dict[str, Any]:
    """
    Возвращает payload для StatusResponse.status:
      {
        "db": {...},
        "blocked": {...},
        "containers": {...}
      }

    storage: get_default_storage()
    pool: app.state.pool
    """
    blocked_items = storage.list_blocked_profiles()  # guest-блокировка профилей
    blocked_ids = _blocked_profile_ids(blocked_items)

    enabled = pool.list_enabled()
    containers: Dict[str, Any] = {}

    for cid in enabled:
        try:
            upstream_status = await pool.get(cid).status()
        except Exception as e:
            containers[cid] = {"status": "error", "error": str(e)}
            continue

        # upstream_status почти всегда dict, но страхуемся
        enriched: Dict[str, Any]
        if isinstance(upstream_status, dict):
            enriched = dict(upstream_status)
        else:
            enriched = {"status": str(upstream_status)}

        page_url = enriched.get("page_url")

        # Привязка текущего page_url к записи в БД (если запись есть)
        chat_sess = None
        try:
            if page_url:
                chat_sess = storage.get_full_chat_session_by_url(str(page_url))
                # защита: если вдруг совпал url из другого контейнера — не матчим
                if chat_sess and getattr(chat_sess, "container_id", None) != cid:
                    chat_sess = None
        except Exception:
            chat_sess = None

        sess_dict = _as_plain_dict(chat_sess)
        chat_id = sess_dict.get("chat_id") if sess_dict else None
        profile_id = sess_dict.get("profile_id") if sess_dict else None
        tag = sess_dict.get("tag") if sess_dict else None  # может отсутствовать в старых БД/типах
        disabled = sess_dict.get("disabled") if sess_dict else None

        flags = {
            "profile_id": profile_id if isinstance(profile_id, str) and profile_id else None,
            "is_profile_blocked": bool(isinstance(profile_id, str) and profile_id in blocked_ids),
            "is_guest_chat": _is_guest_chat(chat_id if isinstance(chat_id, str) else None, tag if isinstance(tag, str) else None, page_url if isinstance(page_url, str) else None),
            "is_archive_chat": _is_archive_chat(chat_id if isinstance(chat_id, str) else None, tag if isinstance(tag, str) else None),
            "disabled": int(disabled) if disabled is not None else None,
        }

        enriched["orchestrator_chat_session"] = sess_dict
        enriched["orchestrator_flags"] = flags
        containers[cid] = enriched

    return {
        "db": {"sqlite_path": storage.sqlite_path},
        "blocked": {
            "reason": "guest",
            "profiles": blocked_items,
            "count": len(blocked_ids),
            "hint_clear_guest": "POST /v1/profiles/{profile_id}/guest/clear",
        },
        "containers": containers,
    }
