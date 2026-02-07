from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..storage import Storage


@dataclass(frozen=True)
class RangeMeta:
    date_from: str
    date_to: str
    limit: int
    offset: int


def containers_usage(st: Storage, *, date_from: str, date_to: str, limit: int, offset: int) -> dict[str, Any]:
    rows = st.fetchall(
        """
        SELECT
            container_id,
            COUNT(*) AS jobs_total,
            SUM(CASE WHEN status='succeeded' THEN 1 ELSE 0 END) AS jobs_succeeded,
            SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) AS jobs_failed
        FROM job_attempts
        WHERE started_at >= ? AND started_at < ?
        GROUP BY container_id
        ORDER BY jobs_total DESC, container_id ASC
        LIMIT ? OFFSET ?;
        """,
        (date_from, date_to, int(limit), int(offset)),
    )
    items = [dict(r) for r in rows]
    return {"ok": True, "items": items, "meta": {"from": date_from, "to": date_to, "limit": limit, "offset": offset}}


def profiles_usage(st: Storage, *, date_from: str, date_to: str, limit: int, offset: int) -> dict[str, Any]:
    rows = st.fetchall(
        """
        SELECT
            profile_id,
            prompt_id,
            COUNT(*) AS jobs_total,
            SUM(CASE WHEN status='succeeded' THEN 1 ELSE 0 END) AS jobs_succeeded,
            SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) AS jobs_failed
        FROM job_attempts
        WHERE started_at >= ? AND started_at < ?
        GROUP BY profile_id, prompt_id
        ORDER BY jobs_total DESC, profile_id ASC, prompt_id ASC
        LIMIT ? OFFSET ?;
        """,
        (date_from, date_to, int(limit), int(offset)),
    )
    items = [dict(r) for r in rows]
    return {"ok": True, "items": items, "meta": {"from": date_from, "to": date_to, "limit": limit, "offset": offset}}


def prompts_usage(st: Storage, *, date_from: str, date_to: str, limit: int, offset: int) -> dict[str, Any]:
    rows = st.fetchall(
        """
        SELECT
            COALESCE(selected_prompt_id, prompt_id) AS prompt_id,
            COUNT(*) AS jobs_total,
            SUM(CASE WHEN status='succeeded' THEN 1 ELSE 0 END) AS jobs_succeeded,
            SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) AS jobs_failed
        FROM jobs
        WHERE started_at >= ? AND started_at < ?
        GROUP BY COALESCE(selected_prompt_id, prompt_id)
        ORDER BY jobs_total DESC, prompt_id ASC
        LIMIT ? OFFSET ?;
        """,
        (date_from, date_to, int(limit), int(offset)),
    )
    summary = [dict(r) for r in rows]
    return {"ok": True, "items": {"summary": summary}, "meta": {"from": date_from, "to": date_to, "limit": limit, "offset": offset}}
