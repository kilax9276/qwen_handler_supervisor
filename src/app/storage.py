#copyright "Kilax @kilax9276"
from __future__ import annotations

import json
import os
import sqlite3
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Optional, Sequence

from .settings import settings


# ==============================================================================
# Data models
# ==============================================================================


@dataclass(frozen=True)
class SocksRow:
    socks_id: str
    url: str
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class ProfileRow:
    profile_id: str
    profile_value: str
    socks_id: Optional[str]
    allowed_containers_json: Optional[str]
    max_uses: Optional[int]
    uses_count: int
    pending_replace: bool
    created_at: str
    updated_at: str

    @property
    def allowed_containers(self) -> list[str]:
        if not self.allowed_containers_json:
            return []
        try:
            v = json.loads(self.allowed_containers_json)
            if isinstance(v, list):
                return [str(x) for x in v]
            return []
        except Exception:
            return []


@dataclass(frozen=True)
class FullChatSession:
    id: int
    container_id: str
    prompt_id: str
    profile_id: str
    socks_id: str
    chat_id: Optional[str]
    page_url: str
    uses_count: int
    disabled: int
    locked_by: Optional[str]
    locked_until: Optional[str]
    tag: Optional[str]
    created_at: str
    updated_at: str


# ==============================================================================
# SQLite bootstrap & helpers
# ==============================================================================


_DB_INIT_LOCK = threading.Lock()
_DB_INITIALIZED = False
_DEFAULT_STORAGE: Optional["Storage"] = None


_BLOCKED_CHAT_IDS = {"guest", "archive"}
_BLOCKED_CHAT_TAGS = {"guest", "archive"}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ensure_parent_dir(path: str) -> None:
    parent = os.path.dirname(os.path.abspath(path))
    if parent:
        os.makedirs(parent, exist_ok=True)


def _json_dumps_best_effort(v: Any) -> Optional[str]:
    if v is None:
        return None
    try:
        return json.dumps(v, ensure_ascii=False)
    except Exception:
        return str(v)


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table});").fetchall()
    return {r[1] for r in rows}


def _parse_iso(dt: Optional[str]) -> Optional[datetime]:
    if not dt:
        return None
    try:
        return datetime.fromisoformat(dt)
    except Exception:
        return None


def _norm_key(v: Optional[str]) -> str:
    return v or ""


def _norm_tag(v: Optional[str]) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def _is_blocked_chat(chat_id: Optional[str], tag: Optional[str]) -> bool:
    cid = (chat_id or "").strip().lower()
    t = (tag or "").strip().lower()
    if cid in _BLOCKED_CHAT_IDS:
        return True
    if t in _BLOCKED_CHAT_TAGS:
        return True
    return False


class Storage:
    """SQLite storage (full version only).

    MVP слой (chat_sessions_mvp и миграции в/из него) удалён намеренно.
    БД допускается пересоздавать с нуля.

    Добавлены пометки чатов:
      - chat_id == 'guest' (или tag == 'guest') => профиль блокируется для работы
      - tag == 'archive' (или chat_id == 'archive') => чат нельзя переиспользовать

    Важно:
      - guest-блок профиля определяется наличием ХОТЯ БЫ ОДНОЙ записи в chat_sessions
        с guest (даже если disabled=1).
      - снятие guest-пометки реализовано удалением таких записей.
    """

    def __init__(self, sqlite_path: str) -> None:
        self.sqlite_path = sqlite_path
        self._initialized = False

    def _connect(self) -> sqlite3.Connection:
        _ensure_parent_dir(self.sqlite_path)
        use_uri = self.sqlite_path.startswith("file:")
        conn = sqlite3.connect(self.sqlite_path, timeout=30, check_same_thread=False, uri=use_uri)
        conn.row_factory = sqlite3.Row
        return conn

    def init(self) -> None:
        if self._initialized:
            return

        with _DB_INIT_LOCK:
            if self._initialized:
                return

            with self._connect() as conn:
                try:
                    conn.execute("PRAGMA journal_mode=WAL;")
                    conn.execute("PRAGMA synchronous=NORMAL;")
                except Exception:
                    pass

                # socks
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS socks (
                        socks_id   TEXT PRIMARY KEY,
                        url        TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    );
                    """
                )

                # profiles
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS profiles (
                        profile_id              TEXT PRIMARY KEY,
                        profile_value           TEXT NOT NULL,
                        socks_id                TEXT,
                        allowed_containers_json TEXT NOT NULL DEFAULT '[]',
                        uses_count              INTEGER NOT NULL DEFAULT 0,
                        max_uses                INTEGER,
                        pending_replace         INTEGER NOT NULL DEFAULT 0,
                        created_at              TEXT NOT NULL,
                        updated_at              TEXT NOT NULL
                    );
                    """
                )
                conn.execute("CREATE INDEX IF NOT EXISTS idx_profiles_pending_replace ON profiles(pending_replace);")

                # chat_sessions (FULL)
                # ВАЖНО: добавили tag в CREATE TABLE для новых БД
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS chat_sessions (
                        id            INTEGER PRIMARY KEY AUTOINCREMENT,
                        container_id  TEXT NOT NULL,
                        prompt_id     TEXT NOT NULL,
                        profile_id    TEXT NOT NULL DEFAULT '',
                        socks_id      TEXT NOT NULL DEFAULT '',
                        chat_id       TEXT,
                        page_url      TEXT NOT NULL,
                        uses_count    INTEGER NOT NULL DEFAULT 0,
                        disabled      INTEGER NOT NULL DEFAULT 0,
                        tag           TEXT,
                        locked_by     TEXT,
                        locked_until  TEXT,
                        created_at    TEXT NOT NULL,
                        updated_at    TEXT NOT NULL
                    );
                    """
                )

                # forward-compat: если БД была создана раньше — добавим недостающие колонки ДО индексов по ним
                cols = _table_columns(conn, "chat_sessions")
                for col_name, col_sql in (
                    ("disabled", "disabled INTEGER NOT NULL DEFAULT 0"),
                    ("tag", "tag TEXT"),
                    ("locked_by", "locked_by TEXT"),
                    ("locked_until", "locked_until TEXT"),
                ):
                    if col_name not in cols:
                        try:
                            conn.execute(f"ALTER TABLE chat_sessions ADD COLUMN {col_sql};")
                        except Exception:
                            # если по какой-то причине ALTER TABLE не прошёл — не валим старт,
                            # но индексы на отсутствующие колонки ниже всё равно не создадим
                            pass

                # индексы на chat_sessions — после миграций
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_chat_sess_lookup ON chat_sessions(container_id, prompt_id, profile_id, socks_id, disabled, id);"
                )
                conn.execute("CREATE INDEX IF NOT EXISTS idx_chat_page_url ON chat_sessions(page_url);")

                # ВАЖНО: индекс по tag создаём только после гарантии, что колонка есть
                # (на очень старых БД/битых миграциях может не появиться — тогда просто пропустим)
                cols = _table_columns(conn, "chat_sessions")
                if "tag" in cols:
                    conn.execute("CREATE INDEX IF NOT EXISTS idx_chat_tag ON chat_sessions(tag);")

                # jobs
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS jobs (
                        job_id      TEXT PRIMARY KEY,
                        request_id  TEXT,
                        prompt_id   TEXT NOT NULL,
                        selected_prompt_id TEXT,
                        decision_mode TEXT,
                        fanout_requested INTEGER,
                        fanout_used INTEGER,
                        container_ids_used_json TEXT,
                        input_text  TEXT,
                        input_image_present INTEGER,
                        input_image_ext TEXT,
                        profile_id  TEXT,
                        socks_id    TEXT,
                        status      TEXT,
                        result_text TEXT,
                        result_raw_json TEXT,
                        error_code  TEXT,
                        error_message TEXT,
                        started_at  TEXT NOT NULL,
                        finished_at TEXT
                    );
                    """
                )
                conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_started_at ON jobs(started_at);")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);")

                # job_attempts
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS job_attempts (
                        attempt_id  TEXT PRIMARY KEY,
                        job_id      TEXT NOT NULL,
                        container_id TEXT NOT NULL,
                        prompt_id   TEXT NOT NULL,
                        role        TEXT NOT NULL,
                        profile_id  TEXT,
                        socks_id    TEXT,
                        chat_id     TEXT,
                        page_url    TEXT,
                        chat_session_id TEXT,
                        status      TEXT,
                        result_text TEXT,
                        result_raw_json TEXT,
                        error_code  TEXT,
                        error_message TEXT,
                        started_at  TEXT NOT NULL,
                        finished_at TEXT
                    );
                    """
                )
                conn.execute("CREATE INDEX IF NOT EXISTS idx_attempts_job_id ON job_attempts(job_id);")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_attempts_started_at ON job_attempts(started_at);")

                conn.commit()

            self._initialized = True


    # ------------------------------------------------------------------
    # Low-level helpers used by reports / executor
    # ------------------------------------------------------------------

    def fetchall(self, sql: str, params: Sequence[Any] = ()) -> list[sqlite3.Row]:
        self.init()
        with self._connect() as conn:
            return list(conn.execute(sql, params).fetchall())

    # ------------------------------------------------------------------
    # Socks
    # ------------------------------------------------------------------

    def upsert_socks(self, socks_id: str, url: str) -> None:
        self.init()
        now = _now_iso()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO socks(socks_id, url, created_at, updated_at)
                VALUES(?, ?, ?, ?)
                ON CONFLICT(socks_id) DO UPDATE SET
                    url=excluded.url,
                    updated_at=excluded.updated_at;
                """,
                (socks_id, url, now, now),
            )
            conn.commit()

    def get_socks(self, socks_id: str) -> Optional[SocksRow]:
        self.init()
        with self._connect() as conn:
            row = conn.execute("SELECT socks_id, url, created_at, updated_at FROM socks WHERE socks_id=?;", (socks_id,)).fetchone()
        if not row:
            return None
        return SocksRow(socks_id=row["socks_id"], url=row["url"], created_at=row["created_at"], updated_at=row["updated_at"])

    # ------------------------------------------------------------------
    # Profiles
    # ------------------------------------------------------------------

    def upsert_profile(
        self,
        *,
        profile_id: str,
        profile_value: str,
        socks_id: Optional[str],
        allowed_containers: list[str],
        max_uses: Optional[int],
        pending_replace: bool,
        preserve_existing_socks: bool = False,
    ) -> None:
        self.init()
        now = _now_iso()
        allowed_json = json.dumps(list(allowed_containers or []), ensure_ascii=False)
        with self._connect() as conn:
            if preserve_existing_socks:
                conn.execute(
                    """
                    INSERT INTO profiles(profile_id, profile_value, socks_id, allowed_containers_json, uses_count, max_uses, pending_replace, created_at, updated_at)
                    VALUES(?, ?, ?, ?, 0, ?, ?, ?, ?)
                    ON CONFLICT(profile_id) DO UPDATE SET
                        profile_value=excluded.profile_value,
                        allowed_containers_json=excluded.allowed_containers_json,
                        max_uses=excluded.max_uses,
                        pending_replace=excluded.pending_replace,
                        updated_at=excluded.updated_at;
                    """,
                    (profile_id, profile_value, socks_id, allowed_json, max_uses, 1 if pending_replace else 0, now, now),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO profiles(profile_id, profile_value, socks_id, allowed_containers_json, uses_count, max_uses, pending_replace, created_at, updated_at)
                    VALUES(?, ?, ?, ?, 0, ?, ?, ?, ?)
                    ON CONFLICT(profile_id) DO UPDATE SET
                        profile_value=excluded.profile_value,
                        socks_id=excluded.socks_id,
                        allowed_containers_json=excluded.allowed_containers_json,
                        max_uses=excluded.max_uses,
                        pending_replace=excluded.pending_replace,
                        updated_at=excluded.updated_at;
                    """,
                    (profile_id, profile_value, socks_id, allowed_json, max_uses, 1 if pending_replace else 0, now, now),
                )
            conn.commit()

    def get_profile(self, profile_id: str) -> Optional[ProfileRow]:
        self.init()
        with self._connect() as conn:
            cur = conn.execute(
                """
                SELECT
                    profile_id,
                    profile_value,
                    socks_id,
                    allowed_containers_json,
                    max_uses,
                    uses_count,
                    pending_replace,
                    created_at,
                    updated_at
                FROM profiles
                WHERE profile_id = ?
                LIMIT 1;
                """,
                (profile_id,),
            )
            r = cur.fetchone()

        if not r:
            return None

        return ProfileRow(
            profile_id=r[0],
            profile_value=r[1],
            socks_id=r[2],
            allowed_containers_json=r[3],
            max_uses=r[4],
            uses_count=int(r[5] or 0),
            pending_replace=bool(r[6]),
            created_at=r[7],
            updated_at=r[8],
        )


    def list_profiles(self) -> list[ProfileRow]:
        self.init()
        with self._connect() as conn:
            cur = conn.execute(
                """
                SELECT
                    profile_id,
                    profile_value,
                    socks_id,
                    allowed_containers_json,
                    max_uses,
                    uses_count,
                    pending_replace,
                    created_at,
                    updated_at
                FROM profiles
                ORDER BY profile_id ASC;
                """
            )
            rows = cur.fetchall()

        out: list[ProfileRow] = []
        for r in rows:
            out.append(
                ProfileRow(
                    profile_id=r[0],
                    profile_value=r[1],
                    socks_id=r[2],
                    allowed_containers_json=r[3],
                    max_uses=r[4],
                    uses_count=int(r[5] or 0),
                    pending_replace=bool(r[6]),
                    created_at=r[7],
                    updated_at=r[8],
                )
            )
        return out


    def increment_profile_use(self, profile_id: str, *, by: int = 1) -> None:
        self.init()
        by_int = int(by)
        if by_int <= 0:
            return
        now = _now_iso()
        with self._connect() as conn:
            conn.execute(
                "UPDATE profiles SET uses_count=COALESCE(uses_count,0)+?, updated_at=? WHERE profile_id=?;",
                (by_int, now, profile_id),
            )
            conn.commit()

    def set_profile_pending_replace(self, profile_id: str, pending_replace: bool) -> None:
        self.init()
        now = _now_iso()
        with self._connect() as conn:
            conn.execute(
                "UPDATE profiles SET pending_replace=?, updated_at=? WHERE profile_id=?;",
                (1 if pending_replace else 0, now, profile_id),
            )
            conn.commit()

    # ------------------------------------------------------------------
    # Jobs
    # ------------------------------------------------------------------

    def insert_job_start(
        self,
        prompt_id: str,
        *,
        job_id: Optional[str] = None,
        request_id: Optional[str] = None,
        input_text: Optional[str] = None,
        input_image_present: Optional[bool] = None,
        input_image_ext: Optional[str] = None,
        profile_id: Optional[str] = None,
        socks_id: Optional[str] = None,
        fanout_requested: Optional[int] = None,
        fanout_used: Optional[int] = None,
        decision_mode: Optional[str] = None,
        selected_prompt_id: Optional[str] = None,
        container_ids_used: Optional[list[str]] = None,
        started_at: Optional[str] = None,
    ) -> str:
        self.init()
        jid = job_id or str(uuid.uuid4())
        started = started_at or _now_iso()

        container_ids_json = _json_dumps_best_effort(list(container_ids_used or []))

        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO jobs (
                    job_id,
                    request_id,
                    prompt_id,
                    selected_prompt_id,
                    decision_mode,
                    fanout_requested,
                    fanout_used,
                    container_ids_used_json,
                    input_text,
                    input_image_present,
                    input_image_ext,
                    profile_id,
                    socks_id,
                    status,
                    result_text,
                    result_raw_json,
                    error_code,
                    error_message,
                    started_at,
                    finished_at
                ) VALUES (
                    ?, ?, ?, ?, ?,
                    ?, ?, ?,
                    ?, ?, ?,
                    ?, ?,
                    NULL, NULL, NULL, NULL, NULL,
                    ?, NULL
                );
                """,
                (
                    jid,
                    request_id,
                    prompt_id,
                    selected_prompt_id,
                    decision_mode,
                    fanout_requested,
                    fanout_used,
                    container_ids_json,
                    input_text,
                    (1 if input_image_present else 0) if input_image_present is not None else None,
                    input_image_ext,
                    profile_id,
                    socks_id,
                    started,
                ),
            )
            conn.commit()

        return jid



    def set_job_selected_containers(self, job_id: str, container_ids: list[str]) -> None:
        self.init()
        with self._connect() as conn:
            conn.execute(
                "UPDATE jobs SET container_ids_used_json=? WHERE job_id=?;",
                (_json_dumps_best_effort(list(container_ids or [])), job_id),
            )
            conn.commit()

    def update_job_finish(
        self,
        job_id: str,
        *,
        succeeded: Optional[bool] = None,
        status: Optional[str] = None,
        result_text: Optional[str] = None,
        result_raw_json: Any = None,
        error_code: Optional[str] = None,
        error_message: Optional[str] = None,
        finished_at: Optional[str] = None,
        decision_mode: Optional[str] = None,
        **_ignored: Any,
    ) -> None:
        self.init()

        if succeeded is not None:
            status_val = "succeeded" if bool(succeeded) else "failed"
        else:
            status_val = (status or "").strip() or None
        if status_val not in ("succeeded", "failed"):
            status_val = None

        finished = finished_at or _now_iso()

        with self._connect() as conn:
            conn.execute(
                """
                UPDATE jobs
                SET
                    status = COALESCE(?, status),
                    result_text = ?,
                    result_raw_json = ?,
                    error_code = ?,
                    error_message = ?,
                    decision_mode = COALESCE(?, decision_mode),
                    finished_at = ?
                WHERE job_id = ?;
                """,
                (
                    status_val,
                    result_text,
                    _json_dumps_best_effort(result_raw_json),
                    error_code,
                    error_message,
                    decision_mode,
                    finished,
                    job_id,
                ),
            )
            conn.commit()


    # ------------------------------------------------------------------
    # Attempts
    # ------------------------------------------------------------------

    def create_job_attempt(
        self,
        job_id: str,
        *,
        container_id: str,
        prompt_id: str,
        role: str,
        profile_id: Optional[str],
        socks_id: Optional[str],
        chat_id: Optional[str],
        page_url: Optional[str],
        chat_session_id: Optional[str],
        started_at: Optional[str] = None,
    ) -> str:
        self.init()
        attempt_id = str(uuid.uuid4())
        started = started_at or _now_iso()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO job_attempts(
                    attempt_id, job_id, container_id, prompt_id, role,
                    profile_id, socks_id, chat_id, page_url, chat_session_id,
                    status, result_text, result_raw_json, error_code, error_message,
                    started_at, finished_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, NULL, ?, NULL);
                """,
                (attempt_id, job_id, container_id, prompt_id, role, profile_id, socks_id, chat_id, page_url, chat_session_id, started),
            )
            conn.commit()
        return attempt_id

    def update_job_attempt_chat_session_id(self, attempt_id: str, chat_session_id: str) -> None:
        self.init()
        with self._connect() as conn:
            conn.execute("UPDATE job_attempts SET chat_session_id=? WHERE attempt_id=?;", (chat_session_id, attempt_id))
            conn.commit()

    def finish_job_attempt(
        self,
        attempt_id: str,
        *,
        status: str,
        result_text: Optional[str],
        result_raw_json: Any,
        error_code: Optional[str],
        error_message: Optional[str],
        finished_at: Optional[str] = None,
    ) -> None:
        self.init()
        finished = finished_at or _now_iso()
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE job_attempts
                SET status=?, result_text=?, result_raw_json=?, error_code=?, error_message=?, finished_at=?
                WHERE attempt_id=?;
                """,
                (status, result_text, _json_dumps_best_effort(result_raw_json), error_code, error_message, finished, attempt_id),
            )
            conn.commit()

    # ------------------------------------------------------------------
    # Chat sessions (full)
    # ------------------------------------------------------------------

    def get_chat_session(
        self,
        prompt_id: str,
        *,
        container_id: str,
        profile_id: Optional[str],
        socks_id: Optional[str],
        preferred_chat_id: Optional[str] = None,
    ) -> Optional[FullChatSession]:
        """Возвращает последний активный (disabled=0) чат для пары (container,prompt,profile,socks).

        Важно:
          - чаты с chat_id='guest' или tag in ('guest','archive') не возвращаем,
            чтобы ChatManager не переиспользовал такие записи.
        """
        self.init()
        cid = str(container_id).strip()
        if not cid:
            return None

        with self._connect() as conn:
            if preferred_chat_id:
                row = conn.execute(
                    """
                    SELECT id, container_id, prompt_id, profile_id, socks_id, chat_id, page_url,
                           uses_count, disabled, locked_by, locked_until, tag, created_at, updated_at
                    FROM chat_sessions
                    WHERE container_id=? AND prompt_id=? AND profile_id=? AND socks_id=?
                      AND disabled=0
                      AND COALESCE(chat_id,'') NOT IN ('guest','archive')
                      AND COALESCE(tag,'') NOT IN ('guest','archive')
                      AND chat_id=?
                    ORDER BY id DESC
                    LIMIT 1;
                    """,
                    (cid, prompt_id, _norm_key(profile_id), _norm_key(socks_id), preferred_chat_id),
                ).fetchone()
            else:
                row = conn.execute(
                    """
                    SELECT id, container_id, prompt_id, profile_id, socks_id, chat_id, page_url,
                           uses_count, disabled, locked_by, locked_until, tag, created_at, updated_at
                    FROM chat_sessions
                    WHERE container_id=? AND prompt_id=? AND profile_id=? AND socks_id=?
                      AND disabled=0
                      AND COALESCE(chat_id,'') NOT IN ('guest','archive')
                      AND COALESCE(tag,'') NOT IN ('guest','archive')
                    ORDER BY id DESC
                    LIMIT 1;
                    """,
                    (cid, prompt_id, _norm_key(profile_id), _norm_key(socks_id)),
                ).fetchone()

        if not row:
            return None

        return FullChatSession(
            id=int(row["id"]),
            container_id=row["container_id"],
            prompt_id=row["prompt_id"],
            profile_id=row["profile_id"],
            socks_id=row["socks_id"],
            chat_id=row["chat_id"],
            page_url=row["page_url"],
            uses_count=int(row["uses_count"] or 0),
            disabled=int(row["disabled"] or 0),
            locked_by=row["locked_by"],
            locked_until=row["locked_until"],
            tag=row["tag"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def create_full_chat_session(
        self,
        *,
        container_id: str,
        prompt_id: str,
        profile_id: str,
        socks_id: str,
        chat_id: Optional[str],
        page_url: str,
    ) -> FullChatSession:
        self.init()
        now = _now_iso()
        with self._connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO chat_sessions (
                    container_id, prompt_id, profile_id, socks_id, chat_id, page_url,
                    uses_count, disabled, locked_by, locked_until, tag, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, 0, 0, NULL, NULL, NULL, ?, ?);
                """,
                (str(container_id), prompt_id, _norm_key(profile_id), _norm_key(socks_id), chat_id, str(page_url), now, now),
            )
            sid = int(cur.lastrowid)
            conn.commit()
            row = conn.execute(
                """
                SELECT id, container_id, prompt_id, profile_id, socks_id, chat_id, page_url,
                       uses_count, disabled, locked_by, locked_until, tag, created_at, updated_at
                FROM chat_sessions
                WHERE id=?;
                """,
                (sid,),
            ).fetchone()

        assert row is not None
        return FullChatSession(
            id=int(row["id"]),
            container_id=row["container_id"],
            prompt_id=row["prompt_id"],
            profile_id=row["profile_id"],
            socks_id=row["socks_id"],
            chat_id=row["chat_id"],
            page_url=row["page_url"],
            uses_count=int(row["uses_count"] or 0),
            disabled=int(row["disabled"] or 0),
            locked_by=row["locked_by"],
            locked_until=row["locked_until"],
            tag=row["tag"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def update_full_chat_session_by_id(
        self,
        chat_session_id: int,
        *,
        chat_id: Optional[str] = None,
        page_url: Optional[str] = None,
        disabled: Optional[bool] = None,
        tag: Optional[str] = None,
    ) -> FullChatSession:
        self.init()
        now = _now_iso()
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE chat_sessions
                SET chat_id=COALESCE(?, chat_id),
                    page_url=COALESCE(?, page_url),
                    disabled=COALESCE(?, disabled),
                    tag=COALESCE(?, tag),
                    updated_at=?
                WHERE id=?;
                """,
                (
                    chat_id,
                    page_url,
                    (1 if disabled else 0) if disabled is not None else None,
                    _norm_tag(tag),
                    now,
                    int(chat_session_id),
                ),
            )
            conn.commit()

            row = conn.execute(
                """
                SELECT id, container_id, prompt_id, profile_id, socks_id, chat_id, page_url,
                       uses_count, disabled, locked_by, locked_until, tag, created_at, updated_at
                FROM chat_sessions
                WHERE id=?;
                """,
                (int(chat_session_id),),
            ).fetchone()

        assert row is not None
        return FullChatSession(
            id=int(row["id"]),
            container_id=row["container_id"],
            prompt_id=row["prompt_id"],
            profile_id=row["profile_id"],
            socks_id=row["socks_id"],
            chat_id=row["chat_id"],
            page_url=row["page_url"],
            uses_count=int(row["uses_count"] or 0),
            disabled=int(row["disabled"] or 0),
            locked_by=row["locked_by"],
            locked_until=row["locked_until"],
            tag=row["tag"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def increment_chat_use(self, chat_session_id: int, *, by: int = 1) -> None:
        self.init()
        by_int = int(by)
        if by_int <= 0:
            return
        now = _now_iso()
        with self._connect() as conn:
            conn.execute(
                "UPDATE chat_sessions SET uses_count=COALESCE(uses_count,0)+?, updated_at=? WHERE id=?;",
                (by_int, now, int(chat_session_id)),
            )
            conn.commit()

    def get_full_chat_session_by_url(self, page_url: str) -> Optional[FullChatSession]:
        """Ищет чат по page_url.

        Важно:
          - возвращаем запись вне зависимости от disabled/tag/chat_id.
            Это нужно, чтобы:
              * корректно диагностировать, что pinned chat_url относится к guest/archive,
              * а также для админских действий.
        """
        self.init()
        url = (page_url or "").strip()
        if not url:
            return None
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT id, container_id, prompt_id, profile_id, socks_id, chat_id, page_url,
                       uses_count, disabled, locked_by, locked_until, tag, created_at, updated_at
                FROM chat_sessions
                WHERE page_url=?
                ORDER BY id DESC
                LIMIT 1;
                """,
                (url,),
            ).fetchone()

        if not row:
            return None

        return FullChatSession(
            id=int(row["id"]),
            container_id=row["container_id"],
            prompt_id=row["prompt_id"],
            profile_id=row["profile_id"],
            socks_id=row["socks_id"],
            chat_id=row["chat_id"],
            page_url=row["page_url"],
            uses_count=int(row["uses_count"] or 0),
            disabled=int(row["disabled"] or 0),
            locked_by=row["locked_by"],
            locked_until=row["locked_until"],
            tag=row["tag"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    # ------------------------------------------------------------------
    # Chat tags & profile blocks
    # ------------------------------------------------------------------

    def profile_has_guest_chat(self, profile_id: str) -> bool:
        """True, если у профиля есть хотя бы один чат с guest (chat_id='guest' или tag='guest')."""
        self.init()
        pid = _norm_key(profile_id)
        if not pid:
            return False
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT 1
                FROM chat_sessions
                WHERE profile_id=? AND (chat_id='guest' OR tag='guest')
                LIMIT 1;
                """,
                (pid,),
            ).fetchone()
        return bool(row)

    def count_guest_chats_for_profile(self, profile_id: str) -> int:
        self.init()
        pid = _norm_key(profile_id)
        if not pid:
            return 0
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) AS n
                FROM chat_sessions
                WHERE profile_id=? AND (chat_id='guest' OR tag='guest');
                """,
                (pid,),
            ).fetchone()
        if not row:
            return 0
        try:
            return int(row["n"] or 0)
        except Exception:
            return 0

    def list_blocked_profiles(self) -> list[dict[str, Any]]:
        """Список профилей, которые заблокированы для использования из-за guest-чата."""
        self.init()
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT profile_id, COUNT(*) AS guest_chats
                FROM chat_sessions
                WHERE profile_id <> '' AND (chat_id='guest' OR tag='guest')
                GROUP BY profile_id
                ORDER BY profile_id ASC;
                """
            ).fetchall()

        out: list[dict[str, Any]] = []
        for r in rows:
            out.append(
                {
                    "profile_id": str(r["profile_id"]),
                    "reason": "guest",
                    "guest_chats": int(r["guest_chats"] or 0),
                }
            )
        return out

    def delete_guest_chats_for_profile(self, profile_id: str) -> int:
        """Удаляет все guest-записи чатов для профиля (chat_id='guest' или tag='guest')."""
        self.init()
        pid = _norm_key(profile_id)
        if not pid:
            return 0
        with self._connect() as conn:
            cur = conn.execute(
                "DELETE FROM chat_sessions WHERE profile_id=? AND (chat_id='guest' OR tag='guest');",
                (pid,),
            )
            conn.commit()
            try:
                return int(cur.rowcount or 0)
            except Exception:
                return 0

    def archive_chats_for_profile(self, profile_id: str) -> int:
        """Помечает ВСЕ чаты профиля как archive и запрещает их дальнейшее использование."""
        self.init()
        pid = _norm_key(profile_id)
        if not pid:
            return 0
        now = _now_iso()
        with self._connect() as conn:
            cur = conn.execute(
                """
                UPDATE chat_sessions
                SET tag='archive', disabled=1, updated_at=?
                WHERE profile_id=?;
                """,
                (now, pid),
            )
            conn.commit()
            try:
                return int(cur.rowcount or 0)
            except Exception:
                return 0

    def mark_chat_session_tag(self, chat_session_id: int, *, tag: str, disabled: Optional[bool] = None) -> None:
        """Best-effort: проставить tag (и опционально disabled) для одной chat_session."""
        self.init()
        now = _now_iso()
        with self._connect() as conn:
            if disabled is None:
                conn.execute(
                    "UPDATE chat_sessions SET tag=?, updated_at=? WHERE id=?;",
                    (_norm_tag(tag), now, int(chat_session_id)),
                )
            else:
                conn.execute(
                    "UPDATE chat_sessions SET tag=?, disabled=?, updated_at=? WHERE id=?;",
                    (_norm_tag(tag), 1 if disabled else 0, now, int(chat_session_id)),
                )
            conn.commit()

    def is_chat_session_usable(self, chat_session: FullChatSession) -> bool:
        if int(getattr(chat_session, "disabled", 0) or 0) != 0:
            return False
        return not _is_blocked_chat(getattr(chat_session, "chat_id", None), getattr(chat_session, "tag", None))

    # ------------------------------------------------------------------
    # Chat locks (full feature, оставлено)
    # ------------------------------------------------------------------

    def is_chat_locked(self, chat_session_id: int) -> bool:
        self.init()
        now = datetime.now(timezone.utc)
        with self._connect() as conn:
            row = conn.execute("SELECT locked_until FROM chat_sessions WHERE id=? LIMIT 1;", (int(chat_session_id),)).fetchone()
            if not row:
                return False
            until = _parse_iso(row["locked_until"])
            if until is None:
                return False
            if until <= now:
                conn.execute("UPDATE chat_sessions SET locked_by=NULL, locked_until=NULL WHERE id=?;", (int(chat_session_id),))
                conn.commit()
                return False
            return True

    def list_locked_containers(self, now_iso: Optional[str] = None) -> set[str]:
        self.init()
        now_iso = now_iso or datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT DISTINCT container_id FROM chat_sessions WHERE locked_until IS NOT NULL AND locked_until > ?;",
                (now_iso,),
            ).fetchall()
        return {str(r["container_id"]) for r in rows}

    def lock_chat_by_url(self, *, page_url: str, locked_by: str, ttl_seconds: int) -> Optional[FullChatSession]:
        self.init()
        url = (page_url or "").strip()
        who = (locked_by or "").strip()
        ttl = int(ttl_seconds)
        if not url or not who or ttl <= 0:
            return None
        now = datetime.now(timezone.utc)
        until = now + timedelta(seconds=ttl)
        with self._connect() as conn:
            conn.execute(
                "UPDATE chat_sessions SET locked_by=?, locked_until=?, updated_at=? WHERE page_url=?;",
                (who, until.isoformat(), now.isoformat(), url),
            )
            conn.commit()
        return self.get_full_chat_session_by_url(url)

    def unlock_chat_by_url(self, *, page_url: str, locked_by: str) -> bool:
        self.init()
        url = (page_url or "").strip()
        who = (locked_by or "").strip()
        if not url or not who:
            return False
        with self._connect() as conn:
            row = conn.execute(
                "SELECT id, locked_by FROM chat_sessions WHERE page_url=? ORDER BY id DESC LIMIT 1;",
                (url,),
            ).fetchone()
            if not row:
                return False
            if (row["locked_by"] or "") != who:
                return False
            conn.execute("UPDATE chat_sessions SET locked_by=NULL, locked_until=NULL WHERE id=?;", (int(row["id"]),))
            conn.commit()
        return True


def get_default_storage() -> Storage:
    global _DEFAULT_STORAGE, _DB_INITIALIZED
    if (_DEFAULT_STORAGE is None) or (not _DB_INITIALIZED) or (_DEFAULT_STORAGE.sqlite_path != settings.SQLITE_PATH):
        _DEFAULT_STORAGE = Storage(settings.SQLITE_PATH)
        _DEFAULT_STORAGE.init()
        _DB_INITIALIZED = True
    return _DEFAULT_STORAGE
