#copyright "Kilax @kilax9276"
from __future__ import annotations

import sqlite3
import time
from typing import Any, Optional

from fastapi.testclient import TestClient

from src.app.main import create_app
from src.app.settings import settings
import src.app.storage as storage_mod
from src.app.storage import get_default_storage


def _write_config(tmp_path, *, idle_seconds: int = 0, rest_ttl_seconds: int = 1) -> str:
    prompts_dir = tmp_path / "prompts"
    prompts_dir.mkdir(parents=True, exist_ok=True)
    (prompts_dir / "default.txt").write_text("SYSTEM PROMPT", encoding="utf-8")

    profile_dir = tmp_path / "profile-p1"
    profile_dir.mkdir(parents=True, exist_ok=True)

    cfg_path = tmp_path / "config.yaml"
    cfg_path.write_text(
        f"""containers:
  - id: camoufox-1
    base_url: http://127.0.0.1:9999
    enabled: true
profiles:
  - profile_id: p1
    profile_value: {profile_dir.as_posix()}
    allowed_containers:
      - camoufox-1
prompts:
  - prompt_id: default
    file: prompts/default.txt
    default_max_chat_uses: 50
chat_policy:
  promptless_idle_seconds: {idle_seconds}
  default_rest_ttl_seconds: {rest_ttl_seconds}
  default_max_chat_uses: 50
""",
        encoding="utf-8",
    )
    return str(cfg_path)


class MockUpstreamPromptless:
    def __init__(self) -> None:
        self._seq = 0
        self.calls: list[dict[str, Any]] = []

    async def status(self, *, request_id: Optional[str] = None) -> dict[str, Any]:
        return {"status": "ok", "busy": False, "page_url": "https://chat.qwen.ai/", "browser_loaded": True}

    async def analyze_text(
        self,
        text: str,
        *,
        url: Optional[str] = None,
        profile: Optional[str] = None,
        socks: Optional[str] = None,
        request_id: Optional[str] = None,
    ) -> Any:
        self.calls.append({"text": text, "url": url, "profile": profile})
        if url and "/c/" in url:
            return {"ok": True, "page_url": url, "text": f"echo:{text}"}
        self._seq += 1
        page_url = f"https://chat.qwen.ai/c/direct-{self._seq}"
        return {"ok": True, "page_url": page_url, "text": f"echo:{text}"}

    async def analyze_image_b64(self, *args: Any, **kwargs: Any) -> Any:  # pragma: no cover
        raise AssertionError("analyze_image_b64() not expected")

    async def aclose(self) -> None:  # pragma: no cover
        return None


def test_promptless_mode_creates_direct_chat_and_allows_direct_reuse(tmp_path) -> None:
    sqlite_path = tmp_path / "test.sqlite"
    old_sqlite_path = settings.SQLITE_PATH
    old_config_path = settings.CONFIG_PATH

    try:
        settings.SQLITE_PATH = str(sqlite_path)
        settings.CONFIG_PATH = _write_config(tmp_path, idle_seconds=0)

        if hasattr(storage_mod, "_DB_INITIALIZED"):
            storage_mod._DB_INITIALIZED = False  # type: ignore[attr-defined]
        if hasattr(storage_mod, "_DEFAULT_STORAGE"):
            storage_mod._DEFAULT_STORAGE = None  # type: ignore[attr-defined]

        app = create_app()
        upstream = MockUpstreamPromptless()

        with TestClient(app) as client:
            app.state.pool._clients["camoufox-1"] = upstream  # type: ignore[attr-defined]

            r1 = client.post(
                "/v1/solve",
                json={
                    "input": {"text": "hello direct"},
                    "options": {"prompt_id": None, "profile_id": "p1", "force_new_chat": True},
                },
            )
            assert r1.status_code == 200
            body1 = r1.json()
            assert body1["ok"] is True
            assert body1["meta"]["prompt_id_selected"] is None
            assert body1["meta"]["promptless_mode"] is True
            page_url = body1["meta"]["page_url"]
            assert page_url.endswith("/c/direct-1")

            r2 = client.post(
                "/v1/solve",
                json={
                    "input": {"text": "continue direct"},
                    "options": {"prompt_id": None, "chat_url": page_url},
                },
            )
            assert r2.status_code == 200
            body2 = r2.json()
            assert body2["ok"] is True
            assert body2["meta"]["page_url"] == page_url
            assert body2["meta"]["profile_id"] == "p1"

        conn = sqlite3.connect(str(sqlite_path))
        try:
            row = conn.execute(
                """
                SELECT prompt_id, chat_id, page_url, uses_count
                FROM chat_sessions
                WHERE page_url = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (page_url,),
            ).fetchone()
            assert row is not None
            assert row[0] == "__direct__"
            assert row[1] == "direct-1"
            assert str(row[2]).endswith("/c/direct-1")
            assert int(row[3]) >= 2
        finally:
            conn.close()
    finally:
        settings.SQLITE_PATH = old_sqlite_path
        settings.CONFIG_PATH = old_config_path
        if hasattr(storage_mod, "_DB_INITIALIZED"):
            storage_mod._DB_INITIALIZED = False  # type: ignore[attr-defined]
        if hasattr(storage_mod, "_DEFAULT_STORAGE"):
            storage_mod._DEFAULT_STORAGE = None  # type: ignore[attr-defined]


def test_rest_and_logged_out_markers_block_chat_until_cleared_or_expired(tmp_path) -> None:
    sqlite_path = tmp_path / "test.sqlite"
    old_sqlite_path = settings.SQLITE_PATH
    old_config_path = settings.CONFIG_PATH

    try:
        settings.SQLITE_PATH = str(sqlite_path)
        settings.CONFIG_PATH = _write_config(tmp_path, idle_seconds=0, rest_ttl_seconds=1)

        if hasattr(storage_mod, "_DB_INITIALIZED"):
            storage_mod._DB_INITIALIZED = False  # type: ignore[attr-defined]
        if hasattr(storage_mod, "_DEFAULT_STORAGE"):
            storage_mod._DEFAULT_STORAGE = None  # type: ignore[attr-defined]

        app = create_app()
        upstream = MockUpstreamPromptless()

        with TestClient(app) as client:
            app.state.pool._clients["camoufox-1"] = upstream  # type: ignore[attr-defined]
            st = get_default_storage()
            sess = st.create_full_chat_session(
                container_id="camoufox-1",
                prompt_id="__direct__",
                profile_id="p1",
                socks_id="",
                chat_id="direct-old",
                page_url="https://chat.qwen.ai/c/direct-old",
            )
            # Сделаем чат достаточно старым, чтобы он считался "отлежавшимся".
            with st._connect() as conn:  # type: ignore[attr-defined]
                conn.execute(
                    "UPDATE chat_sessions SET updated_at=? WHERE id=?;",
                    ("2000-01-01T00:00:00+00:00", sess.id),
                )
                conn.commit()

            rest = client.post("/v1/chats/rest", json={"chat_url": sess.page_url, "ttl_seconds": 1})
            assert rest.status_code == 200
            assert rest.json()["ok"] is True

            # Пока чат на rest — auto promptless не должен брать его и создаст новый direct-чат.
            r1 = client.post(
                "/v1/solve",
                json={"input": {"text": "need free chat"}, "options": {"prompt_id": ""}},
            )
            assert r1.status_code == 200
            page_url_1 = r1.json()["meta"]["page_url"]
            assert page_url_1.endswith("/c/direct-1")
            assert page_url_1 != sess.page_url

            time.sleep(1.05)

            # После истечения rest система снова может выбрать старый чат.
            r2 = client.post(
                "/v1/solve",
                json={"input": {"text": "reuse rested old"}, "options": {"prompt_id": ""}},
            )
            assert r2.status_code == 200
            assert r2.json()["meta"]["page_url"] == sess.page_url

            logout = client.post("/v1/chats/logged-out", json={"chat_url": sess.page_url, "logged_out": True})
            assert logout.status_code == 200
            assert logout.json()["ok"] is True

            # Явный доступ к logged-out чату запрещён.
            r3 = client.post(
                "/v1/solve",
                json={"input": {"text": "should fail"}, "options": {"prompt_id": None, "chat_url": sess.page_url}},
            )
            assert r3.status_code == 400
            assert r3.json()["error"]["code"] == "INVALID_REQUEST"

            clear_logout = client.post("/v1/chats/logged-out/clear", json={"chat_url": sess.page_url})
            assert clear_logout.status_code == 200
            assert clear_logout.json()["ok"] is True

            r4 = client.post(
                "/v1/solve",
                json={"input": {"text": "works again"}, "options": {"prompt_id": None, "chat_url": sess.page_url}},
            )
            assert r4.status_code == 200
            assert r4.json()["meta"]["page_url"] == sess.page_url
    finally:
        settings.SQLITE_PATH = old_sqlite_path
        settings.CONFIG_PATH = old_config_path
        if hasattr(storage_mod, "_DB_INITIALIZED"):
            storage_mod._DB_INITIALIZED = False  # type: ignore[attr-defined]
        if hasattr(storage_mod, "_DEFAULT_STORAGE"):
            storage_mod._DEFAULT_STORAGE = None  # type: ignore[attr-defined]
