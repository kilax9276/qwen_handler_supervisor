from __future__ import annotations

import sqlite3
from typing import Any, Optional

from fastapi.testclient import TestClient

from src.app.main import create_app
from src.app.settings import settings
import src.app.storage as storage_mod


def _write_min_config(tmp_path, *, container_id: str, start_prompt: str = "SYSTEM PROMPT") -> str:
    prompts_dir = tmp_path / "prompts"
    prompts_dir.mkdir(parents=True, exist_ok=True)

    prompt_path = prompts_dir / "default.txt"
    prompt_path.write_text(start_prompt, encoding="utf-8")

    profile_dir = tmp_path / "profile-p1"
    profile_dir.mkdir(parents=True, exist_ok=True)

    cfg_path = tmp_path / "config.yaml"
    cfg_path.write_text(
        f"""containers:
  - id: {container_id}
    base_url: http://127.0.0.1:9999
    enabled: true
profiles:
  - profile_id: p1
    profile_value: {profile_dir.as_posix()}
    allowed_containers:
      - {container_id}
prompts:
  - prompt_id: default
    file: prompts/default.txt
    default_max_chat_uses: 50
""",
        encoding="utf-8",
    )
    return str(cfg_path)


class MockUpstreamChatFlow:
    def __init__(self, *, start_prompt: str, chat_id: str = "abc123") -> None:
        self._start_prompt = start_prompt
        self._chat_id = chat_id
        self.status_calls = 0
        self.analyze_text_calls: list[dict[str, Any]] = []

    async def status(self, *, request_id: Optional[str] = None) -> dict[str, Any]:
        self.status_calls += 1
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
        self.analyze_text_calls.append({"text": text, "url": url, "profile": profile, "socks": socks, "request_id": request_id})

        # Первый вызов — стартовый промпт, должен "создать" чат и вернуть page_url с /c/<id>.
        if text == self._start_prompt:
            return {"ok": True, "page_url": f"https://chat.qwen.ai/c/{self._chat_id}", "text": ""}

        # Второй вызов — пользовательский текст.
        return {"ok": True, "page_url": f"https://chat.qwen.ai/c/{self._chat_id}", "text": "ok"}

    async def analyze_image_b64(self, *args: Any, **kwargs: Any) -> Any:  # pragma: no cover
        raise AssertionError("analyze_image_b64() не ожидается в этом тесте")

    async def aclose(self) -> None:  # pragma: no cover
        return None


def test_first_solve_sends_start_prompt_updates_chat_url_and_reuses_it(tmp_path) -> None:
    sqlite_path = tmp_path / "test.sqlite"
    old_sqlite_path = settings.SQLITE_PATH
    old_config_path = settings.CONFIG_PATH

    try:
        settings.SQLITE_PATH = str(sqlite_path)

        start_prompt = "SYSTEM PROMPT"
        settings.CONFIG_PATH = _write_min_config(tmp_path, container_id="camoufox-1", start_prompt=start_prompt)

        if hasattr(storage_mod, "_DB_INITIALIZED"):
            storage_mod._DB_INITIALIZED = False  # type: ignore[attr-defined]

        app = create_app()

        upstream = MockUpstreamChatFlow(start_prompt=start_prompt, chat_id="abc123")

        with TestClient(app) as client:
            # Подменяем upstream клиента внутри pool на мок.
            app.state.pool._clients["camoufox-1"] = upstream  # type: ignore[attr-defined]

            r = client.post(
                "/v1/solve",
                json={
                    "input": {"text": "hello"},
                    "options": {"profile_id": "p1", "force_new_chat": True},
                },
            )

            assert r.status_code == 200
            body = r.json()
            assert body["ok"] is True
            assert body["final"]["text"] == "ok"

        # Проверяем, что chat_session обновился в БД (chat_id и page_url на /c/<id>)
        conn = sqlite3.connect(str(sqlite_path))
        try:
            row = conn.execute(
                """
                SELECT chat_id, page_url, uses_count
                FROM chat_sessions
                WHERE prompt_id = ? AND container_id = ? AND profile_id = ?
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                ("default", "camoufox-1", "p1"),
            ).fetchone()
            assert row is not None
            chat_id, page_url, uses_count = row
            assert chat_id == "abc123"
            assert str(page_url).endswith("/c/abc123")
            assert int(uses_count) >= 2  # стартовый промпт + пользовательский запрос
        finally:
            conn.close()

        # Проверяем вызовы upstream:
        # 1) стартовый промпт
        # 2) пользовательский текст (уже с /c/<id>)
        assert len(upstream.analyze_text_calls) >= 2
        assert upstream.analyze_text_calls[0]["text"] == start_prompt
        assert upstream.analyze_text_calls[1]["text"] == "hello"
        assert "/c/abc123" in (upstream.analyze_text_calls[1]["url"] or "")

    finally:
        settings.SQLITE_PATH = old_sqlite_path
        settings.CONFIG_PATH = old_config_path
        if hasattr(storage_mod, "_DB_INITIALIZED"):
            storage_mod._DB_INITIALIZED = False
