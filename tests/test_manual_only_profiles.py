#copyright "Kilax @kilax9276"
from __future__ import annotations

from typing import Any, Optional

from fastapi.testclient import TestClient

from src.app.main import create_app
from src.app.settings import settings
import src.app.storage as storage_mod


def _write_config(tmp_path) -> str:
    prompts_dir = tmp_path / "prompts"
    prompts_dir.mkdir(parents=True, exist_ok=True)
    (prompts_dir / "default.txt").write_text("SYSTEM PROMPT", encoding="utf-8")

    profile_auto = tmp_path / "profile-auto"
    profile_manual = tmp_path / "profile-manual"
    profile_auto.mkdir(parents=True, exist_ok=True)
    profile_manual.mkdir(parents=True, exist_ok=True)

    cfg_path = tmp_path / "config.yaml"
    cfg_path.write_text(
        f"""containers:
  - id: camoufox-1
    base_url: http://127.0.0.1:9999
    enabled: true
profiles:
  - profile_id: p-auto
    profile_value: {profile_auto.as_posix()}
    allowed_containers:
      - camoufox-1
  - profile_id: p-manual
    profile_value: {profile_manual.as_posix()}
    allowed_containers:
      - camoufox-1
    manual_only: true
prompts:
  - prompt_id: default
    file: prompts/default.txt
    default_max_chat_uses: 50
""",
        encoding="utf-8",
    )
    return str(cfg_path)


class MockUpstream:
    def __init__(self) -> None:
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
        return {"ok": True, "page_url": "https://chat.qwen.ai/c/test123", "text": "ok"}

    async def analyze_image_b64(self, *args: Any, **kwargs: Any) -> Any:  # pragma: no cover
        raise AssertionError("analyze_image_b64() not expected")

    async def aclose(self) -> None:  # pragma: no cover
        return None


def test_manual_only_profile_is_excluded_from_auto_selection_but_works_explicitly(tmp_path) -> None:
    sqlite_path = tmp_path / "test.sqlite"
    old_sqlite_path = settings.SQLITE_PATH
    old_config_path = settings.CONFIG_PATH

    try:
        settings.SQLITE_PATH = str(sqlite_path)
        settings.CONFIG_PATH = _write_config(tmp_path)

        if hasattr(storage_mod, "_DB_INITIALIZED"):
            storage_mod._DB_INITIALIZED = False

        app = create_app()
        upstream = MockUpstream()

        with TestClient(app) as client:
            app.state.pool._clients["camoufox-1"] = upstream

            r1 = client.post("/v1/solve", json={"input": {"text": "hello auto"}, "options": {}})
            assert r1.status_code == 200
            assert r1.json()["meta"]["profile_id"] == "p-auto"

            r2 = client.post(
                "/v1/solve",
                json={"input": {"text": "hello manual"}, "options": {"profile_id": "p-manual", "force_new_chat": True}},
            )
            assert r2.status_code == 200
            assert r2.json()["meta"]["profile_id"] == "p-manual"
    finally:
        settings.SQLITE_PATH = old_sqlite_path
        settings.CONFIG_PATH = old_config_path
        if hasattr(storage_mod, "_DB_INITIALIZED"):
            storage_mod._DB_INITIALIZED = False
