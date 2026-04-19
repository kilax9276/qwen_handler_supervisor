#copyright "Kilax @kilax9276"
from __future__ import annotations

from fastapi.testclient import TestClient

from src.app import __version__
from src.app.main import create_app
from src.app.settings import settings
import src.app.storage as storage_mod


def test_version_exposed_via_api_and_health(tmp_path) -> None:
    sqlite_path = tmp_path / "test.sqlite"
    cfg_path = tmp_path / "config.yaml"
    prompts_dir = tmp_path / "prompts"
    prompts_dir.mkdir(parents=True, exist_ok=True)
    (prompts_dir / "default.txt").write_text("SYSTEM PROMPT", encoding="utf-8")
    profile_dir = tmp_path / "profile-001"
    profile_dir.mkdir(parents=True, exist_ok=True)

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
""",
        encoding="utf-8",
    )

    old_sqlite_path = settings.SQLITE_PATH
    old_config_path = settings.CONFIG_PATH

    try:
        settings.SQLITE_PATH = str(sqlite_path)
        settings.CONFIG_PATH = str(cfg_path)

        if hasattr(storage_mod, "_DB_INITIALIZED"):
            storage_mod._DB_INITIALIZED = False  # type: ignore[attr-defined]
        if hasattr(storage_mod, "_DEFAULT_STORAGE"):
            storage_mod._DEFAULT_STORAGE = None  # type: ignore[attr-defined]

        app = create_app()
        with TestClient(app) as client:
            r = client.get("/v1/version")
            assert r.status_code == 200
            assert r.json() == {"ok": True, "version": __version__}

            health = client.get("/health")
            assert health.status_code == 200
            assert health.json()["version"] == __version__

            state = client.get("/v1/config/state")
            assert state.status_code == 200
            assert state.json()["version"] == __version__
    finally:
        settings.SQLITE_PATH = old_sqlite_path
        settings.CONFIG_PATH = old_config_path
        if hasattr(storage_mod, "_DB_INITIALIZED"):
            storage_mod._DB_INITIALIZED = False  # type: ignore[attr-defined]
        if hasattr(storage_mod, "_DEFAULT_STORAGE"):
            storage_mod._DEFAULT_STORAGE = None  # type: ignore[attr-defined]
