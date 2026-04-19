#copyright "Kilax @kilax9276"
from __future__ import annotations

import os
import time

from fastapi.testclient import TestClient

from src.app.main import create_app
from src.app.settings import settings
import src.app.storage as storage_mod


def _write_config(cfg_path, *, container_id: str) -> None:
    tmp_path = cfg_path.parent
    prompts_dir = tmp_path / "prompts"
    prompts_dir.mkdir(parents=True, exist_ok=True)
    (prompts_dir / "default.txt").write_text("SYSTEM PROMPT", encoding="utf-8")

    profile_dir = tmp_path / f"profile-{container_id}"
    profile_dir.mkdir(parents=True, exist_ok=True)

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
    os.utime(cfg_path, None)


def test_config_hot_reload_updates_runtime_and_profile_mappings(tmp_path) -> None:
    sqlite_path = tmp_path / "test.sqlite"
    cfg_path = tmp_path / "config.yaml"

    old_sqlite_path = settings.SQLITE_PATH
    old_config_path = settings.CONFIG_PATH

    try:
        settings.SQLITE_PATH = str(sqlite_path)
        settings.CONFIG_PATH = str(cfg_path)
        _write_config(cfg_path, container_id="camoufox-1")

        if hasattr(storage_mod, "_DB_INITIALIZED"):
            storage_mod._DB_INITIALIZED = False  # type: ignore[attr-defined]
        if hasattr(storage_mod, "_DEFAULT_STORAGE"):
            storage_mod._DEFAULT_STORAGE = None  # type: ignore[attr-defined]

        app = create_app()

        with TestClient(app) as client:
            assert app.state.pool.list_enabled() == ["camoufox-1"]
            row = app.state.storage.get_profile("p1")
            assert row is not None
            assert row.allowed_containers == ["camoufox-1"]

            time.sleep(0.02)
            _write_config(cfg_path, container_id="camoufox-2")

            r = client.get("/health")
            assert r.status_code == 200

            assert app.state.pool.list_enabled() == ["camoufox-2"]
            row = app.state.storage.get_profile("p1")
            assert row is not None
            assert row.allowed_containers == ["camoufox-2"]

            state = client.get("/v1/config/state")
            assert state.status_code == 200
            body = state.json()
            assert body["ok"] is True
            assert body["config"]["enabled_container_ids"] == ["camoufox-2"]
            assert body["config"]["reload_count"] >= 2
            assert body["config"]["last_reload_error"] is None
    finally:
        settings.SQLITE_PATH = old_sqlite_path
        settings.CONFIG_PATH = old_config_path
        if hasattr(storage_mod, "_DB_INITIALIZED"):
            storage_mod._DB_INITIALIZED = False  # type: ignore[attr-defined]
        if hasattr(storage_mod, "_DEFAULT_STORAGE"):
            storage_mod._DEFAULT_STORAGE = None  # type: ignore[attr-defined]


def test_invalid_reloaded_config_keeps_last_good_runtime(tmp_path) -> None:
    sqlite_path = tmp_path / "test.sqlite"
    cfg_path = tmp_path / "config.yaml"

    old_sqlite_path = settings.SQLITE_PATH
    old_config_path = settings.CONFIG_PATH

    try:
        settings.SQLITE_PATH = str(sqlite_path)
        settings.CONFIG_PATH = str(cfg_path)
        _write_config(cfg_path, container_id="camoufox-1")

        if hasattr(storage_mod, "_DB_INITIALIZED"):
            storage_mod._DB_INITIALIZED = False  # type: ignore[attr-defined]
        if hasattr(storage_mod, "_DEFAULT_STORAGE"):
            storage_mod._DEFAULT_STORAGE = None  # type: ignore[attr-defined]

        app = create_app()

        with TestClient(app) as client:
            assert app.state.pool.list_enabled() == ["camoufox-1"]

            time.sleep(0.02)
            cfg_path.write_text("containers: [", encoding="utf-8")
            os.utime(cfg_path, None)

            r = client.get("/health")
            assert r.status_code == 200
            assert app.state.pool.list_enabled() == ["camoufox-1"]

            state = client.get("/v1/config/state")
            assert state.status_code == 200
            assert state.json()["config"]["last_reload_error"]

            force = client.post("/v1/config/reload")
            assert force.status_code == 500
            assert force.json()["ok"] is False
            assert app.state.pool.list_enabled() == ["camoufox-1"]
    finally:
        settings.SQLITE_PATH = old_sqlite_path
        settings.CONFIG_PATH = old_config_path
        if hasattr(storage_mod, "_DB_INITIALIZED"):
            storage_mod._DB_INITIALIZED = False  # type: ignore[attr-defined]
        if hasattr(storage_mod, "_DEFAULT_STORAGE"):
            storage_mod._DEFAULT_STORAGE = None  # type: ignore[attr-defined]
