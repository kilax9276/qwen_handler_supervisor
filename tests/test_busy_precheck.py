import sqlite3
from typing import Any, Optional

import pytest
from fastapi.testclient import TestClient

from src.app.main import create_app
from src.app.settings import settings
from src.app import storage as storage_mod


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


@pytest.mark.parametrize(
    "status_payload",
    [
        {"status": "ok", "busy": True, "message": "занят"},
        {"status": "busy", "busy": False, "message": "занят"},
    ],
)
def test_solve_busy_precheck_returns_503_and_records_job(tmp_path, status_payload):
    sqlite_path = tmp_path / "test.sqlite"
    old_sqlite_path = settings.SQLITE_PATH
    old_config_path = settings.CONFIG_PATH

    try:
        settings.SQLITE_PATH = str(sqlite_path)
        settings.CONFIG_PATH = _write_min_config(tmp_path, container_id="camoufox-1")

        # Force re-init DB for the new path (если в storage есть такой флаг).
        if hasattr(storage_mod, "_DB_INITIALIZED"):
            storage_mod._DB_INITIALIZED = False  # type: ignore[attr-defined]

        app = create_app()

        class MockUpstream:
            async def status(self, *, request_id: Optional[str] = None) -> dict[str, Any]:
                return dict(status_payload)

            async def analyze_text(self, *args: Any, **kwargs: Any) -> Any:  # pragma: no cover
                raise AssertionError("analyze_text() не должен вызываться при busy pre-check")

            async def analyze_image_b64(self, *args: Any, **kwargs: Any) -> Any:  # pragma: no cover
                raise AssertionError("analyze_image_b64() не должен вызываться при busy pre-check")

            async def aclose(self) -> None:  # pragma: no cover
                return None

        with TestClient(app) as client:
            # Подменяем upstream клиента внутри pool на мок.
            app.state.pool._clients["camoufox-1"] = MockUpstream()  # type: ignore[attr-defined]

            r = client.post(
                "/v1/solve",
                json={
                    "input": {"text": "hi"},
                    "options": {"profile_id": "p1"},
                },
            )

            assert r.status_code == 503
            body = r.json()
            assert body["ok"] is False
            assert body["error"]["code"] == "CONTAINER_BUSY"

            job_id = body["meta"]["job_id"]
            assert isinstance(job_id, str) and job_id

        conn = sqlite3.connect(str(sqlite_path))
        try:
            row = conn.execute(
                "SELECT status, error_code, error_message FROM jobs WHERE job_id = ?",
                (job_id,),
            ).fetchone()
            assert row is not None
            assert row[0] == "failed"
            assert row[1] == "CONTAINER_BUSY"
            assert isinstance(row[2], str) and row[2]
        finally:
            conn.close()

    finally:
        settings.SQLITE_PATH = old_sqlite_path
        settings.CONFIG_PATH = old_config_path
