#copyright "Kilax @kilax9276"
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse

from .schemas import (
    ChatLockRequest,
    ChatUnlockRequest,
    ChatRestRequest,
    ChatMarkerClearRequest,
    ChatLoggedOutRequest,
    SolveRequest,
    SolveResponse,
    StatusResponse,
)
from .settings import settings
from .storage import get_default_storage
from .status_service import build_status_all
from .version import __version__

logger = logging.getLogger("orchestrator")


def _setup_orchestrator_logging() -> None:
    level_name = (os.getenv("ORCH_LOG_LEVEL") or os.getenv("LOG_LEVEL") or "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    logger.setLevel(level)
    if not logger.handlers:
        h = logging.StreamHandler()
        h.setLevel(level)
        h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
        logger.addHandler(h)
    logger.propagate = False


def _json_log(level: int, payload: dict[str, Any]) -> None:
    try:
        logger.log(level, json.dumps(payload, ensure_ascii=False))
    except Exception:
        logger.log(level, str(payload))


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _config_signature(path: Optional[str]) -> Optional[tuple[int, int]]:
    p = (path or "").strip()
    if not p:
        return None
    st = os.stat(p)
    return int(st.st_mtime_ns), int(st.st_size)


def _runtime_meta(app: FastAPI) -> dict[str, Any]:
    cfg = getattr(app.state, "cfg", None)
    pool = getattr(app.state, "pool", None)

    enabled: list[str] = []
    all_containers: list[dict[str, Any]] = []
    if cfg is not None:
        try:
            all_containers = [
                {
                    "id": str(getattr(c, "id", "") or "").strip(),
                    "base_url": str(getattr(c, "base_url", "") or "").strip(),
                    "enabled": bool(getattr(c, "enabled", True)),
                }
                for c in (cfg.containers or [])
            ]
        except Exception:
            all_containers = []

    if pool is not None:
        try:
            enabled = list(pool.list_enabled())
        except Exception:
            enabled = []

    return {
        "config_path": settings.CONFIG_PATH,
        "config_signature": getattr(app.state, "_config_signature", None),
        "last_loaded_at": getattr(app.state, "_config_last_loaded_at", None),
        "reload_count": int(getattr(app.state, "_config_reload_count", 0) or 0),
        "last_reload_reason": getattr(app.state, "_config_last_reload_reason", None),
        "last_reload_error": getattr(app.state, "_config_last_reload_error", None),
        "last_sync": getattr(app.state, "_config_last_sync", None),
        "containers": all_containers,
        "enabled_container_ids": enabled,
        "profile_ids": [str(getattr(p, "profile_id", "") or "") for p in getattr(cfg, "profiles", [])] if cfg is not None else [],
        "prompt_ids": [str(getattr(p, "prompt_id", "") or "") for p in getattr(cfg, "prompts", [])] if cfg is not None else [],
        "chat_policy": {
            "promptless_idle_seconds": int(getattr(getattr(cfg, "chat_policy", None), "promptless_idle_seconds", 0) or 0) if cfg is not None else 0,
            "default_rest_ttl_seconds": int(getattr(getattr(cfg, "chat_policy", None), "default_rest_ttl_seconds", 0) or 0) if cfg is not None else 0,
            "default_max_chat_uses": int(getattr(getattr(cfg, "chat_policy", None), "default_max_chat_uses", 0) or 0) if cfg is not None else 0,
        },
    }


def _build_runtime_bundle(app: FastAPI) -> dict[str, Any]:
    if not settings.CONFIG_PATH:
        raise RuntimeError("CONFIG_PATH is required")

    from .config_loader import load_config
    from .containers.pool import UpstreamClientPool
    from .containers.selector import ContainerSelector
    from .engine.executor import MultiContainerExecutor
    from .io_logging import ContainerIOLLogger
    from .profiles.manager import ProfileManager
    from .prompts.registry import PromptRegistry

    cfg = load_config(settings.CONFIG_PATH)
    st = app.state.storage
    profile_lock = app.state.profile_lock

    io_logger = ContainerIOLLogger.from_sources(yaml_config=cfg.container_io_log)
    pool = UpstreamClientPool(cfg.containers, io_logger=io_logger)
    selector = ContainerSelector(pool=pool, storage=st)
    prompts = PromptRegistry(app_config=cfg, config_path=settings.CONFIG_PATH)
    profiles = ProfileManager(storage=st, config=cfg)
    sync_stats = profiles.seed_from_config(prune_missing=True)

    executor = MultiContainerExecutor(
        storage=st,
        pool=pool,
        selector=selector,
        profiles=profiles,
        prompts=prompts,
        profile_lock=profile_lock,
        allow_socks_override=bool(cfg.allow_socks_override),
        io_logger=io_logger,
    )

    return {
        "cfg": cfg,
        "pool": pool,
        "selector": selector,
        "prompts": prompts,
        "profiles": profiles,
        "executor": executor,
        "io_logger": io_logger,
        "sync_stats": sync_stats,
    }


async def _install_runtime_bundle(app: FastAPI, bundle: dict[str, Any], *, signature: Optional[tuple[int, int]], reason: str) -> None:
    old_pool = getattr(app.state, "pool", None)

    cfg = bundle["cfg"]
    app.state.cfg = cfg
    app.state.pool = bundle["pool"]
    app.state.selector = bundle["selector"]
    app.state.profiles = bundle["profiles"]
    app.state.prompts = bundle["prompts"]
    app.state.executor = bundle["executor"]
    app.state.io_logger = bundle["io_logger"]
    app.state._config_signature = signature
    app.state._config_last_loaded_at = _utc_now_iso()
    app.state._config_last_reload_reason = reason
    app.state._config_last_reload_error = None
    app.state._config_last_sync = bundle.get("sync_stats") or {}
    app.state._config_reload_count = int(getattr(app.state, "_config_reload_count", 0) or 0) + 1

    _json_log(
        logging.INFO,
        {
            "event": "config_runtime_loaded",
            "reason": reason,
            "config_signature": signature,
            "enabled_container_ids": list(app.state.pool.list_enabled()),
            "profiles": [str(getattr(p, "profile_id", "") or "") for p in cfg.profiles],
            "prompts": [str(getattr(p, "prompt_id", "") or "") for p in cfg.prompts],
            "sync": app.state._config_last_sync,
            "reload_count": app.state._config_reload_count,
            "container_io_log": {
                "enabled": bool(getattr(cfg.container_io_log, "enabled", False)),
                "dir": getattr(cfg.container_io_log, "dir", None),
                "include_bodies": getattr(cfg.container_io_log, "include_bodies", None),
                "redact_secrets": getattr(cfg.container_io_log, "redact_secrets", None),
                "max_bytes": getattr(cfg.container_io_log, "max_bytes", None),
                "backup_count": getattr(cfg.container_io_log, "backup_count", None),
                "max_field_chars": getattr(cfg.container_io_log, "max_field_chars", None),
                "level": getattr(cfg.container_io_log, "level", None),
            },
        },
    )

    if old_pool is not None and old_pool is not app.state.pool:
        try:
            await old_pool.aclose()
        except Exception:
            pass


async def _reload_runtime_if_needed(app: FastAPI, *, force: bool = False, reason: str = "mtime_changed") -> bool:
    path = settings.CONFIG_PATH
    if not path:
        raise RuntimeError("CONFIG_PATH is required")

    signature = _config_signature(path)
    current_signature = getattr(app.state, "_config_signature", None)
    if not force and signature == current_signature:
        return False

    async with app.state._config_reload_lock:
        signature = _config_signature(path)
        current_signature = getattr(app.state, "_config_signature", None)
        if not force and signature == current_signature:
            return False

        bundle = _build_runtime_bundle(app)
        await _install_runtime_bundle(app, bundle, signature=signature, reason=reason)
        return True


async def _try_auto_reload_runtime(app: FastAPI) -> None:
    try:
        await _reload_runtime_if_needed(app, force=False, reason="mtime_changed")
    except Exception as e:
        app.state._config_last_reload_error = str(e)
        _json_log(
            logging.ERROR,
            {
                "event": "config_runtime_reload_failed",
                "reason": "mtime_changed",
                "error": str(e),
                "config_path": settings.CONFIG_PATH,
            },
        )
        logger.exception("config_runtime_reload_failed")


@asynccontextmanager
async def lifespan(app: FastAPI):
    _setup_orchestrator_logging()

    if not settings.CONFIG_PATH:
        raise RuntimeError("CONFIG_PATH is required")

    from .profiles.profile_lock import ProfileLock
    from .reports.router import router as reports_router

    app.state.storage = get_default_storage()
    app.state.profile_lock = ProfileLock()
    app.state._config_reload_lock = asyncio.Lock()
    app.state._config_signature = None
    app.state._config_last_loaded_at = None
    app.state._config_reload_count = 0
    app.state._config_last_reload_reason = None
    app.state._config_last_reload_error = None
    app.state._config_last_sync = None

    await _reload_runtime_if_needed(app, force=True, reason="startup")

    app.include_router(reports_router)

    yield

    pool = getattr(app.state, "pool", None)
    if pool is not None:
        try:
            await pool.aclose()
        except Exception:
            pass


def create_app() -> FastAPI:
    app = FastAPI(lifespan=lifespan)

    @app.middleware("http")
    async def config_reload_middleware(request: Request, call_next):  # type: ignore[override]
        await _try_auto_reload_runtime(request.app)
        return await call_next(request)

    @app.get("/health")
    async def health() -> dict[str, Any]:
        return {
            "ok": True,
            "version": __version__,
            "config": {"last_loaded_at": getattr(app.state, "_config_last_loaded_at", None)},
        }

    @app.get("/v1/version")
    async def v1_version() -> dict[str, Any]:
        return {"ok": True, "version": __version__}

    @app.get("/v1/config/state")
    async def v1_config_state() -> dict[str, Any]:
        return {"ok": True, "version": __version__, "config": _runtime_meta(app)}

    @app.post("/v1/config/reload")
    async def v1_config_reload() -> JSONResponse:
        try:
            reloaded = await _reload_runtime_if_needed(app, force=True, reason="manual")
        except Exception as e:
            app.state._config_last_reload_error = str(e)
            return JSONResponse(status_code=500, content={"ok": False, "error": {"message": str(e)}, "config": _runtime_meta(app)})
        return JSONResponse(status_code=200, content={"ok": True, "reloaded": bool(reloaded), "config": _runtime_meta(app)})

    @app.get("/v1/status")
    async def v1_status(container_id: Optional[str] = Query(default=None)) -> StatusResponse:
        pool = app.state.pool
        if container_id:
            st = await pool.get(container_id).status()
            return StatusResponse(ok=True, status={"container_id": container_id, "status": st})
        enabled = pool.list_enabled()
        if not enabled:
            return StatusResponse(ok=False, status={"error": "no enabled containers"})
        cid = enabled[0]
        st = await pool.get(cid).status()
        return StatusResponse(ok=True, status={"container_id": cid, "status": st})

    @app.get("/v1/status/all")
    async def v1_status_all() -> StatusResponse:
        payload = await build_status_all(storage=app.state.storage, pool=app.state.pool)
        return StatusResponse(ok=True, status=payload)

    async def _do_lock(req: ChatLockRequest) -> dict[str, Any]:
        st = app.state.storage
        sess = st.lock_chat_by_url(page_url=req.chat_url, locked_by=req.locked_by, ttl_seconds=req.ttl_seconds)
        return {"ok": True, "chat_session": (sess.__dict__ if sess else None)}

    async def _do_unlock(req: ChatUnlockRequest) -> dict[str, Any]:
        st = app.state.storage
        ok = st.unlock_chat_by_url(page_url=req.chat_url, locked_by=req.locked_by)
        return {"ok": bool(ok)}

    @app.post("/v1/chat/lock")
    async def v1_chat_lock(req: ChatLockRequest) -> dict[str, Any]:
        return await _do_lock(req)

    @app.post("/v1/chat/unlock")
    async def v1_chat_unlock(req: ChatUnlockRequest) -> dict[str, Any]:
        return await _do_unlock(req)

    @app.post("/v1/chats/lock")
    async def v1_chats_lock(req: ChatLockRequest) -> dict[str, Any]:
        return await _do_lock(req)

    @app.post("/v1/chats/unlock")
    async def v1_chats_unlock(req: ChatUnlockRequest) -> dict[str, Any]:
        return await _do_unlock(req)

    @app.post("/v1/chat/rest")
    async def v1_chat_rest(req: ChatRestRequest) -> dict[str, Any]:
        ttl = int(req.ttl_seconds or getattr(getattr(app.state.cfg, "chat_policy", None), "default_rest_ttl_seconds", 900) or 900)
        sess = app.state.storage.mark_chat_rest_by_url(page_url=req.chat_url, ttl_seconds=ttl)
        return {"ok": bool(sess), "chat_session": (sess.__dict__ if sess else None), "ttl_seconds": ttl}

    @app.post("/v1/chats/rest")
    async def v1_chats_rest(req: ChatRestRequest) -> dict[str, Any]:
        ttl = int(req.ttl_seconds or getattr(getattr(app.state.cfg, "chat_policy", None), "default_rest_ttl_seconds", 900) or 900)
        sess = app.state.storage.mark_chat_rest_by_url(page_url=req.chat_url, ttl_seconds=ttl)
        return {"ok": bool(sess), "chat_session": (sess.__dict__ if sess else None), "ttl_seconds": ttl}

    @app.post("/v1/chat/rest/clear")
    async def v1_chat_rest_clear(req: ChatMarkerClearRequest) -> dict[str, Any]:
        ok = app.state.storage.clear_chat_rest_by_url(page_url=req.chat_url)
        return {"ok": bool(ok)}

    @app.post("/v1/chats/rest/clear")
    async def v1_chats_rest_clear(req: ChatMarkerClearRequest) -> dict[str, Any]:
        ok = app.state.storage.clear_chat_rest_by_url(page_url=req.chat_url)
        return {"ok": bool(ok)}

    @app.post("/v1/chat/logged-out")
    async def v1_chat_logged_out(req: ChatLoggedOutRequest) -> dict[str, Any]:
        sess = app.state.storage.set_chat_logged_out_by_url(page_url=req.chat_url, logged_out=bool(req.logged_out))
        return {"ok": bool(sess), "chat_session": (sess.__dict__ if sess else None)}

    @app.post("/v1/chats/logged-out")
    async def v1_chats_logged_out(req: ChatLoggedOutRequest) -> dict[str, Any]:
        sess = app.state.storage.set_chat_logged_out_by_url(page_url=req.chat_url, logged_out=bool(req.logged_out))
        return {"ok": bool(sess), "chat_session": (sess.__dict__ if sess else None)}

    @app.post("/v1/chat/logged-out/clear")
    async def v1_chat_logged_out_clear(req: ChatMarkerClearRequest) -> dict[str, Any]:
        sess = app.state.storage.set_chat_logged_out_by_url(page_url=req.chat_url, logged_out=False)
        return {"ok": bool(sess), "chat_session": (sess.__dict__ if sess else None)}

    @app.post("/v1/chats/logged-out/clear")
    async def v1_chats_logged_out_clear(req: ChatMarkerClearRequest) -> dict[str, Any]:
        sess = app.state.storage.set_chat_logged_out_by_url(page_url=req.chat_url, logged_out=False)
        return {"ok": bool(sess), "chat_session": (sess.__dict__ if sess else None)}

    @app.get("/v1/profiles/blocked")
    async def v1_profiles_blocked() -> dict[str, Any]:
        items = app.state.storage.list_blocked_profiles()
        return {"ok": True, "items": items, "meta": {"count": len(items)}}

    @app.post("/v1/profiles/{profile_id}/guest/clear")
    async def v1_profile_guest_clear(profile_id: str) -> dict[str, Any]:
        deleted = app.state.storage.delete_guest_chats_for_profile(profile_id)
        return {"ok": True, "profile_id": profile_id, "deleted": int(deleted)}

    @app.post("/v1/profiles/{profile_id}/chats/archive")
    async def v1_profile_chats_archive(profile_id: str) -> dict[str, Any]:
        archived = app.state.storage.archive_chats_for_profile(profile_id)
        return {"ok": True, "profile_id": profile_id, "archived": int(archived)}

    @app.post("/v1/solve")
    async def v1_solve(req: SolveRequest) -> JSONResponse:
        started_monotonic = time.monotonic()
        request_id = req.request_id or str(uuid.uuid4())

        _json_log(
            logging.INFO,
            {
                "event": "solve_start",
                "request_id": request_id,
                "profile_id": (req.options.profile_id if req.options else None),
                "config_signature": getattr(app.state, "_config_signature", None),
            },
        )

        executor = app.state.executor

        try:
            status_code, resp = await executor.execute(req, request_id=request_id)
        except Exception as e:
            duration_ms = int((time.monotonic() - started_monotonic) * 1000)
            _json_log(
                logging.ERROR,
                {
                    "event": "solve_crash",
                    "request_id": request_id,
                    "duration_ms": duration_ms,
                    "error": str(e),
                },
            )
            logger.exception("solve_crash traceback request_id=%s", request_id)

            payload = SolveResponse(
                ok=False,
                final=None,
                attempts=[],
                meta={
                    "job_id": "",
                    "request_id": request_id,
                    "prompt_id_selected": "default",
                    "container_ids_used": [],
                    "profile_id": (req.options.profile_id if req.options else None),
                    "socks_id": (req.options.socks_override if req.options else None),
                    "chat_ids_used": [],
                    "started_at": "",
                    "finished_at": "",
                },
                error={
                    "code": "INTERNAL_ERROR",
                    "message": "Внутренняя ошибка оркестратора.",
                    "details": {"error": str(e)},
                },
            ).model_dump()
            return JSONResponse(status_code=500, content=payload)

        duration_ms = int((time.monotonic() - started_monotonic) * 1000)
        _json_log(
            logging.INFO,
            {
                "event": "solve_done",
                "request_id": request_id,
                "duration_ms": duration_ms,
                "status": "succeeded" if resp.ok else "failed",
                "error_code": (resp.error.code if resp.error else None),
            },
        )

        return JSONResponse(status_code=status_code, content=resp.model_dump())

    return app


app = create_app()
