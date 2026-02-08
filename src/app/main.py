#copyright "Kilax @kilax9276"
from __future__ import annotations

import json
import logging
import os
import time
import uuid
from contextlib import asynccontextmanager
from typing import Any, Optional

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse

from .engine.executor import MultiContainerExecutor
from .io_logging import ContainerIOLLogger
from .schemas import (
    ChatLockRequest,
    ChatUnlockRequest,
    SolveRequest,
    SolveResponse,
    StatusResponse,
)
from .settings import settings
from .storage import get_default_storage
from .status_service import build_status_all

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


@asynccontextmanager
async def lifespan(app: FastAPI):
    _setup_orchestrator_logging()

    if not settings.CONFIG_PATH:
        # MVP single-container удалён намеренно — см. README (обновить).
        raise RuntimeError("CONFIG_PATH is required")

    from .config_loader import load_config
    from .containers.pool import UpstreamClientPool
    from .containers.selector import ContainerSelector
    from .profiles.manager import ProfileManager
    from .profiles.profile_lock import ProfileLock
    from .prompts.registry import PromptRegistry
    from .reports.router import router as reports_router

    cfg = load_config(settings.CONFIG_PATH)
    st = get_default_storage()

    # ВАЖНО: создаём io_logger и прокидываем в пул, чтобы UpstreamClient писал JSONL-логи
    io_logger = ContainerIOLLogger.from_sources(yaml_config=cfg.container_io_log)

    pool = UpstreamClientPool(cfg.containers, io_logger=io_logger)
    selector = ContainerSelector(pool=pool, storage=st)
    prompts = PromptRegistry(app_config=cfg, config_path=settings.CONFIG_PATH)
    profile_lock = ProfileLock()
    profiles = ProfileManager(storage=st, config=cfg)
    profiles.seed_from_config()

    _json_log(
        logging.INFO,
        {
            "event": "container_io_log_config",
            "enabled": bool(getattr(cfg.container_io_log, "enabled", False)),
            "dir": getattr(cfg.container_io_log, "dir", None),
            "include_bodies": getattr(cfg.container_io_log, "include_bodies", None),
            "redact_secrets": getattr(cfg.container_io_log, "redact_secrets", None),
            "max_bytes": getattr(cfg.container_io_log, "max_bytes", None),
            "backup_count": getattr(cfg.container_io_log, "backup_count", None),
            "max_field_chars": getattr(cfg.container_io_log, "max_field_chars", None),
            "level": getattr(cfg.container_io_log, "level", None),
        },
    )

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

    app.state.cfg = cfg
    app.state.storage = st
    app.state.pool = pool
    app.state.selector = selector
    app.state.profiles = profiles
    app.state.prompts = prompts
    app.state.profile_lock = profile_lock
    app.state.executor = executor
    app.state.io_logger = io_logger

    app.include_router(reports_router)

    yield

    try:
        await pool.aclose()
    except Exception:
        pass


def create_app() -> FastAPI:
    app = FastAPI(lifespan=lifespan)

    @app.get("/health")
    async def health() -> dict[str, Any]:
        return {"ok": True}

    @app.get("/v1/status")
    async def v1_status(container_id: Optional[str] = Query(default=None)) -> StatusResponse:
        pool = app.state.pool
        if container_id:
            st = await pool.get(container_id).status()
            return StatusResponse(ok=True, status={"container_id": container_id, "status": st})
        # если не указан container_id — вернём статус первого enabled
        enabled = pool.list_enabled()
        if not enabled:
            return StatusResponse(ok=False, status={"error": "no enabled containers"})
        cid = enabled[0]
        st = await pool.get(cid).status()
        return StatusResponse(ok=True, status={"container_id": cid, "status": st})

    @app.get("/v1/status/all")
    async def v1_status_all() -> StatusResponse:
        st = get_default_storage()
        pool = app.state.pool
        payload = await build_status_all(storage=st, pool=pool)
        return StatusResponse(ok=True, status=payload)

    # --- chat lock (новые/старые пути) ---

    async def _do_lock(req: ChatLockRequest) -> dict[str, Any]:
        st = get_default_storage()
        sess = st.lock_chat_by_url(page_url=req.chat_url, locked_by=req.locked_by, ttl_seconds=req.ttl_seconds)
        return {"ok": True, "chat_session": (sess.__dict__ if sess else None)}

    async def _do_unlock(req: ChatUnlockRequest) -> dict[str, Any]:
        st = get_default_storage()
        ok = st.unlock_chat_by_url(page_url=req.chat_url, locked_by=req.locked_by)
        return {"ok": bool(ok)}

    @app.post("/v1/chat/lock")
    async def v1_chat_lock(req: ChatLockRequest) -> dict[str, Any]:
        return await _do_lock(req)

    @app.post("/v1/chat/unlock")
    async def v1_chat_unlock(req: ChatUnlockRequest) -> dict[str, Any]:
        return await _do_unlock(req)

    # алиасы под README
    @app.post("/v1/chats/lock")
    async def v1_chats_lock(req: ChatLockRequest) -> dict[str, Any]:
        return await _do_lock(req)

    @app.post("/v1/chats/unlock")
    async def v1_chats_unlock(req: ChatUnlockRequest) -> dict[str, Any]:
        return await _do_unlock(req)

    # --- guest/archive управление через API ---

    @app.get("/v1/profiles/blocked")
    async def v1_profiles_blocked() -> dict[str, Any]:
        st = get_default_storage()
        items = st.list_blocked_profiles()
        return {"ok": True, "items": items, "meta": {"count": len(items)}}

    @app.post("/v1/profiles/{profile_id}/guest/clear")
    async def v1_profile_guest_clear(profile_id: str) -> dict[str, Any]:
        st = get_default_storage()
        deleted = st.delete_guest_chats_for_profile(profile_id)
        return {"ok": True, "profile_id": profile_id, "deleted": int(deleted)}

    @app.post("/v1/profiles/{profile_id}/chats/archive")
    async def v1_profile_chats_archive(profile_id: str) -> dict[str, Any]:
        st = get_default_storage()
        archived = st.archive_chats_for_profile(profile_id)
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
