#copyright "Kilax @kilax9276"
from __future__ import annotations

import asyncio
import json
import logging
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

from ..chats.manager import ChatManager
from ..containers.selector import ContainerSelector, NotEnoughContainersError
from ..profiles.manager import ProfileManager
from ..profiles.profile_lock import ProfileBusyError, ProfileLock
from ..prompts.registry import PromptRegistry
from ..schemas import SolveAttempt, SolveError, SolveFinal, SolveRequest, SolveResponse
from ..storage import Storage
from ..upstream_client import (
    UpstreamBadRequestError,
    UpstreamBusyError,
    UpstreamServerError,
    UpstreamTransportError,
    normalize_socks_for_compare,
)

logger = logging.getLogger("orchestrator")


def _jlog(level: int, payload: dict[str, Any]) -> None:
    """
    JSON-лог (best-effort).
    - Нужен для удобного чтения логов в проде (структурированные поля).
    - Никогда не должен ронять процесс даже если payload содержит несериализуемые объекты.
    """
    try:
        logger.log(level, json.dumps(payload, ensure_ascii=False))
    except Exception:
        logger.log(level, str(payload))


def _redact_proxy_url(url: Optional[str]) -> Optional[str]:
    """
    Маскировка пароля в socks URL для логов/ответов:
    socks5://user:pass@host:port -> socks5://user:***@host:port

    ВАЖНО:
    - Это используется только для логов/ответов.
    - В upstream мы отправляем реальный socks_url (без редактирования).
    """
    if not url:
        return url
    try:
        return re.sub(r"://([^:@/]+):([^@/]+)@", r"://\1:***@", str(url))
    except Exception:
        return "<redacted>"


def _iso_now() -> str:
    """Текущее UTC время в ISO-формате (для meta.started_at/finished_at)."""
    return datetime.now(timezone.utc).isoformat()


_JSON_LIKE_RE = re.compile(r"^\s*[\{\[]")


def normalize_text(text: str) -> str:
    """
    Нормализация текста для сравнения/голосования/логов:
    - схлопываем пробелы
    - если строка выглядит как JSON -> пытаемся распарсить и сериализовать стабильно

    Это НЕ влияет на то, что уходит в upstream (там используется исходный `text`),
    но может быть полезно в механизмах дедупликации/сравнения ответов.
    """
    s = (text or "").strip()
    s = re.sub(r"\s+", " ", s)
    if not s:
        return s
    if _JSON_LIKE_RE.match(s):
        try:
            obj = json.loads(s)
            return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        except Exception:
            return s
    return s


def _pick_text_from_raw(raw: Any) -> str:
    """
    Унифицированное извлечение "человекочитаемого ответа" из сырого upstream ответа.
    Upstream может возвращать {"answer": "..."} или {"text": "..."} и т.п.

    ВАЖНО:
    - Здесь нет логики по chat_url/page_url. Это только "достать текст".
    - URL чата хранится в `chat_session.page_url` (БД) и прокидывается отдельно.
    """
    if isinstance(raw, dict):
        # основные варианты ответа
        for k in ("text", "answer", "message", "result"):
            v = raw.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
        # иногда upstream возвращает URL (в т.ч. page_url)
        for k in ("url", "page_url"):
            v = raw.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
    if isinstance(raw, str):
        return raw.strip() or raw
    try:
        return json.dumps(raw, ensure_ascii=False)
    except Exception:
        return str(raw)


_BLOCKED_CHAT_IDS = {"guest", "archive"}


def _is_blocked_chat(chat_id: Optional[str], tag: Optional[str]) -> bool:
    """True если чат помечен как guest/archive (по chat_id или tag).

    Используется как защита на уровне executor:
      - pinned chat_url не должен указывать на guest/archive
      - если upstream создал /c/guest, мы должны быстро исключить профиль
    """
    cid = (chat_id or "").strip().lower()
    t = (tag or "").strip().lower()
    if cid in _BLOCKED_CHAT_IDS:
        return True
    if t in _BLOCKED_CHAT_IDS:
        return True
    return False


# =====================================================================
# Multi-container executor (priority A)
# =====================================================================

@dataclass(frozen=True)
class _ProfileCandidate:
    """
    Кандидат на выполнение запроса.

    В терминах "как выбирается выполнение":
      - profile_id: логический ID профиля оркестратора (например "p1")
      - socks_override: принудительный socks (если разрешено), иначе None
      - preferred_container_id: если известен "предпочтительный" контейнер (чаще всего из chat_url),
        то пытаемся выполнить там, чтобы попасть в тот же уже открытый чат/контекст.
      - preferred_chat_id: если известен chat_id из БД, используем как подсказку для ChatManager.
    """
    profile_id: str
    socks_override: Optional[str]
    preferred_container_id: Optional[str]
    preferred_chat_id: Optional[str]


class MultiContainerExecutor:
    """
    Главный исполнитель для multi-container режима.

    Слои ответственности (важно понимать, чтобы "откуда берётся URL"):

    1) ProfileManager
       - resolve_for_request(profile_id, socks_override, ...)
       - выдаёт:
         * profile_value: путь к профилю браузера (директория)
         * socks_id / socks_url: прокси
         * allowed_containers: какие контейнеры можно использовать

    2) ProfileLock
       - гарантирует, что один и тот же profile_value не используется параллельно двумя запросами.
       - если lock не удалось взять -> ProfileBusyError.

    3) ContainerSelector
       - select_containers(...) выбирает контейнер из доступных,
         учитывая allowed_containers и возможные подсказки.

    4) Pool (self._pool)
       - даёт upstream client по container_id (self._pool.get(container_id))

    5) ChatManager (КЛЮЧЕВО ДЛЯ URL)
       - get_or_create_chat(...):
         * решает reuse чата или создание нового
         * возвращает chat_session (запись в БД) с полями:
             - chat_id
             - page_url  (URL "куда слать" запрос на анализ)
             - uses_count и т.д.
       - ensure_chat_loaded(...):
         * гарантирует, что браузер реально находится в нужном чате
         * при создании нового чата обычно отправляет стартовый промпт
         * может обновить page_url (например с base URL на /c/<id>)

    ВАЖНО про URL и `force_new_chat`:
      - Единственный "планируемый URL" для analyze_* — это `chat_session.page_url`.
      - При `force_new_chat=False` ChatManager пытается reuse существующую chat_session
        (если uses_count < max_chat_uses и чат не disabled/locked и подходит по параметрам).
      - Если reuse невозможен (лимит, инвалидирован, несовпадение, force_new=True) —
        создаётся новая chat_session и её page_url должен быть сохранён.
    """

    def __init__(
        self,
        *,
        storage: Storage,
        selector: Optional[ContainerSelector] = None,
        # --- legacy/backwards-compatible аргументы wiring ---
        pool: Any = None,
        profiles: Optional[ProfileManager] = None,
        prompts: Optional[PromptRegistry] = None,
        profile_manager: Optional[ProfileManager] = None,
        prompt_registry: Optional[PromptRegistry] = None,
        profile_lock: Optional[ProfileLock] = None,
        allow_socks_override: Optional[bool] = None,
        io_logger: Any = None,
    ) -> None:
        """
        Создаёт executor.

        Исторически `main.py` мог собирать executor разными способами.
        Поэтому поддерживаем 2 схемы имен:
          - profiles / profile_manager
          - prompts / prompt_registry

        selector обязателен:
          - он знает, как выбирать контейнеры
          - и в большинстве реализаций хранит ссылку на pool (selector.pool или selector._pool)
        """
        if selector is None:
            raise TypeError("MultiContainerExecutor requires selector=")

        self._storage = storage
        self._selector = selector

        self._profiles = profile_manager or profiles
        if self._profiles is None:
            raise TypeError("MultiContainerExecutor requires profiles= (or profile_manager=)")

        self._prompts = prompt_registry or prompts
        if self._prompts is None:
            raise TypeError("MultiContainerExecutor requires prompts= (or prompt_registry=)")

        self._profile_lock = profile_lock or ProfileLock()

        # pool берём из selector (это источник upstream клиентов по container_id),
        # но поддерживаем fallback на legacy `pool=` для старого wiring.
        self._pool = getattr(selector, "pool", None) or getattr(selector, "_pool", None) or pool
        if self._pool is None:
            raise RuntimeError("ContainerSelector не содержит upstream pool (ожидается selector.pool или selector._pool)")

        # allow_socks_override: если явно передали — используем, иначе пробуем взять из config профилей.
        if allow_socks_override is None:
            allow_socks_override = bool(getattr(getattr(self._profiles, "_config", None), "allow_socks_override", True))
        self._allow_socks_override = bool(allow_socks_override)

        self._io_logger = io_logger

        try:
            enabled = list(self._pool.list_enabled())
        except Exception:
            enabled = []
        _jlog(
            logging.INFO,
            {
                "event": "mc_executor_init",
                "enabled_containers": enabled,
                "enabled_count": len(enabled),
            },
        )

    def _update_job_identity(self, job_id: str, *, profile_id: Optional[str], socks_id: Optional[str]) -> None:
        """
        Best-effort апдейт jobs.profile_id/jobs.socks_id после resolve профиля.

        Зачем:
          - insert_job_start делаем до resolve, чтобы job существовал сразу.
          - но реальные (profile_id/socks_id) могут проясниться чуть позже.
        """
        if not job_id:
            return
        try:
            with self._storage._connect() as conn:
                conn.execute(
                    "UPDATE jobs SET profile_id = ?, socks_id = ? WHERE job_id = ?",
                    (profile_id, socks_id, job_id),
                )
                conn.commit()
        except Exception:
            return

    def _list_recent_prompt_sessions(self, prompt_id: str, *, limit: int = 50) -> list[dict[str, Any]]:
        """
        Возвращает список "свежих" активных chat_sessions (disabled=0) для prompt_id.

        Это используется только когда клиент не передал profile_id (автовыбор),
        чтобы сначала попробовать reuse существующих живых чатов.

        Для твоего кейса (profile_id всегда указан) это почти не участвует,
        но полезно понимать: reuse логика начинается с БД chat_sessions.
        """
        rows: list[dict[str, Any]] = []
        try:
            with self._storage._connect() as conn:
                cur = conn.execute(
                    """
                    SELECT id, container_id, profile_id, socks_id, chat_id, page_url, uses_count, disabled, locked_until, tag, updated_at
                    FROM chat_sessions
                    WHERE prompt_id = ? AND disabled = 0
                      AND COALESCE(chat_id,'') NOT IN ('guest','archive')
                      AND COALESCE(tag,'') NOT IN ('guest','archive')
                    ORDER BY updated_at DESC
                    LIMIT ?
                    """,
                    (prompt_id, int(limit)),
                )
                for r in cur.fetchall():
                    rows.append(dict(r))
        except Exception:
            return []
        return rows

    def _build_candidates(
        self,
        *,
        prompt_id: str,
        profile_id: Optional[str],
        socks_override: Optional[str],
        max_chat_uses: int,
        chat_url: Optional[str],
    ) -> tuple[list[_ProfileCandidate], Optional[dict[str, Any]]]:
        """
        Строим список кандидатов для попытки выполнения.

        ВАЖНО: candidates == "кому дать шанс выполнить запрос".

        Особый путь: если клиент передал chat_url:
          - проверяем, что chat_url существует в БД
          - что он принадлежит prompt_id
          - что он принадлежит profile_id (или подставляем profile_id из него)
          - вытаскиваем container_id/chat_id как preferred, чтобы попасть ровно в тот чат

        Это важно для режима force_new_chat=false:
          - Если chat_url задан, то reuse должен быть максимально "жёстким":
            мы пытаемся попасть в конкретный чат.
        """
        chat_url_row = None
        if chat_url:
            cs = self._storage.get_full_chat_session_by_url(chat_url)
            if cs is None:
                raise ValueError("chat_url не найден в базе (неизвестный чат)")
            if (cs.prompt_id or "") != (prompt_id or ""):
                raise ValueError("chat_url не соответствует prompt_id")

            if int(getattr(cs, "disabled", 0) or 0) != 0 or _is_blocked_chat(getattr(cs, "chat_id", None), getattr(cs, "tag", None)):
                raise ValueError("chat_url относится к заблокированному чату (guest/archive) или disabled=1")

            if not profile_id:
                profile_id = cs.profile_id

            if (profile_id or "") != (cs.profile_id or ""):
                raise ValueError("chat_url не принадлежит указанному профилю")

            socks_override = socks_override or cs.socks_id

            chat_url_row = {
                "container_id": cs.container_id,
                "profile_id": cs.profile_id,
                "socks_id": cs.socks_id,
                "chat_id": cs.chat_id,
                "page_url": cs.page_url,
                "tag": getattr(cs, "tag", None),
                "disabled": int(getattr(cs, "disabled", 0) or 0),
            }

        # КЛЮЧЕВО ДЛЯ ТВОЕГО КЕЙСА:
        # Если profile_id указан явно — candidates ровно один.
        # Значит дальше pipeline НЕ делает авто-выбор "других профилей",
        # а строго работает с указанным.
        if profile_id:
            preferred_container_id = chat_url_row.get("container_id") if chat_url_row else None
            preferred_chat_id = chat_url_row.get("chat_id") if chat_url_row else None
            return [
                _ProfileCandidate(
                    profile_id=profile_id,
                    socks_override=socks_override,
                    preferred_container_id=preferred_container_id,
                    preferred_chat_id=preferred_chat_id,
                )
            ], chat_url_row

        # Если profile_id не указан — авто-выбор (не твой кейс):
        # 1) reuse свежих chat_sessions по prompt_id
        # 2) добор профилей из list_profiles по наименьшему uses_count
        seen: set[tuple[str, Optional[str], Optional[str], Optional[str]]] = set()
        out: list[_ProfileCandidate] = []

        for s in self._list_recent_prompt_sessions(prompt_id, limit=60):
            pid = (s.get("profile_id") or "").strip()
            if not pid:
                continue
            uses = int(s.get("uses_count") or 0)
            if uses >= int(max_chat_uses or 0):
                continue

            if socks_override:
                want = str(socks_override).strip()
                have = str(s.get("socks_id") or "").strip()
                if want and have and want != have:
                    if normalize_socks_for_compare(want) != normalize_socks_for_compare(have):
                        continue

            cand_socks_override = socks_override or (s.get("socks_id") or None)
            cid = (s.get("container_id") or "").strip() or None
            chat_id = (s.get("chat_id") or "").strip() or None

            key = (pid, cand_socks_override, cid, chat_id)
            if key in seen:
                continue
            seen.add(key)
            out.append(
                _ProfileCandidate(
                    profile_id=pid,
                    socks_override=cand_socks_override,
                    preferred_container_id=cid,
                    preferred_chat_id=chat_id,
                )
            )

        try:
            profiles = list(self._storage.list_profiles())
        except Exception:
            profiles = []

        profiles.sort(key=lambda p: (int(p.uses_count or 0), p.profile_id))

        for p in profiles:
            if p.pending_replace:
                continue
            if p.max_uses is not None and int(p.uses_count or 0) >= int(p.max_uses):
                continue
            pid = (p.profile_id or "").strip()
            if not pid:
                continue

            key = (pid, socks_override, None, None)
            if key in seen:
                continue
            seen.add(key)
            out.append(
                _ProfileCandidate(
                    profile_id=pid,
                    socks_override=socks_override,
                    preferred_container_id=None,
                    preferred_chat_id=None,
                )
            )

        return out, None

    async def execute(self, req: SolveRequest, *, request_id: Optional[str] = None) -> tuple[int, SolveResponse]:
        """
        Главный метод multi-container выполнения /v1/solve.

        Ключевой момент по force_new_chat=False:
          - ChatManager.get_or_create_chat(...) решает reuse или создание нового chat_session (БД).
          - ChatManager.ensure_chat_loaded(...) приводит браузер контейнера в состояние "чат открыт и готов",
            и при создании нового чата отправляет start_prompt.
          - URL для дальнейших analyze_* берётся строго из chat_session.page_url.

        ВАЖНО про busy:
          - precheck status может сказать "не busy", но контейнер может стать busy позже.
          - busy может случиться внутри ensure_chat_loaded (на отправке start_prompt через analyze_text).
          - это нужно трактовать как CONTAINER_BUSY/503, а не как 500.
        """
        request_id = request_id or str(uuid.uuid4())
        started_at = _iso_now()

        prompt_id = (req.options.prompt_id if req.options and req.options.prompt_id else None) or req.prompt_id or "default"

        text = (req.input.text or "").strip() if req.input and req.input.text else ""
        has_image = bool(req.input and req.input.image_b64)
        image_b64 = req.input.image_b64 if req.input else None
        image_ext = (req.input.image_ext or "").strip() if req.input and req.input.image_ext else None

        _jlog(
            logging.INFO,
            {
                "event": "mc_solve_start",
                "request_id": request_id,
                "prompt_id": prompt_id,
                "has_text": bool(text),
                "text_len": len(text) if text else 0,
                "has_image": bool(has_image),
                "image_ext": image_ext,
            },
        )

        if not text and not has_image:
            return self._fail(
                job_id="",
                request_id=request_id,
                prompt_id=prompt_id,
                code="INVALID_REQUEST",
                message="Нужно передать text и/или image_b64",
                http_status=400,
                started_at=started_at,
            )

        if has_image and not image_ext:
            return self._fail(
                job_id="",
                request_id=request_id,
                prompt_id=prompt_id,
                code="INVALID_REQUEST",
                message="Если передан image_b64, нужно указать image_ext",
                http_status=400,
                started_at=started_at,
            )

        # ===== 1) Resolve prompt spec =====
        try:
            ps = self._prompts.get_prompt(prompt_id)
        except Exception as e:
            _jlog(
                logging.ERROR,
                {
                    "event": "prompt_resolve_failed",
                    "request_id": request_id,
                    "prompt_id": prompt_id,
                    "error": str(e),
                },
            )
            return self._fail(
                job_id="",
                request_id=request_id,
                prompt_id=prompt_id,
                code="INVALID_REQUEST",
                message=f"Unknown prompt_id: {prompt_id}",
                http_status=400,
                started_at=started_at,
                details={"error": str(e)},
            )

        default_max_chat_uses = int(getattr(ps, "default_max_chat_uses", 50) or 50)

        options = req.options
        profile_id_opt = (options.profile_id or "").strip() if options and options.profile_id else None

        force_new = bool(options.force_new_chat) if options else False
        include_debug = bool(options.include_debug) if options else False
        chat_url = (options.chat_url or "").strip() if options and options.chat_url else None

        max_chat_uses = int(options.max_chat_uses) if options and options.max_chat_uses is not None else default_max_chat_uses

        # ===== 2) Resolve socks override =====
        socks_override = None
        if options:
            socks_override = (options.socks_override or options.socks_id or None)
            socks_override = socks_override.strip() if isinstance(socks_override, str) else socks_override

        allow_override = bool(getattr(getattr(self._profiles, "_config", None), "allow_socks_override", True))

        _jlog(
            logging.INFO,
            {
                "event": "solve_params",
                "request_id": request_id,
                "prompt_id": prompt_id,
                "profile_id": profile_id_opt,
                "socks_override": _redact_proxy_url(socks_override),
                "allow_socks_override": allow_override,
                "force_new_chat": bool(force_new),
                "chat_url": chat_url,
                "max_chat_uses": max_chat_uses,
                "include_debug": bool(include_debug),
            },
        )

        # ===== 3) insert_job_start (multi) =====
        job_id = self._storage.insert_job_start(
            prompt_id,
            request_id=request_id,
            input_text=text or None,
            input_image_present=has_image,
            input_image_ext=image_ext,
            profile_id=profile_id_opt,
            socks_id=None,
            started_at=started_at,
            selected_prompt_id=prompt_id,
            decision_mode="multi",
            fanout_requested=1,
            fanout_used=1,
            container_ids_used=[],
        )

        _jlog(logging.INFO, {"event": "job_inserted", "request_id": request_id, "job_id": job_id, "profile_id": profile_id_opt})

        # ===== 4) Build candidates =====
        try:
            candidates, chat_url_row = self._build_candidates(
                prompt_id=prompt_id,
                profile_id=profile_id_opt,
                socks_override=socks_override,
                max_chat_uses=max_chat_uses,
                chat_url=chat_url,
            )
        except ValueError as e:
            return self._fail(
                job_id=job_id,
                request_id=request_id,
                prompt_id=prompt_id,
                code="INVALID_REQUEST",
                message=str(e),
                http_status=400,
                started_at=started_at,
                details={"error": str(e)},
            )

        if not candidates:
            return self._fail(
                job_id=job_id,
                request_id=request_id,
                prompt_id=prompt_id,
                code="INTERNAL_ERROR",
                message="Нет доступных профилей (storage.list_profiles вернул пусто)",
                http_status=500,
                started_at=started_at,
            )

        explicit_profile = bool(profile_id_opt or chat_url)

        profile_busy = 0
        container_busy = 0

        # ===== 5) Iterate candidates =====
        for cand in candidates:
            profile_id = cand.profile_id

            # ===== 5.1) Resolve profile =====
            try:
                resolved = self._profiles.resolve_for_request(profile_id, cand.socks_override, allow_socks_override=allow_override)
            except KeyError:
                continue
            except Exception as e:
                logger.exception("profile resolve crash request_id=%s error=%s", request_id, str(e))
                return self._fail(
                    job_id=job_id,
                    request_id=request_id,
                    prompt_id=prompt_id,
                    code="INTERNAL_ERROR",
                    message=str(e),
                    http_status=500,
                    started_at=started_at,
                    profile_id=profile_id,
                    details={"error": str(e)},
                )

            socks_key: Optional[str] = resolved.socks_id or resolved.socks_url or None
            socks_url_used: Optional[str] = resolved.socks_url

            _jlog(
                logging.INFO,
                {
                    "event": "profile_resolved",
                    "request_id": request_id,
                    "job_id": job_id,
                    "profile_id": resolved.profile_id,
                    "profile_value": resolved.profile_value,
                    "socks_key": _redact_proxy_url(socks_key),
                    "socks_url": _redact_proxy_url(socks_url_used),
                    "allowed_containers": resolved.allowed_containers,
                    "candidate_preferred_container": cand.preferred_container_id,
                },
            )

            if resolved.max_uses is not None:
                pr = self._storage.get_profile(resolved.profile_id)
                if pr and int(pr.uses_count or 0) >= int(resolved.max_uses):
                    continue

            # ===== 5.1.1) Guest-block: если у профиля есть ХОТЯ БЫ ОДНА запись chat_id='guest' (или tag='guest'),
            # то профиль исключается из работы и НЕЛЬЗЯ создавать новые чаты.
            if self._storage.profile_has_guest_chat(resolved.profile_id):
                guest_n = 0
                try:
                    guest_n = int(self._storage.count_guest_chats_for_profile(resolved.profile_id) or 0)
                except Exception:
                    guest_n = 0

                if explicit_profile:
                    return self._fail(
                        job_id=job_id,
                        request_id=request_id,
                        prompt_id=prompt_id,
                        code="PROFILE_BLOCKED",
                        message="Профиль заблокирован: обнаружены guest-чаты (chat_id='guest'). Новые чаты для этого профиля запрещены до очистки.",
                        http_status=409,
                        started_at=started_at,
                        profile_id=resolved.profile_id,
                        socks_id=socks_key,
                        details={
                            "reason": "guest_chat_present",
                            "profile_id": resolved.profile_id,
                            "guest_chats": guest_n,
                            "hint": "Очистите guest-записи через POST /v1/profiles/{profile_id}/guest/clear",
                        },
                    )

                # в авто-режиме просто пропускаем этот профиль
                continue

            # ===== 5.2) Acquire profile lock =====
            try:
                async with self._profile_lock.try_lock(resolved.profile_id, owner=request_id):
                    _jlog(logging.INFO, {"event": "profile_lock_acquired", "request_id": request_id, "job_id": job_id, "profile_id": resolved.profile_id})

                    # ===== 5.3) Choose container =====
                    container_id: Optional[str] = None

                    preferred_container_id = cand.preferred_container_id
                    if preferred_container_id and resolved.allowed_containers and preferred_container_id not in set(resolved.allowed_containers):
                        preferred_container_id = None

                    if preferred_container_id:
                        try:
                            if getattr(self._pool, "is_enabled", None) and not self._pool.is_enabled(preferred_container_id):
                                preferred_container_id = None
                        except Exception:
                            preferred_container_id = None

                    if preferred_container_id:
                        container_id = preferred_container_id
                    else:
                        try:
                            container_ids = await self._selector.select_containers(
                                prompt_id,
                                profile_id=resolved.profile_id,
                                socks_id=socks_key,
                                fanout_requested=1,
                                chat_url=None,
                                allowed_containers=resolved.allowed_containers,
                                request_id=request_id,
                            )
                            container_id = container_ids[0]
                        except NotEnoughContainersError:
                            container_busy += 1
                            continue

                    assert container_id

                    # ===== 5.4) Acquire upstream client =====
                    try:
                        upstream = self._pool.get(container_id)
                    except Exception:
                        container_busy += 1
                        continue

                    # ===== 5.5) Busy pre-check =====
                    try:
                        st = await upstream.status(request_id=request_id)
                        if st.get("status") == "busy" or bool(st.get("busy") is True):
                            container_busy += 1
                            continue
                    except Exception:
                        pass

                    # ===== 5.6) Update job identity and selected container =====
                    self._update_job_identity(job_id, profile_id=resolved.profile_id, socks_id=socks_key)
                    self._storage.set_job_selected_containers(job_id, [container_id])

                    # ===== 5.7) Chat creation/reuse =====
                    chat_mgr = ChatManager(storage=self._storage)

                    chat_session = await chat_mgr.get_or_create_chat(
                        container_id=container_id,
                        request_id=request_id,
                        prompt_id=prompt_id,
                        prompt_spec=ps,
                        profile_id=resolved.profile_id,
                        socks_id=socks_key,
                        profile_value=resolved.profile_value,
                        socks_url=socks_url_used,
                        force_new=force_new,
                        max_chat_uses=max_chat_uses,
                        chat_url=chat_url,
                        preferred_chat_id=cand.preferred_chat_id,
                    )

                    # ===== 5.7.1) ensure_chat_loaded (FIX: catch UpstreamBusyError here) =====
                    # Здесь отправляется start_prompt (внутри ChatManager) и upstream может ответить 423.
                    # Это не "internal error", это "контейнер занят" => считаем container_busy и пробуем другой.
                    try:
                        chat_session = await chat_mgr.ensure_chat_loaded(
                            upstream=upstream,
                            request_id=request_id,
                            container_id=container_id,
                            chat_session=chat_session,
                            prompt_spec=ps,
                            profile_value=resolved.profile_value,
                            socks_url=socks_url_used,
                        )
                    except UpstreamBusyError:
                        container_busy += 1
                        continue

                    # chat_session_block_check: guest/archive
                    # Если upstream создал /c/guest (или запись ранее была помечена),
                    # то этот профиль должен быть исключён из работы и НЕЛЬЗЯ создавать новые чаты.
                    if int(getattr(chat_session, 'disabled', 0) or 0) != 0 or _is_blocked_chat(getattr(chat_session, 'chat_id', None), getattr(chat_session, 'tag', None)):
                        cid = (getattr(chat_session, 'chat_id', None) or '').strip().lower()
                        tag = (getattr(chat_session, 'tag', None) or '').strip().lower()

                        if cid == 'guest' or tag == 'guest':
                            # Помечаем запись как guest (best-effort) и запрещаем её переиспользование.
                            try:
                                self._storage.mark_chat_session_tag(int(chat_session.id), tag='guest', disabled=True)
                            except Exception:
                                pass

                            guest_n = 0
                            try:
                                guest_n = int(self._storage.count_guest_chats_for_profile(resolved.profile_id) or 0)
                            except Exception:
                                guest_n = 0

                            return self._fail(
                                job_id=job_id,
                                request_id=request_id,
                                prompt_id=prompt_id,
                                code="PROFILE_BLOCKED",
                                message="Профиль заблокирован: upstream вернул guest-чат (chat_id='guest').",
                                http_status=409,
                                started_at=started_at,
                                profile_id=resolved.profile_id,
                                socks_id=socks_key,
                                container_ids_used=[container_id],
                                details={
                                    "reason": "guest_chat_created",
                                    "profile_id": resolved.profile_id,
                                    "guest_chats": guest_n,
                                    "chat_session_id": str(chat_session.id),
                                    "page_url": chat_session.page_url,
                                    "hint": "Очистите guest-записи через POST /v1/profiles/{profile_id}/guest/clear",
                                },
                            )

                        # archive (или иной blocked) — конкретный чат использовать нельзя
                        try:
                            self._storage.mark_chat_session_tag(int(chat_session.id), tag='archive', disabled=True)
                        except Exception:
                            pass

                        if explicit_profile:
                            return self._fail(
                                job_id=job_id,
                                request_id=request_id,
                                prompt_id=prompt_id,
                                code="CHAT_BLOCKED",
                                message="Чат помечен как guest/archive и не может быть использован.",
                                http_status=409,
                                started_at=started_at,
                                profile_id=resolved.profile_id,
                                socks_id=socks_key,
                                container_ids_used=[container_id],
                                details={
                                    "reason": "chat_blocked",
                                    "chat_session_id": str(chat_session.id),
                                    "chat_id": getattr(chat_session, 'chat_id', None),
                                    "tag": getattr(chat_session, 'tag', None),
                                    "page_url": chat_session.page_url,
                                },
                            )

                        # авто-режим — пробуем другого кандидата
                        continue

                    # ===== 5.8) Create attempt row =====
                    attempt_id = self._storage.create_job_attempt(
                        job_id,
                        container_id=container_id,
                        prompt_id=prompt_id,
                        role="single",
                        profile_id=resolved.profile_id,
                        socks_id=socks_key,
                        chat_id=chat_session.chat_id,
                        page_url=chat_session.page_url,
                        chat_session_id=str(chat_session.id),
                        started_at=_iso_now(),
                    )

                    # ===== 5.9) Do analyze =====
                    raw: Any
                    try:
                        if text and not has_image:
                            raw = await upstream.analyze_text(
                                text,
                                url=chat_session.page_url,
                                profile=resolved.profile_value,
                                socks=socks_url_used,
                                request_id=request_id,
                            )
                            self._storage.increment_chat_use(chat_session.id, by=1)

                        elif has_image and not text:
                            raw = await upstream.analyze_image_b64(
                                image_b64=image_b64 or "",
                                ext=image_ext or "png",
                                url=chat_session.page_url,
                                profile=resolved.profile_value,
                                socks=socks_url_used,
                                request_id=request_id,
                            )
                            self._storage.increment_chat_use(chat_session.id, by=1)

                        else:
                            raw1 = await upstream.analyze_text(
                                text,
                                url=chat_session.page_url,
                                profile=resolved.profile_value,
                                socks=socks_url_used,
                                request_id=request_id,
                            )
                            self._storage.increment_chat_use(chat_session.id, by=1)

                            raw2 = await upstream.analyze_image_b64(
                                image_b64=image_b64 or "",
                                ext=image_ext or "png",
                                url=chat_session.page_url,
                                profile=resolved.profile_value,
                                socks=socks_url_used,
                                request_id=request_id,
                            )
                            self._storage.increment_chat_use(chat_session.id, by=1)

                            raw = [raw1, raw2]

                        out_text = _pick_text_from_raw(raw if not isinstance(raw, list) else raw[-1])
                        finished_at = _iso_now()

                        self._storage.finish_job_attempt(
                            attempt_id,
                            status="succeeded",
                            result_text=out_text,
                            result_raw_json=raw,
                            error_code=None,
                            error_message=None,
                            finished_at=finished_at,
                        )

                        self._storage.update_job_finish(
                            job_id,
                            status="succeeded",
                            result_text=out_text,
                            result_raw_json=raw,
                            error_code=None,
                            error_message=None,
                            finished_at=finished_at,
                            decision_mode="multi",
                        )

                        self._storage.increment_profile_use(resolved.profile_id)

                        meta = {
                            "job_id": job_id,
                            "request_id": request_id,
                            "prompt_id_selected": prompt_id,
                            "fanout_requested": 1,
                            "container_ids_used": [container_id],
                            "profile_id": resolved.profile_id,
                            "socks_id": socks_key,
                            "socks_url": _redact_proxy_url(socks_url_used),
                            "chat_ids_used": [chat_session.chat_id] if chat_session.chat_id else [],
                            "page_url": chat_session.page_url,
                            "started_at": started_at,
                            "finished_at": finished_at,
                        }

                        attempts = None
                        if include_debug:
                            attempts = [
                                SolveAttempt(
                                    container_id=container_id,
                                    ok=True,
                                    role="single",
                                    text=out_text,
                                    raw=raw,
                                )
                            ]

                        resp = SolveResponse(ok=True, final=SolveFinal(kind="text", text=out_text, raw=raw), meta=meta, attempts=attempts)
                        return 200, resp

                    except UpstreamBusyError as e:
                        return await self._handle_attempt_error(
                            job_id,
                            attempt_id,
                            started_at,
                            request_id,
                            prompt_id,
                            resolved.profile_id,
                            socks_key,
                            container_id,
                            chat_session,
                            code="CONTAINER_BUSY",
                            http_status=503,
                            message="Контейнер занят",
                            details=e.payload,
                        )
                    except UpstreamBadRequestError as e:
                        return await self._handle_attempt_error(
                            job_id,
                            attempt_id,
                            started_at,
                            request_id,
                            prompt_id,
                            resolved.profile_id,
                            socks_key,
                            container_id,
                            chat_session,
                            code="INVALID_REQUEST",
                            http_status=400,
                            message="Неверный запрос",
                            details=e.payload,
                        )
                    except (UpstreamServerError, UpstreamTransportError) as e:
                        return await self._handle_attempt_error(
                            job_id,
                            attempt_id,
                            started_at,
                            request_id,
                            prompt_id,
                            resolved.profile_id,
                            socks_key,
                            container_id,
                            chat_session,
                            code="UPSTREAM_ERROR",
                            http_status=502,
                            message=str(e),
                            details={"error": str(e)},
                        )
                    except Exception as e:
                        logger.exception("exception request_id=%s job_id=%s error=%s", request_id, job_id, str(e))
                        return await self._handle_attempt_error(
                            job_id,
                            attempt_id,
                            started_at,
                            request_id,
                            prompt_id,
                            resolved.profile_id,
                            socks_key,
                            container_id,
                            chat_session,
                            code="INTERNAL_ERROR",
                            http_status=500,
                            message=str(e),
                            details={"error": str(e)},
                        )

            except ProfileBusyError:
                profile_busy += 1
                continue

        if profile_busy and not container_busy:
            return self._fail(
                job_id=job_id,
                request_id=request_id,
                prompt_id=prompt_id,
                code="PROFILE_BUSY",
                message="Нет свободных профилей",
                http_status=503,
                started_at=started_at,
                details={"profile_busy": profile_busy, "container_busy": container_busy},
            )

        return self._fail(
            job_id=job_id,
            request_id=request_id,
            prompt_id=prompt_id,
            code="CONTAINER_BUSY",
            message="Нет доступных контейнеров",
            http_status=503,
            started_at=started_at,
            details={"profile_busy": profile_busy, "container_busy": container_busy},
        )

    async def _handle_attempt_error(
        self,
        job_id: str,
        attempt_id: str,
        started_at: str,
        request_id: str,
        prompt_id: str,
        profile_id: str,
        socks_id: Optional[str],
        container_id: str,
        chat_session: Any,
        *,
        code: str,
        http_status: int,
        message: str,
        details: Optional[dict[str, Any]] = None,
    ) -> tuple[int, SolveResponse]:
        """
        Унифицированная обработка ошибки во время attempt.

        Инварианты:
          - attempt всегда закрывается (finish_job_attempt)
          - job всегда закрывается (update_job_finish)
          - ответ клиенту всегда содержит error + meta

        Это важно для наблюдаемости: даже при исключении остаётся "след" в БД.
        """
        finished_at = _iso_now()
        self._storage.finish_job_attempt(
            attempt_id,
            status="failed",
            result_text=None,
            result_raw_json=None,
            error_code=code,
            error_message=message,
            finished_at=finished_at,
        )
        self._storage.update_job_finish(
            job_id,
            status="failed",
            result_text=None,
            result_raw_json=None,
            error_code=code,
            error_message=message,
            finished_at=finished_at,
            decision_mode="multi",
        )

        _jlog(
            logging.WARNING,
            {
                "event": "attempt_failed",
                "request_id": request_id,
                "job_id": job_id,
                "attempt_id": attempt_id,
                "container_id": container_id,
                "code": code,
                "message": message,
            },
        )

        resp = SolveResponse(
            ok=False,
            error=SolveError(code=code, message=message, details=details or {}),
            meta={
                "job_id": job_id,
                "request_id": request_id,
                "prompt_id_selected": prompt_id,
                "fanout_requested": 1,
                "container_ids_used": [container_id],
                "profile_id": profile_id,
                "socks_id": socks_id,
                "chat_ids_used": [chat_session.chat_id] if getattr(chat_session, "chat_id", None) else [],
                "page_url": getattr(chat_session, "page_url", None),
                "started_at": started_at,
                "finished_at": finished_at,
            },
        )
        return http_status, resp

    def _fail(
        self,
        *,
        job_id: str,
        request_id: str,
        prompt_id: str,
        code: str,
        message: str,
        http_status: int,
        started_at: str,
        profile_id: Optional[str] = None,
        socks_id: Optional[str] = None,
        container_ids_used: Optional[list[str]] = None,
        details: Optional[dict[str, Any]] = None,
    ) -> tuple[int, SolveResponse]:
        """
        Унифицированная ошибка без attempt (или ошибка до старта attempt).

        Обычно сюда попадают:
          - invalid request (валидация)
          - нет кандидатов
          - все кандидаты заняты/нет контейнеров

        job (если создан) закрывается в статус failed.
        """
        finished_at = _iso_now()

        if job_id:
            self._storage.update_job_finish(
                job_id,
                status="failed",
                result_text=None,
                result_raw_json=None,
                error_code=code,
                error_message=message,
                finished_at=finished_at,
                decision_mode="multi",
            )

        _jlog(
            logging.ERROR if code in ("INTERNAL_ERROR", "UPSTREAM_ERROR") else logging.WARNING,
            {
                "event": "mc_fail",
                "request_id": request_id,
                "job_id": job_id,
                "code": code,
                "message": message,
                "profile_id": profile_id,
                "socks_id": _redact_proxy_url(socks_id),
                "container_ids_used": container_ids_used or [],
                "details": details or {},
            },
        )

        resp = SolveResponse(
            ok=False,
            error=SolveError(code=code, message=message, details=details or {}),
            meta={
                "job_id": job_id,
                "request_id": request_id,
                "prompt_id_selected": prompt_id,
                "fanout_requested": 1,
                "container_ids_used": container_ids_used or [],
                "profile_id": profile_id,
                "socks_id": socks_id,
                "chat_ids_used": [],
                "started_at": started_at,
                "finished_at": finished_at,
            },
        )
        return http_status, resp
