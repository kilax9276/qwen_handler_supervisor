from __future__ import annotations

import re
from typing import Any, Optional

from ..prompts.registry import PromptSpec
from ..storage import FullChatSession, Storage


_CHAT_ID_RE = re.compile(r"/c/([^/?#]+)")


def _extract_chat_id(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    m = _CHAT_ID_RE.search(url)
    if not m:
        return None
    return m.group(1)


class ChatManager:
    """Менеджер чатов (full-mode).

    Задачи:
      - получить существующий chat_session (container_id,prompt_id,profile_id,socks_id)
      - при необходимости создать новый
      - при создании/первом использовании отправить start_prompt и обновить page_url/chat_id
    """

    def __init__(self, *, storage: Storage) -> None:
        self._storage = storage

    async def get_or_create_chat(
        self,
        *,
        container_id: str,
        request_id: Optional[str],
        prompt_id: str,
        prompt_spec: PromptSpec,
        profile_id: str,
        socks_id: Optional[str],
        profile_value: str,
        socks_url: Optional[str],
        force_new: bool,
        max_chat_uses: Optional[int],
        chat_url: Optional[str],
        preferred_chat_id: Optional[str] = None,
    ) -> FullChatSession:
        # 1) Если задан chat_url — это приоритетная привязка к уже существующему чату.
        if chat_url:
            sess = self._storage.get_full_chat_session_by_url(chat_url)
            if sess is None:
                raise KeyError(f"chat_url not registered: {chat_url}")
            if sess.container_id != container_id:
                # Это защитный инвариант: selector обязан выбирать контейнер по chat_url.
                raise RuntimeError(
                    f"chat_url container mismatch: expected {container_id}, got {sess.container_id}"
                )
            return sess

        # 2) Пытаемся переиспользовать существующий chat_session.
        sess = self._storage.get_chat_session(
            prompt_id,
            container_id=container_id,
            profile_id=profile_id,
            socks_id=socks_id,
            preferred_chat_id=preferred_chat_id,
        )

        uses_limit = int(max_chat_uses) if max_chat_uses is not None else int(prompt_spec.default_max_chat_uses or 0) or 50

        if sess is None or bool(force_new) or (sess.uses_count >= uses_limit):
            # Создаём новый чат. На этом этапе мы ещё не знаем /c/<id>.
            # Контейнер обычно стартует на корневом URL.
            sess = self._storage.create_full_chat_session(
                container_id=container_id,
                prompt_id=prompt_id,
                profile_id=profile_id,
                socks_id=socks_id or "",
                chat_id=None,
                page_url="https://chat.qwen.ai/",
            )

        return sess

    async def ensure_chat_loaded(
        self,
        *,
        upstream: Any,
        request_id: Optional[str],
        container_id: str,
        chat_session: FullChatSession,
        prompt_spec: PromptSpec,
        profile_value: str,
        socks_url: Optional[str],
    ) -> FullChatSession:
        # Если уже есть chat_id — считаем чат "созданным".
        if chat_session.chat_id:
            return chat_session

        start_prompt = (prompt_spec.start_prompt or "").strip()
        if not start_prompt:
            return chat_session

        # Отправляем стартовый промпт — контейнер должен создать чат и вернуть page_url с /c/<id>.
        raw = await upstream.analyze_text(
            start_prompt,
            url=chat_session.page_url,
            profile=profile_value,
            socks=socks_url,
            request_id=request_id,
        )

        page_url = (raw or {}).get("page_url") or chat_session.page_url
        chat_id = _extract_chat_id(page_url)

        updated = self._storage.update_full_chat_session_by_id(
            chat_session.id,
            chat_id=chat_id,
            page_url=page_url,
            disabled=False,
        )

        # Стартовый промпт — это тоже использование.
        self._storage.increment_chat_use(updated.id, by=1)

        return updated
