from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from ..config_loader import AppConfig, PromptConfig


@dataclass(frozen=True)
class PromptSpec:
    prompt_id: str
    start_prompt: str
    default_max_chat_uses: int
    file_path: str


class PromptRegistry:
    """Prompt registry backed by files.

    Кэширует содержимое и перечитывает при изменении mtime.
    """

    def __init__(self, *, app_config: AppConfig, config_path: str) -> None:
        self._config = app_config
        self._config_path = config_path
        self._base_dir = str(Path(config_path).resolve().parent)
        self._by_id: dict[str, PromptConfig] = {p.prompt_id: p for p in (app_config.prompts or [])}
        self._cache: dict[str, tuple[float, str]] = {}  # abs_path -> (mtime, text)

    def list_prompt_ids(self) -> list[str]:
        return sorted(self._by_id.keys())

    # ------------------------------------------------------------------
    # Backwards-compatible API expected by engine executors
    # ------------------------------------------------------------------

    def get_prompt(self, prompt_id: str) -> PromptSpec:
        """Compatibility alias: return PromptSpec for a prompt_id."""
        return self.get(prompt_id)

    def get(self, prompt_id: str) -> PromptSpec:
        if prompt_id not in self._by_id:
            raise KeyError(f"Unknown prompt_id: {prompt_id}")
        pcfg = self._by_id[prompt_id]
        abs_path = self._resolve_path(pcfg.file)
        start_prompt = self._read_cached(abs_path)
        return PromptSpec(
            prompt_id=pcfg.prompt_id,
            start_prompt=start_prompt,
            default_max_chat_uses=int(pcfg.default_max_chat_uses or 0) or 50,
            file_path=abs_path,
        )

    def _resolve_path(self, p: str) -> str:
        pp = Path(p)
        if not pp.is_absolute():
            pp = Path(self._base_dir) / pp
        return str(pp.resolve())

    def _read_cached(self, abs_path: str) -> str:
        try:
            st = os.stat(abs_path)
            mtime = float(st.st_mtime)
        except FileNotFoundError:
            return ""

        cached = self._cache.get(abs_path)
        if cached and cached[0] == mtime:
            return cached[1]

        try:
            text = Path(abs_path).read_text(encoding="utf-8")
        except Exception:
            text = ""

        self._cache[abs_path] = (mtime, text)
        return text
