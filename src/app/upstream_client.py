#copyright "Kilax @kilax9276"
from __future__ import annotations

import asyncio
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import unquote, urlsplit, urlunsplit

import httpx


@dataclass(frozen=True)
class UpstreamError(Exception):
    status_code: int
    payload: Any

    def __str__(self) -> str:
        return f"UpstreamError(status_code={self.status_code}, payload={self.payload!r})"


class UpstreamBusyError(UpstreamError):
    """HTTP 423 (upstream занят)."""


class UpstreamBadRequestError(UpstreamError):
    """HTTP 400 (и другие 4xx кроме 423)."""


class UpstreamServerError(UpstreamError):
    """HTTP 5xx."""


class UpstreamTransportError(RuntimeError):
    """Сеть/таймаут/DNS и т.п."""


_CHAT_ID_RE = re.compile(r"/c/([a-zA-Z0-9_-]+)")


def parse_chat_id_from_page_url(page_url: Optional[str]) -> Optional[str]:
    """Best-effort извлечение chat_id из page_url."""
    if not page_url:
        return None
    m = _CHAT_ID_RE.search(page_url)
    return m.group(1) if m else None


def normalize_profile_for_compare(value: Optional[str]) -> Optional[str]:
    """Normalize profile values for equality comparison."""
    if value is None:
        return None
    v = str(value).strip()
    if not v:
        return None

    vv = v.replace("\\", "/").rstrip("/")
    if "/" in vv:
        return os.path.basename(vv) or None
    return v


def normalize_socks_for_compare(value: Optional[str]) -> Optional[str]:
    """Normalize socks URL for equality comparison."""
    if value is None:
        return None
    v = str(value).strip()
    if not v:
        return None
    if "://" not in v:
        return v
    try:
        parts = urlsplit(v)
        scheme = (parts.scheme or "").lower()
        hostname = (parts.hostname or "").lower()
        port = parts.port
        port_str = str(port) if port is not None else ""
        user = unquote(parts.username or "")
        password = unquote(parts.password or "")

        auth = f"{user}:{password}@" if (user or password) else ""
        host = f"{hostname}:{port_str}" if port_str else hostname
        netloc = f"{auth}{host}"
        return urlunsplit((scheme, netloc, "", "", ""))
    except Exception:
        return v


def _safe_json(resp: httpx.Response) -> Any:
    try:
        return resp.json()
    except Exception:
        return {"_raw": resp.text}


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _redact_socks_value(url: Optional[str]) -> Optional[str]:
    if not url:
        return url
    # socks5://user:pass@host:port -> socks5://user:***@host:port
    try:
        return re.sub(r"://([^:@/]+):([^@/]+)@", r"://\1:***@", str(url))
    except Exception:
        return "<redacted>"


def _truncate_payload(payload: Any, *, max_str: int = 256) -> Any:
    """Санитизация payload для логов (не ломаем реальный запрос)."""
    if payload is None:
        return None
    if isinstance(payload, (int, float, bool)):
        return payload
    if isinstance(payload, str):
        s = payload
        if len(s) <= max_str:
            return s
        return s[:max_str] + f"…(len={len(s)})"
    if isinstance(payload, list):
        return [_truncate_payload(x, max_str=max_str) for x in payload[:20]]
    if isinstance(payload, dict):
        out: dict[str, Any] = {}
        for k, v in list(payload.items())[:60]:
            if k in ("image_b64", "image", "b64") and isinstance(v, str):
                out[k] = v[:64] + f"…(len={len(v)})"
            elif k in ("socks", "proxy") and isinstance(v, str):
                out[k] = _redact_socks_value(v)
            else:
                out[k] = _truncate_payload(v, max_str=max_str)
        return out
    try:
        return str(payload)
    except Exception:
        return "<unprintable>"


class UpstreamClient:
    def __init__(
        self,
        base_url: str,
        *,
        connect_timeout_seconds: float = 10.0,
        read_timeout_seconds: float = 120.0,
        analyze_retries: int = 1,
        container_id: Optional[str] = None,
        io_logger: Optional[Any] = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")

        timeout = httpx.Timeout(
            connect=connect_timeout_seconds,
            read=read_timeout_seconds,
            write=read_timeout_seconds,
            pool=connect_timeout_seconds,
        )

        self._analyze_retries = max(0, min(int(analyze_retries), 2))
        self._client = httpx.AsyncClient(base_url=self._base_url, timeout=timeout)
        self._container_id = container_id
        self._io_logger = io_logger

    async def aclose(self) -> None:
        await self._client.aclose()

    async def health(self, *, request_id: Optional[str] = None) -> dict[str, Any]:
        return await self._request_json("GET", "/health", request_id=request_id)

    async def status(self, *, request_id: Optional[str] = None) -> dict[str, Any]:
        return await self._request_json("GET", "/status", request_id=request_id)

    async def open(
        self,
        url: str,
        *,
        profile: Optional[str] = None,
        socks: Optional[str] = None,
        request_id: Optional[str] = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {"url": url}
        if profile:
            payload["profile"] = profile
        if socks:
            payload["socks"] = socks
        return await self._request_json("POST", "/open", json=payload, request_id=request_id)

    async def analyze_text(
        self,
        text: str,
        *,
        url: Optional[str] = None,
        profile: Optional[str] = None,
        socks: Optional[str] = None,
        request_id: Optional[str] = None,
    ) -> Any:
        """Текстовый анализ.

        Актуальный upstream ожидает POST /analyze с полем "text" и (опционально) "url".
        Но для совместимости со старыми контейнерами пробуем /analyze и при 404/405
        откатываемся на /analyze_text.
        """
        payload: dict[str, Any] = {"text": text}
        if url:
            payload["url"] = url
        if profile:
            payload["profile"] = profile
        if socks:
            payload["socks"] = socks

        try:
            return await self._request_json_with_retries("POST", "/analyze", json=payload, request_id=request_id)
        except UpstreamBadRequestError as e:
            if int(getattr(e, "status_code", 0)) in (404, 405):
                return await self._request_json_with_retries("POST", "/analyze_text", json=payload, request_id=request_id)
            raise

    async def analyze_text_via_analyze(
        self,
        text: str,
        *,
        url: Optional[str] = None,
        profile: Optional[str] = None,
        socks: Optional[str] = None,
        request_id: Optional[str] = None,
    ) -> Any:
        payload: dict[str, Any] = {"text": text}
        if url:
            payload["url"] = url
        if profile:
            payload["profile"] = profile
        if socks:
            payload["socks"] = socks
        return await self._request_json_with_retries("POST", "/analyze", json=payload, request_id=request_id)

    async def analyze_image_b64(
        self,
        image_b64: str,
        ext: str,
        *,
        url: Optional[str] = None,
        profile: Optional[str] = None,
        socks: Optional[str] = None,
        request_id: Optional[str] = None,
    ) -> Any:
        payload: dict[str, Any] = {"image_b64": image_b64, "ext": ext}
        if url:
            payload["url"] = url
        if profile:
            payload["profile"] = profile
        if socks:
            payload["socks"] = socks
        return await self._request_json_with_retries("POST", "/analyze", json=payload, request_id=request_id)

    async def analyze_image_path(
        self,
        image_path: str,
        *,
        url: Optional[str] = None,
        profile: Optional[str] = None,
        socks: Optional[str] = None,
        request_id: Optional[str] = None,
    ) -> Any:
        payload: dict[str, Any] = {"image_path": image_path}
        if url:
            payload["url"] = url
        if profile:
            payload["profile"] = profile
        if socks:
            payload["socks"] = socks
        return await self._request_json_with_retries("POST", "/analyze", json=payload, request_id=request_id)

    async def _request_json_with_retries(
        self,
        method: str,
        path: str,
        *,
        json: Optional[dict[str, Any]] = None,
        request_id: Optional[str] = None,
    ) -> Any:
        last_exc: Optional[Exception] = None

        for attempt in range(self._analyze_retries + 1):
            try:
                return await self._request_json(method, path, json=json, request_id=request_id)
            except UpstreamTransportError as e:
                last_exc = e
                if attempt >= self._analyze_retries:
                    break
                backoff = min(0.25 * (2**attempt), 2.0)
                await asyncio.sleep(backoff)

        assert last_exc is not None
        raise last_exc

    def _emit_io_log_entry(self, entry: dict[str, Any]) -> None:
        """Fallback IO log (best-effort).

        Поддерживает io_logger.log / io_logger.write / callable.
        """
        if not self._io_logger:
            return
        try:
            if hasattr(self._io_logger, "log"):
                self._io_logger.log(entry)
                return
            if hasattr(self._io_logger, "write"):
                self._io_logger.write(entry)
                return
            if callable(self._io_logger):
                self._io_logger(entry)
        except Exception:
            return

    def _emit_io_log_exchange(
        self,
        *,
        request_id: Optional[str],
        method: str,
        path: str,
        url: str,
        request_json: Optional[dict[str, Any]],
        status_code: Optional[int],
        response: Any,
        duration_ms: Optional[int],
        error: Optional[str],
    ) -> None:
        """Unified IO logging.

        Fix: ContainerIOLLogger из io_logging.py не имеет метода .log(), он пишет через
        .log_exchange(...). Поэтому раньше "молчало".

        Здесь поддерживаем оба API:
          - io_logger.log_exchange(...) (ContainerIOLLogger)
          - io_logger.log(entry) / write(entry) / callable(entry)
        """
        if not self._io_logger:
            return

        # Primary: ContainerIOLLogger-compatible API
        try:
            if hasattr(self._io_logger, "log_exchange"):
                self._io_logger.log_exchange(
                    container_id=str(self._container_id or "unknown"),
                    request_id=request_id,
                    method=method,
                    path=path,
                    url=url,
                    request_json=request_json,
                    status_code=status_code,
                    response=response,
                    duration_ms=duration_ms,
                    error=error,
                )
                return
        except Exception:
            # fall back below
            pass

        # Fallback: plain JSONL-ish logger (entry dict)
        entry = {
            "ts": _iso_now(),
            "container_id": self._container_id,
            "request_id": request_id,
            "method": method,
            "path": path,
            "url": url,
            "status_code": status_code,
            "duration_ms": duration_ms,
            "error": error,
            "request": _truncate_payload(request_json),
            "response": _truncate_payload(response, max_str=512),
        }
        self._emit_io_log_entry(entry)

    async def _request_json(
        self,
        method: str,
        path: str,
        *,
        json: Optional[dict[str, Any]] = None,
        request_id: Optional[str] = None,
    ) -> Any:
        headers: Optional[dict[str, str]]
        if request_id:
            headers = {"X-Request-Id": request_id}
        else:
            headers = None

        t0 = time.perf_counter()
        url = f"{self._base_url}{path}"
        try:
            resp = await self._client.request(method, path, json=json, headers=headers)
        except (httpx.TimeoutException, httpx.RequestError) as e:
            self._emit_io_log_exchange(
                request_id=request_id,
                method=method,
                path=path,
                url=url,
                request_json=json,
                status_code=None,
                response=None,
                duration_ms=int((time.perf_counter() - t0) * 1000),
                error=f"{type(e).__name__}: {e}",
            )
            raise UpstreamTransportError(f"Upstream transport error: {e}") from e

        payload = _safe_json(resp)
        self._emit_io_log_exchange(
            request_id=request_id,
            method=method,
            path=path,
            url=url,
            request_json=json,
            status_code=int(resp.status_code),
            response=payload,
            duration_ms=int((time.perf_counter() - t0) * 1000),
            error=None,
        )

        if resp.status_code == 423:
            raise UpstreamBusyError(resp.status_code, payload)
        if 400 <= resp.status_code < 500:
            raise UpstreamBadRequestError(resp.status_code, payload)
        if resp.status_code >= 500:
            raise UpstreamServerError(resp.status_code, payload)

        return payload
