#copyright "Kilax @kilax9276"
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Optional

import httpx


class ServiceError(RuntimeError):
    pass


class ServiceTransportError(ServiceError):
    pass


class ServiceBadResponseError(ServiceError):
    pass


@dataclass(frozen=True)
class ServiceSpec:
    service_id: str
    base_url: str
    timeout_seconds: float = 10.0


class HttpServicePlugin:
    """
    HTTP-плагин для внешних сервисов.

    Контракты:
      POST {base_url}/preprocess  -> JSON dict (опционально):
        - prompt_id_override: str
        - fanout_override: int
        - text_override: str

      POST {base_url}/postprocess -> JSON dict (опционально):
        - final_text_override: str

    Ошибки сети/таймауты -> ServiceTransportError.
    Невалидный JSON/тип -> ServiceBadResponseError.
    """

    def __init__(
        self,
        *,
        services: dict[str, ServiceSpec],
        connect_timeout_seconds: float = 5.0,
        read_timeout_seconds: Optional[float] = None,
    ) -> None:
        self._services = services
        self._connect_timeout = float(connect_timeout_seconds)
        self._read_timeout = float(read_timeout_seconds) if read_timeout_seconds is not None else None

        # Один клиент на все сервисы (base_url различаются => полный URL в request)
        timeout = httpx.Timeout(
            connect=self._connect_timeout,
            read=self._read_timeout if self._read_timeout is not None else 30.0,
            write=30.0,
            pool=30.0,
        )
        self._client = httpx.AsyncClient(timeout=timeout)

    async def aclose(self) -> None:
        await self._client.aclose()

    def _service(self, service_id: str) -> ServiceSpec:
        spec = self._services.get(service_id)
        if spec is None:
            raise ServiceError(f"Unknown service_id={service_id}")
        return spec

    async def call_preprocess(self, service_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        spec = self._service(service_id)
        url = spec.base_url.rstrip("/") + "/preprocess"
        return await self._post_json(url, payload, service_timeout_seconds=spec.timeout_seconds)

    async def call_postprocess(self, service_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        spec = self._service(service_id)
        url = spec.base_url.rstrip("/") + "/postprocess"
        return await self._post_json(url, payload, service_timeout_seconds=spec.timeout_seconds)

    async def _post_json(self, url: str, payload: dict[str, Any], *, service_timeout_seconds: float) -> dict[str, Any]:
        # На уровне конкретного запроса можем переопределить read timeout
        per_req_timeout = httpx.Timeout(
            connect=self._connect_timeout,
            read=float(service_timeout_seconds),
            write=30.0,
            pool=30.0,
        )

        try:
            r = await self._client.post(url, json=payload, timeout=per_req_timeout)
        except (httpx.TimeoutException, httpx.TransportError) as e:
            raise ServiceTransportError(str(e)) from e

        # 4xx/5xx: считаем ошибкой сервиса (но executor может сделать fail-soft)
        if r.status_code >= 400:
            body = r.text
            msg = f"Service HTTP {r.status_code} for {url}: {body[:500]}"
            raise ServiceError(msg)

        try:
            data = r.json()
        except json.JSONDecodeError as e:
            raise ServiceBadResponseError(f"Invalid JSON from service {url}: {r.text[:500]}") from e

        if not isinstance(data, dict):
            raise ServiceBadResponseError(f"Service {url} must return JSON object, got {type(data).__name__}")

        return data
