from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, Field


class SolveInput(BaseModel):
    text: Optional[str] = None
    image_b64: Optional[str] = None
    image_path: Optional[str] = None
    image_ext: Optional[str] = None


class SolveOptions(BaseModel):
    prompt_id: str = "default"
    profile_id: Optional[str] = None
    socks_override: Optional[str] = None
    socks_id: Optional[str] = None  # legacy alias
    force_new_chat: bool = False
    max_chat_uses: Optional[int] = None
    include_debug: bool = False
    chat_url: Optional[str] = None


class SolveRequest(BaseModel):
    prompt_id: Optional[str] = None
    request_id: Optional[str] = None
    input: SolveInput
    options: SolveOptions = Field(default_factory=SolveOptions)


class SolveError(BaseModel):
    code: str
    message: str
    details: Optional[Any] = None


class SolveFinal(BaseModel):
    text: str


class SolveAttempt(BaseModel):
    container_id: str
    status: str
    result_text: Optional[str] = None
    error_code: Optional[str] = None
    error_message: Optional[str] = None


class SolveResponse(BaseModel):
    ok: bool
    final: Optional[SolveFinal] = None
    error: Optional[SolveError] = None
    attempts: Optional[list[SolveAttempt]] = None
    meta: dict[str, Any] = Field(default_factory=dict)


class StatusResponse(BaseModel):
    ok: bool
    status: dict[str, Any] = Field(default_factory=dict)


class ChatLockRequest(BaseModel):
    chat_url: str
    locked_by: str
    ttl_seconds: int = 600


class ChatUnlockRequest(BaseModel):
    chat_url: str
    locked_by: str
