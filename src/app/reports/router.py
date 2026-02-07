from __future__ import annotations

from datetime import datetime
from typing import Any

from fastapi import APIRouter, Query

from ..storage import get_default_storage
from . import queries

router = APIRouter(prefix="/v1/reports", tags=["reports"])


def _iso(s: str) -> str:
    # Accept offset-naive or offset-aware; store as string.
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        # treat as UTC
        return dt.replace(tzinfo=None).isoformat()
    return dt.isoformat()


@router.get("/containers")
async def report_containers(
    from_: str = Query(..., alias="from"),
    to: str = Query(..., alias="to"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> dict[str, Any]:
    st = get_default_storage()
    return queries.containers_usage(st, date_from=_iso(from_), date_to=_iso(to), limit=limit, offset=offset)


@router.get("/profiles")
async def report_profiles(
    from_: str = Query(..., alias="from"),
    to: str = Query(..., alias="to"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> dict[str, Any]:
    st = get_default_storage()
    return queries.profiles_usage(st, date_from=_iso(from_), date_to=_iso(to), limit=limit, offset=offset)


@router.get("/prompts")
async def report_prompts(
    from_: str = Query(..., alias="from"),
    to: str = Query(..., alias="to"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> dict[str, Any]:
    st = get_default_storage()
    return queries.prompts_usage(st, date_from=_iso(from_), date_to=_iso(to), limit=limit, offset=offset)
