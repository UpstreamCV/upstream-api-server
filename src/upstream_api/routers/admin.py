from __future__ import annotations

import asyncio

import asyncpg
from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse

from upstream_api import __version__
from upstream_api.db.pool import get_pool

router = APIRouter(tags=["admin"])


@router.get("/")
async def root(request: Request) -> dict[str, str]:
    """Service-info landing page for anyone who navigates to the bare host."""
    settings = request.app.state.settings
    return {
        "service": "upstream-api",
        "version": __version__,
        "environment": settings.environment,
        "docs": "/docs",
        "openapi": "/openapi.json",
        "health": "/healthz",
        "ready": "/readyz",
    }


@router.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/readyz")
async def readyz(pool: asyncpg.Pool = Depends(get_pool)) -> JSONResponse:
    try:
        async with asyncio.timeout(0.5):
            async with pool.acquire() as conn:
                await conn.fetchval("select 1")
    except (TimeoutError, Exception) as exc:
        return JSONResponse(
            status_code=503,
            content={"status": "not_ready", "error": str(exc)},
        )
    return JSONResponse(status_code=200, content={"status": "ok"})
