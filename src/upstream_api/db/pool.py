from __future__ import annotations

import asyncpg
from fastapi import Request

from upstream_api.config import Settings


async def create_pool(settings: Settings) -> asyncpg.Pool:
    return await asyncpg.create_pool(
        dsn=settings.database_url.get_secret_value(),
        min_size=settings.db_pool_min_size,
        max_size=settings.db_pool_max_size,
        command_timeout=10,
        server_settings={"application_name": "upstream-api"},
    )


def get_pool(request: Request) -> asyncpg.Pool:
    pool = getattr(request.app.state, "db_pool", None)
    if pool is None:
        raise RuntimeError("Database pool not initialized on app.state.db_pool")
    return pool
