from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from starlette.exceptions import HTTPException as StarletteHTTPException

from upstream_api.config import Settings, get_settings
from upstream_api.db.pool import create_pool
from upstream_api.errors import (
    ApiError,
    api_error_handler,
    http_exception_handler,
    unhandled_exception_handler,
    validation_exception_handler,
)
from upstream_api.observability.logging import configure_logging, get_logger
from upstream_api.observability.middleware import AccessLogMiddleware, RequestIdMiddleware
from upstream_api.routers import (
    accounts,
    admin,
    agents,
    alerts,
    api_tokens,
    cameras,
    dashboard,
    heartbeats,
    ingest,
    models,
    processor,
    reliability,
    rules,
    snapshots,
    stream_sessions,
    users,
    workflow_runs,
)
from upstream_api.services.http_client import build_http_client

_logger = get_logger("app")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings: Settings = app.state.settings
    pool = await create_pool(settings)
    app.state.db_pool = pool
    app.state.http_client = build_http_client()
    _logger.info("startup", environment=settings.environment, port=settings.port)
    try:
        yield
    finally:
        _logger.info("shutdown")
        await app.state.http_client.aclose()
        await pool.close()


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    configure_logging(level=settings.log_level, json_logs=settings.is_production)

    # Interactive docs (Swagger UI, ReDoc) are dev-only. The machine-readable
    # OpenAPI spec is always available for tooling (Postman, codegen, etc.).
    app = FastAPI(
        title="Upstream API",
        version="0.1.0",
        lifespan=lifespan,
        docs_url=None if settings.is_production else "/docs",
        redoc_url=None if settings.is_production else "/redoc",
        openapi_url="/openapi.json",
    )
    app.state.settings = settings

    # `allow_credentials=True` combined with `*` in methods/headers is silently
    # downgraded by browsers; enumerate the actual surface so credentialed
    # cross-origin requests work and any new method gets a deliberate review.
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.allowed_origins,
        allow_credentials=True,
        allow_methods=["GET", "POST", "DELETE"],
        allow_headers=["Authorization", "Content-Type", "X-Request-ID"],
        expose_headers=["X-Request-ID"],
    )
    app.add_middleware(AccessLogMiddleware)
    app.add_middleware(RequestIdMiddleware)

    app.add_exception_handler(ApiError, api_error_handler)
    app.add_exception_handler(StarletteHTTPException, http_exception_handler)
    app.add_exception_handler(RequestValidationError, validation_exception_handler)
    app.add_exception_handler(Exception, unhandled_exception_handler)

    app.include_router(admin.router)
    app.include_router(agents.router)
    app.include_router(cameras.router)
    app.include_router(stream_sessions.router)
    app.include_router(api_tokens.router)
    app.include_router(heartbeats.router)
    app.include_router(heartbeats.history_router)  # /v1/agents/{id}/heartbeats etc.
    app.include_router(snapshots.router)
    app.include_router(snapshots.camera_router)  # /v1/cameras/{id}/latest-frame
    app.include_router(snapshots.ingest_router)  # /v1/ingest/snapshots
    app.include_router(reliability.router)  # /v1/cameras/{id}/reliability/*
    app.include_router(ingest.router)
    app.include_router(processor.router)  # /v1/internal/processor/* (M8.0)
    app.include_router(models.router)  # /v1/models
    app.include_router(users.router)
    app.include_router(accounts.router)
    app.include_router(dashboard.router)
    app.include_router(rules.router)  # /v1/rules (Workflow Engine M1)
    app.include_router(alerts.router)  # /v1/alerts (Workflow Engine M1)
    app.include_router(workflow_runs.router)  # /v1/workflow-runs (Workflow Engine M2)

    return app
