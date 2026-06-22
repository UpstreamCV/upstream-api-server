from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
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
    audit_logs,
    billing,
    cameras,
    dashboard,
    event_delivery_attempts,
    event_destinations,
    events,
    heartbeats,
    ingest,
    metrics,
    models,
    platform_admin,
    processing,
    processing_evidence,
    processor,
    reliability,
    reliability_ingest,
    reliability_issues,
    rules,
    snapshots,
    stream_sessions,
    stripe_webhooks,
    usage,
    users,
    workflow_evaluator,
    workflow_runs,
    workflows,
)
from upstream_api.services.health_sampler import health_sampler_loop
from upstream_api.services.http_client import build_http_client
from upstream_api.services.webhook_delivery import webhook_delivery_loop
from upstream_api.services.workflow_evaluator import workflow_evaluator_loop

_logger = get_logger("app")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings: Settings = app.state.settings
    pool = await create_pool(settings)
    app.state.db_pool = pool
    app.state.http_client = build_http_client()
    # Workflow Engine M4: in-process time-based rule evaluator. Gated by
    # config (0 disables — external cron can drive the internal endpoint
    # instead). Single-instance deployments only; see
    # services/workflow_evaluator.py for the duplicate-prevention posture.
    evaluator_task: asyncio.Task[None] | None = None
    if settings.workflow_evaluator_interval_seconds > 0:
        evaluator_task = asyncio.create_task(
            workflow_evaluator_loop(pool, settings.workflow_evaluator_interval_seconds)
        )
        _logger.info(
            "workflow_evaluator_loop_started",
            interval_seconds=settings.workflow_evaluator_interval_seconds,
        )
    # Admin-panel health history: server-side sampler so the panel's graphs
    # read persisted history instead of collecting client-side. Same gated
    # single-instance posture as the evaluator.
    sampler_task: asyncio.Task[None] | None = None
    if settings.health_sample_interval_seconds > 0:
        sampler_task = asyncio.create_task(
            health_sampler_loop(
                pool,
                app.state.http_client,
                settings,
                settings.health_sample_interval_seconds,
            )
        )
        _logger.info(
            "health_sampler_loop_started",
            interval_seconds=settings.health_sample_interval_seconds,
        )
    # Module 6 M6.4: in-process webhook delivery worker. Defaults OFF — enable
    # in prod with UPSTREAM_WEBHOOK_DELIVERY_ENABLED=true. Uses a dedicated
    # httpx client (tight timeout) so a slow tenant endpoint can't starve the
    # shared WHEP/MediaMTX pool. Multi-worker safe via FOR UPDATE SKIP LOCKED.
    webhook_delivery_task: asyncio.Task[None] | None = None
    webhook_http_client: httpx.AsyncClient | None = None
    if settings.webhook_delivery_enabled and settings.webhook_delivery_interval_seconds > 0:
        webhook_http_client = httpx.AsyncClient(
            timeout=httpx.Timeout(settings.webhook_delivery_timeout_seconds)
        )
        app.state.webhook_http_client = webhook_http_client
        webhook_delivery_task = asyncio.create_task(
            webhook_delivery_loop(pool, webhook_http_client, settings)
        )
        _logger.info(
            "webhook_delivery_loop_started",
            interval_seconds=settings.webhook_delivery_interval_seconds,
            batch_size=settings.webhook_delivery_batch_size,
        )
    _logger.info("startup", environment=settings.environment, port=settings.port)
    try:
        yield
    finally:
        _logger.info("shutdown")
        for task in (evaluator_task, sampler_task, webhook_delivery_task):
            if task is not None:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
        await app.state.http_client.aclose()
        if webhook_http_client is not None:
            await webhook_http_client.aclose()
        await pool.close()


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    configure_logging(level=settings.log_level, json_logs=settings.is_production)

    # M9.3 — Sentry error tracking. No-op when the DSN is unset (local dev,
    # CI). send_default_pii=False: request bodies, headers with credentials,
    # and user identifiers never leave the box.
    if settings.sentry_dsn:
        import sentry_sdk

        sentry_sdk.init(
            dsn=settings.sentry_dsn,
            environment=settings.environment,
            send_default_pii=False,
        )

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
    app.include_router(reliability_ingest.router)  # /v1/reliability/observations (M1a)
    app.include_router(reliability_issues.router)  # /v1/reliability/issues (M1a)
    app.include_router(ingest.router)
    app.include_router(processor.router)  # /v1/internal/processor/* (M8.0)
    app.include_router(models.router)  # /v1/models
    app.include_router(processing.router)  # /v1/processing/packages (P5-F catalog)
    app.include_router(
        processing_evidence.router
    )  # /v1/cameras/{id}/processing/evidence (P5-J)
    app.include_router(users.router)
    app.include_router(accounts.router)
    app.include_router(dashboard.router)
    app.include_router(rules.router)  # /v1/rules (Workflow Engine M1)
    app.include_router(alerts.router)  # /v1/alerts (Workflow Engine M1)
    app.include_router(workflow_runs.router)  # /v1/workflow-runs (Workflow Engine M2)
    app.include_router(events.router)  # /v1/events (Module 6 M6.2)
    app.include_router(event_destinations.router)  # /v1/event-destinations (Module 6 M6.3)
    app.include_router(event_delivery_attempts.router)  # /v1/event-delivery-attempts (M6.4)
    app.include_router(workflow_evaluator.router)  # /v1/internal/workflow-evaluator (M4)
    app.include_router(workflows.router)  # /v1/workflows (Workflow Engine M5)
    app.include_router(usage.router)  # /v1/usage (Module 7 M1)
    app.include_router(audit_logs.router)  # /v1/audit-logs (Module 7 M3)
    app.include_router(billing.router)  # /v1/billing (Module 7 M2)
    app.include_router(stripe_webhooks.router)  # /v1/webhooks/stripe (Module 7 M2)
    app.include_router(platform_admin.router)  # /v1/admin/* (admin panel)
    app.include_router(metrics.router)  # GET /metrics (M9.3 observability)

    return app
