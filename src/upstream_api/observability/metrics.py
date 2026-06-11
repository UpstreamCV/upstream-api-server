"""Prometheus metrics for the api-server control plane (M9.3, design doc 01 §3).

All collectors register against a module-level ``CollectorRegistry`` instead
of ``prometheus_client.REGISTRY`` — tests build many app instances (and may
re-import modules) within one process, and the process-global default
registry would raise duplicate-timeseries errors on re-registration.
``GET /metrics`` (routers/metrics.py) exposes exactly this registry.

Call sites stay one-line via the ``record_*`` helpers and the
``track_ingest`` decorator; nothing here touches the DB or blocks.
"""

from __future__ import annotations

import functools
from collections.abc import Awaitable, Callable
from typing import ParamSpec, TypeVar

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

P = ParamSpec("P")
R = TypeVar("R")

registry = CollectorRegistry()

ingest_webhook_total = Counter(
    "ingest_webhook_total",
    "Ingest webhook handler results by endpoint and HTTP outcome.",
    labelnames=("endpoint", "outcome"),
    registry=registry,
)

mediamtx_auth_total = Counter(
    "mediamtx_auth_total",
    "MediaMTX authHTTP gate decisions on the SRT publish path.",
    labelnames=("decision",),
    registry=registry,
)

session_transitions_total = Counter(
    "session_transitions_total",
    "Stream-session state-machine transitions.",
    labelnames=("from_state", "to_state"),
    registry=registry,
)

# Buckets bracket the time-to-streaming SLI (doc 01 §5: p95 < 8 s) with room
# on both sides so regressions stay visible instead of saturating +Inf.
session_start_to_streaming_seconds = Histogram(
    "session_start_to_streaming_seconds",
    "Seconds from session creation (/start) to the stream-started webhook.",
    buckets=(0.5, 1.0, 2.0, 4.0, 8.0, 15.0, 30.0, 60.0),
    registry=registry,
)

db_pool_size = Gauge(
    "db_pool_size",
    "asyncpg pool size (open connections).",
    registry=registry,
)

db_pool_in_use = Gauge(
    "db_pool_in_use",
    "asyncpg pool connections currently checked out.",
    registry=registry,
)


def record_ingest(endpoint: str, outcome: str) -> None:
    ingest_webhook_total.labels(endpoint=endpoint, outcome=outcome).inc()


def record_auth(decision: str) -> None:
    mediamtx_auth_total.labels(decision=decision).inc()


def record_transition(frm: str, to: str) -> None:
    session_transitions_total.labels(from_state=frm, to_state=to).inc()


def observe_time_to_streaming(seconds: float) -> None:
    session_start_to_streaming_seconds.observe(seconds)


def _outcome_from_status(status_code: int) -> str:
    if status_code >= 500:
        return "server_error"
    if status_code >= 400:
        return "client_error"
    return "ok"


def track_ingest(
    endpoint: str,
) -> Callable[[Callable[P, Awaitable[R]]], Callable[P, Awaitable[R]]]:
    """Record ``ingest_webhook_total{endpoint, outcome}`` around an async handler.

    Outcome is classified from the HTTP status: a returned ``Response`` (or a
    raised ``ApiError``/``HTTPException``) carries ``status_code``; plain
    model returns count as 200 → ``ok``; any other exception counts as
    ``server_error``. Failures raised by router *dependencies* (e.g. the
    webhook-secret check) happen before the handler runs and are deliberately
    not counted here — the counter measures handler results, not auth churn.

    ``functools.wraps`` preserves ``__wrapped__`` so FastAPI's signature
    inspection still sees the original parameters for dependency injection.
    """

    def decorator(func: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
        @functools.wraps(func)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            try:
                result = await func(*args, **kwargs)
            except Exception as exc:
                record_ingest(endpoint, _outcome_from_status(getattr(exc, "status_code", 500)))
                raise
            record_ingest(endpoint, _outcome_from_status(getattr(result, "status_code", 200)))
            return result

        return wrapper

    return decorator
