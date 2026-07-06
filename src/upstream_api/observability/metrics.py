"""Prometheus metrics for the api-server control plane (M9.3, design doc 01 §3).

All collectors register against a module-level ``CollectorRegistry`` instead
of ``prometheus_client.REGISTRY`` — tests build many app instances (and may
re-import modules) within one process, and the process-global default
registry would raise duplicate-timeseries errors on re-registration.
``GET /metrics`` (routers/metrics.py) exposes exactly this registry.
The registry is per-process (single-instance deployment posture); a
multi-worker deployment would need prometheus_client's multiprocess collector.

Call sites stay one-line via the ``record_*`` helpers and the
``track_ingest`` decorator; nothing here touches the DB or blocks.
"""

from __future__ import annotations

import functools
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, ParamSpec, TypeVar

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

P = ParamSpec("P")
R = TypeVar("R")

registry = CollectorRegistry()

# Wall-clock process start, for the admin panel's "counters since restart"
# framing. Module import time ≈ process start (the app imports this module
# during startup), which is close enough for an uptime readout.
process_started_at = time.time()

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

# M10.1 — session sweeper actions per tick. Actions: expired_approved /
# expired_streaming (expiry clause enforce), would_expire (expiry report
# mode), would_sweep_stale (staleness report mode), swept_stale (staleness
# enforce).
sweeper_actions_total = Counter(
    "sweeper_actions_total",
    "Session-maintenance sweeper actions.",
    labelnames=("action",),
    registry=registry,
)

# M10.2 — MediaMTX path drift observed by the restart reconciler:
# kind=missing (live session, no MediaMTX path — re-provisioned) and
# kind=extra (sess_* path with no live session — logged only).
mediamtx_path_drift = Gauge(
    "mediamtx_path_drift",
    "Per-reconcile-tick drift between live sessions and MediaMTX paths.",
    labelnames=("kind",),
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


def record_sweeper_action(action: str, count: int = 1) -> None:
    if count > 0:
        sweeper_actions_total.labels(action=action).inc(count)


def record_path_drift(*, missing: int, extra: int) -> None:
    mediamtx_path_drift.labels(kind="missing").set(missing)
    mediamtx_path_drift.labels(kind="extra").set(extra)


@dataclass(frozen=True, slots=True)
class TransitionRecord:
    """Deferred metric facts for one session state-machine transition.

    Services that write a transition inside a caller-owned DB transaction
    return one of these instead of incrementing counters inline: a counter
    bumped inside the transaction survives a rollback (a phantom transition),
    and the webhook retry then counts the same transition twice. The caller
    calls ``record()`` exactly once, just after its ``conn.transaction()``
    block exits (i.e. after COMMIT).

    The default-constructed instance (``NO_TRANSITION``) is a no-op so
    idempotent paths (reconnects, already-terminal sessions) keep the call
    site a one-liner.
    """

    from_state: str | None = None
    to_state: str | None = None
    time_to_streaming_seconds: float | None = None

    def record(self) -> None:
        if self.from_state is not None and self.to_state is not None:
            record_transition(self.from_state, self.to_state)
        if self.time_to_streaming_seconds is not None:
            observe_time_to_streaming(self.time_to_streaming_seconds)


NO_TRANSITION = TransitionRecord()


def _collect_samples(metric_name: str) -> list[tuple[dict[str, str], float]]:
    """Return (labels, value) pairs for one metric family in the registry.

    prometheus_client strips the ``_total`` suffix from counter family
    names, so match on the family and filter to the value-carrying sample
    names (``_total`` for counters, exact name for gauges, ``_bucket``
    handled separately by the histogram reader).
    """
    family = metric_name.removesuffix("_total")
    out: list[tuple[dict[str, str], float]] = []
    for metric in registry.collect():
        if metric.name != family:
            continue
        for sample in metric.samples:
            if sample.name in (metric_name, family):
                out.append((dict(sample.labels), sample.value))
    return out


def _gauge_value(metric_name: str) -> float:
    samples = _collect_samples(metric_name)
    return samples[0][1] if samples else 0.0


def _counter_by_label(metric_name: str, label: str) -> dict[str, int]:
    totals: dict[str, int] = {}
    for labels, value in _collect_samples(metric_name):
        key = labels.get(label, "")
        totals[key] = totals.get(key, 0) + int(value)
    return totals


def _histogram_read(metric_name: str) -> tuple[int, list[tuple[float, float]]]:
    """Return (count, cumulative buckets sorted by le) for a histogram."""
    count = 0
    buckets: list[tuple[float, float]] = []
    for metric in registry.collect():
        if metric.name != metric_name:
            continue
        for sample in metric.samples:
            if sample.name == f"{metric_name}_count":
                count = int(sample.value)
            elif sample.name == f"{metric_name}_bucket":
                buckets.append((float(sample.labels["le"]), sample.value))
    buckets.sort(key=lambda b: b[0])
    return count, buckets


def _histogram_quantile(buckets: list[tuple[float, float]], q: float) -> float | None:
    """Estimate a quantile from cumulative histogram buckets (Prometheus-style
    linear interpolation). Returns the lower bound of the +Inf bucket when the
    quantile lands there — a floor, not a fabricated point estimate."""
    if not buckets:
        return None
    total = buckets[-1][1]
    if total <= 0:
        return None
    target = q * total
    prev_le, prev_count = 0.0, 0.0
    for le, count in buckets:
        if count >= target:
            if le == float("inf"):
                return prev_le
            span = count - prev_count
            if span <= 0:
                return le
            return prev_le + (le - prev_le) * ((target - prev_count) / span)
        prev_le, prev_count = le, count
    return None


def snapshot() -> dict[str, Any]:
    """Parsed point-in-time view of the registry for /v1/admin/system/metrics.

    Counters are process-lifetime (they reset on restart) — the response
    carries ``process_uptime_seconds`` so the panel can say "since restart"
    honestly. Reads the same registry GET /metrics serves; no DB, no I/O.
    """
    ingest = _counter_by_label("ingest_webhook_total", "outcome")
    auth = _counter_by_label("mediamtx_auth_total", "decision")
    transitions = _counter_by_label("session_transitions_total", "to_state")
    tts_count, tts_buckets = _histogram_read("session_start_to_streaming_seconds")
    return {
        "process_uptime_seconds": max(0.0, time.time() - process_started_at),
        "ingest_webhooks": {
            "ok": ingest.get("ok", 0),
            "client_error": ingest.get("client_error", 0),
            "server_error": ingest.get("server_error", 0),
        },
        "stream_auth": {
            "allowed": auth.get("allow", 0),
            "denied": auth.get("deny", 0),
        },
        "session_transitions_total": sum(transitions.values()),
        "sessions_failed_total": transitions.get("failed", 0),
        "time_to_streaming": {
            "count": tts_count,
            "p50_seconds": _histogram_quantile(tts_buckets, 0.50),
            "p95_seconds": _histogram_quantile(tts_buckets, 0.95),
        },
        "db_pool": {
            "size": int(_gauge_value("db_pool_size")),
            "in_use": int(_gauge_value("db_pool_in_use")),
        },
    }


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
