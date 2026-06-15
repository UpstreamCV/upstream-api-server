"""Module 6 M6.4 — webhook delivery worker (HTTP send, signing, retry policy).

The DB side (enqueue/claim/record) lives in ``services/event_delivery_attempts``.
This module is the worker: it claims due attempts, signs + POSTs the canonical
event to each destination, classifies the response, schedules retries, and
records the outcome. Modeled on the existing in-process loops
(workflow_evaluator / health_sampler): lifespan-spawned, sleep-first,
log-and-continue, cancel on shutdown. Uses a dedicated httpx client so a slow
tenant endpoint can't starve the shared (WHEP/MediaMTX) pool.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

import asyncpg
import httpx

from upstream_api.config import Settings
from upstream_api.observability.logging import get_logger
from upstream_api.services.event_delivery_attempts import (
    DeliveryContext,
    claim_due_attempts,
    load_delivery_contexts,
    record_delivery_result,
)

_logger = get_logger("webhook_delivery")

_PREVIEW_CAP = 500  # response body preview chars stored
_ERROR_CAP = 500  # error message chars stored

# Retryable HTTP statuses beyond the 5xx range.
_RETRYABLE_STATUS = frozenset({408, 429})

# Retry backoff: seconds to wait before the NEXT attempt, keyed by the number of
# attempts already made. After attempt 1 fails wait 1m, then 5m, 15m, 60m.
_BACKOFF_SECONDS: dict[int, int] = {1: 60, 2: 300, 3: 900, 4: 3600}
_BACKOFF_MAX = 3600


# ---------------------------------------------------------------------------
# Pure helpers (no I/O) — unit-testable.
# ---------------------------------------------------------------------------


def compute_backoff_seconds(attempt_count: int) -> int:
    """Delay before the next attempt, given the number of attempts made so far."""
    return _BACKOFF_SECONDS.get(attempt_count, _BACKOFF_MAX)


def is_retryable_status(status_code: int) -> bool:
    return status_code in _RETRYABLE_STATUS or 500 <= status_code <= 599


def sign_payload(secret: str, timestamp: int, raw_body: bytes) -> str:
    """HMAC-SHA256 over ``<timestamp>.<raw_body>`` (the EXACT bytes sent),
    hex-encoded. The destination recomputes this with its shared secret."""
    message = f"{timestamp}.".encode() + raw_body
    return hmac.new(secret.encode(), message, hashlib.sha256).hexdigest()


def build_payload(ctx: DeliveryContext, sent_at: datetime) -> dict[str, object]:
    return {
        "delivery_id": str(ctx.attempt_id),
        "attempt": ctx.attempt_count,
        "sent_at": sent_at.isoformat(),
        "event": ctx.event.model_dump(mode="json"),
    }


def build_headers(ctx: DeliveryContext, timestamp: int, signature: str) -> dict[str, str]:
    return {
        "Content-Type": "application/json",
        "User-Agent": "Upstream-Webhooks/1.0",
        "Upstream-Event-Id": str(ctx.event.id),
        "Upstream-Event-Type": ctx.event.event_type,
        "Upstream-Delivery-Id": str(ctx.attempt_id),
        "Upstream-Timestamp": str(timestamp),
        "Upstream-Signature": f"v1={signature}",
        "Idempotency-Key": str(ctx.event.id),
    }


@dataclass(slots=True)
class DeliveryOutcome:
    status: str  # delivered | failed | dead
    response_status_code: int | None
    response_body_preview: str | None
    error_message: str | None
    next_attempt_at: datetime | None
    completed_at: datetime


def _retry_or_dead(
    ctx: DeliveryContext,
    *,
    now: datetime,
    response_status_code: int | None,
    response_body_preview: str | None,
    error: str,
) -> DeliveryOutcome:
    """A retryable failure: schedule a retry, or mark dead if exhausted.
    ``attempt_count`` was already incremented by the claim, so it is the number
    of attempts made so far including this one."""
    if ctx.attempt_count >= ctx.max_attempts:
        return DeliveryOutcome(
            status="dead",
            response_status_code=response_status_code,
            response_body_preview=response_body_preview,
            error_message=(error + " (max attempts exhausted)")[:_ERROR_CAP],
            next_attempt_at=None,
            completed_at=now,
        )
    delay = compute_backoff_seconds(ctx.attempt_count)
    return DeliveryOutcome(
        status="failed",
        response_status_code=response_status_code,
        response_body_preview=response_body_preview,
        error_message=error[:_ERROR_CAP],
        next_attempt_at=now + timedelta(seconds=delay),
        completed_at=now,
    )


# ---------------------------------------------------------------------------
# HTTP send + classify (one attempt).
# ---------------------------------------------------------------------------


async def deliver_context(
    http: httpx.AsyncClient,
    settings: Settings,
    ctx: DeliveryContext,
    *,
    now: datetime,
) -> DeliveryOutcome:
    """Sign + POST one event to one destination and classify the result. No DB
    writes — the caller persists the returned outcome."""
    timestamp = int(now.timestamp())
    payload = build_payload(ctx, now)
    # Serialize ONCE and send those exact bytes, so the signature matches.
    raw_body = json.dumps(payload, separators=(",", ":"), default=str).encode()
    signature = sign_payload(ctx.secret, timestamp, raw_body)
    headers = build_headers(ctx, timestamp, signature)
    try:
        resp = await http.post(
            ctx.url,
            content=raw_body,
            headers=headers,
            timeout=settings.webhook_delivery_timeout_seconds,
        )
    except (httpx.TimeoutException, httpx.TransportError) as exc:
        return _retry_or_dead(
            ctx,
            now=now,
            response_status_code=None,
            response_body_preview=None,
            error=f"{type(exc).__name__}: {exc}",
        )

    code = resp.status_code
    body_preview = (resp.text or "")[:_PREVIEW_CAP]
    if 200 <= code < 300:
        return DeliveryOutcome(
            status="delivered",
            response_status_code=code,
            response_body_preview=body_preview,
            error_message=None,
            next_attempt_at=None,
            completed_at=now,
        )
    if is_retryable_status(code):
        return _retry_or_dead(
            ctx,
            now=now,
            response_status_code=code,
            response_body_preview=body_preview,
            error=f"HTTP {code}",
        )
    # Most 4xx: permanent — do not retry.
    return DeliveryOutcome(
        status="dead",
        response_status_code=code,
        response_body_preview=body_preview,
        error_message=f"HTTP {code} (non-retryable)",
        next_attempt_at=None,
        completed_at=now,
    )


# ---------------------------------------------------------------------------
# Drain + loop.
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class DrainStats:
    claimed: int = 0
    delivered: int = 0
    failed: int = 0
    dead: int = 0
    errors: int = 0
    _by_status: dict[str, int] = field(default_factory=dict)

    def bump(self, status: str) -> None:
        if status == "delivered":
            self.delivered += 1
        elif status == "failed":
            self.failed += 1
        elif status == "dead":
            self.dead += 1


async def drain_due_deliveries(
    pool: asyncpg.Pool,
    http: httpx.AsyncClient,
    settings: Settings,
) -> DrainStats:
    """One drain cycle: claim a batch, deliver each, record outcomes. Safe under
    concurrent workers (claim uses FOR UPDATE SKIP LOCKED)."""
    enc_key = settings.webhook_secret_enc_key
    if enc_key is None:
        _logger.warning("webhook_delivery_skipped_no_enc_key")
        return DrainStats()

    async with pool.acquire() as conn, conn.transaction():
        claimed = await claim_due_attempts(conn, limit=settings.webhook_delivery_batch_size)
    if not claimed:
        return DrainStats()

    attempt_ids = [c["id"] for c in claimed]
    async with pool.acquire() as conn:
        contexts = await load_delivery_contexts(
            conn, attempt_ids=attempt_ids, enc_key=enc_key.get_secret_value()
        )

    stats = DrainStats(claimed=len(claimed))
    for ctx in contexts:
        try:
            outcome = await deliver_context(http, settings, ctx, now=datetime.now(UTC))
            async with pool.acquire() as conn:
                await record_delivery_result(
                    conn,
                    attempt_id=ctx.attempt_id,
                    destination_id=ctx.destination_id,
                    status=outcome.status,
                    response_status_code=outcome.response_status_code,
                    response_body_preview=outcome.response_body_preview,
                    error_message=outcome.error_message,
                    next_attempt_at=outcome.next_attempt_at,
                    completed_at=outcome.completed_at,
                )
            stats.bump(outcome.status)
        except Exception as exc:  # one bad delivery must not stop the batch
            stats.errors += 1
            _logger.warning(
                "webhook_delivery_attempt_failed",
                attempt_id=str(ctx.attempt_id),
                error=type(exc).__name__,
            )
    return stats


async def webhook_delivery_loop(
    pool: asyncpg.Pool,
    http: httpx.AsyncClient,
    settings: Settings,
) -> None:
    """In-process drain loop, spawned from lifespan when enabled. Sleeps first
    so a crash-restart can't hot-loop; a failed cycle is logged and the loop
    continues; cancels cleanly on shutdown."""
    interval = settings.webhook_delivery_interval_seconds
    while True:
        await asyncio.sleep(interval)
        try:
            stats = await drain_due_deliveries(pool, http, settings)
            if stats.claimed or stats.errors:
                _logger.info(
                    "webhook_delivery_tick",
                    claimed=stats.claimed,
                    delivered=stats.delivered,
                    failed=stats.failed,
                    dead=stats.dead,
                    errors=stats.errors,
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _logger.warning("webhook_delivery_tick_failed", error=type(exc).__name__)
