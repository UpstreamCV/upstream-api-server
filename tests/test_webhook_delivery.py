"""Module 6 M6.4 — webhook delivery worker tests.

Pure helpers (backoff, signing, retry classification), one-attempt delivery via
an httpx MockTransport (success / retryable / permanent / network / timeout),
and the drain cycle end-to-end against a fake pool (status + destination health).
"""

from __future__ import annotations

import hashlib
import hmac
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest

from upstream_api.config import get_settings
from upstream_api.schemas.events import CanonicalEvent
from upstream_api.services.event_delivery_attempts import DeliveryContext
from upstream_api.services.webhook_delivery import (
    compute_backoff_seconds,
    deliver_context,
    drain_due_deliveries,
    is_retryable_status,
    sign_payload,
)

_NOW = datetime(2026, 6, 20, 12, 0, 0, tzinfo=UTC)
_SECRET = "whsec_test_ABCDEFGHIJKLMNOPQRSTUV"


def _event(account: UUID, **overrides: Any) -> CanonicalEvent:
    base: dict[str, Any] = {
        "id": uuid4(),
        "seq": 1,
        "event_type": "alert.created",
        "schema_version": 1,
        "account_id": account,
        "camera_id": uuid4(),
        "session_id": None,
        "source": "rule",
        "workflow_run_id": uuid4(),
        "rule_id": uuid4(),
        "dedupe_key": "k1",
        "occurred_at": _NOW,
        "created_at": _NOW,
        "payload": {"status": "completed"},
        "evidence": {"zone_id": "z-1"},
    }
    base.update(overrides)
    return CanonicalEvent(**base)


def _ctx(
    account: UUID,
    *,
    attempt_count: int = 1,
    max_attempts: int = 5,
    url: str = "https://hook.example.com/x",
) -> DeliveryContext:
    return DeliveryContext(
        attempt_id=uuid4(),
        account_id=account,
        destination_id=uuid4(),
        attempt_count=attempt_count,
        max_attempts=max_attempts,
        url=url,
        secret=_SECRET,
        event=_event(account),
    )


def _client(handler: Any) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


# ---------------------------------------------------------------------------
# Pure helpers.
# ---------------------------------------------------------------------------


def test_backoff_schedule() -> None:
    assert compute_backoff_seconds(1) == 60
    assert compute_backoff_seconds(2) == 300
    assert compute_backoff_seconds(3) == 900
    assert compute_backoff_seconds(4) == 3600
    assert compute_backoff_seconds(9) == 3600  # capped


@pytest.mark.parametrize(
    ("code", "retryable"),
    [(408, True), (429, True), (500, True), (503, True), (599, True),
     (400, False), (401, False), (403, False), (404, False), (422, False)],
)
def test_is_retryable_status(code: int, retryable: bool) -> None:
    assert is_retryable_status(code) is retryable


def test_signature_is_deterministic_over_exact_bytes() -> None:
    body = b'{"event":{"id":"x"}}'
    ts = 1_700_000_000
    expected = hmac.new(_SECRET.encode(), f"{ts}.".encode() + body, hashlib.sha256).hexdigest()
    assert sign_payload(_SECRET, ts, body) == expected
    # Different body -> different signature (binds the exact bytes).
    assert sign_payload(_SECRET, ts, body + b" ") != expected


# ---------------------------------------------------------------------------
# Single-attempt delivery via MockTransport.
# ---------------------------------------------------------------------------


async def test_delivery_success_marks_delivered_and_sends_signed_headers() -> None:
    account = uuid4()
    ctx = _ctx(account)
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["headers"] = request.headers
        captured["body"] = request.content
        return httpx.Response(200, text="ok")

    async with _client(handler) as http:
        outcome = await deliver_context(http, get_settings(), ctx, now=_NOW)

    assert outcome.status == "delivered"
    assert outcome.response_status_code == 200
    h = captured["headers"]
    assert h["Upstream-Event-Id"] == str(ctx.event.id)
    assert h["Upstream-Event-Type"] == "alert.created"
    assert h["Upstream-Delivery-Id"] == str(ctx.attempt_id)
    assert h["Idempotency-Key"] == str(ctx.event.id)
    assert h["User-Agent"] == "Upstream-Webhooks/1.0"
    assert h["Upstream-Signature"].startswith("v1=")
    # The signature verifies against the EXACT bytes sent + the timestamp header.
    ts = int(h["Upstream-Timestamp"])
    signed_message = f"{ts}.".encode() + captured["body"]
    expected = hmac.new(_SECRET.encode(), signed_message, hashlib.sha256).hexdigest()
    assert h["Upstream-Signature"] == f"v1={expected}"


async def test_delivery_5xx_is_retryable_failed_with_backoff() -> None:
    account = uuid4()
    ctx = _ctx(account, attempt_count=1, max_attempts=5)

    async with _client(lambda req: httpx.Response(503, text="busy")) as http:
        outcome = await deliver_context(http, get_settings(), ctx, now=_NOW)

    assert outcome.status == "failed"
    assert outcome.response_status_code == 503
    assert outcome.next_attempt_at is not None
    assert (outcome.next_attempt_at - _NOW).total_seconds() == 60  # backoff(1)


@pytest.mark.parametrize("code", [408, 429])
async def test_delivery_408_429_retry(code: int) -> None:
    ctx = _ctx(uuid4(), attempt_count=1, max_attempts=5)
    async with _client(lambda req: httpx.Response(code)) as http:
        outcome = await deliver_context(http, get_settings(), ctx, now=_NOW)
    assert outcome.status == "failed"


async def test_delivery_4xx_is_permanent_dead() -> None:
    ctx = _ctx(uuid4())
    async with _client(lambda req: httpx.Response(400, text="bad")) as http:
        outcome = await deliver_context(http, get_settings(), ctx, now=_NOW)
    assert outcome.status == "dead"
    assert outcome.next_attempt_at is None


async def test_delivery_network_error_is_retryable() -> None:
    ctx = _ctx(uuid4(), attempt_count=1, max_attempts=5)

    def boom(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    async with _client(boom) as http:
        outcome = await deliver_context(http, get_settings(), ctx, now=_NOW)
    assert outcome.status == "failed"
    assert outcome.response_status_code is None
    assert outcome.error_message is not None


async def test_delivery_timeout_is_retryable() -> None:
    ctx = _ctx(uuid4(), attempt_count=1, max_attempts=5)

    def slow(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("timed out", request=request)

    async with _client(slow) as http:
        outcome = await deliver_context(http, get_settings(), ctx, now=_NOW)
    assert outcome.status == "failed"


async def test_delivery_dead_when_attempts_exhausted() -> None:
    # attempt_count already == max_attempts after the claim's increment.
    ctx = _ctx(uuid4(), attempt_count=5, max_attempts=5)
    async with _client(lambda req: httpx.Response(500)) as http:
        outcome = await deliver_context(http, get_settings(), ctx, now=_NOW)
    assert outcome.status == "dead"
    assert outcome.next_attempt_at is None


async def test_response_body_preview_is_capped() -> None:
    ctx = _ctx(uuid4())
    huge = "x" * 5000
    async with _client(lambda req: httpx.Response(500, text=huge)) as http:
        outcome = await deliver_context(http, get_settings(), ctx, now=_NOW)
    assert outcome.response_body_preview is not None
    assert len(outcome.response_body_preview) <= 500


# ---------------------------------------------------------------------------
# Drain cycle against a fake pool.
# ---------------------------------------------------------------------------


class _Txn:
    async def __aenter__(self) -> _Txn:
        return self

    async def __aexit__(self, *_exc: Any) -> None:
        return None


class FakeDeliveryConn:
    def __init__(self, *, claim_rows: list[dict[str, Any]], contexts: list[dict[str, Any]]) -> None:
        self._claim_rows = claim_rows
        self._contexts = contexts
        self.results: list[tuple[Any, ...]] = []
        self.health_success: list[UUID] = []
        self.health_failure: list[UUID] = []

    def transaction(self) -> _Txn:
        return _Txn()

    async def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        if "for update skip locked" in query.lower():
            limit = args[0]
            return self._claim_rows[:limit]
        if "pgp_sym_decrypt" in query:
            ids = set(args[0])
            return [c for c in self._contexts if c["attempt_id"] in ids]
        raise AssertionError(f"unexpected fetch: {query}")

    async def execute(self, query: str, *args: Any) -> str:
        if "update event_delivery_attempts" in query:
            # args: (attempt_id, status, code, preview, err, next_at, completed)
            self.results.append(args)
        elif "last_success_at = now()" in query:
            self.health_success.append(args[0])
        elif "last_failure_at = now()" in query:
            self.health_failure.append(args[0])
        else:
            raise AssertionError(f"unexpected execute: {query}")
        return "UPDATE 1"


class AcquireCtx:
    def __init__(self, conn: FakeDeliveryConn) -> None:
        self.conn = conn

    async def __aenter__(self) -> FakeDeliveryConn:
        return self.conn

    async def __aexit__(self, *_exc: Any) -> None:
        return None


class FakePool:
    def __init__(self, conn: FakeDeliveryConn) -> None:
        self.conn = conn

    def acquire(self) -> AcquireCtx:
        return AcquireCtx(self.conn)


def _claim_row(
    account: UUID, attempt_id: UUID, dest_id: UUID, *, attempt_count: int = 1
) -> dict[str, Any]:
    return {
        "id": attempt_id,
        "account_id": account,
        "event_id": uuid4(),
        "destination_id": dest_id,
        "attempt_count": attempt_count,
        "max_attempts": 5,
    }


def _context_row(
    account: UUID, attempt_id: UUID, dest_id: UUID, url: str, *, attempt_count: int = 1
) -> dict[str, Any]:
    return {
        "attempt_id": attempt_id,
        "account_id": account,
        "destination_id": dest_id,
        "attempt_count": attempt_count,
        "max_attempts": 5,
        "url": url,
        "secret": _SECRET,
        "e_id": uuid4(),
        "seq": 7,
        "event_type": "alert.created",
        "schema_version": 1,
        "camera_id": uuid4(),
        "session_id": None,
        "source": "rule",
        "source_observation_id": None,
        "workflow_run_id": uuid4(),
        "source_alert_id": uuid4(),
        "source_finding_id": None,
        "rule_id": uuid4(),
        "workflow_id": None,
        "dedupe_key": "k1",
        "occurred_at": _NOW,
        "e_created_at": _NOW,
        "payload": {"status": "completed"},
        "evidence": {},
        "reliability_context": None,
    }


async def test_drain_delivers_and_updates_health_on_success() -> None:
    account, attempt_id, dest_id = uuid4(), uuid4(), uuid4()
    conn = FakeDeliveryConn(
        claim_rows=[_claim_row(account, attempt_id, dest_id)],
        contexts=[_context_row(account, attempt_id, dest_id, "https://ok.example.com/h")],
    )
    async with _client(lambda req: httpx.Response(200, text="ok")) as http:
        stats = await drain_due_deliveries(FakePool(conn), http, get_settings())
    assert stats.claimed == 1
    assert stats.delivered == 1
    assert conn.results[0][1] == "delivered"  # status arg
    assert conn.health_success == [dest_id]
    assert conn.health_failure == []


async def test_drain_marks_failure_and_health_on_5xx() -> None:
    account, attempt_id, dest_id = uuid4(), uuid4(), uuid4()
    conn = FakeDeliveryConn(
        claim_rows=[_claim_row(account, attempt_id, dest_id)],
        contexts=[_context_row(account, attempt_id, dest_id, "https://bad.example.com/h")],
    )
    async with _client(lambda req: httpx.Response(500, text="err")) as http:
        stats = await drain_due_deliveries(FakePool(conn), http, get_settings())
    assert stats.failed == 1
    assert conn.results[0][1] == "failed"
    assert conn.health_failure == [dest_id]
    assert conn.health_success == []


async def test_drain_empty_when_nothing_due() -> None:
    conn = FakeDeliveryConn(claim_rows=[], contexts=[])
    async with _client(lambda req: httpx.Response(200)) as http:
        stats = await drain_due_deliveries(FakePool(conn), http, get_settings())
    assert stats.claimed == 0
    assert conn.results == []


async def test_drain_one_bad_delivery_does_not_stop_batch() -> None:
    account = uuid4()
    a1, d1 = uuid4(), uuid4()
    a2, d2 = uuid4(), uuid4()
    conn = FakeDeliveryConn(
        claim_rows=[_claim_row(account, a1, d1), _claim_row(account, a2, d2)],
        contexts=[
            _context_row(account, a1, d1, "https://raise.example.com/h"),
            _context_row(account, a2, d2, "https://ok.example.com/h"),
        ],
    )

    def handler(request: httpx.Request) -> httpx.Response:
        if "raise.example.com" in str(request.url):
            raise RuntimeError("unexpected boom during send")
        return httpx.Response(200, text="ok")

    async with _client(handler) as http:
        stats = await drain_due_deliveries(FakePool(conn), http, get_settings())
    # One errored, the other still delivered.
    assert stats.errors == 1
    assert stats.delivered == 1


def test_worker_disabled_by_default() -> None:
    assert get_settings().webhook_delivery_enabled is False
