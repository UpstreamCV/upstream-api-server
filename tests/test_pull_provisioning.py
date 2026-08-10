"""Pull-based MediaMTX provisioning — route-level tests.

Same pattern as tests/test_processor.py: TestClient + a fake asyncpg pool, no
real DB or network.

The behaviours worth pinning are the ones that made the 2026-08 incident
expensive:

- push mode must stay byte-identical (pull is opt-in, not a silent switch),
- a box polling a push-mode server must get an empty list, not an error, so
  the two sides can be deployed in either order,
- an ack must be idempotent, because a lost response is the common case,
- a row carrying no passphrase must never be handed out, since MediaMTX would
  then accept an unauthenticated publisher.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from fastapi.testclient import TestClient

from upstream_api.app import create_app
from upstream_api.config import Settings, get_settings
from upstream_api.db.pool import get_pool

_STREAM_KEY = "sess_0123456789abcdef"
_STREAMID = f"publish:{_STREAM_KEY}"
_PASSPHRASE = "s3cret-passphrase-value"


class AcquireCtx:
    def __init__(self, conn: Any) -> None:
        self.conn = conn

    async def __aenter__(self) -> Any:
        return self.conn

    async def __aexit__(self, *_: object) -> bool:
        return False


class TxCtx:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *_: object) -> bool:
        return False


class FakePool:
    def __init__(self, conn: Any) -> None:
        self.conn = conn

    def acquire(self) -> AcquireCtx:
        return AcquireCtx(self.conn)


@dataclass
class _Pending:
    session_id: UUID
    camera_id: UUID
    streamid: str = _STREAMID
    passphrase: str | None = _PASSPHRASE

    def as_row(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "account_id": uuid4(),
            "camera_id": self.camera_id,
            "streamid": self.streamid,
            "passphrase": self.passphrase,
            "created_at": datetime.now(tz=UTC) - timedelta(seconds=1),
        }


class FakeProvisionConn:
    """Serves pending rows and records ack updates."""

    def __init__(self, pending: list[_Pending], *, already_acked: bool = False) -> None:
        self._pending = pending
        self._already_acked = already_acked
        self.acked: list[UUID] = []

    def transaction(self) -> TxCtx:
        return TxCtx()

    async def fetch(self, _sql: str, *_args: Any) -> list[dict[str, Any]]:
        return [p.as_row() for p in self._pending]

    async def fetchrow(self, sql: str, *args: Any) -> dict[str, Any] | None:
        if "set provisioned_at = now()" in sql:
            if self._already_acked:
                return None  # `provisioned_at is null` matched zero rows
            self.acked.append(args[0])
            return {"id": args[0]}
        return None


def _client(conn: Any, *, mode: str = "pull") -> TestClient:
    app = create_app()
    app.dependency_overrides[get_pool] = lambda: FakePool(conn)
    base = get_settings()
    overridden: Settings = base.model_copy(update={"mediamtx_provisioning_mode": mode})
    app.dependency_overrides[get_settings] = lambda: overridden
    return TestClient(app)


def _headers() -> dict[str, str]:
    return {"X-Upstream-Webhook-Secret": get_settings().processor_webhook_secret.get_secret_value()}


# --- auth -------------------------------------------------------------------


def test_pending_provisions_requires_processor_secret() -> None:
    r = _client(FakeProvisionConn([])).get("/v1/internal/processor/pending-provisions")
    assert r.status_code == 401
    assert r.json()["error"]["code"] == "invalid_webhook_secret"


def test_ack_requires_processor_secret() -> None:
    r = _client(FakeProvisionConn([])).post(
        f"/v1/internal/processor/pending-provisions/{uuid4()}/ack",
        json={"ok": True},
    )
    assert r.status_code == 401


# --- mode gating ------------------------------------------------------------


def test_push_mode_returns_empty_even_with_pending_rows() -> None:
    """A newer box polling an older/push-mode server must see no work.

    This is what lets the two sides deploy in either order.
    """
    conn = FakeProvisionConn([_Pending(uuid4(), uuid4())])
    r = _client(conn, mode="push").get(
        "/v1/internal/processor/pending-provisions", headers=_headers()
    )
    assert r.status_code == 200
    assert r.json()["items"] == []


def test_pull_mode_returns_pending_rows() -> None:
    sid, cid = uuid4(), uuid4()
    conn = FakeProvisionConn([_Pending(sid, cid)])
    r = _client(conn).get("/v1/internal/processor/pending-provisions", headers=_headers())
    assert r.status_code == 200
    items = r.json()["items"]
    assert len(items) == 1
    assert items[0]["session_id"] == str(sid)
    assert items[0]["camera_id"] == str(cid)
    assert items[0]["stream_key"] == _STREAM_KEY
    assert items[0]["srt_publish_passphrase"] == _PASSPHRASE


# --- row-level safety -------------------------------------------------------


def test_row_without_passphrase_is_skipped() -> None:
    """A path with no passphrase would accept any publisher — never emit it."""
    conn = FakeProvisionConn([_Pending(uuid4(), uuid4(), passphrase=None)])
    r = _client(conn).get("/v1/internal/processor/pending-provisions", headers=_headers())
    assert r.status_code == 200
    assert r.json()["items"] == []


def test_malformed_streamid_is_skipped_not_500() -> None:
    """Our own data corruption must not break the whole poll."""
    conn = FakeProvisionConn(
        [
            _Pending(uuid4(), uuid4(), streamid="not-a-publish-streamid"),
            _Pending(uuid4(), uuid4()),
        ]
    )
    r = _client(conn).get("/v1/internal/processor/pending-provisions", headers=_headers())
    assert r.status_code == 200
    assert len(r.json()["items"]) == 1


# --- ack --------------------------------------------------------------------


def test_ack_success_applies() -> None:
    sid = uuid4()
    conn = FakeProvisionConn([])
    r = _client(conn).post(
        f"/v1/internal/processor/pending-provisions/{sid}/ack",
        json={"ok": True},
        headers=_headers(),
    )
    assert r.status_code == 200
    assert r.json()["applied"] is True
    assert conn.acked == [sid]


def test_duplicate_ack_is_idempotent() -> None:
    """A retried ack after a lost response must not error or double-apply."""
    conn = FakeProvisionConn([], already_acked=True)
    r = _client(conn).post(
        f"/v1/internal/processor/pending-provisions/{uuid4()}/ack",
        json={"ok": True},
        headers=_headers(),
    )
    assert r.status_code == 200
    assert r.json()["applied"] is False
