"""Device self-enrollment mint endpoint — route-level tests.

Pattern mirrors tests/test_processor.py: TestClient + a fake asyncpg pool,
no real DB / network. The fake's ``fetch`` re-implements the revoke UPDATE's
WHERE clause (account + name + device scope + not already revoked) so the
rotation matrix — rotate the prior device token, never touch an agent-scoped
namesake — is genuinely exercised; the production SQL text itself is pinned
predicate-by-predicate so the re-implementation cannot drift from it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr

from upstream_api.app import create_app
from upstream_api.config import Settings, get_settings
from upstream_api.db.pool import get_pool
from upstream_api.routers.internal_tokens import _REVOKE_PRIOR_TOKENS_SQL
from upstream_api.security.api_tokens import DEVICE_SCOPE
from upstream_api.services.account_audit import reset_audit_table_cache

_SECRET = "test-provision"
_FLEET_ACCOUNT = uuid4()
_DEVICE_ID = "P9386"


@dataclass
class _TokenRow:
    """An api_tokens row, reduced to the columns the revoke predicate reads."""

    account_id: UUID
    name: str
    scopes: list[str]
    revoked_at: datetime | None = None
    id: UUID = field(default_factory=uuid4)


class FakeTransaction:
    async def __aenter__(self) -> FakeTransaction:
        return self

    async def __aexit__(self, *_exc: Any) -> None:
        return None


class FakeProvisionConn:
    def __init__(self, rows: list[_TokenRow] | None = None) -> None:
        self.rows = rows if rows is not None else []
        self.inserts: list[tuple[Any, ...]] = []
        self.executes: list[tuple[str, tuple[Any, ...]]] = []

    def transaction(self) -> FakeTransaction:
        return FakeTransaction()

    async def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        # The revoke UPDATE ... RETURNING id. Re-implements its WHERE clause;
        # test_production_revoke_sql_pins_every_predicate guards the drift.
        assert query is _REVOKE_PRIOR_TOKENS_SQL
        account_id, name, scope = args
        revoked: list[dict[str, Any]] = []
        for r in self.rows:
            if (
                r.account_id == account_id
                and r.name == name
                and scope in r.scopes
                and r.revoked_at is None
            ):
                r.revoked_at = datetime.now(tz=UTC)
                revoked.append({"id": r.id})
        return revoked

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        assert "insert into api_tokens" in query
        self.inserts.append(args)
        row = _TokenRow(account_id=args[0], name=args[1], scopes=list(args[5]))
        self.rows.append(row)
        return {"id": row.id}

    async def fetchval(self, _query: str, *_args: Any) -> Any:
        # The account-audit to_regclass probe — report the table absent so
        # the best-effort audit writes no-op under the fake.
        return False

    async def execute(self, query: str, *args: Any) -> str:
        self.executes.append((query, args))
        return "INSERT 0 1"


class AcquireCtx:
    def __init__(self, conn: Any) -> None:
        self.conn = conn

    async def __aenter__(self) -> Any:
        return self.conn

    async def __aexit__(self, *_exc: Any) -> None:
        return None


class FakePool:
    def __init__(self, conn: Any) -> None:
        self.conn = conn

    def acquire(self) -> AcquireCtx:
        return AcquireCtx(self.conn)


def _client(
    conn: Any,
    *,
    secret: str | None = _SECRET,
    fleet_account_id: str | None = str(_FLEET_ACCOUNT),
) -> TestClient:
    reset_audit_table_cache()
    app = create_app()
    app.dependency_overrides[get_pool] = lambda: FakePool(conn)
    base = get_settings()
    overridden: Settings = base.model_copy(
        update={
            "token_provision_secret": SecretStr(secret) if secret is not None else None,
            "fleet_account_id": fleet_account_id,
        }
    )
    app.dependency_overrides[get_settings] = lambda: overridden
    return TestClient(app)


def _headers(secret: str = _SECRET) -> dict[str, str]:
    return {"X-Upstream-Provision-Secret": secret}


def _body(external_id: str = _DEVICE_ID) -> dict[str, str]:
    return {"external_id": external_id}


# ---------------------------------------------------------------------------
# Auth.
# ---------------------------------------------------------------------------


def test_provision_requires_secret_header() -> None:
    client = _client(FakeProvisionConn())
    r = client.post("/v1/internal/tokens/provision", json=_body())
    assert r.status_code == 401
    assert r.json()["error"]["code"] == "invalid_provision_secret"


def test_provision_rejects_wrong_secret() -> None:
    client = _client(FakeProvisionConn())
    r = client.post(
        "/v1/internal/tokens/provision",
        json=_body(),
        headers=_headers("definitely-wrong"),
    )
    assert r.status_code == 401
    assert r.json()["error"]["code"] == "invalid_provision_secret"


def test_provision_rejects_when_secret_unconfigured() -> None:
    """No configured secret means no header can ever match — including an
    empty one, which must not compare equal to an empty/unset secret."""
    client = _client(FakeProvisionConn(), secret=None)
    for headers in (_headers(_SECRET), _headers("")):
        r = client.post("/v1/internal/tokens/provision", json=_body(), headers=headers)
        assert r.status_code == 401
        assert r.json()["error"]["code"] == "invalid_provision_secret"


def test_provision_secret_is_distinct_from_processor_secret() -> None:
    """The processor webhook secret must NOT authenticate a provision call."""
    client = _client(FakeProvisionConn())
    r = client.post(
        "/v1/internal/tokens/provision",
        json=_body(),
        headers=_headers(get_settings().processor_webhook_secret.get_secret_value()),
    )
    assert r.status_code == 401
    assert r.json()["error"]["code"] == "invalid_provision_secret"


# ---------------------------------------------------------------------------
# Fleet-account configuration gate.
# ---------------------------------------------------------------------------


def test_provision_unconfigured_fleet_account_returns_503() -> None:
    conn = FakeProvisionConn()
    client = _client(conn, fleet_account_id=None)
    r = client.post("/v1/internal/tokens/provision", json=_body(), headers=_headers())
    assert r.status_code == 503
    assert r.json()["error"]["code"] == "provision_not_configured"
    assert conn.inserts == []


def test_provision_malformed_fleet_account_returns_503() -> None:
    conn = FakeProvisionConn()
    client = _client(conn, fleet_account_id="not-a-uuid")
    r = client.post("/v1/internal/tokens/provision", json=_body(), headers=_headers())
    assert r.status_code == 503
    assert r.json()["error"]["code"] == "provision_not_configured"
    assert conn.inserts == []


# ---------------------------------------------------------------------------
# Happy path: revoke-then-mint rotation.
# ---------------------------------------------------------------------------


def test_provision_mints_and_rotates_prior_device_token() -> None:
    prior_device = _TokenRow(account_id=_FLEET_ACCOUNT, name=_DEVICE_ID, scopes=[DEVICE_SCOPE])
    agent_namesake = _TokenRow(account_id=_FLEET_ACCOUNT, name=_DEVICE_ID, scopes=["agent"])
    conn = FakeProvisionConn([prior_device, agent_namesake])
    client = _client(conn)

    r = client.post("/v1/internal/tokens/provision", json=_body(), headers=_headers())
    assert r.status_code == 200, r.text
    body = r.json()

    # A fresh plaintext, minted for the configured fleet account. Environment
    # is "development" under tests, so the env label is "test".
    assert body["token"].startswith("ups_test_")
    assert body["token"].startswith(f"{body['prefix']}_")
    assert body["last_four"] == body["token"][-4:]
    assert body["account_id"] == str(_FLEET_ACCOUNT)

    # The prior device token was revoked; the agent-scoped namesake was not.
    assert body["revoked"] == 1
    assert prior_device.revoked_at is not None
    assert agent_namesake.revoked_at is None

    # The insert carries the fleet account, the device name, the device
    # scope, no expiry — and the minted prefix/last_four match the response.
    assert len(conn.inserts) == 1
    args = conn.inserts[0]
    assert args[0] == _FLEET_ACCOUNT
    assert args[1] == _DEVICE_ID
    assert args[2] == body["prefix"]
    assert args[4] == body["last_four"]
    assert args[5] == [DEVICE_SCOPE]
    assert args[6] is None
    # The stored hash is argon2, never the plaintext.
    assert body["token"] not in args[3]


def test_provision_with_no_prior_token_revokes_zero() -> None:
    conn = FakeProvisionConn()
    client = _client(conn)
    r = client.post("/v1/internal/tokens/provision", json=_body(), headers=_headers())
    assert r.status_code == 200, r.text
    assert r.json()["revoked"] == 0
    assert len(conn.inserts) == 1


def test_provision_ignores_already_revoked_and_other_named_tokens() -> None:
    already_revoked = _TokenRow(
        account_id=_FLEET_ACCOUNT,
        name=_DEVICE_ID,
        scopes=[DEVICE_SCOPE],
        revoked_at=datetime.now(tz=UTC),
    )
    other_device = _TokenRow(account_id=_FLEET_ACCOUNT, name="P1193", scopes=[DEVICE_SCOPE])
    conn = FakeProvisionConn([already_revoked, other_device])
    client = _client(conn)
    r = client.post("/v1/internal/tokens/provision", json=_body(), headers=_headers())
    assert r.status_code == 200, r.text
    assert r.json()["revoked"] == 0
    assert other_device.revoked_at is None


def test_production_revoke_sql_pins_every_predicate() -> None:
    """Guards the FakeProvisionConn simulation against drifting from the
    real query that runs in production. Positional placeholders are pinned
    too — the fake unpacks ``account_id, name, scope = args`` in that order."""
    sql = _REVOKE_PRIOR_TOKENS_SQL
    assert "update api_tokens" in sql
    assert "set revoked_at = now()" in sql
    assert "where account_id = $1" in sql
    assert "and name = $2" in sql
    # Device tokens only — an agent-scoped namesake is never touched.
    assert "and $3 = any(scopes)" in sql
    # Already-revoked rows stay untouched (no re-revoke, no audit rows).
    assert "and revoked_at is null" in sql
    assert "returning id" in sql


def test_provision_strips_external_id() -> None:
    conn = FakeProvisionConn()
    client = _client(conn)
    r = client.post(
        "/v1/internal/tokens/provision",
        json=_body(f"  {_DEVICE_ID}  "),
        headers=_headers(),
    )
    assert r.status_code == 200, r.text
    assert conn.inserts[0][1] == _DEVICE_ID


# ---------------------------------------------------------------------------
# external_id validation.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "bad",
    [
        "",
        "   ",
        "-P9386",  # must start alphanumeric
        "_P9386",
        "P 9386",  # no spaces
        "P/9386",  # no slashes
        "P" * 65,  # too long
    ],
)
def test_provision_rejects_invalid_external_id(bad: str) -> None:
    conn = FakeProvisionConn()
    client = _client(conn)
    r = client.post("/v1/internal/tokens/provision", json=_body(bad), headers=_headers())
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "validation_error"
    assert conn.inserts == []
