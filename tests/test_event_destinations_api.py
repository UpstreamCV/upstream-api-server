"""Module 6 M6.3 — route-level tests for the webhook destination API.

TestClient + fake asyncpg pool (no real DB). The fake re-implements the CRUD
predicates, the camera/workflow ownership lookups, and records audit-log inserts
so we can assert no plaintext secret leaks. SQL-pin + migration-shape tests keep
the fake honest about the real statements (incl. pgp_sym_encrypt at rest).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient

from upstream_api.app import create_app
from upstream_api.config import get_settings
from upstream_api.db.pool import get_pool
from upstream_api.errors import ForbiddenError
from upstream_api.security.api_tokens import ALLOWED_SCOPES
from upstream_api.security.deps import AgentAuth, CallerAuth, UserAuth, current_caller
from upstream_api.services.account_audit import reset_audit_table_cache

_NOW = datetime(2026, 6, 14, 12, 0, 0, tzinfo=UTC)
_PATH = "/v1/event-destinations"


# ---------------------------------------------------------------------------
# Fake pool / conn.
# ---------------------------------------------------------------------------


class FakeTransaction:
    async def __aenter__(self) -> FakeTransaction:
        return self

    async def __aexit__(self, *_exc: Any) -> None:
        return None


class FakeConn:
    def __init__(
        self,
        *,
        destinations: list[dict[str, Any]] | None = None,
        cameras: set[tuple[UUID, UUID]] | None = None,
        workflows: set[tuple[UUID, UUID]] | None = None,
    ) -> None:
        self.destinations = destinations or []
        self.cameras = cameras or set()
        self.workflows = workflows or set()
        self.audit_logs: list[tuple[Any, ...]] = []
        self._created_count = 0

    def transaction(self) -> FakeTransaction:
        return FakeTransaction()

    async def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        if "from cameras" in query:
            account_id, ids = args
            return [{"id": cid} for cid in ids if (account_id, cid) in self.cameras]
        if "from workflows" in query:
            account_id, ids = args
            return [{"id": wid} for wid in ids if (account_id, wid) in self.workflows]
        assert "from event_destinations" in query
        account_id, enabled, dtype = args
        rows = [
            d
            for d in self.destinations
            if d["account_id"] == account_id
            and (enabled is None or d["enabled"] == enabled)
            and (dtype is None or d["destination_type"] == dtype)
        ]
        rows.sort(key=lambda d: d["created_at"], reverse=True)
        return rows

    async def fetchval(self, query: str, *args: Any) -> Any:
        if "to_regclass" in query:
            return "public.account_audit_logs"  # audit table present
        raise AssertionError(f"unexpected fetchval: {query}")

    async def execute(self, query: str, *args: Any) -> str:
        if "insert into account_audit_logs" in query:
            self.audit_logs.append(args)
            return "INSERT 0 1"
        raise AssertionError(f"unexpected execute: {query}")

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        if "insert into event_destinations" in query:
            self._created_count += 1
            row = {
                "id": uuid4(),
                "account_id": args[0],
                "destination_type": "webhook",
                "name": args[1],
                "url": args[2],
                "enabled": args[3],
                "event_type_filters": list(args[4]),
                "camera_id_filters": list(args[5]),
                "workflow_id_filters": list(args[6]),
                # args[7]=plaintext secret, args[8]=enc key -> NOT stored here
                # (pgp_sym_encrypt encrypts; only ciphertext lives in the DB).
                "signing_secret_preview": args[9],
                "created_at": _NOW + timedelta(seconds=self._created_count),
                "updated_at": _NOW + timedelta(seconds=self._created_count),
                "last_success_at": None,
                "last_failure_at": None,
                "consecutive_failures": 0,
                "disabled_at": None,
            }
            self.destinations.append(row)
            return row
        if "update event_destinations" in query:
            account_id, dest_id, name, url, enabled, etf, cif, wif = args
            for d in self.destinations:
                if d["account_id"] != account_id or d["id"] != dest_id:
                    continue
                if name is not None:
                    d["name"] = name
                if url is not None:
                    d["url"] = url
                if enabled is not None:
                    d["enabled"] = enabled
                    d["disabled_at"] = None if enabled else (d["disabled_at"] or _NOW)
                if etf is not None:
                    d["event_type_filters"] = list(etf)
                if cif is not None:
                    d["camera_id_filters"] = list(cif)
                if wif is not None:
                    d["workflow_id_filters"] = list(wif)
                d["updated_at"] = _NOW + timedelta(seconds=99)
                return d
            return None
        if "delete from event_destinations" in query:
            account_id, dest_id = args
            for i, d in enumerate(self.destinations):
                if d["account_id"] == account_id and d["id"] == dest_id:
                    del self.destinations[i]
                    return {"id": dest_id}
            return None
        # get-by-id select
        assert "from event_destinations" in query
        account_id, dest_id = args
        for d in self.destinations:
            if d["account_id"] == account_id and d["id"] == dest_id:
                return d
        return None


class AcquireCtx:
    def __init__(self, conn: FakeConn) -> None:
        self.conn = conn

    async def __aenter__(self) -> FakeConn:
        return self.conn

    async def __aexit__(self, *_exc: Any) -> None:
        return None


class FakePool:
    def __init__(self, conn: FakeConn) -> None:
        self.conn = conn

    def acquire(self) -> AcquireCtx:
        return AcquireCtx(self.conn)


def _user_caller(account_id: UUID, role: str = "owner") -> CallerAuth:
    return CallerAuth(
        agent=None,
        user=UserAuth(
            user_id=uuid4(), memberships={account_id: role}, aal="aal1", amr=("password",)
        ),
    )


def _agent_caller(account_id: UUID, scopes: tuple[str, ...]) -> CallerAuth:
    return CallerAuth(
        agent=AgentAuth(account_id=account_id, token_id=uuid4(), scopes=scopes), user=None
    )


def _client(
    conn: FakeConn, caller: CallerAuth | None, *, settings: Any | None = None
) -> TestClient:
    reset_audit_table_cache()  # force the audit probe to hit our fake
    app = create_app()
    app.dependency_overrides[get_pool] = lambda: FakePool(conn)
    if caller is not None:
        app.dependency_overrides[current_caller] = lambda: caller
    if settings is not None:
        app.dependency_overrides[get_settings] = lambda: settings
    return TestClient(app)


def _create_body(**overrides: Any) -> dict[str, Any]:
    body = {
        "name": "Production webhook",
        "url": "https://example.com/upstream/webhook",
        "enabled": True,
        "event_type_filters": [],
        "camera_id_filters": [],
        "workflow_id_filters": [],
    }
    body.update(overrides)
    return body


# ---------------------------------------------------------------------------
# Create.
# ---------------------------------------------------------------------------


def test_owner_can_create_and_secret_revealed_once() -> None:
    account = uuid4()
    conn = FakeConn()
    r = _client(conn, _user_caller(account, "owner")).post(
        _PATH, params={"account_id": str(account)}, json=_create_body()
    )
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["signing_secret"].startswith("whsec_")
    assert "..." in body["signing_secret_preview"]
    assert body["signing_secret_preview"] != body["signing_secret"]
    assert body["destination_type"] == "webhook"
    assert body["enabled"] is True


def test_admin_can_create() -> None:
    account = uuid4()
    r = _client(FakeConn(), _user_caller(account, "admin")).post(
        _PATH, params={"account_id": str(account)}, json=_create_body()
    )
    assert r.status_code == 201, r.text


def test_viewer_cannot_create() -> None:
    account = uuid4()
    r = _client(FakeConn(), _user_caller(account, "viewer")).post(
        _PATH, params={"account_id": str(account)}, json=_create_body()
    )
    assert r.status_code == 403
    assert r.json()["error"]["code"] == "forbidden.role"


def test_create_audit_log_has_no_plaintext_secret() -> None:
    account = uuid4()
    conn = FakeConn()
    r = _client(conn, _user_caller(account)).post(
        _PATH, params={"account_id": str(account)}, json=_create_body()
    )
    secret = r.json()["signing_secret"]
    assert len(conn.audit_logs) == 1
    args = conn.audit_logs[0]
    assert args[3] == "event_destination.created"  # event_type
    assert secret not in str(args)  # secret never reaches the audit row


def test_create_rejects_non_https_url() -> None:
    account = uuid4()
    r = _client(FakeConn(), _user_caller(account)).post(
        _PATH, params={"account_id": str(account)}, json=_create_body(url="http://example.com/x")
    )
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "invalid_webhook_url"


@pytest.mark.parametrize(
    "bad_url",
    [
        "ftp://example.com/hook",
        "https://localhost/hook",
        "https://127.0.0.1/hook",
        "https://169.254.169.254/latest/meta-data",
        "https://10.0.0.5/hook",
        "https://192.168.1.5/hook",
    ],
)
def test_create_rejects_unsafe_urls(bad_url: str) -> None:
    account = uuid4()
    r = _client(FakeConn(), _user_caller(account)).post(
        _PATH, params={"account_id": str(account)}, json=_create_body(url=bad_url)
    )
    assert r.status_code == 400, bad_url


def test_create_accepts_known_event_type_filters() -> None:
    account = uuid4()
    r = _client(FakeConn(), _user_caller(account)).post(
        _PATH,
        params={"account_id": str(account)},
        json=_create_body(event_type_filters=["alert.created", "workflow.cooldown_suppressed"]),
    )
    assert r.status_code == 201, r.text
    assert set(r.json()["event_type_filters"]) == {"alert.created", "workflow.cooldown_suppressed"}


def test_create_rejects_unknown_event_type_filter() -> None:
    account = uuid4()
    r = _client(FakeConn(), _user_caller(account)).post(
        _PATH,
        params={"account_id": str(account)},
        json=_create_body(event_type_filters=["bogus.type"]),
    )
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "invalid_event_type_filter"


def test_create_accepts_own_account_camera_filter() -> None:
    account = uuid4()
    cam = uuid4()
    conn = FakeConn(cameras={(account, cam)})
    r = _client(conn, _user_caller(account)).post(
        _PATH, params={"account_id": str(account)}, json=_create_body(camera_id_filters=[str(cam)])
    )
    assert r.status_code == 201, r.text
    assert r.json()["camera_id_filters"] == [str(cam)]


def test_create_rejects_cross_account_camera_filter() -> None:
    account = uuid4()
    other_cam = uuid4()  # not in conn.cameras for this account
    conn = FakeConn(cameras=set())
    r = _client(conn, _user_caller(account)).post(
        _PATH,
        params={"account_id": str(account)},
        json=_create_body(camera_id_filters=[str(other_cam)]),
    )
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "invalid_camera_filter"


def test_create_rejects_cross_account_workflow_filter() -> None:
    account = uuid4()
    conn = FakeConn(workflows=set())
    r = _client(conn, _user_caller(account)).post(
        _PATH,
        params={"account_id": str(account)},
        json=_create_body(workflow_id_filters=[str(uuid4())]),
    )
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "invalid_workflow_filter"


def test_create_503_when_enc_key_unconfigured() -> None:
    account = uuid4()
    settings = get_settings().model_copy(update={"webhook_secret_enc_key": None})
    r = _client(FakeConn(), _user_caller(account), settings=settings).post(
        _PATH, params={"account_id": str(account)}, json=_create_body()
    )
    assert r.status_code == 503
    assert r.json()["error"]["code"] == "webhook_encryption_unconfigured"


# ---------------------------------------------------------------------------
# List.
# ---------------------------------------------------------------------------


def _seed_one(account: UUID, **overrides: Any) -> dict[str, Any]:
    row = {
        "id": uuid4(),
        "account_id": account,
        "destination_type": "webhook",
        "name": "Hook",
        "url": "https://example.com/h",
        "enabled": True,
        "event_type_filters": [],
        "camera_id_filters": [],
        "workflow_id_filters": [],
        "signing_secret_preview": "whsec_test_...AB12",
        "created_at": _NOW,
        "updated_at": _NOW,
        "last_success_at": None,
        "last_failure_at": None,
        "consecutive_failures": 0,
        "disabled_at": None,
    }
    row.update(overrides)
    return row


def test_list_returns_account_destinations_without_secret() -> None:
    account = uuid4()
    conn = FakeConn(destinations=[_seed_one(account)])
    r = _client(conn, _user_caller(account)).get(_PATH, params={"account_id": str(account)})
    assert r.status_code == 200, r.text
    items = r.json()["items"]
    assert len(items) == 1
    assert "signing_secret" not in items[0]
    assert items[0]["signing_secret_preview"] == "whsec_test_...AB12"


def test_list_account_isolation() -> None:
    account_a, account_b = uuid4(), uuid4()
    conn = FakeConn(destinations=[_seed_one(account_a), _seed_one(account_b)])
    r = _client(conn, _user_caller(account_a)).get(_PATH, params={"account_id": str(account_a)})
    items = r.json()["items"]
    assert len(items) == 1
    assert items[0]["account_id"] == str(account_a)


def test_list_enabled_filter() -> None:
    account = uuid4()
    conn = FakeConn(
        destinations=[
            _seed_one(account, enabled=True),
            _seed_one(account, enabled=False),
        ]
    )
    r = _client(conn, _user_caller(account)).get(
        _PATH, params={"account_id": str(account), "enabled": "true"}
    )
    items = r.json()["items"]
    assert len(items) == 1
    assert items[0]["enabled"] is True


# ---------------------------------------------------------------------------
# Get.
# ---------------------------------------------------------------------------


def test_get_own_destination_masked() -> None:
    account = uuid4()
    row = _seed_one(account)
    conn = FakeConn(destinations=[row])
    r = _client(conn, _user_caller(account)).get(
        f"{_PATH}/{row['id']}", params={"account_id": str(account)}
    )
    assert r.status_code == 200, r.text
    assert "signing_secret" not in r.json()
    assert r.json()["id"] == str(row["id"])


def test_get_unknown_404() -> None:
    account = uuid4()
    r = _client(FakeConn(), _user_caller(account)).get(
        f"{_PATH}/{uuid4()}", params={"account_id": str(account)}
    )
    assert r.status_code == 404


def test_get_cross_account_404() -> None:
    account_a, account_b = uuid4(), uuid4()
    row_b = _seed_one(account_b)
    conn = FakeConn(destinations=[row_b])
    r = _client(conn, _user_caller(account_a)).get(
        f"{_PATH}/{row_b['id']}", params={"account_id": str(account_a)}
    )
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Update.
# ---------------------------------------------------------------------------


def test_owner_can_update_fields() -> None:
    account = uuid4()
    row = _seed_one(account, name="Old")
    conn = FakeConn(destinations=[row])
    r = _client(conn, _user_caller(account)).patch(
        f"{_PATH}/{row['id']}",
        params={"account_id": str(account)},
        json={"name": "New", "url": "https://new.example.com/h"},
    )
    assert r.status_code == 200, r.text
    assert r.json()["name"] == "New"
    assert r.json()["url"] == "https://new.example.com/h"
    assert "signing_secret" not in r.json()


def test_disable_then_enable_sets_disabled_at() -> None:
    account = uuid4()
    row = _seed_one(account, enabled=True)
    conn = FakeConn(destinations=[row])
    client = _client(conn, _user_caller(account))
    rd = client.patch(
        f"{_PATH}/{row['id']}", params={"account_id": str(account)}, json={"enabled": False}
    )
    assert rd.status_code == 200, rd.text
    assert rd.json()["enabled"] is False
    assert rd.json()["disabled_at"] is not None
    re = client.patch(
        f"{_PATH}/{row['id']}", params={"account_id": str(account)}, json={"enabled": True}
    )
    assert re.json()["enabled"] is True
    assert re.json()["disabled_at"] is None


def test_viewer_cannot_update() -> None:
    account = uuid4()
    row = _seed_one(account)
    conn = FakeConn(destinations=[row])
    r = _client(conn, _user_caller(account, "viewer")).patch(
        f"{_PATH}/{row['id']}", params={"account_id": str(account)}, json={"name": "x"}
    )
    assert r.status_code == 403


def test_update_cross_account_404() -> None:
    account_a, account_b = uuid4(), uuid4()
    row_b = _seed_one(account_b)
    conn = FakeConn(destinations=[row_b])
    r = _client(conn, _user_caller(account_a)).patch(
        f"{_PATH}/{row_b['id']}", params={"account_id": str(account_a)}, json={"name": "x"}
    )
    assert r.status_code == 404


def test_update_rejects_bad_url() -> None:
    account = uuid4()
    row = _seed_one(account)
    conn = FakeConn(destinations=[row])
    r = _client(conn, _user_caller(account)).patch(
        f"{_PATH}/{row['id']}",
        params={"account_id": str(account)},
        json={"url": "http://insecure.example.com"},
    )
    assert r.status_code == 400


# ---------------------------------------------------------------------------
# Delete.
# ---------------------------------------------------------------------------


def test_owner_can_delete() -> None:
    account = uuid4()
    row = _seed_one(account)
    conn = FakeConn(destinations=[row])
    client = _client(conn, _user_caller(account))
    r = client.delete(f"{_PATH}/{row['id']}", params={"account_id": str(account)})
    assert r.status_code == 204
    # No longer present.
    r2 = client.get(f"{_PATH}/{row['id']}", params={"account_id": str(account)})
    assert r2.status_code == 404


def test_viewer_cannot_delete() -> None:
    account = uuid4()
    row = _seed_one(account)
    conn = FakeConn(destinations=[row])
    r = _client(conn, _user_caller(account, "viewer")).delete(
        f"{_PATH}/{row['id']}", params={"account_id": str(account)}
    )
    assert r.status_code == 403


def test_delete_cross_account_404() -> None:
    account_a, account_b = uuid4(), uuid4()
    row_b = _seed_one(account_b)
    conn = FakeConn(destinations=[row_b])
    r = _client(conn, _user_caller(account_a)).delete(
        f"{_PATH}/{row_b['id']}", params={"account_id": str(account_a)}
    )
    assert r.status_code == 404
    # Still present for its real owner.
    assert len(conn.destinations) == 1


# ---------------------------------------------------------------------------
# API token scopes.
# ---------------------------------------------------------------------------


def test_token_with_destinations_read_can_list() -> None:
    account = uuid4()
    conn = FakeConn(destinations=[_seed_one(account)])
    r = _client(conn, _agent_caller(account, ("destinations:read",))).get(_PATH)
    assert r.status_code == 200, r.text
    assert len(r.json()["items"]) == 1


def test_token_without_destinations_read_403() -> None:
    account = uuid4()
    r = _client(FakeConn(), _agent_caller(account, ("agent",))).get(_PATH)
    assert r.status_code == 403
    assert r.json()["error"]["code"] == "forbidden.scope"


def test_token_with_destinations_write_can_create() -> None:
    account = uuid4()
    r = _client(FakeConn(), _agent_caller(account, ("destinations:write",))).post(
        _PATH, json=_create_body()
    )
    assert r.status_code == 201, r.text


def test_token_without_destinations_write_403_on_create() -> None:
    account = uuid4()
    r = _client(FakeConn(), _agent_caller(account, ("destinations:read",))).post(
        _PATH, json=_create_body()
    )
    assert r.status_code == 403


def test_scope_vocabulary() -> None:
    assert {"destinations:read", "destinations:write"} <= ALLOWED_SCOPES
    assert {"bogus"} - ALLOWED_SCOPES == {"bogus"}  # unknown still rejected
    assert "agent" in ALLOWED_SCOPES  # unchanged
    assert "events:read" in ALLOWED_SCOPES  # unchanged


def test_user_does_not_need_token_scope() -> None:
    # A dashboard user with no agent token still lists fine (membership-gated).
    account = uuid4()
    conn = FakeConn(destinations=[_seed_one(account)])
    r = _client(conn, _user_caller(account)).get(_PATH, params={"account_id": str(account)})
    assert r.status_code == 200


def test_unauthenticated_401() -> None:
    r = _client(FakeConn(), None).get(_PATH)
    assert r.status_code == 401


# ---------------------------------------------------------------------------
# SQL / migration pins.
# ---------------------------------------------------------------------------


def test_service_sql_encrypts_secret_and_never_selects_ciphertext() -> None:
    from upstream_api.services.event_destinations import (
        _DEST_COLUMNS,
        _GET_SQL,
        _INSERT_SQL,
        _LIST_SQL,
    )

    assert "pgp_sym_encrypt($8, $9)" in _INSERT_SQL
    # The encrypted blob is never read back by the API.
    assert "signing_secret_encrypted" not in _DEST_COLUMNS
    assert "signing_secret_encrypted" not in _LIST_SQL
    assert "signing_secret_encrypted" not in _GET_SQL
    assert "signing_secret_preview" in _DEST_COLUMNS


def test_migration_shape_pin() -> None:
    root = Path(__file__).resolve().parents[1]
    sql = (
        root / "supabase" / "migrations" / "20260620120000_event_destinations.sql"
    ).read_text().lower()
    assert "create table event_destinations" in sql
    assert "signing_secret_encrypted  bytea not null" in sql
    assert "signing_secret_preview    text not null" in sql
    assert "event_type_filters        text[] not null default '{}'" in sql
    assert "camera_id_filters         uuid[] not null default '{}'" in sql
    assert "workflow_id_filters       uuid[] not null default '{}'" in sql
    assert "event_destinations_account_created_idx" in sql
    assert "event_destinations_account_enabled_idx" in sql
    assert "set_updated_at" in sql
    # RLS member-select + grant; no insert/update/delete policy.
    assert "enable row level security" in sql
    assert "event_destinations_member_select" in sql
    assert "grant select on public.event_destinations to authenticated" in sql
    assert "for insert" not in sql
    assert "for update" not in sql
    assert "for delete" not in sql
    # No plaintext secret column.
    assert "signing_secret text" not in sql


def test_require_token_scope_is_used_for_writes() -> None:
    # Sanity: the write helper raises forbidden.scope for a token lacking write.
    from upstream_api.routers.event_destinations import _resolve_for_write

    with pytest.raises(ForbiddenError) as exc:
        _resolve_for_write(_agent_caller(uuid4(), ("destinations:read",)), None)
    assert exc.value.code == "forbidden.scope"
