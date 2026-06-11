"""Unit tests for the fifth identity class: ``current_platform_admin``.

Exercises the dep directly (pytest-asyncio auto mode) with a fake pool and
real HS256 tokens minted with the conftest Supabase JWT secret — same
approach as test_supabase_jwt.py but through the FastAPI dependency.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import jwt
import pytest

from upstream_api.config import get_settings
from upstream_api.errors import ForbiddenError, UnauthorizedError
from upstream_api.security.deps import current_platform_admin


def _secret() -> str:
    """The active Supabase JWT secret. CI exports its own UPSTREAM_* values
    (which beat conftest's setdefault fallbacks), so tokens must be minted
    with whatever the settings actually resolve to — never a hardcoded
    copy of the local default."""
    return get_settings().supabase_jwt_secret.get_secret_value()


class FakeConn:
    def __init__(self, allowlisted: bool) -> None:
        self.allowlisted = allowlisted
        self.queries: list[tuple[str, tuple[Any, ...]]] = []

    async def fetchval(self, query: str, *args: Any) -> Any:
        self.queries.append((query, args))
        return 1 if self.allowlisted else None


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


def _mint(**overrides: object) -> str:
    payload: dict[str, object] = {
        "sub": str(uuid4()),
        "aud": "authenticated",
        "exp": int((datetime.now(tz=UTC) + timedelta(hours=1)).timestamp()),
        "role": "authenticated",
        "email": "admin@example.com",
    }
    payload.update(overrides)
    return jwt.encode(payload, _secret(), algorithm="HS256")


async def test_allowlisted_user_resolves() -> None:
    user_id = uuid4()
    conn = FakeConn(allowlisted=True)
    admin = await current_platform_admin(
        pool=FakePool(conn),
        settings=get_settings(),
        authorization=f"Bearer {_mint(sub=str(user_id))}",
    )
    assert admin.user_id == user_id
    assert admin.email == "admin@example.com"
    # The allowlist lookup is keyed on the token's sub.
    assert conn.queries[0][1] == (user_id,)


async def test_valid_token_but_not_allowlisted_is_403() -> None:
    conn = FakeConn(allowlisted=False)
    with pytest.raises(ForbiddenError) as exc:
        await current_platform_admin(
            pool=FakePool(conn),
            settings=get_settings(),
            authorization=f"Bearer {_mint()}",
        )
    assert exc.value.code == "not_platform_admin"
    assert exc.value.status_code == 403


async def test_invalid_token_is_401_without_db_lookup() -> None:
    conn = FakeConn(allowlisted=True)
    with pytest.raises(UnauthorizedError) as exc:
        await current_platform_admin(
            pool=FakePool(conn),
            settings=get_settings(),
            authorization="Bearer not-a-jwt",
        )
    assert exc.value.code == "invalid_user_token"
    assert conn.queries == []


async def test_expired_token_is_401() -> None:
    expired = _mint(exp=int((datetime.now(tz=UTC) - timedelta(seconds=1)).timestamp()))
    with pytest.raises(UnauthorizedError):
        await current_platform_admin(
            pool=FakePool(FakeConn(allowlisted=True)),
            settings=get_settings(),
            authorization=f"Bearer {expired}",
        )


async def test_missing_header_is_401() -> None:
    with pytest.raises(UnauthorizedError) as exc:
        await current_platform_admin(
            pool=FakePool(FakeConn(allowlisted=True)),
            settings=get_settings(),
            authorization=None,
        )
    assert exc.value.code == "missing_credentials"


async def test_aal_and_amr_are_carried() -> None:
    admin = await current_platform_admin(
        pool=FakePool(FakeConn(allowlisted=True)),
        settings=get_settings(),
        authorization=f"Bearer {_mint(aal='aal2', amr=['password', 'totp'])}",
    )
    assert admin.aal == "aal2"
    assert admin.amr == ("password", "totp")
