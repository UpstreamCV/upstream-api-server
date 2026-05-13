from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import jwt
import pytest

from upstream_api.security.supabase_jwt import (
    SupabaseJwtError,
    verify_supabase_token,
)

_SECRET = "test-supabase-jwt-secret-32bytes-min!!"


def _mint(payload: dict[str, object]) -> str:
    return jwt.encode(payload, _SECRET, algorithm="HS256")


def _base_payload(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = {
        "sub": str(uuid4()),
        "aud": "authenticated",
        "exp": int((datetime.now(tz=UTC) + timedelta(hours=1)).timestamp()),
        "role": "authenticated",
        "email": "user@example.com",
    }
    base.update(overrides)
    return base


def test_verify_extracts_aal2_and_amr() -> None:
    token = _mint(_base_payload(aal="aal2", amr=["password", "totp"]))
    claims = verify_supabase_token(token, secret=_SECRET)
    assert claims.aal == "aal2"
    assert claims.amr == ("password", "totp")


def test_verify_extracts_amr_when_supabase_format_is_objects() -> None:
    token = _mint(
        _base_payload(
            aal="aal2",
            amr=[{"method": "password", "timestamp": 1700000000}, {"method": "totp"}],
        )
    )
    claims = verify_supabase_token(token, secret=_SECRET)
    assert claims.amr == ("password", "totp")


def test_verify_defaults_to_aal1_when_claim_absent() -> None:
    token = _mint(_base_payload())
    claims = verify_supabase_token(token, secret=_SECRET)
    assert claims.aal == "aal1"
    assert claims.amr == ()


def test_verify_rejects_expired_token() -> None:
    token = _mint(_base_payload(exp=int((datetime.now(tz=UTC) - timedelta(seconds=1)).timestamp())))
    with pytest.raises(SupabaseJwtError):
        verify_supabase_token(token, secret=_SECRET)


def test_verify_rejects_wrong_audience() -> None:
    token = _mint(_base_payload(aud="someone-else"))
    with pytest.raises(SupabaseJwtError):
        verify_supabase_token(token, secret=_SECRET)
