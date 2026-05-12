from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import pytest

from upstream_api.security.session_jwt import (
    SessionLimits,
    SessionTokenClaims,
    SessionTokenError,
    mint_session_token,
    verify_session_token,
)

_SECRET = "test-session-jwt-secret-32bytes-min!!"

_LIMITS = SessionLimits(width=1920, height=1080, fps=30, codec="h264", bitrate_kbps=6000)


def _mint(**overrides: Any) -> tuple[str, SessionTokenClaims]:
    kwargs: dict[str, Any] = {
        "secret": _SECRET,
        "account_id": uuid4(),
        "agent_id": uuid4(),
        "camera_id": uuid4(),
        "session_id": uuid4(),
        "limits": _LIMITS,
        "ttl_seconds": 60,
    }
    kwargs.update(overrides)
    return mint_session_token(**kwargs)


def test_mint_and_verify_round_trip() -> None:
    token, claims = _mint()
    decoded = verify_session_token(token, secret=_SECRET)
    assert decoded.account_id == claims.account_id
    assert decoded.session_id == claims.session_id
    assert decoded.limits == _LIMITS


def test_verify_rejects_bad_signature() -> None:
    token, _ = _mint()
    with pytest.raises(SessionTokenError):
        verify_session_token(token, secret="different-secret-32bytes-min!!!!!!")


def test_verify_rejects_expired_token() -> None:
    # Issue a token that was already expired when minted.
    past = datetime.now(tz=UTC) - timedelta(hours=1)
    token, _ = _mint(now=past, ttl_seconds=10)
    with pytest.raises(SessionTokenError):
        verify_session_token(token, secret=_SECRET)


def test_verify_rejects_tampered_payload() -> None:
    token, _ = _mint()
    head, payload, sig = token.split(".")
    tampered = f"{head}.{payload}X.{sig}"
    with pytest.raises(SessionTokenError):
        verify_session_token(tampered, secret=_SECRET)
