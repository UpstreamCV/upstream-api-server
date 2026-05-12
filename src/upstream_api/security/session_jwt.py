"""Short-lived session JWTs embedded in SRT streamids.

Signed with our own ``SESSION_JWT_SECRET`` (HS256). The streaming server
validates these by POSTing the streamid to /v1/ingest/validate-stream, which
verifies the signature here and returns the expected stream parameters.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import jwt

_ALGORITHM = "HS256"
_ISSUER = "upstream-api"
_AUDIENCE = "srt-ingest"


@dataclass(frozen=True, slots=True)
class SessionLimits:
    width: int
    height: int
    fps: int
    codec: str
    bitrate_kbps: int

    def to_claim(self) -> dict[str, Any]:
        return {
            "w": self.width,
            "h": self.height,
            "fps": self.fps,
            "codec": self.codec,
            "kbps": self.bitrate_kbps,
        }

    @classmethod
    def from_claim(cls, data: dict[str, Any]) -> SessionLimits:
        return cls(
            width=int(data["w"]),
            height=int(data["h"]),
            fps=int(data["fps"]),
            codec=str(data["codec"]),
            bitrate_kbps=int(data["kbps"]),
        )


@dataclass(frozen=True, slots=True)
class SessionTokenClaims:
    account_id: UUID
    agent_id: UUID
    camera_id: UUID
    session_id: UUID
    jti: UUID
    issued_at: datetime
    expires_at: datetime
    limits: SessionLimits


def mint_session_token(
    *,
    secret: str,
    account_id: UUID,
    agent_id: UUID,
    camera_id: UUID,
    session_id: UUID,
    limits: SessionLimits,
    ttl_seconds: int,
    jti: UUID | None = None,
    now: datetime | None = None,
) -> tuple[str, SessionTokenClaims]:
    issued_at = now or datetime.now(tz=UTC)
    expires_at = issued_at + timedelta(seconds=ttl_seconds)
    jti = jti or uuid4()

    payload: dict[str, Any] = {
        "iss": _ISSUER,
        "aud": _AUDIENCE,
        "sub": str(agent_id),
        "acc": str(account_id),
        "sid": str(session_id),
        "cam": str(camera_id),
        "jti": str(jti),
        "iat": int(issued_at.timestamp()),
        "exp": int(expires_at.timestamp()),
        "lim": limits.to_claim(),
    }
    encoded = jwt.encode(payload, secret, algorithm=_ALGORITHM)
    claims = SessionTokenClaims(
        account_id=account_id,
        agent_id=agent_id,
        camera_id=camera_id,
        session_id=session_id,
        jti=jti,
        issued_at=issued_at,
        expires_at=expires_at,
        limits=limits,
    )
    return encoded, claims


class SessionTokenError(Exception):
    pass


def verify_session_token(token: str, *, secret: str) -> SessionTokenClaims:
    try:
        payload = jwt.decode(
            token,
            secret,
            algorithms=[_ALGORITHM],
            audience=_AUDIENCE,
            issuer=_ISSUER,
            options={"require": ["exp", "iat", "sub", "acc", "sid", "cam", "jti", "lim"]},
        )
    except jwt.ExpiredSignatureError as exc:
        raise SessionTokenError("Session token expired.") from exc
    except jwt.InvalidTokenError as exc:
        raise SessionTokenError(f"Invalid session token: {exc}") from exc

    return SessionTokenClaims(
        account_id=UUID(payload["acc"]),
        agent_id=UUID(payload["sub"]),
        camera_id=UUID(payload["cam"]),
        session_id=UUID(payload["sid"]),
        jti=UUID(payload["jti"]),
        issued_at=datetime.fromtimestamp(payload["iat"], tz=UTC),
        expires_at=datetime.fromtimestamp(payload["exp"], tz=UTC),
        limits=SessionLimits.from_claim(payload["lim"]),
    )
