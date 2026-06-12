"""Perception fan-out subscriber tokens.

Short-lived HMAC tokens that let a browser open a WebSocket to the
perception-fanout sidecar (upstream-streaming-server) for one camera.
The api-server mints (after Supabase JWT auth + camera-ownership checks
in the route); the sidecar verifies with the shared secret
``UPSTREAM_FANOUT_TOKEN_SECRET`` — both sides implement the identical
algorithm with stdlib hmac/base64/json, no JWT dependency:

    token   := "upt1." + b64url(payload_json)
                       + "." + b64url(HMAC_SHA256(secret, "upt1." + b64url(payload_json)))
    payload := {"cam": "<camera uuid>", "acct": "<account uuid>",
                "sub": "<caller id>", "iat": <unix>, "exp": <unix>}

The subscriber connection is bound to ``cam``; ``acct``/``sub`` exist
for the sidecar's logging only. Tokens ride a WS query parameter
(browsers cannot set WS headers), so they must stay short-lived and
must never be logged on either side.

``verify_fanout_token`` is the test-side counterpart kept here so the
whole algorithm lives in one file.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
from dataclasses import dataclass

TOKEN_PREFIX = "upt1"


@dataclass(frozen=True)
class FanoutTokenClaims:
    camera_id: str
    account_id: str
    subject: str
    issued_at: int
    expires_at: int


def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64url_decode(data: str) -> bytes:
    padding = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode(data + padding)


def _sign(payload_b64: str, secret: str) -> str:
    signing_input = f"{TOKEN_PREFIX}.{payload_b64}"
    digest = hmac.new(
        secret.encode("utf-8"), signing_input.encode("ascii"), hashlib.sha256
    ).digest()
    return _b64url_encode(digest)


def mint_fanout_token(
    *,
    camera_id: str,
    account_id: str,
    subject: str,
    issued_at: int,
    expires_at: int,
    secret: str,
) -> str:
    payload = {
        "cam": camera_id,
        "acct": account_id,
        "sub": subject,
        "iat": issued_at,
        "exp": expires_at,
    }
    payload_b64 = _b64url_encode(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    return f"{TOKEN_PREFIX}.{payload_b64}.{_sign(payload_b64, secret)}"


def verify_fanout_token(token: str, secret: str, now: float) -> FanoutTokenClaims | None:
    """Return claims when authentic and unexpired, else None. Never raises."""
    if not isinstance(token, str) or not token:
        return None
    parts = token.split(".")
    if len(parts) != 3 or parts[0] != TOKEN_PREFIX:
        return None
    payload_b64, signature = parts[1], parts[2]
    if not hmac.compare_digest(_sign(payload_b64, secret), signature):
        return None
    try:
        payload = json.loads(_b64url_decode(payload_b64))
    except (ValueError, UnicodeDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    camera_id = payload.get("cam")
    expires_at = payload.get("exp")
    if not isinstance(camera_id, str) or not camera_id:
        return None
    if not isinstance(expires_at, int) or expires_at <= now:
        return None
    issued_at = payload.get("iat")
    return FanoutTokenClaims(
        camera_id=camera_id,
        account_id=str(payload.get("acct") or ""),
        subject=str(payload.get("sub") or ""),
        issued_at=issued_at if isinstance(issued_at, int) else 0,
        expires_at=expires_at,
    )
