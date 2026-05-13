"""Verify Supabase Auth JWTs presented by dashboard users.

Supports both signing-key generations:
  - **HS256** (legacy / "JWT secret" projects): verified with the project's
    static `UPSTREAM_SUPABASE_JWT_SECRET`.
  - **ES256 / RS256** (new asymmetric "JWT signing keys" — the default for
    fresh Supabase projects since late 2024): public key fetched from the
    project's JWKS endpoint at `<supabase_url>/auth/v1/.well-known/jwks.json`
    and matched against the token's `kid`.

The algorithm is detected from the unverified JWT header, so both kinds of
tokens are accepted seamlessly.
"""

from __future__ import annotations

from dataclasses import dataclass
from threading import Lock
from uuid import UUID

import jwt


@dataclass(frozen=True, slots=True)
class SupabaseUserClaims:
    user_id: UUID
    email: str | None
    role: str  # supabase role, e.g. "authenticated"
    # MFA assurance level + auth-methods reference. Surfaced so handlers can
    # gate sensitive operations on `aal == "aal2"` or specific `amr` values.
    # Default to aal1/() when the claim is absent (legacy tokens).
    aal: str
    amr: tuple[str, ...]


class SupabaseJwtError(Exception):
    pass


# JWKS caching. PyJWKClient already caches HTTP responses for `lifespan`
# seconds, but it doesn't internally key on URL — so we keep one client per
# Supabase URL. Lock prevents racing the first fetch.
_JWKS_LIFESPAN_SECONDS = 600
_jwks_clients: dict[str, jwt.PyJWKClient] = {}
_jwks_lock = Lock()


def _get_jwks_client(supabase_url: str) -> jwt.PyJWKClient:
    with _jwks_lock:
        client = _jwks_clients.get(supabase_url)
        if client is None:
            jwks_url = f"{supabase_url.rstrip('/')}/auth/v1/.well-known/jwks.json"
            client = jwt.PyJWKClient(jwks_url, lifespan=_JWKS_LIFESPAN_SECONDS)
            _jwks_clients[supabase_url] = client
        return client


_HMAC_ALGS = {"HS256", "HS384", "HS512"}
_ASYMMETRIC_ALGS = {"ES256", "ES384", "ES512", "RS256", "RS384", "RS512"}


def verify_supabase_token(
    token: str,
    *,
    secret: str,
    supabase_url: str | None = None,
) -> SupabaseUserClaims:
    try:
        header = jwt.get_unverified_header(token)
    except jwt.InvalidTokenError as exc:
        raise SupabaseJwtError(f"Invalid user token: {exc}") from exc

    alg = header.get("alg")
    if not isinstance(alg, str):
        raise SupabaseJwtError("Invalid user token: missing alg in header.")

    try:
        if alg in _HMAC_ALGS:
            payload = jwt.decode(
                token,
                secret,
                algorithms=sorted(_HMAC_ALGS),
                audience="authenticated",
                options={"require": ["exp", "sub"]},
            )
        elif alg in _ASYMMETRIC_ALGS:
            if not supabase_url:
                raise SupabaseJwtError("Supabase URL is required to verify asymmetric JWTs.")
            try:
                signing_key = _get_jwks_client(supabase_url).get_signing_key_from_jwt(token)
            except jwt.PyJWKClientError as exc:
                raise SupabaseJwtError(f"Could not fetch JWKS: {exc}") from exc
            payload = jwt.decode(
                token,
                signing_key.key,
                algorithms=sorted(_ASYMMETRIC_ALGS),
                audience="authenticated",
                options={"require": ["exp", "sub"]},
            )
        else:
            raise SupabaseJwtError(f"Unsupported JWT algorithm: {alg}")
    except SupabaseJwtError:
        raise
    except jwt.ExpiredSignatureError as exc:
        raise SupabaseJwtError("User token expired.") from exc
    except jwt.InvalidTokenError as exc:
        raise SupabaseJwtError(f"Invalid user token: {exc}") from exc

    raw_amr = payload.get("amr") or ()
    amr_methods: tuple[str, ...]
    if isinstance(raw_amr, list):
        amr_methods = tuple(
            entry["method"] if isinstance(entry, dict) and "method" in entry else str(entry)
            for entry in raw_amr
        )
    else:
        amr_methods = ()

    return SupabaseUserClaims(
        user_id=UUID(payload["sub"]),
        email=payload.get("email"),
        role=payload.get("role", "authenticated"),
        aal=payload.get("aal", "aal1"),
        amr=amr_methods,
    )
