"""Verify Supabase Auth JWTs presented by dashboard users."""

from __future__ import annotations

from dataclasses import dataclass
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


def verify_supabase_token(token: str, *, secret: str) -> SupabaseUserClaims:
    try:
        payload = jwt.decode(
            token,
            secret,
            algorithms=["HS256"],
            audience="authenticated",
            options={"require": ["exp", "sub"]},
        )
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
