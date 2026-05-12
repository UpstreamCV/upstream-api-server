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

    return SupabaseUserClaims(
        user_id=UUID(payload["sub"]),
        email=payload.get("email"),
        role=payload.get("role", "authenticated"),
    )
