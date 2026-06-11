"""GoTrue Admin API client (service-role) for platform-admin user operations.

Used by the /v1/admin/* surface to create auth users and mint recovery links
without touching auth.* tables directly — GoTrue owns its own schema and the
Admin REST API is the supported write path. Calls go through the shared
httpx client from the app lifespan.
"""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

import httpx

from upstream_api.config import Settings
from upstream_api.errors import BadGatewayError, BadRequestError


@dataclass(frozen=True, slots=True)
class CreatedAuthUser:
    user_id: UUID
    email: str


def _admin_headers(settings: Settings) -> dict[str, str]:
    key = settings.supabase_service_role_key.get_secret_value()
    return {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }


async def create_auth_user(
    client: httpx.AsyncClient,
    settings: Settings,
    *,
    email: str,
    password: str,
    workspace_name: str | None = None,
) -> CreatedAuthUser:
    """Create a confirmed email/password auth user via the Admin API.

    NOTE: the handle_new_user trigger fires on the insert and auto-provisions
    a tenant for the new user (workspace from ``workspace_name`` metadata or
    the email handle) — callers that attach the user to an EXISTING account
    end up with the user holding two memberships, which is supported.
    """
    url = f"{settings.supabase_url.rstrip('/')}/auth/v1/admin/users"
    payload: dict[str, object] = {
        "email": email,
        "password": password,
        "email_confirm": True,
    }
    if workspace_name:
        payload["user_metadata"] = {"workspace_name": workspace_name}
    try:
        response = await client.post(url, json=payload, headers=_admin_headers(settings))
    except httpx.HTTPError as exc:
        raise BadGatewayError(
            "Could not reach Supabase Auth.",
            code="auth_admin_unreachable",
            details={"reason": str(exc)},
        ) from exc
    if response.status_code == 422:
        raise BadRequestError(
            "Supabase Auth rejected the user.",
            code="auth_user_invalid",
            details={"body": response.text[:300]},
        )
    if not 200 <= response.status_code < 300:
        raise BadGatewayError(
            "Supabase Auth admin call failed.",
            code="auth_admin_error",
            details={"status": response.status_code, "body": response.text[:300]},
        )
    body = response.json()
    return CreatedAuthUser(user_id=UUID(body["id"]), email=body["email"])


async def generate_recovery_link(
    client: httpx.AsyncClient,
    settings: Settings,
    *,
    email: str,
) -> str:
    """Mint a one-time password-recovery action link for an existing user.

    The link is returned to the calling admin for out-of-band delivery — no
    email is sent by this call (``generate_link`` only creates the link).
    """
    url = f"{settings.supabase_url.rstrip('/')}/auth/v1/admin/generate_link"
    try:
        response = await client.post(
            url,
            json={"type": "recovery", "email": email},
            headers=_admin_headers(settings),
        )
    except httpx.HTTPError as exc:
        raise BadGatewayError(
            "Could not reach Supabase Auth.",
            code="auth_admin_unreachable",
            details={"reason": str(exc)},
        ) from exc
    if not 200 <= response.status_code < 300:
        raise BadGatewayError(
            "Supabase Auth admin call failed.",
            code="auth_admin_error",
            details={"status": response.status_code, "body": response.text[:300]},
        )
    body = response.json()
    link = body.get("action_link")
    if not isinstance(link, str) or not link:
        raise BadGatewayError(
            "Supabase Auth returned no action link.",
            code="auth_admin_error",
        )
    return link
