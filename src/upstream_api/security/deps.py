"""FastAPI dependencies for the five identity classes.

Importable as ``Annotated`` aliases so handlers can write
``def handler(agent: CurrentAgent): ...`` without restating Depends.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Annotated
from uuid import UUID

import asyncpg
from cachetools import TTLCache
from fastapi import Depends, Header
from fastapi.security.utils import get_authorization_scheme_param

from upstream_api.config import Settings, get_settings
from upstream_api.db.pool import get_pool
from upstream_api.errors import ForbiddenError, UnauthorizedError
from upstream_api.observability.context import account_id_var, agent_id_var
from upstream_api.security.api_tokens import split_token, verify_secret_async
from upstream_api.security.shared_secret import verify_shared_secret
from upstream_api.security.supabase_jwt import SupabaseJwtError, verify_supabase_token

# Skip writing api_tokens.last_used_at if it was bumped within this window.
# Avoids hot-row contention from a chatty agent fleet without losing freshness.
_LAST_USED_REFRESH = timedelta(seconds=60)

# Per-user membership cache. 60 s TTL means a role change propagates within a
# minute — acceptable for the dashboard, and saves one DB round-trip per call.
_membership_cache: TTLCache[UUID, dict[UUID, str]] = TTLCache(maxsize=2048, ttl=60)


def clear_membership_cache() -> None:
    """Reset the per-user membership cache. Tests only."""
    _membership_cache.clear()


@dataclass(frozen=True, slots=True)
class AgentAuth:
    account_id: UUID
    token_id: UUID
    scopes: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class UserAuth:
    user_id: UUID
    # account_id -> role
    memberships: dict[UUID, str]
    # MFA assurance level + auth-methods. Handlers can gate sensitive ops on
    # ``aal == "aal2"`` once an account-level MFA policy table lands.
    aal: str
    amr: tuple[str, ...]


SettingsDep = Annotated[Settings, Depends(get_settings)]
PoolDep = Annotated[asyncpg.Pool, Depends(get_pool)]


def _extract_bearer(authorization: str | None) -> str:
    if not authorization:
        raise UnauthorizedError("Missing Authorization header.", code="missing_credentials")
    scheme, token = get_authorization_scheme_param(authorization)
    if scheme.lower() != "bearer" or not token:
        raise UnauthorizedError("Expected 'Bearer <token>'.", code="invalid_auth_scheme")
    return token


async def current_agent(
    pool: PoolDep,
    authorization: Annotated[str | None, Header()] = None,
) -> AgentAuth:
    plaintext = _extract_bearer(authorization)
    try:
        prefix, secret = split_token(plaintext)
    except ValueError as exc:
        raise UnauthorizedError("Invalid token format.", code="invalid_token") from exc

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            select id, account_id, hash, scopes, expires_at, revoked_at, last_used_at
              from api_tokens
             where prefix = $1
            """,
            prefix,
        )

        if row is None:
            raise UnauthorizedError("Token not recognized.", code="invalid_token")
        if row["revoked_at"] is not None:
            raise UnauthorizedError("Token has been revoked.", code="token_revoked")
        if row["expires_at"] is not None and row["expires_at"] < datetime.now(tz=UTC):
            # asyncpg returns timezone-aware datetimes for timestamptz columns.
            raise UnauthorizedError("Token has expired.", code="token_expired")
        if not await verify_secret_async(secret, row["hash"]):
            raise UnauthorizedError("Token not recognized.", code="invalid_token")

        last_used_at: datetime | None = row["last_used_at"]
        if last_used_at is None or (datetime.now(tz=UTC) - last_used_at) > _LAST_USED_REFRESH:
            await conn.execute(
                "update api_tokens set last_used_at = now() where id = $1",
                row["id"],
            )

    auth = AgentAuth(
        account_id=row["account_id"],
        token_id=row["id"],
        scopes=tuple(row["scopes"]),
    )
    account_id_var.set(auth.account_id)
    return auth


CurrentAgent = Annotated[AgentAuth, Depends(current_agent)]


async def current_user(
    pool: PoolDep,
    settings: SettingsDep,
    authorization: Annotated[str | None, Header()] = None,
) -> UserAuth:
    token = _extract_bearer(authorization)
    try:
        claims = verify_supabase_token(
            token,
            secret=settings.supabase_jwt_secret.get_secret_value(),
            supabase_url=settings.supabase_url,
        )
    except SupabaseJwtError as exc:
        raise UnauthorizedError(str(exc), code="invalid_user_token") from exc

    cached = _membership_cache.get(claims.user_id)
    if cached is None:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "select account_id, role from account_members where user_id = $1",
                claims.user_id,
            )
        cached = {r["account_id"]: r["role"] for r in rows}
        _membership_cache[claims.user_id] = cached

    return UserAuth(
        user_id=claims.user_id,
        memberships=cached,
        aal=claims.aal,
        amr=claims.amr,
    )


CurrentUser = Annotated[UserAuth, Depends(current_user)]


@dataclass(frozen=True, slots=True)
class PlatformAdminAuth:
    """Fifth identity class: internal staff operating across ALL tenants.

    Backed by the ``platform_admins`` allowlist table. Same Supabase JWT as a
    dashboard user, but deliberately different posture from ``current_user``:
    no membership lookup and no TTL cache (admin traffic is tiny; an uncached
    lookup makes allowlist revocation instant), and ``account_id_var`` is
    never set — admin requests are cross-tenant, so per-request target
    accounts belong in the admin_audit_log, not the logging context.
    """

    user_id: UUID
    email: str | None
    # Carried so an `aal == "aal2"` MFA gate can be added without a
    # signature change.
    aal: str
    amr: tuple[str, ...]


async def current_platform_admin(
    pool: PoolDep,
    settings: SettingsDep,
    authorization: Annotated[str | None, Header()] = None,
) -> PlatformAdminAuth:
    token = _extract_bearer(authorization)
    try:
        claims = verify_supabase_token(
            token,
            secret=settings.supabase_jwt_secret.get_secret_value(),
            supabase_url=settings.supabase_url,
        )
    except SupabaseJwtError as exc:
        raise UnauthorizedError(str(exc), code="invalid_user_token") from exc

    async with pool.acquire() as conn:
        allowed = await conn.fetchval(
            "select 1 from platform_admins where user_id = $1",
            claims.user_id,
        )
    if allowed is None:
        # 403, not 401: the token is valid, the user just isn't on the
        # allowlist — lets the panel distinguish "log in again" from
        # "this account is not a platform admin".
        raise ForbiddenError("Not a platform admin.", code="not_platform_admin")

    return PlatformAdminAuth(
        user_id=claims.user_id,
        email=claims.email,
        aal=claims.aal,
        amr=claims.amr,
    )


CurrentPlatformAdmin = Annotated[PlatformAdminAuth, Depends(current_platform_admin)]


@dataclass(frozen=True, slots=True)
class IngestAuth:
    pass


async def current_ingest(
    settings: SettingsDep,
    x_upstream_webhook_secret: Annotated[str | None, Header()] = None,
) -> IngestAuth:
    if not verify_shared_secret(
        x_upstream_webhook_secret,
        settings.mediamtx_webhook_secret.get_secret_value(),
    ):
        raise UnauthorizedError("Invalid webhook secret.", code="invalid_webhook_secret")
    return IngestAuth()


CurrentIngest = Annotated[IngestAuth, Depends(current_ingest)]


@dataclass(frozen=True, slots=True)
class ProcessorAuth:
    pass


async def current_processor(
    settings: SettingsDep,
    x_upstream_webhook_secret: Annotated[str | None, Header()] = None,
) -> ProcessorAuth:
    if not verify_shared_secret(
        x_upstream_webhook_secret,
        settings.processor_webhook_secret.get_secret_value(),
    ):
        raise UnauthorizedError("Invalid webhook secret.", code="invalid_webhook_secret")
    return ProcessorAuth()


CurrentProcessor = Annotated[ProcessorAuth, Depends(current_processor)]


# ---------------------------------------------------------------------------
# Dual auth: agent token OR dashboard user JWT.
#
# Read endpoints (GET /v1/cameras, /v1/agents, /v1/stream-sessions) are useful
# both for the agent (discovering its own registered cameras after re-install)
# and the dashboard. Disambiguating on token shape (agent tokens begin with
# ``ups_``) keeps one dependency rather than two near-duplicate routes.
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class CallerAuth:
    agent: AgentAuth | None
    user: UserAuth | None

    @property
    def is_agent(self) -> bool:
        return self.agent is not None

    def resolve_account(self, account_id_hint: UUID | None) -> UUID:
        """Pick the account this call should operate on.

        Agents are scoped to a single account by their token. Users may belong
        to several, so they must hint via a query parameter unless they only
        have one membership.
        """
        if self.agent is not None:
            return self.agent.account_id
        assert self.user is not None
        memberships = self.user.memberships
        if account_id_hint is None:
            if len(memberships) == 1:
                return next(iter(memberships))
            raise ForbiddenError(
                "account_id query parameter is required for users with multiple accounts.",
                code="account_id_required",
            )
        if account_id_hint not in memberships:
            raise ForbiddenError(
                "User is not a member of this account.",
                code="forbidden_account",
            )
        return account_id_hint


async def current_caller(
    pool: PoolDep,
    settings: SettingsDep,
    authorization: Annotated[str | None, Header()] = None,
) -> CallerAuth:
    plaintext = _extract_bearer(authorization)
    if plaintext.startswith("ups_"):
        agent_auth = await current_agent(pool=pool, authorization=authorization)
        return CallerAuth(agent=agent_auth, user=None)
    user_auth = await current_user(pool=pool, settings=settings, authorization=authorization)
    return CallerAuth(agent=None, user=user_auth)


CurrentCaller = Annotated[CallerAuth, Depends(current_caller)]


def require_account_role(
    user: UserAuth,
    account_id: UUID,
    *allowed: str,
) -> None:
    """Raise ForbiddenError if the user's role on `account_id` isn't in `allowed`.

    Shared by routes that need finer-grained gating than CallerAuth provides —
    e.g. POST /v1/api-tokens (owner/admin only), GET /v1/api-tokens (same).
    """
    role = user.memberships.get(account_id)
    if role not in allowed:
        raise ForbiddenError(
            "Insufficient role for this account.",
            code="forbidden.role",
            details={"required": sorted(allowed), "account_id": str(account_id)},
        )


def bind_agent_context(auth: AgentAuth, agent_id: UUID) -> None:
    """Bind agent + account into logging context vars for the rest of the request."""
    account_id_var.set(auth.account_id)
    agent_id_var.set(agent_id)
