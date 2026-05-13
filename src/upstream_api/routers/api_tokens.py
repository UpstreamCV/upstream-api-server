from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Query

from upstream_api.errors import ForbiddenError, NotFoundError
from upstream_api.schemas.api_tokens import (
    ApiTokenCreateRequest,
    ApiTokenCreateResponse,
    ApiTokenListResponse,
    ApiTokenSummary,
)
from upstream_api.security.api_tokens import ALLOWED_SCOPES, issue_token_async
from upstream_api.security.deps import (
    CurrentUser,
    PoolDep,
    SettingsDep,
    require_account_role,
)

router = APIRouter(prefix="/v1/api-tokens", tags=["api-tokens"])


def _resolve_account_for_user(user: CurrentUser, account_id: UUID | None) -> UUID:
    """Pick the account scope for a user-only endpoint.

    Mirrors `CallerAuth.resolve_account` semantics but for routes that don't
    accept agent tokens.
    """
    if account_id is not None:
        if account_id not in user.memberships:
            raise ForbiddenError(
                "User is not a member of this account.",
                code="forbidden_account",
            )
        return account_id
    if len(user.memberships) == 1:
        return next(iter(user.memberships))
    raise ForbiddenError(
        "account_id query parameter is required for users with multiple accounts.",
        code="account_id_required",
    )


@router.get("", response_model=ApiTokenListResponse)
async def list_tokens(
    user: CurrentUser,
    pool: PoolDep,
    account_id: Annotated[UUID | None, Query()] = None,
) -> ApiTokenListResponse:
    """List API tokens for the active account. Owner/admin only."""
    scope_account = _resolve_account_for_user(user, account_id)
    require_account_role(user, scope_account, "owner", "admin")

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            select id, account_id, name, prefix, last_four, scopes,
                   last_used_at, expires_at, revoked_at, created_at
              from api_tokens
             where account_id = $1
             order by created_at desc
            """,
            scope_account,
        )
    tokens = [
        ApiTokenSummary(
            id=r["id"],
            account_id=r["account_id"],
            name=r["name"],
            prefix=r["prefix"],
            last_four=r["last_four"],
            scopes=list(r["scopes"]),
            last_used_at=r["last_used_at"],
            expires_at=r["expires_at"],
            revoked_at=r["revoked_at"],
            created_at=r["created_at"],
        )
        for r in rows
    ]
    return ApiTokenListResponse(tokens=tokens)


@router.post("", response_model=ApiTokenCreateResponse, status_code=201)
async def create_token(
    body: ApiTokenCreateRequest,
    user: CurrentUser,
    pool: PoolDep,
    settings: SettingsDep,
) -> ApiTokenCreateResponse:
    require_account_role(user, body.account_id, "owner", "admin")

    unknown_scopes = set(body.scopes) - ALLOWED_SCOPES
    if unknown_scopes:
        raise ForbiddenError(
            "Requested scopes are not permitted.",
            code="invalid_scopes",
            details={"unknown": sorted(unknown_scopes), "allowed": sorted(ALLOWED_SCOPES)},
        )

    env_label = "live" if settings.is_production else "test"
    issued = await issue_token_async(environment=env_label)

    async with pool.acquire() as conn, conn.transaction():
        row = await conn.fetchrow(
            """
            insert into api_tokens
              (account_id, name, prefix, hash, last_four, scopes, expires_at, created_by)
            values ($1, $2, $3, $4, $5, $6, $7, $8)
            returning id, account_id, name, prefix, last_four, scopes,
                      last_used_at, expires_at, revoked_at, created_at
            """,
            body.account_id,
            body.name,
            issued.prefix,
            issued.hash,
            issued.last_four,
            body.scopes,
            body.expires_at,
            user.user_id,
        )
    assert row is not None

    summary = ApiTokenSummary(
        id=row["id"],
        account_id=row["account_id"],
        name=row["name"],
        prefix=row["prefix"],
        last_four=row["last_four"],
        scopes=list(row["scopes"]),
        last_used_at=row["last_used_at"],
        expires_at=row["expires_at"],
        revoked_at=row["revoked_at"],
        created_at=row["created_at"],
    )
    return ApiTokenCreateResponse(token=summary, plaintext=issued.plaintext)


@router.delete("/{token_id}", status_code=204)
async def revoke_token(
    token_id: UUID,
    user: CurrentUser,
    pool: PoolDep,
) -> None:
    # Returning 404 for both "doesn't exist" and "exists but you can't admin
    # this account" prevents token-id enumeration. The JOIN also obviates the
    # separate _require_admin call.
    async with pool.acquire() as conn, conn.transaction():
        row = await conn.fetchrow(
            """
            select t.revoked_at
              from api_tokens t
              join account_members m
                on m.account_id = t.account_id
               and m.user_id = $2
               and m.role in ('owner', 'admin')
             where t.id = $1
            """,
            token_id,
            user.user_id,
        )
        if row is None:
            raise NotFoundError("Token not found.", code="token_not_found")
        if row["revoked_at"] is not None:
            return
        await conn.execute(
            "update api_tokens set revoked_at = now() where id = $1",
            token_id,
        )
