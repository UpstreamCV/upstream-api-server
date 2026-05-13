from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter

from upstream_api.errors import ForbiddenError, NotFoundError
from upstream_api.schemas.api_tokens import (
    ApiTokenCreateRequest,
    ApiTokenCreateResponse,
    ApiTokenSummary,
)
from upstream_api.security.api_tokens import ALLOWED_SCOPES, issue_token_async
from upstream_api.security.deps import CurrentUser, PoolDep, SettingsDep

router = APIRouter(prefix="/v1/api-tokens", tags=["api-tokens"])


def _require_admin(user: CurrentUser, account_id: UUID) -> None:
    role = user.memberships.get(account_id)
    if role not in {"owner", "admin"}:
        raise ForbiddenError(
            "Insufficient role for this account.",
            code="forbidden.role",
            details={"required": ["owner", "admin"], "account_id": str(account_id)},
        )


@router.post("", response_model=ApiTokenCreateResponse, status_code=201)
async def create_token(
    body: ApiTokenCreateRequest,
    user: CurrentUser,
    pool: PoolDep,
    settings: SettingsDep,
) -> ApiTokenCreateResponse:
    _require_admin(user, body.account_id)

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
