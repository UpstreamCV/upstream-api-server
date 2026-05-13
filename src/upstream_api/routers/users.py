from __future__ import annotations

from fastapi import APIRouter

from upstream_api.schemas.users import CurrentUserResponse, MembershipSummary
from upstream_api.security.deps import CurrentUser, PoolDep

router = APIRouter(prefix="/v1/me", tags=["users"])


@router.get("", response_model=CurrentUserResponse)
async def get_me(user: CurrentUser, pool: PoolDep) -> CurrentUserResponse:
    """Return the authenticated user plus their account memberships.

    The dashboard uses this to populate the active-account picker without
    going to Supabase directly for `account_members`.
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            select a.id as account_id, a.name as account_name,
                   a.slug as account_slug, m.role
              from account_members m
              join accounts a on a.id = m.account_id
             where m.user_id = $1
             order by a.created_at asc
            """,
            user.user_id,
        )

    memberships = [
        MembershipSummary(
            account_id=r["account_id"],
            account_name=r["account_name"],
            account_slug=r["account_slug"],
            role=r["role"],
        )
        for r in rows
    ]
    return CurrentUserResponse(
        user_id=user.user_id,
        aal=user.aal,
        amr=list(user.amr),
        memberships=memberships,
    )
