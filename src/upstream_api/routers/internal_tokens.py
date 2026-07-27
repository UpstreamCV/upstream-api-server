"""Device self-enrollment (migration design doc 15) — internal token mint.

crowdcount-server (a trusted backend) calls this with the dedicated
provision secret to obtain a freshly-minted device-scoped token for a fleet
appliance. Provision is revoke-then-mint, atomically: token secrets are
argon2id-hashed and never retrievable, so a re-enrolling device can only be
handed a brand-new plaintext — and the prior active token MUST be revoked
first or the partial-unique one-active-bound-token index blocks the new
token's TOFU bind at /v1/agents/register.

The plaintext appears in the response exactly once and is never logged —
logs and audit rows carry the prefix at most.
"""

from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter
from pydantic import BaseModel, Field, field_validator

from upstream_api.errors import ApiError
from upstream_api.observability.logging import get_logger
from upstream_api.security.api_tokens import DEVICE_SCOPE, issue_token_async
from upstream_api.security.deps import CurrentProvisioner, PoolDep, SettingsDep
from upstream_api.services.account_audit import try_record_account_audit_log

_logger = get_logger("internal_tokens")

router = APIRouter(prefix="/v1/internal/tokens", tags=["internal-tokens"])

# The rotation's revoke. Every predicate matters: fleet account + token name
# select the device's prior tokens, the scope check spares an agent-scoped
# namesake, and the null check keeps already-revoked rows out of the audit
# trail. Pinned predicate-by-predicate in tests/test_internal_tokens.py so the
# fake conn's re-implementation cannot silently drift from this text.
_REVOKE_PRIOR_TOKENS_SQL = """
update api_tokens
   set revoked_at = now()
 where account_id = $1
   and name = $2
   and $3 = any(scopes)
   and revoked_at is null
returning id
"""


class ProvisionNotConfiguredError(ApiError):
    status_code = 503
    code = "provision_not_configured"


class TokenProvisionRequest(BaseModel):
    # The device's stable identity (CrowdCount device_id, e.g. "P9386").
    # Becomes the token's name — the handle the dashboard shows and the
    # revoke-by-name key for the next rotation.
    external_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")

    @field_validator("external_id", mode="before")
    @classmethod
    def _strip(cls, v: object) -> object:
        return v.strip() if isinstance(v, str) else v


class TokenProvisionResponse(BaseModel):
    token: str  # plaintext, shown exactly once
    prefix: str
    last_four: str
    account_id: UUID
    # Prior active device tokens of the same name revoked by this rotation.
    revoked: int


@router.post("/provision", response_model=TokenProvisionResponse)
async def provision_token(
    body: TokenProvisionRequest,
    _auth: CurrentProvisioner,
    pool: PoolDep,
    settings: SettingsDep,
) -> TokenProvisionResponse:
    """Revoke-then-mint a device-scoped token for one fleet appliance.

    Returns 503 ``provision_not_configured`` when ``fleet_account_id`` is
    unset (same convention as ``billing_not_configured``). The revoke is
    scoped to device tokens only — an agent-scoped token that happens to
    share the name is never touched. The new token is minted unbound
    (``agent_id`` NULL); TOFU binds it at the device's next
    /v1/agents/register.
    """
    # A malformed UUID is the same operator mistake as an unset one — surface
    # the clean 503 rather than a 500 from the UUID constructor.
    if not settings.fleet_account_id:
        raise ProvisionNotConfiguredError(
            "Device token provisioning is not configured for this deployment."
        )
    try:
        fleet_account_id = UUID(settings.fleet_account_id)
    except ValueError as exc:
        raise ProvisionNotConfiguredError(
            "Device token provisioning is not configured for this deployment."
        ) from exc

    env_label = "live" if settings.is_production else "test"
    issued = await issue_token_async(environment=env_label)

    async with pool.acquire() as conn, conn.transaction():
        revoked_rows = await conn.fetch(
            _REVOKE_PRIOR_TOKENS_SQL,
            fleet_account_id,
            body.external_id,
            DEVICE_SCOPE,
        )
        for revoked_row in revoked_rows:
            await try_record_account_audit_log(
                conn,
                account_id=fleet_account_id,
                actor_type="internal",
                event_type="api_token.revoked",
                resource_type="api_token",
                resource_id=str(revoked_row["id"]),
                summary=f"API token {body.external_id!r} revoked.",
                metadata={"token_name": body.external_id},
            )
        row = await conn.fetchrow(
            """
            insert into api_tokens
              (account_id, name, prefix, hash, last_four, scopes, expires_at)
            values ($1, $2, $3, $4, $5, $6, $7)
            returning id
            """,
            fleet_account_id,
            body.external_id,
            issued.prefix,
            issued.hash,
            issued.last_four,
            [DEVICE_SCOPE],
            None,
        )
        assert row is not None
        await try_record_account_audit_log(
            conn,
            account_id=fleet_account_id,
            actor_type="internal",
            event_type="api_token.created",
            resource_type="api_token",
            resource_id=str(row["id"]),
            summary=f"API token {body.external_id!r} created.",
            metadata={"token_name": body.external_id, "scopes": [DEVICE_SCOPE]},
        )

    _logger.info(
        "device_token_provisioned",
        external_id=body.external_id,
        prefix=issued.prefix,
        revoked=len(revoked_rows),
    )
    return TokenProvisionResponse(
        token=issued.plaintext,
        prefix=issued.prefix,
        last_four=issued.last_four,
        account_id=fleet_account_id,
        revoked=len(revoked_rows),
    )
