"""Module 6 M6.3 — webhook destination service.

CRUD over ``event_destinations`` plus the supporting logic: server-side signing
secret generation, pgcrypto encryption of that secret at rest (the API never
decrypts — only the future delivery worker will), strict SSRF-aware URL
validation, and account-scoped filter validation. No outbound delivery here.
"""

from __future__ import annotations

import ipaddress
import secrets
from collections.abc import Sequence
from typing import Any
from urllib.parse import urlsplit
from uuid import UUID

import asyncpg

from upstream_api.errors import BadRequestError
from upstream_api.schemas.event_destinations import (
    EventDestination,
    EventDestinationCreate,
    EventDestinationUpdate,
)
from upstream_api.schemas.events import KNOWN_EVENT_TYPES

# ---------------------------------------------------------------------------
# Signing secret generation.
# ---------------------------------------------------------------------------

_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567"  # base32 without 01OI ambiguity
_SECRET_LEN = 32


def generate_signing_secret(env_label: str) -> tuple[str, str]:
    """Return ``(plaintext, preview)``. Plaintext ``whsec_<env>_<random>`` is
    shown once and stored encrypted; the masked ``preview`` is safe to display."""
    body = "".join(secrets.choice(_ALPHABET) for _ in range(_SECRET_LEN))
    plaintext = f"whsec_{env_label}_{body}"
    preview = f"whsec_{env_label}_...{body[-4:]}"
    return plaintext, preview


# ---------------------------------------------------------------------------
# URL validation (SSRF-aware). https-only; reject loopback/private/link-local/
# reserved IP literals and localhost. DNS-rebinding protection is deferred to
# the M6.4 delivery worker (documented).
# ---------------------------------------------------------------------------

_BLOCKED_HOSTNAMES = {"localhost", "ip6-localhost", "ip6-loopback"}


def validate_webhook_url(url: str) -> str:
    raw = (url or "").strip()
    if not raw:
        raise BadRequestError("Webhook URL is required.", code="invalid_webhook_url")
    parts = urlsplit(raw)
    if parts.scheme != "https":
        raise BadRequestError("Webhook URL must use https://.", code="invalid_webhook_url")
    host = parts.hostname
    if not host:
        raise BadRequestError("Webhook URL must include a host.", code="invalid_webhook_url")
    lowered = host.lower()
    if lowered in _BLOCKED_HOSTNAMES or lowered.endswith(".localhost"):
        raise BadRequestError("Webhook URL host is not allowed.", code="invalid_webhook_url")
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        ip = None
    if ip is not None and (
        ip.is_private
        or ip.is_loopback
        or ip.is_link_local  # incl. 169.254.169.254 cloud metadata
        or ip.is_reserved
        or ip.is_multicast
        or ip.is_unspecified
    ):
        raise BadRequestError(
            "Webhook URL points to a disallowed IP range.", code="invalid_webhook_url"
        )
    return raw


def validate_event_type_filters(filters: Sequence[str]) -> None:
    unknown = [f for f in filters if f not in KNOWN_EVENT_TYPES]
    if unknown:
        raise BadRequestError(
            "Unknown event_type filter values.",
            code="invalid_event_type_filter",
            details={"unknown": unknown, "known": sorted(KNOWN_EVENT_TYPES)},
        )


async def _validate_owned_ids(
    conn: asyncpg.Connection,
    *,
    table: str,
    account_id: UUID,
    ids: Sequence[UUID],
    code: str,
) -> None:
    """Reject filter ids that don't belong to the account (no silent accept)."""
    if not ids:
        return
    # `table` is a fixed internal literal ('cameras' / 'workflows'), never user
    # input — safe to interpolate; the values are parameterized.
    rows = await conn.fetch(
        f"select id from {table} where account_id = $1 and id = any($2::uuid[])",
        account_id,
        list(ids),
    )
    found = {r["id"] for r in rows}
    unknown = [str(i) for i in ids if i not in found]
    if unknown:
        raise BadRequestError(
            "Filter references unknown or cross-account ids.",
            code=code,
            details={"unknown": unknown},
        )


# ---------------------------------------------------------------------------
# SQL.
# ---------------------------------------------------------------------------

# Never selects signing_secret_encrypted — the API never returns/decrypts it.
_DEST_COLUMNS = """
    id, account_id, destination_type, name, url, enabled,
    event_type_filters, camera_id_filters, workflow_id_filters,
    signing_secret_preview, created_at, updated_at,
    last_success_at, last_failure_at, consecutive_failures, disabled_at
"""

_INSERT_SQL = f"""
insert into event_destinations (
    account_id, destination_type, name, url, enabled,
    event_type_filters, camera_id_filters, workflow_id_filters,
    signing_secret_encrypted, signing_secret_preview, created_by
)
values (
    $1, 'webhook', $2, $3, $4,
    $5::text[], $6::uuid[], $7::uuid[],
    pgp_sym_encrypt($8, $9), $10, $11
)
returning {_DEST_COLUMNS}
"""

_LIST_SQL = f"""
select
{_DEST_COLUMNS}
  from event_destinations
 where account_id = $1
   and ($2::boolean is null or enabled = $2)
   and ($3::text is null or destination_type = $3)
 order by created_at desc, id desc
"""

_GET_SQL = f"""
select
{_DEST_COLUMNS}
  from event_destinations
 where account_id = $1
   and id = $2
"""

# Partial update via coalesce: a NULL param leaves the column unchanged. The
# set_updated_at trigger handles updated_at. Toggling `enabled` maintains
# disabled_at (cleared on enable, stamped on disable).
_UPDATE_SQL = f"""
update event_destinations
   set name = coalesce($3::text, name),
       url = coalesce($4::text, url),
       enabled = coalesce($5::boolean, enabled),
       event_type_filters = coalesce($6::text[], event_type_filters),
       camera_id_filters = coalesce($7::uuid[], camera_id_filters),
       workflow_id_filters = coalesce($8::uuid[], workflow_id_filters),
       disabled_at = case
                       when $5::boolean is null then disabled_at
                       when $5::boolean then null
                       else coalesce(disabled_at, now())
                     end
 where account_id = $1
   and id = $2
returning {_DEST_COLUMNS}
"""

_DELETE_SQL = """
delete from event_destinations
 where account_id = $1
   and id = $2
returning id
"""


def _row_to_destination(row: Any) -> EventDestination:
    return EventDestination(
        id=row["id"],
        account_id=row["account_id"],
        destination_type=row["destination_type"],
        name=row["name"],
        url=row["url"],
        enabled=row["enabled"],
        event_type_filters=list(row["event_type_filters"]),
        camera_id_filters=list(row["camera_id_filters"]),
        workflow_id_filters=list(row["workflow_id_filters"]),
        signing_secret_preview=row["signing_secret_preview"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        last_success_at=row["last_success_at"],
        last_failure_at=row["last_failure_at"],
        consecutive_failures=row["consecutive_failures"],
        disabled_at=row["disabled_at"],
    )


async def create_destination(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    created_by: UUID | None,
    payload: EventDestinationCreate,
    enc_key: str,
    env_label: str,
) -> tuple[EventDestination, str]:
    """Validate, generate + encrypt a signing secret, insert. Returns the masked
    destination plus the plaintext secret (to be revealed once by the caller)."""
    url = validate_webhook_url(payload.url)
    validate_event_type_filters(payload.event_type_filters)
    await _validate_owned_ids(
        conn,
        table="cameras",
        account_id=account_id,
        ids=payload.camera_id_filters,
        code="invalid_camera_filter",
    )
    await _validate_owned_ids(
        conn,
        table="workflows",
        account_id=account_id,
        ids=payload.workflow_id_filters,
        code="invalid_workflow_filter",
    )
    plaintext, preview = generate_signing_secret(env_label)
    row = await conn.fetchrow(
        _INSERT_SQL,
        account_id,
        payload.name,
        url,
        payload.enabled,
        list(payload.event_type_filters),
        list(payload.camera_id_filters),
        list(payload.workflow_id_filters),
        plaintext,
        enc_key,
        preview,
        created_by,
    )
    assert row is not None
    return _row_to_destination(row), plaintext


async def list_destinations(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    enabled: bool | None = None,
    destination_type: str | None = None,
) -> list[EventDestination]:
    rows = await conn.fetch(_LIST_SQL, account_id, enabled, destination_type)
    return [_row_to_destination(r) for r in rows]


async def get_destination(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    destination_id: UUID,
) -> EventDestination | None:
    row = await conn.fetchrow(_GET_SQL, account_id, destination_id)
    return _row_to_destination(row) if row is not None else None


async def update_destination(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    destination_id: UUID,
    patch: EventDestinationUpdate,
) -> EventDestination | None:
    """Apply a partial update. Returns ``None`` when the destination does not
    exist or belongs to another account (router → 404)."""
    url = validate_webhook_url(patch.url) if patch.url is not None else None
    if patch.event_type_filters is not None:
        validate_event_type_filters(patch.event_type_filters)
    if patch.camera_id_filters is not None:
        await _validate_owned_ids(
            conn,
            table="cameras",
            account_id=account_id,
            ids=patch.camera_id_filters,
            code="invalid_camera_filter",
        )
    if patch.workflow_id_filters is not None:
        await _validate_owned_ids(
            conn,
            table="workflows",
            account_id=account_id,
            ids=patch.workflow_id_filters,
            code="invalid_workflow_filter",
        )
    row = await conn.fetchrow(
        _UPDATE_SQL,
        account_id,
        destination_id,
        patch.name,
        url,
        patch.enabled,
        list(patch.event_type_filters) if patch.event_type_filters is not None else None,
        list(patch.camera_id_filters) if patch.camera_id_filters is not None else None,
        list(patch.workflow_id_filters) if patch.workflow_id_filters is not None else None,
    )
    return _row_to_destination(row) if row is not None else None


async def delete_destination(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    destination_id: UUID,
) -> bool:
    """Hard-delete (safe before delivery attempts exist — M6.4 may switch to
    soft-delete). Returns False when nothing matched (router → 404)."""
    row = await conn.fetchrow(_DELETE_SQL, account_id, destination_id)
    return row is not None
