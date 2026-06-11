"""Workflow Engine M5 — workflow definition CRUD.

account_id is NOT selected onto read responses (the caller is already
account-scoped) — same posture as the rules service.
"""

from __future__ import annotations

import json
from typing import Any
from uuid import UUID

import asyncpg

from upstream_api.schemas.workflows import WorkflowCreateRequest

_INSERT_WORKFLOW_SQL = """
insert into workflows (
    account_id, camera_id, name, description, workflow_type, enabled,
    config, severity, cooldown_seconds, created_by
)
values ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9, $10)
returning id, camera_id, name, description, workflow_type, enabled, config,
          severity, cooldown_seconds, created_by, created_at, updated_at
"""

_LIST_WORKFLOWS_SQL = """
select
    id, camera_id, name, description, workflow_type, enabled, config,
    severity, cooldown_seconds, created_by, created_at, updated_at
  from workflows
 where account_id = $1
   and ($2::uuid is null or camera_id = $2)
   and ($3::text is null or workflow_type = $3)
 order by created_at desc
"""

_FETCH_WORKFLOW_SQL = """
select
    id, camera_id, name, description, workflow_type, enabled, config,
    severity, cooldown_seconds, created_by, created_at, updated_at
  from workflows
 where account_id = $1
   and id = $2
"""

# PATCH semantics: a null parameter keeps the current value. ``description``
# uses a sentinel-free posture: passing null keeps it (clearing a
# description is not supported in M5 — set a new one instead).
_UPDATE_WORKFLOW_SQL = """
update workflows
   set name             = coalesce($3, name),
       description      = coalesce($4, description),
       enabled          = coalesce($5, enabled),
       config           = coalesce($6::jsonb, config),
       severity         = coalesce($7, severity),
       cooldown_seconds = coalesce($8, cooldown_seconds)
 where account_id = $1
   and id = $2
returning id, camera_id, name, description, workflow_type, enabled, config,
          severity, cooldown_seconds, created_by, created_at, updated_at
"""

_DELETE_WORKFLOW_SQL = """
delete from workflows
 where account_id = $1
   and id = $2
"""


async def insert_workflow(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    created_by: UUID | None,
    req: WorkflowCreateRequest,
) -> dict[str, Any]:
    row = await conn.fetchrow(
        _INSERT_WORKFLOW_SQL,
        account_id,
        req.camera_id,
        req.name,
        req.description,
        req.workflow_type,
        req.enabled,
        json.dumps(req.config),
        req.severity,
        req.cooldown_seconds,
        created_by,
    )
    assert row is not None  # INSERT ... RETURNING always yields a row.
    return dict(row)


async def list_workflows(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    camera_id: UUID | None,
    workflow_type: str | None,
) -> list[dict[str, Any]]:
    rows = await conn.fetch(_LIST_WORKFLOWS_SQL, account_id, camera_id, workflow_type)
    return [dict(r) for r in rows]


async def fetch_workflow(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    workflow_id: UUID,
) -> dict[str, Any] | None:
    row = await conn.fetchrow(_FETCH_WORKFLOW_SQL, account_id, workflow_id)
    return dict(row) if row is not None else None


async def update_workflow(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    workflow_id: UUID,
    name: str | None,
    description: str | None,
    enabled: bool | None,
    config: dict[str, Any] | None,
    severity: str | None,
    cooldown_seconds: int | None,
) -> dict[str, Any] | None:
    row = await conn.fetchrow(
        _UPDATE_WORKFLOW_SQL,
        account_id,
        workflow_id,
        name,
        description,
        enabled,
        json.dumps(config) if config is not None else None,
        severity,
        cooldown_seconds,
    )
    return dict(row) if row is not None else None


async def delete_workflow(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    workflow_id: UUID,
) -> bool:
    status = await conn.execute(_DELETE_WORKFLOW_SQL, account_id, workflow_id)
    return bool(status == "DELETE 1")
