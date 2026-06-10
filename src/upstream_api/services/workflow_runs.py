"""Workflow Engine M2 — workflow run lifecycle and history reads.

Runs are created and finalized only by the rule evaluator
(services/rules.py) inside the observation-ingest savepoint; this module
owns the SQL for that lifecycle plus the user-facing history reads.
account_id is NOT selected onto read responses (the caller is already
account-scoped).
"""

from __future__ import annotations

import json
from typing import Any
from uuid import UUID

import asyncpg

# A run opens 'active' and is finalized in the same request once its action
# step resolves. The two-step shape (insert → finalize) exists so the alert
# insert can reference workflow_run_id without a circular FK, and so future
# multi-step workflows can hold runs open across requests.
_INSERT_RUN_SQL = """
insert into workflow_runs (
    account_id, camera_id, rule_id, trigger_observation_id,
    workflow_type, title, summary, trigger_type, evidence, payload
)
values ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10::jsonb)
returning id
"""

_FINALIZE_RUN_SQL = """
update workflow_runs
   set status        = $2,
       action_result = $3,
       ended_at      = now()
 where id = $1
"""

# History reads join the alert the run's action created (alerts carry the
# workflow_run_id FK — see 20260613120000_workflow_runs.sql). At most one
# alert per run in the current flow, so the join cannot fan out.
_RUN_COLUMNS = """
    r.id, r.camera_id, r.rule_id, r.trigger_observation_id,
    r.workflow_type, r.status, r.title, r.summary, r.trigger_type,
    r.action_result, r.evidence, r.payload,
    r.started_at, r.ended_at, r.created_at, r.updated_at,
    a.id as alert_id, a.status as alert_status, a.severity as alert_severity
"""

_LIST_RUNS_SQL = f"""
select
{_RUN_COLUMNS}
  from workflow_runs r
  left join alerts a on a.workflow_run_id = r.id
 where r.account_id = $1
   and ($2::text is null or r.status = $2)
   and ($3::uuid is null or r.camera_id = $3)
   and ($4::uuid is null or r.rule_id = $4)
 order by r.created_at desc
 limit $5
"""

_FETCH_RUN_SQL = f"""
select
{_RUN_COLUMNS}
  from workflow_runs r
  left join alerts a on a.workflow_run_id = r.id
 where r.account_id = $1
   and r.id = $2
"""


async def insert_workflow_run(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    camera_id: UUID,
    rule_id: UUID,
    trigger_observation_id: UUID,
    workflow_type: str,
    title: str,
    summary: str,
    trigger_type: str,
    evidence: dict[str, Any],
    payload: dict[str, Any],
) -> UUID:
    new_id = await conn.fetchval(
        _INSERT_RUN_SQL,
        account_id,
        camera_id,
        rule_id,
        trigger_observation_id,
        workflow_type,
        title,
        summary,
        trigger_type,
        json.dumps(evidence),
        json.dumps(payload),
    )
    assert isinstance(new_id, UUID)
    return new_id


async def finalize_workflow_run(
    conn: asyncpg.Connection,
    *,
    workflow_run_id: UUID,
    status: str,
    action_result: str,
) -> None:
    await conn.execute(_FINALIZE_RUN_SQL, workflow_run_id, status, action_result)


async def list_workflow_runs(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    status: str | None,
    camera_id: UUID | None,
    rule_id: UUID | None,
    limit: int,
) -> list[dict[str, Any]]:
    rows = await conn.fetch(_LIST_RUNS_SQL, account_id, status, camera_id, rule_id, limit)
    return [dict(r) for r in rows]


async def fetch_workflow_run(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    workflow_run_id: UUID,
) -> dict[str, Any] | None:
    row = await conn.fetchrow(_FETCH_RUN_SQL, account_id, workflow_run_id)
    return dict(row) if row is not None else None
