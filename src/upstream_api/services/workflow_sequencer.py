"""Workflow Engine M5 — the two-step sequencer (A → B within N seconds).

Runs on the observation-ingest path in its own savepoint (mirroring the
rule evaluator's posture — a failing sequencer can never break ingest) and
does two things per observation, completions FIRST so an observation that
both closes one run and starts another behaves deterministically:

1. **Complete**: match the observation against active sequence runs on the
   same camera — follow-up event type, deadline not passed, step
   conditions, and (when configured) same-track correlation. Completion is
   a guarded UPDATE (``where status = 'active'``), so a duplicated
   follow-up observation can never complete a run twice.
2. **Start**: match the observation against enabled sequence workflows'
   start steps. Starting is a guarded INSERT — NOT EXISTS an active run
   for the same workflow with the same ``dedupe_key`` (the start
   observation's processor idempotency key, falling back to the
   observation id), so a retried/duplicated start observation can never
   spawn a second active run. Distinct physical start events (new
   idempotency keys) DO start parallel runs — each is a real instance
   (e.g. two people entering the zone).

Timeouts are closed by the M4 evaluator loop (see
``workflow_evaluator.expire_sequence_runs``): the failure UPDATE is guarded
the same way, so a run fails exactly once, and failure alerts are
cooldown-guarded ACROSS runs of the same workflow (alerts carry no
workflow column, so the guard joins through workflow_runs).

Honesty posture unchanged from M3: a step condition over a field the
payload can't prove never matches, and ``same_track_id`` refuses to start
on a track-less observation (fail closed, validated at authoring too).
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID

import asyncpg

from upstream_api.observability.logging import get_logger
from upstream_api.schemas.workflows import STEP_TARGET_KEY
from upstream_api.services.processor import coerce_jsonb
from upstream_api.services.rules import (
    _classes_allowed,
    _confidence_ok,
    _track_ok,
    schedule_allows,
)

_logger = get_logger("workflow_sequencer")

_ENABLED_SEQUENCES_FOR_CAMERA_SQL = """
select id, name, config, severity, cooldown_seconds
  from workflows
 where account_id = $1
   and camera_id = $2
   and workflow_type = 'sequence'
   and enabled
"""

_ACTIVE_SEQUENCE_RUNS_SQL = """
select r.id, r.workflow_id, r.deadline_at, r.evidence, r.payload,
       w.name as workflow_name, w.config, w.severity, w.cooldown_seconds
  from workflow_runs r
  join workflows w on w.id = r.workflow_id
 where r.account_id = $1
   and r.camera_id = $2
   and r.status = 'active'
   and w.enabled
"""

# Duplicate-start guard: one ACTIVE run per (workflow, dedupe_key). Same
# single-statement posture as every other guard in this codebase.
_START_SEQUENCE_RUN_SQL = """
insert into workflow_runs (
    account_id, camera_id, workflow_id, trigger_observation_id,
    workflow_type, status, title, summary, trigger_type,
    evidence, payload, deadline_at, dedupe_key
)
select $1, $2, $3, $4, 'sequence', 'active', $5, $6, $7,
       $8::jsonb, $9::jsonb, $10, $11
 where not exists (
    select 1
      from workflow_runs
     where workflow_id = $3
       and status = 'active'
       and dedupe_key = $11
 )
returning id
"""

# status = 'active' in the WHERE makes both transitions single-shot.
_COMPLETE_SEQUENCE_RUN_SQL = """
update workflow_runs
   set status = 'completed',
       action_result = $2,
       summary = $3,
       evidence = $4::jsonb,
       ended_at = now()
 where id = $1
   and status = 'active'
returning id
"""

# Failure alerts are cooldown-guarded ACROSS runs of the same workflow —
# alerts have no workflow column, so the guard joins through workflow_runs.
_INSERT_WORKFLOW_ALERT_SQL = """
insert into alerts (
    account_id, camera_id, rule_id, observation_id, workflow_run_id,
    severity, title, message, payload
)
select $1, $2, null, $3, $4, $5, $6, $7, $8::jsonb
 where not exists (
    select 1
      from alerts a
      join workflow_runs r on r.id = a.workflow_run_id
     where r.workflow_id = $9
       and a.created_at > now() - make_interval(secs => $10)
 )
returning id
"""


def step_conditions_match(
    event_type: str, conditions: dict[str, Any], payload: dict[str, Any]
) -> bool:
    """Pure step-condition matcher — M3 semantics: a missing optional key
    means match-any; a payload missing a needed field never matches."""
    target_key = STEP_TARGET_KEY.get(event_type)
    if target_key is not None:
        wanted = conditions.get(target_key)
        if wanted is not None and payload.get(target_key) != wanted:
            return False
    if not _classes_allowed(conditions, payload):
        return False
    if not _track_ok(conditions, payload):
        return False
    if not _confidence_ok(conditions, payload):
        return False
    min_duration = conditions.get("min_duration_seconds")
    if min_duration is not None:
        duration = payload.get("duration_seconds")
        if not isinstance(duration, (int, float)) or duration < min_duration:
            return False
    return True


def _observation_evidence(
    *,
    observation_id: UUID,
    observation_type: str,
    observed_at: datetime,
    payload: dict[str, Any],
) -> dict[str, Any]:
    return {
        "observation_id": str(observation_id),
        "observation_type": observation_type,
        "observed_at": observed_at.isoformat(),
        "payload": payload,
    }


async def _create_workflow_alert(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    camera_id: UUID,
    observation_id: UUID | None,
    workflow_run_id: UUID,
    workflow_id: UUID,
    severity: str,
    title: str,
    message: str,
    evidence: dict[str, Any],
    cooldown_seconds: float,
) -> UUID | None:
    alert_id = await conn.fetchval(
        _INSERT_WORKFLOW_ALERT_SQL,
        account_id,
        camera_id,
        observation_id,
        workflow_run_id,
        severity,
        title,
        message,
        json.dumps(evidence),
        workflow_id,
        cooldown_seconds,
    )
    assert alert_id is None or isinstance(alert_id, UUID)
    return alert_id


async def _try_complete_runs(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    camera_id: UUID,
    observation_id: UUID,
    observation_type: str,
    payload: dict[str, Any],
    observed_at: datetime,
) -> None:
    runs = await conn.fetch(_ACTIVE_SEQUENCE_RUNS_SQL, account_id, camera_id)
    for run in runs:
        config = coerce_jsonb(run["config"])
        then = config.get("then")
        if not isinstance(then, dict) or then.get("event_type") != observation_type:
            continue
        deadline = run["deadline_at"]
        if deadline is not None and observed_at > deadline:
            continue  # too late — the evaluator's expiry sweep owns this run
        conditions = then.get("conditions")
        if isinstance(conditions, dict) and not step_conditions_match(
            observation_type, conditions, payload
        ):
            continue
        run_payload = coerce_jsonb(run["payload"])
        correlation = run_payload.get("correlation")
        if (
            isinstance(correlation, dict)
            and correlation.get("track_id") is not None
            and payload.get("track_id") != correlation["track_id"]
        ):
            continue
        start_at_raw = coerce_jsonb(run["evidence"]).get("start", {}).get("observed_at")
        elapsed: float | None = None
        if isinstance(start_at_raw, str):
            try:
                elapsed = (observed_at - datetime.fromisoformat(start_at_raw)).total_seconds()
            except ValueError:
                elapsed = None
        summary = (
            f"Sequence completed — follow-up arrived after {int(elapsed)}s."
            if elapsed is not None
            else "Sequence completed — follow-up event arrived in time."
        )
        evidence = coerce_jsonb(run["evidence"])
        evidence["follow_up"] = _observation_evidence(
            observation_id=observation_id,
            observation_type=observation_type,
            observed_at=observed_at,
            payload=payload,
        )

        on_success = config.get("on_success")
        wants_alert = isinstance(on_success, dict) and on_success.get("create_alert") is True
        action_result = "sequence_completed"
        completed = await conn.fetchval(
            _COMPLETE_SEQUENCE_RUN_SQL,
            run["id"],
            action_result,
            summary,
            json.dumps(evidence),
        )
        if completed is None:
            continue  # already closed by a racing completion/expiry
        if wants_alert:
            alert_id = await _create_workflow_alert(
                conn,
                account_id=account_id,
                camera_id=camera_id,
                observation_id=observation_id,
                workflow_run_id=run["id"],
                workflow_id=run["workflow_id"],
                severity=run["severity"],
                title=run["workflow_name"],
                message=summary,
                evidence=evidence,
                cooldown_seconds=float(run["cooldown_seconds"]),
            )
            await conn.execute(
                "update workflow_runs set action_result = $2 where id = $1",
                run["id"],
                "alert_created" if alert_id is not None else "alert_suppressed_cooldown",
            )


async def _try_start_runs(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    camera_id: UUID,
    observation_id: UUID,
    observation_type: str,
    payload: dict[str, Any],
    observed_at: datetime,
) -> None:
    workflows = await conn.fetch(_ENABLED_SEQUENCES_FOR_CAMERA_SQL, account_id, camera_id)
    for workflow in workflows:
        config = coerce_jsonb(workflow["config"])
        start = config.get("start")
        then = config.get("then")
        if not isinstance(start, dict) or not isinstance(then, dict):
            continue  # malformed stored config — fail closed
        if start.get("event_type") != observation_type:
            continue
        if not schedule_allows(config, observed_at):
            continue
        conditions = start.get("conditions")
        if isinstance(conditions, dict) and not step_conditions_match(
            observation_type, conditions, payload
        ):
            continue
        correlation = config.get("correlation")
        same_track = isinstance(correlation, dict) and correlation.get("same_track_id") is True
        track_id = payload.get("track_id")
        if same_track and not (isinstance(track_id, str) and track_id):
            continue  # can't correlate without a track — fail closed

        within = then.get("within_seconds")
        if not isinstance(within, int) or isinstance(within, bool) or within <= 0:
            continue  # malformed stored config — fail closed
        deadline = observed_at + timedelta(seconds=within)
        dedupe_raw = payload.get("idempotency_key")
        dedupe_key = (
            dedupe_raw if isinstance(dedupe_raw, str) and dedupe_raw else str(observation_id)
        )
        then_label = str(then.get("event_type", "follow-up"))
        summary = f"Step 1 matched — waiting for {then_label} within {within}s."
        evidence = {
            "start": _observation_evidence(
                observation_id=observation_id,
                observation_type=observation_type,
                observed_at=observed_at,
                payload=payload,
            )
        }
        run_payload: dict[str, Any] = {
            "workflow": {
                "id": str(workflow["id"]),
                "workflow_type": "sequence",
                "config": config,
                "severity": workflow["severity"],
                "cooldown_seconds": workflow["cooldown_seconds"],
            }
        }
        if same_track:
            run_payload["correlation"] = {"track_id": track_id}
        await conn.fetchval(
            _START_SEQUENCE_RUN_SQL,
            account_id,
            camera_id,
            workflow["id"],
            observation_id,
            workflow["name"],
            summary,
            observation_type,
            json.dumps(evidence),
            json.dumps(run_payload),
            deadline,
            dedupe_key,
        )


async def evaluate_sequences(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    camera_id: UUID,
    observation_id: UUID,
    observation_type: str,
    payload: dict[str, Any],
    observed_at: datetime,
) -> None:
    """Run completions then starts for one just-ingested observation."""
    await _try_complete_runs(
        conn,
        account_id=account_id,
        camera_id=camera_id,
        observation_id=observation_id,
        observation_type=observation_type,
        payload=payload,
        observed_at=observed_at,
    )
    await _try_start_runs(
        conn,
        account_id=account_id,
        camera_id=camera_id,
        observation_id=observation_id,
        observation_type=observation_type,
        payload=payload,
        observed_at=observed_at,
    )
