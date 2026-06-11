"""Workflow Engine M4 — internal trigger for the time-based evaluator.

Auth reuses the processor shared secret (``CurrentProcessor``): this is an
internal infrastructure surface like the processor endpoints, and reusing
the already-provisioned secret avoids a new credential. Safe to call
repeatedly — the run-insert cooldown guard makes overlapping invocations
best-effort idempotent. Useful for external cron when the in-process loop
is disabled (``UPSTREAM_WORKFLOW_EVALUATOR_INTERVAL_SECONDS=0``) and for
deterministic tests.
"""

from __future__ import annotations

from fastapi import APIRouter
from pydantic import BaseModel

from upstream_api.security.deps import CurrentProcessor, PoolDep
from upstream_api.services.workflow_evaluator import run_workflow_evaluator

router = APIRouter(prefix="/v1/internal/workflow-evaluator", tags=["workflow-evaluator"])


class EvaluatorRunResponse(BaseModel):
    evaluated_rules: int
    schedule_skipped: int
    matched_rules: int
    workflow_runs_created: int
    alerts_created: int
    suppressed_by_cooldown: int
    sequence_runs_expired: int
    errors: int


@router.post("/run", response_model=EvaluatorRunResponse)
async def run_evaluator(
    _auth: CurrentProcessor,
    pool: PoolDep,
) -> EvaluatorRunResponse:
    stats = await run_workflow_evaluator(pool)
    return EvaluatorRunResponse(
        evaluated_rules=stats.evaluated_rules,
        schedule_skipped=stats.schedule_skipped,
        matched_rules=stats.matched_rules,
        workflow_runs_created=stats.workflow_runs_created,
        alerts_created=stats.alerts_created,
        suppressed_by_cooldown=stats.suppressed_by_cooldown,
        sequence_runs_expired=stats.sequence_runs_expired,
        errors=stats.errors,
    )
