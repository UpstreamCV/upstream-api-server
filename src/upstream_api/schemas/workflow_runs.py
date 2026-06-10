"""Workflow Engine M2 — workflow runs, the internal execution history.

A run is one execution instance of a workflow. In M2 every run is a simple
rule match (``workflow_type = 'simple_rule'``) recorded whether or not an
alert was sent — cooldown-suppressed matches still produce a run, making run
history the system of record and alerts one possible action/output.

Read-side fields are plain ``str`` where the DB column is open vocabulary
(workflow_type, trigger_type, action_result) or where a newer deploy may
write values this build doesn't know (status) — same posture as the
observations read path.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel

WorkflowRunStatus = Literal[
    "active",
    "completed",
    "failed",
    "action_required",
    "suppressed",
]


class WorkflowRunAlertRef(BaseModel):
    """The related alert, when the run's action created one."""

    id: UUID
    status: str
    severity: str


class WorkflowRunSummary(BaseModel):
    id: UUID
    camera_id: UUID | None
    rule_id: UUID | None
    trigger_observation_id: UUID | None
    workflow_type: str
    status: str
    title: str
    summary: str
    trigger_type: str
    action_result: str | None
    evidence: dict[str, Any]
    payload: dict[str, Any]
    alert: WorkflowRunAlertRef | None
    started_at: datetime
    ended_at: datetime | None
    created_at: datetime
    updated_at: datetime


class WorkflowRunListResponse(BaseModel):
    runs: list[WorkflowRunSummary]
