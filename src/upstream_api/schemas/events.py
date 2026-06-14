"""Module 6 M6.1 — canonical event ledger schema.

The durable, append-only record of meaningful product events. M6.1 emits only
workflow-run-derived events (see ``services/events.py``); the envelope is
shaped to also carry processor / reliability / observation-derived events in
later milestones without a migration.

Read-side fields are plain ``str`` where the DB column is open vocabulary
(``event_type``, ``source``) — same posture as the workflow_runs / observations
read paths: a newer deploy may write values this build doesn't know, and the
ledger is replayed at the version each row was written at.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, Field

# Bumped per event_type when its payload contract changes in a breaking way.
EVENT_SCHEMA_VERSION = 1

# The canonical event_type vocabulary this build understands. Stored
# open-vocab in the DB (no CHECK); this Literal documents and whitelists the
# supported set for the future Event REST API (M6.2). M6.1's finalize seam
# only ever produces ``alert.created`` (a run completed by creating an alert)
# and ``workflow.cooldown_suppressed`` (a match suppressed inside cooldown);
# the remaining types are defined for future producers (sequence
# success/failure/timeout, run start) that will route through this seam later.
EventType = Literal[
    "workflow.started",
    "workflow.succeeded",
    "workflow.failed",
    "workflow.timed_out",
    "workflow.cooldown_suppressed",
    "alert.created",
    "workflow.finalized",
]

# Producer family that emitted the event.
EventSource = Literal["rule", "timer", "workflow", "processor", "reliability"]


class CanonicalEvent(BaseModel):
    """One row of the ``events`` ledger.

    Not yet exposed by any route — the Event REST API is M6.2. This model
    exists so the ledger has a typed, validated representation that the future
    read API and tests can share.
    """

    id: UUID
    seq: int
    event_type: str
    schema_version: int
    account_id: UUID
    camera_id: UUID | None = None
    session_id: UUID | None = None
    source: str
    source_observation_id: UUID | None = None
    workflow_run_id: UUID | None = None
    source_alert_id: UUID | None = None
    source_finding_id: UUID | None = None
    rule_id: UUID | None = None
    workflow_id: UUID | None = None
    dedupe_key: str
    occurred_at: datetime
    created_at: datetime
    payload: dict[str, Any] = Field(default_factory=dict)
    evidence: dict[str, Any] = Field(default_factory=dict)
    reliability_context: dict[str, Any] | None = None


class EventListResponse(BaseModel):
    """Keyset-paginated page of canonical events, newest-first by ``seq``."""

    items: list[CanonicalEvent]
    # ``seq`` of the last (smallest-seq) item when the page is full — pass back
    # as ?before_seq= for the next, older page. Null when there are no more
    # rows. A seq keyset cursor, not an opaque token.
    next_before_seq: int | None

