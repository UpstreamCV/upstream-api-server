"""Reliability findings -- rule-based evaluator.

One entry point, ``evaluate_camera``. It pulls a small set of facts the
api-server already persists (latest stream session, latest processor status,
latest snapshot timestamp + image bytes, latest observation), runs each rule,
and upserts/resolves findings via services/reliability_repo.

Five rules ship today:

- ``stream_valid_processor_silent``  (critical) -- camera is sending frames
  but the processor isn't producing observations. "Live" here means an
  un-ended, un-expired session in {streaming, approved} OR a fresh snapshot
  -- not "the WebRTC player is showing video", which is a client-side concern.
- ``latest_frame_stale``             (warning)  -- the snapshot pipeline has
  fallen behind.
- ``processor_using_default_geometry`` (info | warning) -- the processor never
  picked up operator-saved geometry, or fell back from it on error.
- ``scene_too_dark_for_reliable_detection`` (warning, M1) -- mean grayscale
  brightness of the latest frame is below ``_BRIGHTNESS_THRESHOLD``. First
  rule on the platform that talks about scene quality rather than workflow
  plumbing. Brightness lookup happens during fact gathering through
  ``services/snapshot_brightness`` (cached) so the rule stays a pure function.
- ``unstable_detection_confidence`` (warning, M2) -- avg / low_ratio / stddev
  of the per-detection confidences in the latest perception snapshot crosses
  any warning threshold. First rule that talks about model detection quality
  rather than scene or plumbing. Stats are computed during fact gathering
  through ``services/detection_confidence`` (pure functions) so the rule
  stays a pure function.

Each rule returns one of:

- ``_RuleOpen(...)``      -- open or refresh a finding (drives an upsert)
- ``_RuleResolved(reason=...)`` -- preconditions met but no problem; resolve any
  existing open finding under this dedupe_key with the given reason
- ``None``                -- insufficient data; do nothing (neither open nor
  resolve). This is how "we don't know yet" stays distinct from "healthy".
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import asyncpg

from upstream_api.config import Settings
from upstream_api.services import snapshot_brightness as snapshot_brightness_module
from upstream_api.services.detection_confidence import (
    AVG_CONFIDENCE_WARNING_THRESHOLD,
    CONFIDENCE_DROP_RATIO_WARNING_THRESHOLD,
    CONFIDENCE_STDDEV_WARNING_THRESHOLD,
    LOW_CONFIDENCE_RATIO_WARNING_THRESHOLD,
    LOW_CONFIDENCE_THRESHOLD,
    MIN_SAMPLE_COUNT,
    RECENT_WINDOW_SECONDS,
    DetectionConfidenceStats,
    compute_detection_confidence_stats,
    extract_detection_confidences,
)
from upstream_api.services.image_analysis import ImageBrightness
from upstream_api.services.processor import (
    coerce_jsonb,
    fetch_latest_perception_snapshot,
    fetch_latest_processor_status,
)
from upstream_api.services.reliability_repo import (
    resolve_open_findings,
    upsert_open_finding,
)

# Bump when a rule's trigger logic or thresholds change. Stamped on every
# finding row so historical entries stay attributable to the version of the
# detector that wrote them.
#   v0.1 -- M0 plumbing (scene_too_dark reserved/no-op)
#   v0.2 -- M1 ships scene_too_dark for real
#   v0.3 -- M2 ships unstable_detection_confidence
#   v0.4 -- M2.1 tightens _is_session_live (expires_at + ended_at) and
#           rewords stream_valid_processor_silent away from "stream is live"
DETECTOR_VERSION = "reliability.v0.4"

# Thresholds. Single source of truth for both the rule and the tests. Numbers
# are intentionally conservative — better to flag a healthy session than to
# silently miss a broken one. Tune later with real telemetry.
_LATEST_FRAME_STALE_THRESHOLD_S = 60
_PROCESSOR_STATUS_STALE_THRESHOLD_S = 60
_OBSERVATION_STALE_THRESHOLD_S = 90
# How recently the stream / processor must have ticked for an "unknown" verdict
# to flip into "healthy" instead. Aligns with the summary endpoint's contract.
_UNKNOWN_DATA_WINDOW_S = 300

# Mean grayscale brightness below which a scene is "too dark for reliable
# detection". 0..255 scale. Heuristic only -- not a model-accuracy claim.
# Picked below typical indoor / dusk lighting; tune with real telemetry in M2+.
_BRIGHTNESS_THRESHOLD = 45

# Dedupe keys are the persistent identity of a finding under one
# (camera, finding_type) — kept as constants so the evaluator and the repo
# resolve helper agree on spelling.
_DEDUPE_STREAM_SILENT = "stream:stream_valid_processor_silent"
_DEDUPE_FRAME_STALE = "stream:latest_frame_stale"
_DEDUPE_SCENE_DARK = "scene_quality:scene_too_dark_for_reliable_detection"
_DEDUPE_DEFAULT_GEOMETRY = "geometry:processor_using_default_geometry"
_DEDUPE_DETECTION_CONFIDENCE = "detection:unstable_detection_confidence"


def _dedupe_key(camera_id: UUID, suffix: str) -> str:
    return f"camera:{camera_id}:{suffix}"


# ---------------------------------------------------------------------------
# Rule outputs.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _RuleOpen:
    finding_type: str
    category: str
    severity: str
    title: str
    message: str
    likely_cause: str | None
    recommended_action: str | None
    evidence: dict[str, Any]
    stream_session_id: UUID | None = None
    processor_status_id: UUID | None = None


@dataclass(frozen=True)
class _RuleResolved:
    # Optional rule-specific resolve reason — stamped onto resolved_reason on
    # the row so history records why a finding closed. Defaults to "healthy"
    # for rules that don't have a more specific story to tell. The brightness
    # rule uses "brightness_above_threshold" to distinguish the recovered case
    # from a fact-shape change.
    reason: str = "healthy"


_RuleOutput = _RuleOpen | _RuleResolved | None


# ---------------------------------------------------------------------------
# Fact gathering.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _Facts:
    now: datetime
    latest_session: dict[str, Any] | None
    latest_processor_status: dict[str, Any] | None
    latest_snapshot: dict[str, Any] | None
    latest_observation_at: datetime | None
    has_saved_geometry: bool
    # M1: brightness of the latest snapshot. Populated only when the snapshot
    # is recent enough to be worth evaluating (see _gather_facts). ``None``
    # means "skip the brightness rule" — either there's no snapshot, it's
    # stale (the stale-frame rule owns that case), or the decode failed.
    snapshot_brightness: ImageBrightness | None = None
    # M2: latest perception snapshot row + summary stats over the per-detection
    # confidences in its ``objects`` array. Populated only when the underlying
    # snapshot row is recent (latest_perception_snapshots tracks "updated_at",
    # not capture time, but it's the freshest signal we have). ``None`` means
    # "skip the confidence rule" -- no row, stale, too few samples, or no
    # confidences in the payload.
    latest_perception_snapshot: dict[str, Any] | None = None
    detection_confidence_stats: DetectionConfidenceStats | None = None


# Minimal column set — we never need the full row, and dragging the whole row
# in would couple the evaluator to schema churn elsewhere.
_LATEST_SESSION_SQL = """
select id, state, created_at, started_at, ended_at, expires_at
  from stream_sessions
 where account_id = $1
   and camera_id  = $2
 order by created_at desc
 limit 1
"""

_LATEST_SNAPSHOT_SQL = """
select id, captured_at, created_at, session_id, storage_key, width, height
  from snapshots
 where account_id = $1
   and camera_id  = $2
 order by captured_at desc
 limit 1
"""

_LATEST_OBSERVATION_AT_SQL = """
select max(observed_at) as observed_at
  from observations
 where account_id = $1
   and camera_id  = $2
"""

_SAVED_GEOMETRY_EXISTS_SQL = """
select 1
  from camera_geometry_configs
 where account_id = $1
   and camera_id  = $2
 limit 1
"""


async def _gather_facts(
    conn: asyncpg.Connection,
    *,
    settings: Settings,
    account_id: UUID,
    camera_id: UUID,
    now: datetime,
) -> _Facts:
    session_row = await conn.fetchrow(_LATEST_SESSION_SQL, account_id, camera_id)
    status_row = await fetch_latest_processor_status(
        conn,
        account_id=account_id,
        camera_id=camera_id,
        stream_session_id=None,
    )
    snapshot_row = await conn.fetchrow(_LATEST_SNAPSHOT_SQL, account_id, camera_id)
    obs_row = await conn.fetchrow(_LATEST_OBSERVATION_AT_SQL, account_id, camera_id)
    geom_row = await conn.fetchrow(_SAVED_GEOMETRY_EXISTS_SQL, account_id, camera_id)
    perception_row = await fetch_latest_perception_snapshot(
        conn, account_id=account_id, camera_id=camera_id
    )

    snapshot = dict(snapshot_row) if snapshot_row is not None else None
    snapshot_brightness = await _maybe_load_brightness(
        settings=settings, snapshot=snapshot, now=now
    )
    confidence_stats = _maybe_compute_confidence_stats(perception=perception_row, now=now)

    return _Facts(
        now=now,
        latest_session=dict(session_row) if session_row is not None else None,
        latest_processor_status=status_row,
        latest_snapshot=snapshot,
        latest_observation_at=(obs_row["observed_at"] if obs_row is not None else None),
        has_saved_geometry=geom_row is not None,
        snapshot_brightness=snapshot_brightness,
        latest_perception_snapshot=perception_row,
        detection_confidence_stats=confidence_stats,
    )


async def _maybe_load_brightness(
    *,
    settings: Settings,
    snapshot: dict[str, Any] | None,
    now: datetime,
) -> ImageBrightness | None:
    """Load brightness only when it's worth paying for.

    Skip when:
      - there is no snapshot
      - the snapshot is older than the stale-frame threshold (the stale-frame
        rule already owns that case; warning about darkness on a stale image
        would be lying about current scene state)
      - the snapshot row is missing ``storage_key`` (defensive — shouldn't
        happen in prod, but tests using minimal row stubs may omit it)

    Otherwise the cached brightness loader runs and returns either a fresh
    reading or ``None`` (download / decode failure). The caller stamps the
    result on ``_Facts``; the rule does the comparison.
    """
    if snapshot is None:
        return None
    storage_key = snapshot.get("storage_key")
    if not storage_key:
        return None
    captured_at = snapshot.get("captured_at")
    age = _age_seconds(now, captured_at)
    if age is None or age > _LATEST_FRAME_STALE_THRESHOLD_S:
        return None
    snapshot_id = snapshot.get("id")
    if snapshot_id is None:
        return None
    return await snapshot_brightness_module.get_brightness_for_snapshot(
        settings=settings,
        snapshot_id=snapshot_id,
        storage_key=storage_key,
    )


def _maybe_compute_confidence_stats(
    *,
    perception: dict[str, Any] | None,
    now: datetime,
) -> DetectionConfidenceStats | None:
    """Compute confidence stats from the latest perception snapshot, when worth it.

    Skip when:
      - no perception snapshot row
      - the row is older than the stale-frame threshold (the stale-frame rule
        owns that case; warning about detection confidence on out-of-date
        evidence would be lying about current model behavior)
      - the snapshot JSONB is missing or not a dict
      - the extractor finds fewer than MIN_SAMPLE_COUNT confidences (handled
        by ``compute_detection_confidence_stats`` returning ``None``)

    The perception_row's ``observed_at`` is the freshness signal -- it's set
    by the processor at frame time, not at UPSERT time.
    """
    if perception is None:
        return None
    observed_at = perception.get("observed_at")
    age = _age_seconds(now, observed_at)
    if age is None or age > _LATEST_FRAME_STALE_THRESHOLD_S:
        return None
    payload = coerce_jsonb(perception.get("snapshot"))
    if not payload:
        return None
    values = extract_detection_confidences(payload)
    return compute_detection_confidence_stats(values)


# ---------------------------------------------------------------------------
# Helpers shared by rules.
# ---------------------------------------------------------------------------


def _age_seconds(now: datetime, value: datetime | None) -> float | None:
    """Seconds between ``now`` and ``value``. ``None`` if value missing."""
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return (now - value).total_seconds()


def _is_session_live(session: dict[str, Any] | None, *, now: datetime | None = None) -> bool:
    """A session counts as live for reliability purposes when:

      - state is ``streaming`` or ``approved`` (waiting on the agent to start),
      - ``ended_at`` is unset (an ended session is not live regardless of state),
      - ``expires_at`` is in the future when set (a session past its expiry is
        not live -- the cleanup is best-effort and the state column can lag
        the eligibility-gate SQL by minutes or hours in practice).

    The ``state`` + ``expires_at`` combination mirrors the eligibility-gate
    in ``services/processor._ELIGIBLE_SESSIONS_SQL`` so the reliability rule
    and the processor's own "is this session worth working on" check agree.
    Without these guards an expired-but-not-cleaned session looks live to
    the rule, producing a false "stream is live but processor silent"
    finding for a workflow that has actually been over for hours.
    """
    if session is None:
        return False
    if session.get("state") not in {"streaming", "approved"}:
        return False
    if session.get("ended_at") is not None:
        return False
    expires_at = session.get("expires_at")
    if expires_at is not None and now is not None:
        # _age_seconds returns positive when now > expires_at, i.e. expired.
        age = _age_seconds(now, expires_at)
        if age is not None and age >= 0:
            return False
    return True


def _coerce_payload(value: Any) -> dict[str, Any]:
    """Same posture as services/processor.coerce_jsonb but tolerant of test
    fake rows that pre-serialize ``payload`` as a JSON string. Lets us reuse
    facts() from both prod (asyncpg returns ``str``) and tests (FakeConn
    returns ``dict``)."""
    if isinstance(value, dict):
        return value
    if value in (None, ""):
        return {}
    try:
        return coerce_jsonb(value)
    except (ValueError, TypeError, json.JSONDecodeError):
        return {}


# ---------------------------------------------------------------------------
# Rules.
# ---------------------------------------------------------------------------


def _evaluate_stream_valid_processor_silent(facts: _Facts) -> _RuleOutput:
    """Stream appears live but the processor is not producing observations.

    Precondition: stream is "live" (session in {streaming, approved}) OR the
    snapshot pipeline ticked in the last minute. If neither — there's nothing
    for the processor to be silent about; insufficient data.

    Trigger: processor_status is missing OR > 60s old, AND there's been no
    observation for > 90s.

    Reuse: ``camera_id`` / ``account_id`` / ``stream_session_id`` come from the
    session row; ``processor_status.id`` is linked if a status row exists so
    operators can trace which run was silent.
    """
    session = facts.latest_session
    snapshot = facts.latest_snapshot
    snapshot_age = _age_seconds(facts.now, snapshot["captured_at"]) if snapshot else None
    session_live = _is_session_live(session, now=facts.now)
    snapshot_fresh = snapshot_age is not None and snapshot_age < _LATEST_FRAME_STALE_THRESHOLD_S

    if not session_live and not snapshot_fresh:
        # The workflow that could have triggered this finding is no longer
        # current: the session has ended or expired AND there's no fresh
        # snapshot. A previously-open finding describes a past run and is
        # not actionable any more -- resolve it. ``resolve_open_findings``
        # is a no-op when no open row matches, so this is safe on cameras
        # that have never had a finding.
        return _RuleResolved(reason="stream_no_longer_live")

    status = facts.latest_processor_status
    status_age = _age_seconds(facts.now, status["updated_at"]) if status else None
    obs_age = _age_seconds(facts.now, facts.latest_observation_at)

    status_stale = status is None or (
        status_age is not None and status_age > _PROCESSOR_STATUS_STALE_THRESHOLD_S
    )
    observations_stale = obs_age is None or obs_age > _OBSERVATION_STALE_THRESHOLD_S

    if not (status_stale and observations_stale):
        return _RuleResolved()

    evidence: dict[str, Any] = {
        "stream_state": session.get("state") if session else None,
        "latest_frame_age_seconds": (int(snapshot_age) if snapshot_age is not None else None),
        "processor_status_age_seconds": (int(status_age) if status_age is not None else None),
        "last_observation_age_seconds": (int(obs_age) if obs_age is not None else None),
        "stream_session_id": str(session["id"]) if session else None,
    }

    return _RuleOpen(
        finding_type="stream_valid_processor_silent",
        category="stream",
        severity="critical",
        title="Camera is sending frames but processor is silent",
        message=(
            "The camera is uploading frames, but the processor is not "
            "producing recent observations. This workflow should not be "
            "trusted until processor output resumes."
        ),
        likely_cause="processor_runtime_or_observation_write_issue",
        recommended_action=(
            "Check processor worker health, observation writes, and package "
            "eligibility for this session."
        ),
        evidence=evidence,
        stream_session_id=session["id"] if session else None,
        processor_status_id=status["id"] if status else None,
    )


def _evaluate_latest_frame_stale(facts: _Facts) -> _RuleOutput:
    """The latest snapshot is older than the freshness threshold while a
    session is meant to be running.

    Precondition: a snapshot has ever been captured AND the latest session is
    in {streaming, approved}. A camera that's deliberately offline (ended /
    expired session) is not a reliability problem — it's just idle.
    """
    snapshot = facts.latest_snapshot
    session = facts.latest_session

    if snapshot is None:
        # No snapshot at all -- we can't claim staleness; leave any
        # previously-open finding alone (insufficient data).
        return None
    if not _is_session_live(session, now=facts.now):
        # The camera is intentionally idle (session ended / expired). A
        # previously-open stale-frame finding describes a past run and is
        # no longer actionable; resolve it.
        return _RuleResolved(reason="stream_no_longer_live")

    age = _age_seconds(facts.now, snapshot["captured_at"])
    if age is None:
        return None

    if age <= _LATEST_FRAME_STALE_THRESHOLD_S:
        return _RuleResolved()

    return _RuleOpen(
        finding_type="latest_frame_stale",
        category="stream",
        severity="warning",
        title="Latest frame is stale",
        message=(
            "The latest frame for this camera is stale. The dashboard preview "
            "or processor evidence may not reflect the current scene."
        ),
        likely_cause="stream_snapshot_or_media_path_issue",
        recommended_action=(
            "Check stream health, snapshot extraction, and recent camera heartbeats."
        ),
        evidence={
            "latest_frame_age_seconds": int(age),
            "threshold_seconds": _LATEST_FRAME_STALE_THRESHOLD_S,
            "stream_state": session.get("state") if session else None,
            "stream_session_id": str(session["id"]) if session else None,
        },
        stream_session_id=session["id"] if session else None,
    )


def _evaluate_scene_too_dark(facts: _Facts) -> _RuleOutput:
    """Mean grayscale brightness of the latest snapshot is below threshold.

    This is the first **production CV** signal on the platform — every other
    rule today talks about workflow plumbing (stream / processor / geometry).
    A dark scene is a heuristic warning that detection confidence may be
    unreliable, regardless of whether the rest of the stack is healthy.

    Brightness lookup happens during fact gathering (see
    ``_maybe_load_brightness``) so the rule itself stays a pure function. The
    fact gatherer skips the brightness load entirely when the snapshot is
    stale, so the precondition here just checks "did we get a reading".

    Trigger:
        snapshot_brightness.mean < _BRIGHTNESS_THRESHOLD

    Resolve:
        snapshot_brightness.mean >= _BRIGHTNESS_THRESHOLD

    Skip (no finding either way):
        snapshot_brightness is None — no snapshot, stale, or decode failed.
        We don't lie about a dark scene from data we don't have.
    """
    brightness = facts.snapshot_brightness
    snapshot = facts.latest_snapshot
    if brightness is None or snapshot is None:
        return None

    if brightness.mean >= _BRIGHTNESS_THRESHOLD:
        return _RuleResolved(reason="brightness_above_threshold")

    return _RuleOpen(
        finding_type="scene_too_dark_for_reliable_detection",
        category="scene_quality",
        severity="warning",
        title="Scene may be too dark for reliable detection",
        message=(
            "This camera appears dark, which may reduce detection confidence "
            "and make events less reliable."
        ),
        likely_cause="low_light",
        recommended_action=(
            "Improve lighting, move the camera, or test a low-light package for this camera."
        ),
        evidence={
            # Round mean to one decimal — the precision in the underlying
            # ImageStat mean is misleading for a 0-255 heuristic.
            "brightness_mean": round(brightness.mean, 1),
            "brightness_threshold": _BRIGHTNESS_THRESHOLD,
            "snapshot_id": str(snapshot["id"]),
            "captured_at": (
                snapshot["captured_at"].isoformat()
                if isinstance(snapshot["captured_at"], datetime)
                else snapshot["captured_at"]
            ),
            "image_width": brightness.width,
            "image_height": brightness.height,
            "source": "latest_frame_snapshot",
        },
        stream_session_id=(snapshot.get("session_id") if isinstance(snapshot, dict) else None),
    )


# Map applied_geometry shape → (severity, title_suffix, evidence_subset). One
# place to read when the contract shifts.
def _evaluate_default_geometry(facts: _Facts) -> _RuleOutput:
    """Processor is running on default (or fallback) geometry instead of the
    operator-saved one.

    Precondition: a processor status exists and is recent (otherwise we don't
    know what geometry the processor is using). Insufficient otherwise.
    """
    status = facts.latest_processor_status
    if status is None:
        return None
    status_age = _age_seconds(facts.now, status["updated_at"])
    if status_age is None or status_age > _UNKNOWN_DATA_WINDOW_S:
        return None

    error_code = status.get("error_code")
    payload = _coerce_payload(status.get("payload"))
    applied = payload.get("applied_geometry") if isinstance(payload, dict) else None

    # Resolution order mirrors the dashboard's applied-geometry-format.ts so
    # the chip and the finding agree on which variant wins.
    if error_code == "geometry_invalid":
        severity = "warning"
        message = (
            "Saved geometry failed validation. The processor fell back to "
            "defaults, so observations may not match the configured lines and "
            "ROIs."
        )
        evidence: dict[str, Any] = {
            "error_code": error_code,
            "error_message": status.get("error_message"),
            "processor_status_id": str(status["id"]),
        }
    elif not isinstance(applied, dict):
        # Applied-geometry block missing — same posture as the dashboard's
        # `missing_applied_geometry` panel state: we can't say it's default,
        # only that the lineage block is absent. Treat as insufficient data.
        return None
    else:
        source = applied.get("source")
        if source == "operator_saved":
            return _RuleResolved()
        if source == "default":
            severity = "warning" if facts.has_saved_geometry else "info"
            message = (
                "Processor is using default geometry instead of operator-saved "
                "geometry. Observations may not match the configured lines and "
                "ROIs."
            )
            evidence = {
                "applied_geometry_source": source,
                "has_saved_geometry": facts.has_saved_geometry,
                "processor_status_id": str(status["id"]),
            }
        elif source == "fallback_default_on_error":
            severity = "warning"
            message = (
                "Processor fell back to default geometry after saved geometry "
                "failed validation. Observations may not match the configured "
                "lines and ROIs."
            )
            evidence = {
                "applied_geometry_source": source,
                "error_reason": applied.get("error_reason"),
                "processor_status_id": str(status["id"]),
            }
        else:
            # Forward-compat: a new source value we don't recognize yet is
            # not a known degrade — leave it to the dashboard's unknown_source
            # variant to flag and treat as insufficient.
            return None

    return _RuleOpen(
        finding_type="processor_using_default_geometry",
        category="geometry_config",
        severity=severity,
        title="Processor is using default geometry",
        message=message,
        likely_cause="processor_did_not_pick_up_saved_geometry",
        recommended_action=(
            "Open Configure, verify saved geometry, and restart the processor session if needed."
        ),
        evidence=evidence,
        stream_session_id=status.get("stream_session_id"),
        processor_status_id=status["id"],
    )


def _evaluate_unstable_detection_confidence(facts: _Facts) -> _RuleOutput:
    """The model is producing weak or unstable detections on the latest frame.

    M2's first production-CV signal that talks about detection quality rather
    than workflow plumbing or scene visibility. Per-frame, within-window stats
    over ``snapshot.objects[].confidence`` -- avg, low_confidence_ratio,
    stddev. A frame with 10 detections at mean confidence 0.25 is a real
    reliability concern even when the stream and lighting are healthy.

    Brightness lookup happens during fact gathering (see
    ``_maybe_compute_confidence_stats``) so the rule stays a pure function.
    The fact gatherer skips the compute on stale snapshots, so the
    precondition here just checks "did we get stats".

    Trigger (any one):
        avg_confidence    <  _AVG_CONFIDENCE_WARNING_THRESHOLD
        low_conf_ratio    >  _LOW_CONFIDENCE_RATIO_WARNING_THRESHOLD
        stddev            >  _CONFIDENCE_STDDEV_WARNING_THRESHOLD

    Resolve (all of):
        avg_confidence    >= threshold
        low_conf_ratio    <= threshold
        stddev            <= threshold

    Skip (no finding either way):
        stats is None -- no perception snapshot, stale, too few samples, or
        no confidence values in the payload. Critically, "too few samples"
        does NOT auto-resolve: a model that suddenly stops producing
        detections shouldn't silently close an open unstable-confidence
        finding from when it WAS producing them.

    Co-exists with scene_too_dark by design: a dark frame and weak detections
    are two different signals worth surfacing. The dashboard ranks by
    severity, so warnings stack in top_findings cleanly.

    Previous-window drop comparison is M3: ``latest_perception_snapshots``
    is UPSERT-keyed on camera_id (one row per camera, no history), so we
    have no baseline to compare against today. Evidence reserves
    ``previous_avg_confidence`` and ``drop_ratio`` as null so the contract
    is stable when M3 wires history.
    """
    stats = facts.detection_confidence_stats
    perception = facts.latest_perception_snapshot
    if stats is None or perception is None:
        return None

    healthy = (
        stats.avg_confidence >= AVG_CONFIDENCE_WARNING_THRESHOLD
        and stats.low_confidence_ratio <= LOW_CONFIDENCE_RATIO_WARNING_THRESHOLD
        and stats.confidence_stddev <= CONFIDENCE_STDDEV_WARNING_THRESHOLD
    )
    if healthy:
        return _RuleResolved(reason="detection_confidence_stabilized")

    captured_at = perception.get("observed_at")
    evidence: dict[str, Any] = {
        "window_seconds": RECENT_WINDOW_SECONDS,
        # observation_count is reserved for an M3 over-time slice that pulls
        # from per-event observations; today's source is one perception row.
        "observation_count": None,
        "confidence_sample_count": stats.sample_count,
        "avg_confidence": round(stats.avg_confidence, 3),
        "median_confidence": round(stats.median_confidence, 3),
        "min_confidence": round(stats.min_confidence, 3),
        "max_confidence": round(stats.max_confidence, 3),
        "low_confidence_ratio": round(stats.low_confidence_ratio, 3),
        "confidence_stddev": round(stats.confidence_stddev, 3),
        # M3 will fill these when a baseline window is available. The keys
        # stay in the schema so dashboards can render the same layout.
        "previous_avg_confidence": None,
        "drop_ratio": None,
        "thresholds": {
            "min_sample_count": MIN_SAMPLE_COUNT,
            "low_confidence_threshold": LOW_CONFIDENCE_THRESHOLD,
            "avg_confidence_warning_threshold": AVG_CONFIDENCE_WARNING_THRESHOLD,
            "low_confidence_ratio_warning_threshold": (LOW_CONFIDENCE_RATIO_WARNING_THRESHOLD),
            "confidence_stddev_warning_threshold": (CONFIDENCE_STDDEV_WARNING_THRESHOLD),
            "confidence_drop_ratio_warning_threshold": (CONFIDENCE_DROP_RATIO_WARNING_THRESHOLD),
        },
        "snapshot_id": str(perception["id"]) if perception.get("id") else None,
        "captured_at": (
            captured_at.isoformat() if isinstance(captured_at, datetime) else captured_at
        ),
        "source": "latest_perception_snapshot",
    }

    return _RuleOpen(
        finding_type="unstable_detection_confidence",
        category="detection",
        severity="warning",
        title="Detection confidence is unstable",
        message=(
            "Recent detections on this camera have low or unstable confidence, "
            "which may lead to missed events, false positives, or unstable "
            "tracking."
        ),
        likely_cause="weak_or_unstable_model_confidence",
        recommended_action=(
            "Review recent frames for lighting, angle, occlusion, or "
            "background clutter. Consider adjusting the confidence threshold, "
            "using a different package, or collecting more examples for this "
            "camera."
        ),
        evidence=evidence,
        stream_session_id=perception.get("stream_session_id"),
    )


# Rule ordering doesn't affect correctness but matters for tests: the
# summary's `top_findings` already re-orders by severity, so this list is
# effectively rule-declaration order.
_RULES: tuple[tuple[str, Any], ...] = (
    (_DEDUPE_STREAM_SILENT, _evaluate_stream_valid_processor_silent),
    (_DEDUPE_FRAME_STALE, _evaluate_latest_frame_stale),
    (_DEDUPE_SCENE_DARK, _evaluate_scene_too_dark),
    (_DEDUPE_DEFAULT_GEOMETRY, _evaluate_default_geometry),
    (_DEDUPE_DETECTION_CONFIDENCE, _evaluate_unstable_detection_confidence),
)


# ---------------------------------------------------------------------------
# Entry point.
# ---------------------------------------------------------------------------


async def evaluate_camera(
    conn: asyncpg.Connection,
    *,
    settings: Settings,
    account_id: UUID,
    camera_id: UUID,
    now: datetime | None = None,
) -> tuple[datetime, _Facts]:
    """Run all rules for one camera, upsert/resolve findings, return facts.

    Returns ``(evaluated_at, facts)``. The caller (services/reliability_service)
    then reads back the open findings from the repo — this keeps a single source
    of truth (the table) for what the dashboard sees, even if a rule and a
    concurrent writer race.

    ``settings`` reaches the brightness loader through ``_gather_facts``; the
    rules layer never touches it. Must be called inside a transaction so the
    upsert SELECT-FOR-UPDATE inside ``upsert_open_finding`` holds its row lock
    until commit.
    """
    evaluated_at = now or datetime.now(tz=UTC)
    facts = await _gather_facts(
        conn,
        settings=settings,
        account_id=account_id,
        camera_id=camera_id,
        now=evaluated_at,
    )

    # Group resolves by reason so each call carries a meaningful resolved_reason
    # rather than a single bulk "healthy" stamp. Most rules default to "healthy"
    # (workflow recovered); the brightness rule uses
    # "brightness_above_threshold" so post-mortem history can tell the two
    # apart without re-deriving from evidence.
    resolves_by_reason: dict[str, list[str]] = {}
    for dedupe_suffix, rule in _RULES:
        verdict = rule(facts)
        if verdict is None:
            continue
        dedupe_key = _dedupe_key(camera_id, dedupe_suffix)
        if isinstance(verdict, _RuleResolved):
            resolves_by_reason.setdefault(verdict.reason, []).append(dedupe_key)
            continue
        await upsert_open_finding(
            conn,
            account_id=account_id,
            camera_id=camera_id,
            dedupe_key=dedupe_key,
            finding_type=verdict.finding_type,
            category=verdict.category,
            severity=verdict.severity,
            title=verdict.title,
            message=verdict.message,
            likely_cause=verdict.likely_cause,
            recommended_action=verdict.recommended_action,
            evidence=verdict.evidence,
            detector_version=DETECTOR_VERSION,
            stream_session_id=verdict.stream_session_id,
            processor_status_id=verdict.processor_status_id,
        )

    for reason, keys in resolves_by_reason.items():
        await resolve_open_findings(
            conn,
            account_id=account_id,
            camera_id=camera_id,
            dedupe_keys=keys,
            reason=reason,
        )

    return evaluated_at, facts


# Exported for the summary endpoint to decide between "healthy" and "unknown".
def has_recent_evaluable_signal(facts: _Facts) -> bool:
    """True when at least one of {processor status, snapshot} is recent enough
    to claim we know the camera's reliability state."""
    status = facts.latest_processor_status
    if status is not None:
        status_age = _age_seconds(facts.now, status["updated_at"])
        if status_age is not None and status_age <= _UNKNOWN_DATA_WINDOW_S:
            return True

    snapshot = facts.latest_snapshot
    if snapshot is not None:
        snap_age = _age_seconds(facts.now, snapshot["captured_at"])
        if snap_age is not None and snap_age <= _UNKNOWN_DATA_WINDOW_S:
            return True

    return False
