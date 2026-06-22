"""Upstream Reliability M1a — observation detectors.

Simple, explainable reliability checks over recently-ingested observations. Run
from the ingest router after the batch is stored, isolated in a savepoint so a
detector failure can never break ingest. Mirrors the structure of
services/reliability_evaluator.py: pure rule functions decide
``_Open`` / ``_Resolved`` / nothing, and the engine applies their verdicts via
services/reliability_issues.py.

Design:

- We evaluate only the entities the current batch touched (a camera_id, or an
  external_camera_key). Source-level observations with neither key (e.g. a bare
  ``pipeline_heartbeat``) get no per-camera detector in M1a.
- Facts are gathered ONCE per entity by reading ``cv_observations`` over two
  DISJOINT ``observed_at`` windows: the current window ``(now - W, now]`` and a
  baseline ``[now - W*(1+N), now - W]``. The just-ingested batch's rows fall in
  the current window — no double-counting because the windows don't overlap.
- "Insufficient data" is NOT "healthy": a rule with too little baseline returns
  nothing (leaves any open issue alone). A rule only resolves an issue when it
  has enough data AND the signal is actually healthy. This preserves the
  distinction between "we don't know" and "recovered".

The dedupe_key is ``<entity>:<suffix>`` (e.g. ``camera:<id>:event_count_drop:forklift``)
so distinct entities/conditions stay separate under the account-level
``cv_issues`` open-dedupe index.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any
from uuid import UUID

import asyncpg

from upstream_api.services.processor import coerce_jsonb, parse_iso_datetime
from upstream_api.services.reliability_issues import (
    resolve_open_issues_by_dedupe,
    upsert_open_issue,
)

if TYPE_CHECKING:
    from upstream_api.schemas.reliability_observations import ReliabilityObservationItem

# Bumped when rule logic / thresholds change so historical issues stay
# attributable to the detector version that opened them.
RELIABILITY_OBSERVATION_DETECTOR_VERSION = "reliability.observations.v0.1"

# Window geometry. Current window = the most recent W; baseline = the N windows
# before it. Disjoint by construction.
_CURRENT_WINDOW = timedelta(minutes=10)
_BASELINE_WINDOWS = 6

# Thresholds — deliberately conservative so sparse data never false-fires.
_MIN_BASELINE_EVENTS = 6  # need >= this many baseline events to compute a rate
_MIN_BASELINE_SUMMARIES = 3  # need >= this many baseline detection_summary rows
_DROP_RATIO = 0.34  # current below 34% of expected => a drop
_SPIKE_RATIO = 3.0  # current above 3x expected => a spike
_MIN_SPIKE_COUNT = 5  # ...and at least this many, so tiny counts don't spike
_CONFIDENCE_DROP_ABS = 0.15  # mean-confidence drop this large (absolute) => issue
_CONFIDENCE_DROP_CRITICAL = 0.30
_DUP_THRESHOLD = 5  # same (event_name, track_id) this many times in a window


def _window_label() -> str:
    return f"{int(_CURRENT_WINDOW.total_seconds() // 60)} min"


def _num(value: Any) -> float | None:
    """Coerce a JSON number to float; ``None`` for anything non-numeric."""
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


@dataclass(frozen=True)
class _Entity:
    key_prefix: str
    camera_id: UUID | None
    external_camera_key: str | None


@dataclass(frozen=True)
class _Obs:
    type: str
    payload: dict[str, Any]
    observed_at: datetime


@dataclass(frozen=True)
class _Facts:
    now: datetime
    current: list[_Obs]
    baseline: list[_Obs]
    baseline_windows: int


@dataclass(frozen=True)
class _Open:
    suffix: str
    issue_type: str
    category: str
    severity: str
    title: str
    message: str
    likely_cause: str | None
    recommended_action: str | None
    evidence: dict[str, Any]


@dataclass(frozen=True)
class _Resolved:
    suffix: str
    reason: str


_Verdict = _Open | _Resolved


# ---------------------------------------------------------------------------
# Pure rule functions (no I/O) — testable in isolation against a built _Facts.
# ---------------------------------------------------------------------------


def _event_name(obs: _Obs) -> str:
    return str(obs.payload.get("event_name") or "_unnamed")


def detect_event_counts(facts: _Facts, entity: _Entity) -> list[_Verdict]:
    """Per event_name: open a drop/spike issue when the current-window rate
    departs materially from the baseline per-window rate. Resolves when healthy
    with enough data; does nothing when the baseline is too sparse."""
    verdicts: list[_Verdict] = []
    cur = Counter(_event_name(o) for o in facts.current if o.type == "event_observed")
    base = Counter(_event_name(o) for o in facts.baseline if o.type == "event_observed")
    for name in set(cur) | set(base):
        base_total = base[name]
        if base_total < _MIN_BASELINE_EVENTS:
            continue  # insufficient history — leave any open issue alone
        expected = base_total / facts.baseline_windows
        current = cur[name]
        drop = f"event_count_drop:{name}"
        spike = f"event_count_spike:{name}"

        if expected >= 1 and current < expected * _DROP_RATIO:
            verdicts.append(
                _Open(
                    suffix=drop,
                    issue_type="event_count_drop",
                    category="count",
                    severity="critical" if current == 0 else "warning",
                    title=f"Event rate dropped for '{name}'",
                    message=(
                        f"Saw {current} '{name}' event(s) in the last {_window_label()}, "
                        f"versus a baseline of ~{expected:.1f} per {_window_label()}."
                    ),
                    likely_cause="The source stopped emitting this event, or the scene changed.",
                    recommended_action="Check the source CV pipeline and camera for this event.",
                    evidence={
                        "event_name": name,
                        "current_count": current,
                        "baseline_per_window": round(expected, 3),
                        "baseline_total": base_total,
                        "baseline_windows": facts.baseline_windows,
                        "window_minutes": int(_CURRENT_WINDOW.total_seconds() // 60),
                    },
                )
            )
        else:
            verdicts.append(_Resolved(drop, "event_rate_recovered"))

        if current >= _MIN_SPIKE_COUNT and expected >= 0.5 and current > expected * _SPIKE_RATIO:
            verdicts.append(
                _Open(
                    suffix=spike,
                    issue_type="event_count_spike",
                    category="count",
                    severity="warning",
                    title=f"Event rate spiked for '{name}'",
                    message=(
                        f"Saw {current} '{name}' event(s) in the last {_window_label()}, "
                        f"versus a baseline of ~{expected:.1f} per {_window_label()}."
                    ),
                    likely_cause="A burst of activity, duplicated emission, or a noisier scene.",
                    recommended_action="Confirm the source isn't double-emitting this event.",
                    evidence={
                        "event_name": name,
                        "current_count": current,
                        "baseline_per_window": round(expected, 3),
                        "baseline_total": base_total,
                        "baseline_windows": facts.baseline_windows,
                        "window_minutes": int(_CURRENT_WINDOW.total_seconds() // 60),
                    },
                )
            )
        else:
            verdicts.append(_Resolved(spike, "event_rate_recovered"))
    return verdicts


def _avg_by_class(summaries: list[_Obs], field: str) -> dict[str, float]:
    """Average a per-class numeric map (``counts_by_class`` /
    ``avg_confidence_by_class``) across summaries, ignoring non-numeric values
    and classes absent from a given summary."""
    totals: dict[str, float] = defaultdict(float)
    seen: dict[str, int] = defaultdict(int)
    for summary in summaries:
        by_class = summary.payload.get(field)
        if not isinstance(by_class, dict):
            continue
        for cls, raw in by_class.items():
            value = _num(raw)
            if value is None:
                continue
            totals[str(cls)] += value
            seen[str(cls)] += 1
    return {cls: totals[cls] / seen[cls] for cls in totals if seen[cls] > 0}


def detect_detection_counts(facts: _Facts, entity: _Entity) -> list[_Verdict]:
    """Per class: open an issue when the current average detection count is well
    below the baseline average. Needs current summaries to make any claim."""
    cur = [o for o in facts.current if o.type == "detection_summary"]
    base = [o for o in facts.baseline if o.type == "detection_summary"]
    if not cur or len(base) < _MIN_BASELINE_SUMMARIES:
        return []
    cur_avg = _avg_by_class(cur, "counts_by_class")
    base_avg = _avg_by_class(base, "counts_by_class")
    verdicts: list[_Verdict] = []
    for cls, base_count in base_avg.items():
        if base_count < 1:
            continue
        current = cur_avg.get(cls, 0.0)
        suffix = f"detection_count_drop:{cls}"
        if current < base_count * _DROP_RATIO:
            verdicts.append(
                _Open(
                    suffix=suffix,
                    issue_type="detection_count_drop",
                    category="count",
                    severity="warning",
                    title=f"Detection count dropped for '{cls}'",
                    message=(
                        f"Average '{cls}' detections per summary fell to {current:.2f} "
                        f"from a baseline of {base_count:.2f}."
                    ),
                    likely_cause="Model/source degraded, camera moved/occluded, or scene changed.",
                    recommended_action="Inspect the camera view and model output for the class.",
                    evidence={
                        "class": cls,
                        "current_avg_count": round(current, 3),
                        "baseline_avg_count": round(base_count, 3),
                        "baseline_summaries": len(base),
                    },
                )
            )
        else:
            verdicts.append(_Resolved(suffix, "detection_count_recovered"))
    return verdicts


def detect_confidence(facts: _Facts, entity: _Entity) -> list[_Verdict]:
    """Per class: open an issue when the current mean confidence has dropped
    materially (absolute) from the baseline mean confidence."""
    cur = [o for o in facts.current if o.type == "detection_summary"]
    base = [o for o in facts.baseline if o.type == "detection_summary"]
    if not cur or len(base) < _MIN_BASELINE_SUMMARIES:
        return []
    cur_conf = _avg_by_class(cur, "avg_confidence_by_class")
    base_conf = _avg_by_class(base, "avg_confidence_by_class")
    verdicts: list[_Verdict] = []
    for cls, base_value in base_conf.items():
        current = cur_conf.get(cls)
        if current is None:
            continue
        suffix = f"confidence_drop:{cls}"
        drop = base_value - current
        if drop >= _CONFIDENCE_DROP_ABS:
            verdicts.append(
                _Open(
                    suffix=suffix,
                    issue_type="confidence_drop",
                    category="confidence",
                    severity="critical" if drop >= _CONFIDENCE_DROP_CRITICAL else "warning",
                    title=f"Mean confidence dropped for '{cls}'",
                    message=(
                        f"Average '{cls}' confidence fell to {current:.2f} "
                        f"from a baseline of {base_value:.2f} (down {drop:.2f})."
                    ),
                    likely_cause="Lighting/scene change, camera focus, or a model regression.",
                    recommended_action="Check camera image quality and the model for this class.",
                    evidence={
                        "class": cls,
                        "current_avg_confidence": round(current, 3),
                        "baseline_avg_confidence": round(base_value, 3),
                        "drop": round(drop, 3),
                        "baseline_summaries": len(base),
                    },
                )
            )
        else:
            verdicts.append(_Resolved(suffix, "confidence_recovered"))
    return verdicts


def detect_duplicate_events(facts: _Facts, entity: _Entity) -> list[_Verdict]:
    """Open an issue when the same (event_name, track_id) repeats suspiciously
    often within the current window — a classic double-counting signal."""
    repeats: Counter[tuple[str, str]] = Counter()
    for obs in facts.current:
        if obs.type != "event_observed":
            continue
        track_id = obs.payload.get("track_id")
        if track_id is None:
            continue
        repeats[(_event_name(obs), str(track_id))] += 1
    max_by_name: dict[str, int] = defaultdict(int)
    for (name, _track), count in repeats.items():
        max_by_name[name] = max(max_by_name[name], count)
    verdicts: list[_Verdict] = []
    for name, max_repeats in max_by_name.items():
        suffix = f"duplicate_events:{name}"
        if max_repeats >= _DUP_THRESHOLD:
            verdicts.append(
                _Open(
                    suffix=suffix,
                    issue_type="duplicate_event_increase",
                    category="count",
                    severity="warning",
                    title=f"Duplicate '{name}' events for one track",
                    message=(
                        f"A single track produced '{name}' {max_repeats} times in the last "
                        f"{_window_label()}, suggesting double-counting."
                    ),
                    likely_cause="Debounce/dedup disabled upstream, or a flapping tracker.",
                    recommended_action="Check the source's event de-duplication for this track.",
                    evidence={
                        "event_name": name,
                        "max_repeats_same_track": max_repeats,
                        "threshold": _DUP_THRESHOLD,
                        "window_minutes": int(_CURRENT_WINDOW.total_seconds() // 60),
                    },
                )
            )
        else:
            verdicts.append(_Resolved(suffix, "duplicates_subsided"))
    return verdicts


def detect_liveness(facts: _Facts, entity: _Entity) -> list[_Verdict]:
    """DEFERRED — 'no recent observations' is detection-of-absence: a silent
    entity produces no ingest to evaluate, so it cannot fire at ingest time. It
    needs a scheduled sweep (tracked for M1b/M2). Kept as a visible no-op rule
    slot rather than an off-list TODO."""
    return []


_RULES = (
    detect_event_counts,
    detect_detection_counts,
    detect_confidence,
    detect_duplicate_events,
    detect_liveness,
)


# ---------------------------------------------------------------------------
# I/O: gather facts + apply verdicts.
# ---------------------------------------------------------------------------

_HISTORY_BY_CAMERA_SQL = """
select type, payload, observed_at
  from cv_observations
 where account_id = $1
   and camera_id = $2
   and observed_at >= $3
   and observed_at <= $4
 order by observed_at
"""

_HISTORY_BY_EXTERNAL_SQL = """
select type, payload, observed_at
  from cv_observations
 where account_id = $1
   and external_camera_key = $2
   and observed_at >= $3
   and observed_at <= $4
 order by observed_at
"""


def _entities_from_batch(
    observations: list[ReliabilityObservationItem],
) -> list[_Entity]:
    entities: dict[str, _Entity] = {}
    for obs in observations:
        if obs.camera_id is not None:
            key = f"camera:{obs.camera_id}"
            entities.setdefault(key, _Entity(key, obs.camera_id, None))
        elif obs.external_camera_key:
            key = f"external:{obs.external_camera_key}"
            entities.setdefault(key, _Entity(key, None, obs.external_camera_key))
        # else: source-only observation — no per-camera detector in M1a.
    return list(entities.values())


async def _gather_facts(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    entity: _Entity,
    now: datetime,
) -> _Facts:
    lookback_start = now - _CURRENT_WINDOW * (1 + _BASELINE_WINDOWS)
    current_start = now - _CURRENT_WINDOW
    if entity.camera_id is not None:
        rows = await conn.fetch(
            _HISTORY_BY_CAMERA_SQL, account_id, entity.camera_id, lookback_start, now
        )
    else:
        rows = await conn.fetch(
            _HISTORY_BY_EXTERNAL_SQL,
            account_id,
            entity.external_camera_key,
            lookback_start,
            now,
        )
    observations = [
        _Obs(
            type=row["type"],
            payload=coerce_jsonb(row["payload"]),
            observed_at=parse_iso_datetime(row["observed_at"]) or now,
        )
        for row in rows
    ]
    current = [o for o in observations if o.observed_at > current_start]
    baseline = [o for o in observations if o.observed_at <= current_start]
    return _Facts(
        now=now,
        current=current,
        baseline=baseline,
        baseline_windows=_BASELINE_WINDOWS,
    )


async def evaluate_observation_batch(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    source: str,
    batch_id: UUID,
    observations: list[ReliabilityObservationItem],
    now: datetime,
) -> None:
    """Run the detectors over the entities the batch touched and open/refresh/
    resolve issues. Must run inside the caller's transaction (the savepoint the
    router wraps it in isolates any failure from the committed observation rows).
    """
    for entity in _entities_from_batch(observations):
        facts = await _gather_facts(conn, account_id=account_id, entity=entity, now=now)
        opens: list[_Open] = []
        resolves: list[_Resolved] = []
        for rule in _RULES:
            for verdict in rule(facts, entity):
                if isinstance(verdict, _Open):
                    opens.append(verdict)
                else:
                    resolves.append(verdict)

        for verdict in opens:
            await upsert_open_issue(
                conn,
                account_id=account_id,
                dedupe_key=f"{entity.key_prefix}:{verdict.suffix}",
                issue_type=verdict.issue_type,
                category=verdict.category,
                severity=verdict.severity,
                title=verdict.title,
                message=verdict.message,
                likely_cause=verdict.likely_cause,
                recommended_action=verdict.recommended_action,
                evidence=verdict.evidence,
                detector_version=RELIABILITY_OBSERVATION_DETECTOR_VERSION,
                camera_id=entity.camera_id,
                external_camera_key=entity.external_camera_key,
                source_batch_id=batch_id,
                source_observation_ids=None,
                occurred_at=now,
            )

        by_reason: dict[str, list[str]] = defaultdict(list)
        for verdict in resolves:
            by_reason[verdict.reason].append(f"{entity.key_prefix}:{verdict.suffix}")
        for reason, dedupe_keys in by_reason.items():
            await resolve_open_issues_by_dedupe(
                conn,
                account_id=account_id,
                dedupe_keys=dedupe_keys,
                reason=reason,
                occurred_at=now,
            )
