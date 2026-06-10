"""Config comparison report card (model-config M11).

Pure read-side, DESCRIPTIVE synthesis over the M9/M9.1 by-config groups: reuse
the by-config trend read (one query, rollups only), aggregate each group over
the window, pick a baseline and a candidate, and report what changed.

This is a report card, NOT a recommender. Hard rules, mirrored in every
generated sentence:

- No judgments. No "better/worse/best/recommended", no verdicts — only factual
  directions ("higher", "lower", "roughly unchanged") plus the deltas
  themselves. What the user does with the numbers is their call.
- Never causal. Language: "in this window", "based on available samples",
  "directional comparison only, not causal proof". Never "caused"/"optimized".
- Never a GPU claim. processing_fps is the M7E OUTPUT CADENCE only; a sample
  count increase under a higher FPS is cadence, not inference work.
- Population-change honesty is first-class: a different confidence_threshold
  admits/excludes weaker detections and a different selected_classes changes
  the measured object population — both make averages NOT directly comparable.
  That is exactly what ``comparison_status="mixed_metrics"`` plus
  ``comparability_notes`` communicate (instead of implying the model got
  better or worse).
- Conservative gating: a single group, missing aggregates, or fewer than
  ``min_samples`` samples on either side is ``inconclusive`` — never a guess.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID

import asyncpg

from upstream_api.schemas.reliability import (
    ConfidenceTrendConfigGroup,
    ConfidenceTrendRange,
    ConfigComparisonDeltas,
    ConfigComparisonGroup,
    ConfigComparisonMode,
    ConfigComparisonStatus,
    ConfigComparisonSummaryResponse,
)
from upstream_api.services.reliability_trends import (
    get_confidence_trend_by_config_for_camera,
)

# Conservative defaults. ``DEFAULT_MIN_SAMPLES`` gates the status (below it ->
# inconclusive); the epsilons keep noise from being reported as a direction —
# a delta smaller than these reads as "roughly unchanged".
DEFAULT_MIN_SAMPLES = 30
_CONFIDENCE_EPSILON = 0.02
_RATIO_EPSILON = 0.02

_FPS_CADENCE_WARNING = "processing_fps_output_cadence_only"
_ANALYTICS_COUNTS_WARNING = "selected_classes_analytics_counts_not_filtered"

_ALWAYS_CAVEAT = "Directional comparison only, not causal proof."
_FPS_CAVEAT = "Processing FPS is output cadence only; it does not reduce GPU/inference work."
_ANALYTICS_CAVEAT = "Line/ROI aggregate counts may still include all classes."


@dataclass(frozen=True)
class _GroupAggregate:
    """One config group reduced to window aggregates."""

    group: ConfidenceTrendConfigGroup
    sample_count: int
    avg_confidence: float | None
    low_confidence_ratio: float | None
    latest_bucket_start: datetime | None


def _weighted(points: list[tuple[float | None, int]]) -> float | None:
    """Sample-count-weighted mean over (value, weight) pairs whose value is
    present — the same per-field posture the rollup math uses."""
    num = 0.0
    den = 0
    for value, weight in points:
        if value is None or weight <= 0:
            continue
        num += value * weight
        den += weight
    return num / den if den else None


def _aggregate(group: ConfidenceTrendConfigGroup) -> _GroupAggregate:
    return _GroupAggregate(
        group=group,
        sample_count=sum(p.confidence_sample_count for p in group.points),
        avg_confidence=_weighted(
            [(p.avg_confidence, p.confidence_sample_count) for p in group.points]
        ),
        low_confidence_ratio=_weighted(
            [(p.low_confidence_ratio, p.confidence_sample_count) for p in group.points]
        ),
        latest_bucket_start=group.points[-1].bucket_start if group.points else None,
    )


def _label(group: ConfidenceTrendConfigGroup) -> str:
    """Human label, mirroring the dashboard's config-group-label rules so the
    API report card and the Reliability by Config cards read identically."""
    if (
        group.processing_config_source is None
        and group.processing_config_revision is None
        and group.processing_config_status is None
        and group.applied_selected_classes_key is None
        and group.applied_confidence_threshold is None
    ):
        return "No config metadata"
    if group.applied_selected_classes_key == "unknown":
        return "unknown config"
    classes = (
        "All classes"
        if group.applied_selected_classes_key in (None, "all")
        else group.applied_selected_classes_key
    )
    confidence = (
        "default confidence"
        if group.applied_confidence_threshold is None
        else f"confidence {group.applied_confidence_threshold:.2f}"
    )
    fps = (
        "default FPS"
        if group.applied_processing_fps is None
        else f"{group.applied_processing_fps} FPS"
    )
    return f"{classes} · {confidence} · {fps}"


def _group_key(group: ConfidenceTrendConfigGroup) -> str:
    return "·".join(
        str(part) if part is not None else "∅"
        for part in (
            group.processing_config_source,
            group.processing_config_revision,
            group.processing_config_status,
            group.applied_model_key,
            group.applied_selected_classes_key,
            group.applied_confidence_threshold,
            group.requested_processing_fps,
            group.applied_processing_fps,
            "|".join(group.processing_config_warnings or ()),
        )
    )


def _to_response_group(agg: _GroupAggregate) -> ConfigComparisonGroup:
    g = agg.group
    return ConfigComparisonGroup(
        group_key=_group_key(g),
        label=_label(g),
        processing_config_source=g.processing_config_source,
        processing_config_revision=g.processing_config_revision,
        processing_config_status=g.processing_config_status,
        applied_model_key=g.applied_model_key,
        applied_selected_classes_key=g.applied_selected_classes_key,
        applied_confidence_threshold=g.applied_confidence_threshold,
        requested_processing_fps=g.requested_processing_fps,
        applied_processing_fps=g.applied_processing_fps,
        warnings=list(g.processing_config_warnings or []),
        sample_count=agg.sample_count,
        avg_confidence=agg.avg_confidence,
        low_confidence_ratio=agg.low_confidence_ratio,
        latest_bucket_start=agg.latest_bucket_start,
    )


def _sort_recency_key(agg: _GroupAggregate) -> tuple[datetime, int]:
    floor = datetime.min.replace(tzinfo=UTC)
    return (agg.latest_bucket_start or floor, agg.sample_count)


def _pick_by_revision(aggregates: list[_GroupAggregate], revision: int) -> _GroupAggregate | None:
    """The group at ``revision`` — when several share it (e.g. warning
    variants), the one with the most samples."""
    matching = [a for a in aggregates if a.group.processing_config_revision == revision]
    if not matching:
        return None
    return max(matching, key=lambda a: a.sample_count)


def _select_pair(
    aggregates: list[_GroupAggregate],
    baseline_revision: int | None,
    candidate_revision: int | None,
) -> tuple[_GroupAggregate | None, _GroupAggregate | None, ConfigComparisonMode]:
    """Baseline/candidate selection. Explicit revisions win; otherwise the two
    highest distinct revisions (latest = candidate); when revisions are absent
    (pre-M8 history), the two most recent groups by latest bucket."""
    if baseline_revision is not None and candidate_revision is not None:
        return (
            _pick_by_revision(aggregates, baseline_revision),
            _pick_by_revision(aggregates, candidate_revision),
            "explicit",
        )
    revisions = sorted(
        {
            a.group.processing_config_revision
            for a in aggregates
            if a.group.processing_config_revision is not None
        },
        reverse=True,
    )
    if len(revisions) >= 2:
        return (
            _pick_by_revision(aggregates, revisions[1]),
            _pick_by_revision(aggregates, revisions[0]),
            "latest_vs_previous",
        )
    if len(aggregates) >= 2:
        ordered = sorted(aggregates, key=_sort_recency_key)
        return ordered[-2], ordered[-1], "recent_vs_prior"
    return None, None, "none"


def _rev_label(group: ConfidenceTrendConfigGroup) -> str:
    if group.processing_config_revision is not None:
        return f"Config r{group.processing_config_revision}"
    return "the earlier config group"


def _fmt_threshold(value: float | None) -> str:
    return "default" if value is None else f"{value:.2f}"


async def get_config_comparison_summary(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    camera_id: UUID,
    range_: ConfidenceTrendRange,
    baseline_revision: int | None = None,
    candidate_revision: int | None = None,
    min_samples: int = DEFAULT_MIN_SAMPLES,
    now: datetime | None = None,
) -> ConfigComparisonSummaryResponse:
    """Build the report card for one camera/window (see module docstring)."""
    if now is None:
        now = datetime.now(tz=UTC)
    by_config = await get_confidence_trend_by_config_for_camera(
        conn, account_id=account_id, camera_id=camera_id, range_=range_, now=now
    )
    aggregates = [_aggregate(g) for g in by_config.groups]

    def respond(
        *,
        mode: ConfigComparisonMode,
        baseline: _GroupAggregate | None,
        candidate: _GroupAggregate | None,
        deltas: ConfigComparisonDeltas | None,
        status: ConfigComparisonStatus,
        directions: list[str],
        summary: str,
        bullets: list[str],
        notes: list[str],
        caveats: list[str],
    ) -> ConfigComparisonSummaryResponse:
        return ConfigComparisonSummaryResponse(
            camera_id=camera_id,
            range=range_,
            generated_at=now,
            comparison_mode=mode,
            min_samples=min_samples,
            baseline=_to_response_group(baseline) if baseline else None,
            candidate=_to_response_group(candidate) if candidate else None,
            metric_deltas=deltas,
            comparison_status=status,
            metric_directions=directions,
            summary=summary,
            bullets=bullets,
            comparability_notes=notes,
            caveats=caveats,
        )

    if not aggregates:
        return respond(
            mode="none",
            baseline=None,
            candidate=None,
            deltas=None,
            status="no_data",
            directions=[],
            summary="No config-aware reliability data in this window.",
            bullets=[],
            notes=[],
            caveats=[_ALWAYS_CAVEAT],
        )

    baseline, candidate, mode = _select_pair(aggregates, baseline_revision, candidate_revision)
    if baseline is None or candidate is None or baseline is candidate:
        # Show the lone (or best-matching) group as the candidate so the UI
        # has something concrete to display next to the inconclusive status.
        only = max(aggregates, key=lambda a: a.sample_count)
        return respond(
            mode=mode if mode == "explicit" else "none",
            baseline=None,
            candidate=only,
            deltas=None,
            status="inconclusive",
            directions=[],
            summary=(
                "Not enough distinct config groups to compare in this window — "
                "a comparison needs reliability data from at least two configs."
            ),
            bullets=[],
            notes=[],
            caveats=[_ALWAYS_CAVEAT],
        )

    # ---- deltas (candidate minus baseline; descriptive only) ---------------
    b, c = baseline, candidate
    bg, cg = b.group, c.group
    avg_delta = (
        c.avg_confidence - b.avg_confidence
        if c.avg_confidence is not None and b.avg_confidence is not None
        else None
    )
    pct_delta = (
        round(avg_delta / b.avg_confidence * 100, 1)
        if avg_delta is not None and b.avg_confidence is not None and b.avg_confidence != 0.0
        else None
    )
    ratio_delta = (
        c.low_confidence_ratio - b.low_confidence_ratio
        if c.low_confidence_ratio is not None and b.low_confidence_ratio is not None
        else None
    )
    fps_delta = (
        (cg.applied_processing_fps or 0) - (bg.applied_processing_fps or 0)
        if cg.applied_processing_fps is not None or bg.applied_processing_fps is not None
        else None
    )
    deltas = ConfigComparisonDeltas(
        avg_confidence_delta=round(avg_delta, 4) if avg_delta is not None else None,
        avg_confidence_percent_delta=pct_delta,
        low_confidence_ratio_delta=round(ratio_delta, 4) if ratio_delta is not None else None,
        sample_count_delta=c.sample_count - b.sample_count,
        applied_processing_fps_delta=fps_delta,
    )

    # ---- comparability notes + caveats --------------------------------------
    threshold_changed = bg.applied_confidence_threshold != cg.applied_confidence_threshold
    classes_changed = bg.applied_selected_classes_key != cg.applied_selected_classes_key
    fps_changed = bg.applied_processing_fps != cg.applied_processing_fps
    all_warnings = set(bg.processing_config_warnings or []) | set(
        cg.processing_config_warnings or []
    )

    notes: list[str] = []
    if threshold_changed:
        notes.append(
            "Confidence threshold changed "
            f"({_fmt_threshold(bg.applied_confidence_threshold)} → "
            f"{_fmt_threshold(cg.applied_confidence_threshold)}), so weaker detections "
            "may now be included or excluded — averages are not directly comparable."
        )
    if classes_changed:
        notes.append(
            "Selected classes differ, so the measured object population changed and "
            "confidence metrics may not be directly comparable."
        )

    caveats = [_ALWAYS_CAVEAT]
    if fps_changed or _FPS_CADENCE_WARNING in all_warnings:
        caveats.append(_FPS_CAVEAT)
    if _ANALYTICS_COUNTS_WARNING in all_warnings:
        caveats.append(_ANALYTICS_CAVEAT)

    # ---- factual directions --------------------------------------------------
    directions: list[str] = []
    avg_phrase: str | None = None
    if avg_delta is not None:
        if avg_delta > _CONFIDENCE_EPSILON:
            directions.append("avg_confidence_higher")
            avg_phrase = "higher average confidence"
        elif avg_delta < -_CONFIDENCE_EPSILON:
            directions.append("avg_confidence_lower")
            avg_phrase = "lower average confidence"
        else:
            directions.append("avg_confidence_unchanged")
            avg_phrase = "roughly unchanged average confidence"
    ratio_phrase: str | None = None
    if ratio_delta is not None:
        if ratio_delta > _RATIO_EPSILON:
            directions.append("low_confidence_ratio_higher")
            ratio_phrase = "a higher low-confidence ratio"
        elif ratio_delta < -_RATIO_EPSILON:
            directions.append("low_confidence_ratio_lower")
            ratio_phrase = "a lower low-confidence ratio"
        else:
            directions.append("low_confidence_ratio_unchanged")
    samples_delta = c.sample_count - b.sample_count
    if samples_delta > 0:
        directions.append("samples_higher")
        samples_phrase = "more samples"
    elif samples_delta < 0:
        directions.append("samples_lower")
        samples_phrase = "fewer samples"
    else:
        directions.append("samples_unchanged")
        samples_phrase = "the same sample count"

    # ---- status (descriptive gates, not judgments) ---------------------------
    too_small = b.sample_count < min_samples or c.sample_count < min_samples
    if too_small or avg_delta is None:
        status: ConfigComparisonStatus = "inconclusive"
        reason = (
            "one config group has too few samples"
            if too_small
            else "a confidence aggregate is missing on one side"
        )
        summary = (
            f"Comparison of {_rev_label(cg)} vs {_rev_label(bg)} is inconclusive "
            f"because {reason} in this window."
        )
    else:
        status = "mixed_metrics" if (threshold_changed or classes_changed) else "comparable"
        phrases = [p for p in (avg_phrase, ratio_phrase, samples_phrase) if p]
        if len(phrases) > 2:
            listed = ", ".join(phrases[:-1]) + f", and {phrases[-1]}"
        else:
            listed = " and ".join(phrases)
        summary = f"{_rev_label(cg)} had {listed} than {_rev_label(bg)} in this window."
        if status == "mixed_metrics":
            summary += (
                " The config also changed what gets measured, so these metrics are "
                "not directly comparable — see the comparability notes."
            )

    bullets = _bullets(b, c, avg_delta, ratio_delta, fps_changed, threshold_changed)
    return respond(
        mode=mode,
        baseline=b,
        candidate=c,
        deltas=deltas,
        status=status,
        directions=directions,
        summary=summary,
        bullets=bullets,
        notes=notes,
        caveats=caveats,
    )


def _bullets(
    b: _GroupAggregate,
    c: _GroupAggregate,
    avg_delta: float | None,
    ratio_delta: float | None,
    fps_changed: bool,
    threshold_changed: bool,
) -> list[str]:
    """Plain factual statements — directions and numbers, never advice."""
    bullets: list[str] = []
    if avg_delta is not None and b.avg_confidence is not None and c.avg_confidence is not None:
        if abs(avg_delta) <= _CONFIDENCE_EPSILON:
            bullets.append(
                f"Average confidence is roughly unchanged "
                f"({b.avg_confidence:.2f} → {c.avg_confidence:.2f})."
            )
        else:
            direction = "higher" if avg_delta > 0 else "lower"
            bullets.append(
                f"Average confidence is {direction} by {abs(avg_delta):.2f} "
                f"({b.avg_confidence:.2f} → {c.avg_confidence:.2f}) in this window."
            )
            if threshold_changed and avg_delta < 0:
                bullets.append(
                    "The confidence threshold also changed, so this likely includes "
                    "weaker detections that were previously filtered out."
                )
    if ratio_delta is not None and abs(ratio_delta) > _RATIO_EPSILON:
        direction = "higher" if ratio_delta > 0 else "lower"
        bullets.append(
            f"Low-confidence ratio is {direction} by "
            f"{abs(ratio_delta) * 100:.0f} percentage points."
        )
    sample_line = f"Sample count went from {b.sample_count:,} to {c.sample_count:,}."
    if fps_changed:
        fps = c.group.applied_processing_fps
        cadence = f"{fps} FPS" if fps is not None else "the default cadence"
        sample_line += (
            f" Snapshot output cadence is configured at {cadence}, which changes how "
            "many samples are recorded (output cadence only, not inference work)."
        )
    bullets.append(sample_line)
    return bullets
