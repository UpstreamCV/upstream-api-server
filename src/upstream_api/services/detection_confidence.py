"""Detection-confidence extractor + summary stats for reliability M2.

Pure functions — no DB, no Pillow, no settings. Designed to be tolerant of
the multiple shapes a model-output payload can take:

- top-level: ``{"confidence": 0.72}``
- per-detection: ``{"detections": [{"confidence": 0.61}, ...]}``
- per-object (the shape ``latest_perception_snapshots.snapshot`` uses):
  ``{"objects": [{"confidence": 0.51, "track_id": "t-7", ...}, ...]}``
- per-track: ``{"tracks": [{"confidence": 0.43}, ...]}``
- and score-aliased: ``{"objects": [{"score": 0.55}, ...]}``

The extractor accepts only float-coercible values in ``[0.0, 1.0]`` and walks
only known nesting locations -- a payload key like ``track_id: 42`` or
``class_id: 2`` or ``frame_num: 99`` must never be mistaken for a confidence.
The anti-test in ``tests/test_detection_confidence.py`` pins that promise.

This is M2-side infrastructure. The evaluator wires it to the latest
perception snapshot today; future processor enrichment of per-event
observation payloads can reuse the same extractor.
"""

from __future__ import annotations

import math
import statistics
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

# Names that count as a confidence value. Closed set so a future model field
# named e.g. "frame_score" (frame index, not a model confidence) can't sneak
# in. Add explicitly when a real source needs it.
_CONFIDENCE_KEYS: tuple[str, ...] = (
    "confidence",
    "score",
    "detection_confidence",
    "model_confidence",
)

# Places we walk for nested confidences. Also a closed set -- a payload like
# ``{"rois": [{"score": 0.7, "id": "x"}]}`` will not produce a confidence here
# because ``rois`` isn't in this list (an roi.score would be region-quality,
# not a detection confidence). Add explicitly when a real source needs it.
_ARRAY_KEYS: tuple[str, ...] = ("objects", "detections", "tracks")

# Threshold below which a single sample counts as "low confidence" for the
# low_confidence_ratio statistic. Picked at the typical default detection
# threshold; tune in M3 with telemetry.
LOW_CONFIDENCE_THRESHOLD: float = 0.45

# Minimum samples needed before stats are computable. Below this we don't
# pretend to have a signal -- the evaluator treats ``None`` as "skip the
# rule, insufficient data" the same way the brightness rule does for missing
# snapshots.
MIN_SAMPLE_COUNT: int = 5

# Warning thresholds. Hit any of these and the rule opens.
AVG_CONFIDENCE_WARNING_THRESHOLD: float = 0.45
LOW_CONFIDENCE_RATIO_WARNING_THRESHOLD: float = 0.50
CONFIDENCE_STDDEV_WARNING_THRESHOLD: float = 0.20

# Drop-vs-recent-baseline thresholds and window sizes (reliability M5). The
# evaluator compares the current snapshot's average confidence against a
# weighted baseline computed from `snapshot_detection_metrics` history and
# opens the finding when confidence has dropped by more than the threshold.
#
# - RECENT_WINDOW_SECONDS: how recent the "current" metric history is (the
#   evidence reports this as the current window; the live comparison uses the
#   latest snapshot's stats).
# - PREVIOUS_WINDOW_SECONDS: how far back the baseline reaches.
# - BASELINE_EXCLUSION_SECONDS: the most-recent slice excluded from the
#   baseline so the current snapshot isn't compared against itself.
# - MIN_BASELINE_SAMPLE_COUNT: minimum total confidence samples across the
#   baseline window before the baseline is trusted (too little history -> no
#   drop comparison, and missing baseline never blocks resolution).
CONFIDENCE_DROP_RATIO_WARNING_THRESHOLD: float = 0.30
RECENT_WINDOW_SECONDS: int = 600
PREVIOUS_WINDOW_SECONDS: int = 1800
BASELINE_EXCLUSION_SECONDS: int = 120
MIN_BASELINE_SAMPLE_COUNT: int = 10


@dataclass(frozen=True)
class DetectionConfidenceStats:
    """Summary statistics over one set of confidence values.

    All fields are derived from ``values``. ``stddev`` is the population
    standard deviation (``statistics.pstdev``) -- we are describing the sample
    we have, not estimating a population.
    """

    sample_count: int
    avg_confidence: float
    median_confidence: float
    min_confidence: float
    max_confidence: float
    confidence_stddev: float
    low_confidence_count: int
    low_confidence_ratio: float


def _coerce_confidence(value: Any) -> float | None:
    """Accept only finite float-coercible values in ``[0.0, 1.0]``.

    Rejects ``None``, ``bool`` (Python ``True`` is ``int`` is ``float`` --
    would otherwise round-trip as 1.0), strings (even if numeric -- a payload
    that strings its confidences is wrong upstream and we shouldn't paper
    over it), ``NaN``, infinities, negatives, and values above 1.0.
    """
    if value is None or isinstance(value, bool):
        return None
    if not isinstance(value, (int, float)):
        return None
    f = float(value)
    if math.isnan(f) or math.isinf(f):
        return None
    if not 0.0 <= f <= 1.0:
        return None
    return f


def _pick_confidence_from(item: Mapping[str, Any]) -> float | None:
    """Take the first confidence-like key in ``item`` (in declared order)."""
    for key in _CONFIDENCE_KEYS:
        if key in item:
            coerced = _coerce_confidence(item[key])
            if coerced is not None:
                return coerced
    return None


def extract_detection_confidences(payload: Any) -> list[float]:
    """Return every detection-confidence value in ``payload``, in walk order.

    Walks (in this order):

    1. The top-level payload itself -- ``payload["confidence"]`` style.
    2. Each ``payload[k]`` where ``k`` is in ``_ARRAY_KEYS``, descending into
       each dict item.

    Non-dict items in arrays are silently skipped (a payload with
    ``objects: [null, {"confidence": 0.5}]`` returns ``[0.5]``, not an
    error). Non-mapping ``payload`` returns ``[]`` -- ``payload`` is typed
    ``Any`` so callers don't need to narrow before calling; the function is
    defensive against jsonb deserialization quirks.
    """
    if not isinstance(payload, Mapping):
        return []

    values: list[float] = []

    top = _pick_confidence_from(payload)
    if top is not None:
        values.append(top)

    for array_key in _ARRAY_KEYS:
        array = payload.get(array_key)
        if not isinstance(array, Sequence) or isinstance(array, (str, bytes)):
            continue
        for item in array:
            if not isinstance(item, Mapping):
                continue
            picked = _pick_confidence_from(item)
            if picked is not None:
                values.append(picked)

    return values


def count_detection_objects(payload: Any) -> int:
    """Count detection entries in ``payload`` (objects / detections / tracks).

    The number of detections on a frame, independent of whether each carried a
    usable confidence value -- so it can exceed ``len(extract_detection_confidences(payload))``
    when some objects lack a confidence field. Walks the same closed set of
    array keys as the confidence extractor; non-mapping items are skipped.
    Returns ``0`` for a non-mapping payload.
    """
    if not isinstance(payload, Mapping):
        return 0
    count = 0
    for array_key in _ARRAY_KEYS:
        array = payload.get(array_key)
        if not isinstance(array, Sequence) or isinstance(array, (str, bytes)):
            continue
        count += sum(1 for item in array if isinstance(item, Mapping))
    return count


def compute_detection_confidence_stats(
    values: Sequence[float],
    *,
    low_confidence_threshold: float = LOW_CONFIDENCE_THRESHOLD,
) -> DetectionConfidenceStats | None:
    """Summarize ``values``. Returns ``None`` when there are too few samples.

    ``low_confidence_threshold`` defaults to the module constant so the
    evaluator can pass through whatever future tuning surface lives upstream
    without rewiring this function. Single-sample ``pstdev`` is 0.0 (not an
    error) -- this is the population stddev definition; we don't have any
    inferential claim to make over one detection.
    """
    if len(values) < MIN_SAMPLE_COUNT:
        return None
    avg = statistics.mean(values)
    median = statistics.median(values)
    low_count = sum(1 for v in values if v < low_confidence_threshold)
    return DetectionConfidenceStats(
        sample_count=len(values),
        avg_confidence=avg,
        median_confidence=median,
        min_confidence=min(values),
        max_confidence=max(values),
        confidence_stddev=statistics.pstdev(values),
        low_confidence_count=low_count,
        low_confidence_ratio=low_count / len(values),
    )
