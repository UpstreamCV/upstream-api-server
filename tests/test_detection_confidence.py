"""Detection-confidence extractor + stats -- unit tests.

The extractor is the load-bearing piece: a permissive walk that accepts only
real model-confidence values from known nesting locations, with a hard ban on
mistaking unrelated numeric fields for confidence. Tests pin every payload
shape the evaluator might encounter today (top-level / detections / objects /
tracks / score-aliased) plus an explicit anti-test that proves a payload full
of ids and coordinates produces zero phantom confidences.

Stats tests pin the math + the MIN_SAMPLE_COUNT skip behavior. ``pstdev`` (not
``stdev``) is intentional: the evaluator describes the sample it has, not an
inferred population.
"""

from __future__ import annotations

import math

from upstream_api.services.detection_confidence import (
    AVG_CONFIDENCE_WARNING_THRESHOLD,
    CONFIDENCE_STDDEV_WARNING_THRESHOLD,
    LOW_CONFIDENCE_RATIO_WARNING_THRESHOLD,
    MIN_SAMPLE_COUNT,
    DetectionConfidenceStats,
    compute_detection_confidence_stats,
    extract_detection_confidences,
)

# ---------------------------------------------------------------------------
# Extractor: positive paths.
# ---------------------------------------------------------------------------


def test_top_level_confidence_extracted() -> None:
    assert extract_detection_confidences({"confidence": 0.72}) == [0.72]


def test_objects_array_confidence_extracted() -> None:
    """The shape ``latest_perception_snapshots.snapshot`` actually uses today."""
    payload = {
        "objects": [
            {"track_id": "0", "class_id": 2, "confidence": 0.82, "label": "person"},
            {"track_id": "1", "class_id": 2, "confidence": 0.41, "label": "person"},
        ],
    }
    assert extract_detection_confidences(payload) == [0.82, 0.41]


def test_objects_score_alias_extracted() -> None:
    """``score`` is a synonym for confidence in some model packages."""
    payload = {"objects": [{"score": 0.67}, {"score": 0.52}]}
    assert extract_detection_confidences(payload) == [0.67, 0.52]


def test_detections_array_confidence_extracted() -> None:
    payload = {"detections": [{"confidence": 0.72}, {"confidence": 0.41}]}
    assert extract_detection_confidences(payload) == [0.72, 0.41]


def test_tracks_array_confidence_extracted() -> None:
    payload = {"tracks": [{"confidence": 0.61}, {"confidence": 0.55}]}
    assert extract_detection_confidences(payload) == [0.61, 0.55]


def test_detection_confidence_alias_extracted() -> None:
    payload = {"detections": [{"detection_confidence": 0.83}]}
    assert extract_detection_confidences(payload) == [0.83]


def test_model_confidence_alias_extracted() -> None:
    payload = {"objects": [{"model_confidence": 0.31}]}
    assert extract_detection_confidences(payload) == [0.31]


def test_first_confidence_key_wins_when_multiple_present() -> None:
    """If an object has both ``confidence`` and ``score``, pick ``confidence``.

    The declared key order in ``_CONFIDENCE_KEYS`` is the priority. Pin it
    here so a future reorder is a deliberate change, not a silent semantic
    drift in production payloads."""
    payload = {"objects": [{"confidence": 0.9, "score": 0.1}]}
    assert extract_detection_confidences(payload) == [0.9]


def test_top_level_and_nested_both_extracted() -> None:
    payload = {
        "confidence": 0.50,
        "objects": [{"confidence": 0.80}],
    }
    assert extract_detection_confidences(payload) == [0.50, 0.80]


def test_multiple_arrays_all_walked() -> None:
    payload = {
        "objects": [{"confidence": 0.5}],
        "detections": [{"confidence": 0.6}],
        "tracks": [{"confidence": 0.7}],
    }
    # Walk order is _ARRAY_KEYS: objects, detections, tracks.
    assert extract_detection_confidences(payload) == [0.5, 0.6, 0.7]


# ---------------------------------------------------------------------------
# Extractor: rejection paths -- the never-fake-it promise.
# ---------------------------------------------------------------------------


def test_empty_payload_returns_empty() -> None:
    assert extract_detection_confidences({}) == []


def test_non_dict_payload_returns_empty() -> None:
    """Defensive: a row whose JSONB is unexpectedly a list / string / None
    must not crash the evaluator."""
    assert extract_detection_confidences(None) == []  # type: ignore[arg-type]
    assert extract_detection_confidences([]) == []  # type: ignore[arg-type]
    assert extract_detection_confidences("nope") == []  # type: ignore[arg-type]


def test_unrelated_numeric_fields_never_treated_as_confidence() -> None:
    """The promise: an object full of ids, counts, coordinates, and frame
    metadata must produce ZERO confidences. This is the anti-test for the
    whole module -- if it ever fails, the extractor has gotten too generous.
    """
    payload = {
        "frame": {"width": 1280, "height": 720, "frame_num": 99, "fps": 30.0},
        "geometry": {"rois": [{"id": "entry-roi", "points": [{"x": 64, "y": 36}]}]},
        "objects": [
            {
                "track_id": "0",
                "class_id": 2,
                "label": "person",
                "bbox": {"x": 410, "y": 120, "w": 180, "h": 520},
                "roi_labels": ["entry-roi"],
                # NOTE: NO confidence / score / detection_confidence /
                # model_confidence key. Class id 2 and track id 0 must NOT
                # be mistaken for confidence values.
            },
        ],
    }
    assert extract_detection_confidences(payload) == []


def test_rois_with_score_not_walked() -> None:
    """``rois`` is intentionally NOT in ``_ARRAY_KEYS``. An ROI's score is
    region-quality metadata, not a detection confidence. Pin that the
    extractor doesn't descend into it."""
    payload = {"rois": [{"id": "x", "score": 0.8}]}
    assert extract_detection_confidences(payload) == []


def test_string_confidence_rejected() -> None:
    """A payload that strings its confidences ('0.5') is wrong upstream; the
    extractor must not paper over it."""
    payload = {"objects": [{"confidence": "0.5"}]}
    assert extract_detection_confidences(payload) == []


def test_negative_confidence_rejected() -> None:
    assert extract_detection_confidences({"objects": [{"confidence": -0.1}]}) == []


def test_over_one_confidence_rejected() -> None:
    assert extract_detection_confidences({"objects": [{"confidence": 1.5}]}) == []


def test_nan_confidence_rejected() -> None:
    assert extract_detection_confidences({"objects": [{"confidence": math.nan}]}) == []


def test_inf_confidence_rejected() -> None:
    assert extract_detection_confidences({"objects": [{"confidence": math.inf}]}) == []


def test_bool_confidence_rejected() -> None:
    """``isinstance(True, int)`` is True in Python -- without the explicit
    bool ban, ``True`` would round-trip as 1.0 and pollute stats."""
    assert extract_detection_confidences({"objects": [{"confidence": True}]}) == []


def test_none_in_object_skipped() -> None:
    assert extract_detection_confidences({"objects": [{"confidence": None}]}) == []


def test_non_dict_items_in_array_skipped() -> None:
    """A payload like ``{"objects": [null, "x", {"confidence": 0.5}]}`` must
    not crash, and must extract just the one valid item."""
    payload = {"objects": [None, "x", 7, {"confidence": 0.5}]}
    assert extract_detection_confidences(payload) == [0.5]


def test_boundary_values_accepted() -> None:
    """0.0 and 1.0 are valid confidence values -- the rejection is strictly
    outside the closed interval."""
    payload = {"objects": [{"confidence": 0.0}, {"confidence": 1.0}]}
    assert extract_detection_confidences(payload) == [0.0, 1.0]


def test_mixed_valid_and_invalid_keeps_only_valid() -> None:
    payload = {
        "objects": [
            {"confidence": 0.5},
            {"confidence": "0.5"},  # rejected
            {"confidence": -0.1},  # rejected
            {"score": 0.7},  # accepted via alias
            {"track_id": 7},  # no confidence key -- skipped
        ]
    }
    assert extract_detection_confidences(payload) == [0.5, 0.7]


# ---------------------------------------------------------------------------
# Stats: math correctness + MIN_SAMPLE_COUNT.
# ---------------------------------------------------------------------------


def test_stats_below_min_sample_count_returns_none() -> None:
    """The evaluator treats ``None`` as 'insufficient data; skip the rule'.
    Critical that we return ``None`` instead of computing on a tiny sample
    and producing a noisy verdict."""
    assert compute_detection_confidence_stats([0.1, 0.2, 0.3, 0.4]) is None
    assert compute_detection_confidence_stats([]) is None


def test_stats_at_min_sample_count_returns_value() -> None:
    out = compute_detection_confidence_stats([0.1, 0.2, 0.3, 0.4, 0.5])
    assert isinstance(out, DetectionConfidenceStats)
    assert out.sample_count == MIN_SAMPLE_COUNT


def test_stats_avg_median_min_max() -> None:
    out = compute_detection_confidence_stats([0.1, 0.3, 0.5, 0.7, 0.9])
    assert out is not None
    assert math.isclose(out.avg_confidence, 0.5)
    assert math.isclose(out.median_confidence, 0.5)
    assert math.isclose(out.min_confidence, 0.1)
    assert math.isclose(out.max_confidence, 0.9)


def test_stats_pstdev_zero_for_constant_values() -> None:
    out = compute_detection_confidence_stats([0.5, 0.5, 0.5, 0.5, 0.5])
    assert out is not None
    assert out.confidence_stddev == 0.0


def test_stats_low_confidence_count_and_ratio() -> None:
    # Default LOW_CONFIDENCE_THRESHOLD is 0.45.
    # Below-threshold: 0.1, 0.2, 0.3, 0.4 -> 4 values.
    # At-or-above: 0.45, 0.5 -> 2 values.
    out = compute_detection_confidence_stats([0.1, 0.2, 0.3, 0.4, 0.45, 0.5])
    assert out is not None
    assert out.low_confidence_count == 4
    assert math.isclose(out.low_confidence_ratio, 4 / 6)


def test_stats_low_threshold_is_strict_below() -> None:
    """Values exactly at the threshold are NOT counted low -- the boundary
    is ``< threshold``, matching the evaluator's "warning if < 0.45"
    intuition."""
    out = compute_detection_confidence_stats([0.45] * 5)
    assert out is not None
    assert out.low_confidence_count == 0


def test_stats_high_confidence_values_are_healthy() -> None:
    """8 detections all in the 0.75-0.95 range: avg high, stddev low, low
    ratio zero. Pin that none of the warning thresholds would fire."""
    values = [0.75, 0.80, 0.82, 0.85, 0.88, 0.90, 0.92, 0.95]
    out = compute_detection_confidence_stats(values)
    assert out is not None
    assert out.avg_confidence >= AVG_CONFIDENCE_WARNING_THRESHOLD
    assert out.low_confidence_ratio <= LOW_CONFIDENCE_RATIO_WARNING_THRESHOLD
    assert out.confidence_stddev <= CONFIDENCE_STDDEV_WARNING_THRESHOLD


def test_stats_low_avg_would_warn() -> None:
    """A frame with low mean confidence trips the avg threshold."""
    values = [0.10, 0.20, 0.25, 0.30, 0.35]
    out = compute_detection_confidence_stats(values)
    assert out is not None
    assert out.avg_confidence < AVG_CONFIDENCE_WARNING_THRESHOLD


def test_stats_high_spread_would_warn() -> None:
    """Bimodal payload (some very high, some very low) trips the stddev
    threshold even though the avg is mid-range."""
    values = [0.95, 0.90, 0.10, 0.15, 0.92, 0.05]
    out = compute_detection_confidence_stats(values)
    assert out is not None
    assert out.confidence_stddev > CONFIDENCE_STDDEV_WARNING_THRESHOLD


def test_stats_high_low_ratio_would_warn() -> None:
    """More than half the samples below threshold trips the low-ratio
    warning even when avg sits near the boundary."""
    values = [0.10, 0.20, 0.30, 0.40, 0.42, 0.85]
    out = compute_detection_confidence_stats(values)
    assert out is not None
    assert out.low_confidence_ratio > LOW_CONFIDENCE_RATIO_WARNING_THRESHOLD
