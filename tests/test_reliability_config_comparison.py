"""Config comparison report card (model-config M11) — service tests.

Pure unit tests against a fake asyncpg connection serving canned by-config
rollup rows (the same row shape the by-config trend query returns). The two
properties pinned hardest:

- the report card is DESCRIPTIVE: statuses/directions/deltas only, and the
  generated text never contains judgment or causal vocabulary,
- gating is conservative: one group, small samples, or missing aggregates is
  ``inconclusive``; an empty window is ``no_data``.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

from upstream_api.services.reliability_config_comparison import (
    DEFAULT_MIN_SAMPLES,
    get_config_comparison_summary,
)

_NOW = datetime(2026, 6, 10, 12, 0, 0, tzinfo=UTC)
_T0 = datetime(2026, 6, 10, 0, 0, 0, tzinfo=UTC)


class FakeByConfigConn:
    """Serves the by-config trend query (the only fetch the service makes)."""

    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows
        self.calls: list[tuple[Any, ...]] = []

    async def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        assert "from detection_confidence_hourly_rollups" in query
        self.calls.append(args)
        return list(self.rows)


def _row(
    hour: int,
    *,
    avg: float | None,
    samples: int,
    low_ratio: float | None = None,
    revision: int | None = None,
    source: str | None = "operator_saved",
    status: str | None = "applied",
    model_key: str | None = "trafficcamnet_v1",
    classes_key: str | None = "all",
    threshold: float | None = None,
    requested_fps: int | None = None,
    applied_fps: int | None = None,
    warnings: list[str] | None = None,
) -> dict[str, Any]:
    start = _T0 + timedelta(hours=hour)
    return {
        "processing_config_source": source,
        "processing_config_revision": revision,
        "processing_config_status": status,
        "applied_model_key": model_key,
        "applied_selected_classes_key": classes_key,
        "applied_confidence_threshold": threshold,
        "requested_processing_fps": requested_fps,
        "applied_processing_fps": applied_fps,
        "processing_config_warnings": warnings,
        "bucket_start": start,
        "bucket_end": start + timedelta(hours=1),
        "avg_confidence": avg,
        "min_confidence": None,
        "max_confidence": None,
        "low_confidence_ratio": low_ratio,
        "confidence_stddev": None,
        "metric_count": samples,
        "object_count_total": samples,
        "confidence_sample_count": samples,
    }


async def _summary(rows: list[dict[str, Any]], **kwargs: Any) -> Any:
    conn = FakeByConfigConn(rows)
    return await get_config_comparison_summary(
        conn,
        account_id=uuid4(),
        camera_id=uuid4(),
        range_="24h",
        now=_NOW,
        **kwargs,
    )


# Two clean groups: same threshold/classes/fps (comparable population),
# candidate r5 has HIGHER avg confidence and LOWER low-conf ratio.
def _comparable_rows() -> list[dict[str, Any]]:
    return [
        _row(0, avg=0.40, samples=100, low_ratio=0.30, revision=4),
        _row(1, avg=0.60, samples=120, low_ratio=0.10, revision=5),
    ]


# The live r4->r5 shape: threshold + fps changed, candidate has lower avg.
def _population_change_rows() -> list[dict[str, Any]]:
    return [
        _row(0, avg=0.45, samples=120, low_ratio=0.18, revision=4),
        _row(
            1,
            avg=0.21,
            samples=268,
            low_ratio=0.44,
            revision=5,
            status="partially_applied",
            threshold=0.1,
            requested_fps=5,
            applied_fps=5,
            warnings=["processing_fps_output_cadence_only"],
        ),
    ]


# --- gates ----------------------------------------------------------------


async def test_no_data_returns_no_data_status() -> None:
    s = await _summary([])
    assert s.comparison_status == "no_data"
    assert s.baseline is None and s.candidate is None
    assert s.metric_deltas is None
    assert s.metric_directions == []
    assert "Directional comparison only, not causal proof." in s.caveats


async def test_single_group_is_inconclusive_with_candidate_shown() -> None:
    s = await _summary([_row(0, avg=0.5, samples=200, revision=5)])
    assert s.comparison_status == "inconclusive"
    assert s.baseline is None
    assert s.candidate is not None  # the lone group is shown for context
    assert s.candidate.processing_config_revision == 5
    assert "at least two configs" in s.summary


async def test_small_samples_is_inconclusive() -> None:
    rows = [
        _row(0, avg=0.4, samples=10, revision=4),
        _row(1, avg=0.6, samples=200, revision=5),
    ]
    s = await _summary(rows)
    assert s.comparison_status == "inconclusive"
    assert "too few samples" in s.summary
    # Deltas/groups still ride along for context.
    assert s.metric_deltas is not None
    assert s.baseline is not None and s.candidate is not None


async def test_min_samples_param_is_honored() -> None:
    s = await _summary(_comparable_rows(), min_samples=1000)
    assert s.comparison_status == "inconclusive"
    s = await _summary(_comparable_rows(), min_samples=50)
    assert s.comparison_status == "comparable"


# --- selection --------------------------------------------------------------


async def test_latest_vs_previous_revision_selected() -> None:
    rows = [
        _row(0, avg=0.5, samples=100, revision=3),
        _row(1, avg=0.5, samples=100, revision=4),
        _row(2, avg=0.5, samples=100, revision=5),
    ]
    s = await _summary(rows)
    assert s.comparison_mode == "latest_vs_previous"
    assert s.baseline.processing_config_revision == 4
    assert s.candidate.processing_config_revision == 5


async def test_explicit_revisions_win() -> None:
    rows = [
        _row(0, avg=0.5, samples=100, revision=3),
        _row(1, avg=0.5, samples=100, revision=4),
        _row(2, avg=0.5, samples=100, revision=5),
    ]
    s = await _summary(rows, baseline_revision=3, candidate_revision=5)
    assert s.comparison_mode == "explicit"
    assert s.baseline.processing_config_revision == 3
    assert s.candidate.processing_config_revision == 5


async def test_missing_explicit_revision_is_inconclusive() -> None:
    s = await _summary(_comparable_rows(), baseline_revision=99, candidate_revision=5)
    assert s.comparison_status == "inconclusive"


async def test_revisionless_history_falls_back_to_recency() -> None:
    """Old NULL-dimension rows are handled safely: the two most recent groups
    by latest bucket are compared (mode=recent_vs_prior)."""
    rows = [
        _row(0, avg=0.5, samples=100, revision=None, source=None, status=None, classes_key=None),
        _row(
            5,
            avg=0.6,
            samples=100,
            revision=None,
            source="default",
            status="applied_defaults",
        ),
    ]
    s = await _summary(rows)
    assert s.comparison_mode == "recent_vs_prior"
    assert s.comparison_status in ("comparable", "mixed_metrics")
    assert s.candidate.processing_config_status == "applied_defaults"


# --- statuses + directions ---------------------------------------------------


async def test_comparable_status_with_factual_directions() -> None:
    s = await _summary(_comparable_rows())
    assert s.comparison_status == "comparable"
    assert "avg_confidence_higher" in s.metric_directions
    assert "low_confidence_ratio_lower" in s.metric_directions
    assert "samples_higher" in s.metric_directions
    assert s.metric_deltas.avg_confidence_delta == 0.2
    assert s.metric_deltas.low_confidence_ratio_delta == -0.2
    assert s.metric_deltas.sample_count_delta == 20
    assert "Config r5" in s.summary and "Config r4" in s.summary
    assert "in this window" in s.summary
    assert s.comparability_notes == []


async def test_lower_metrics_are_described_not_judged() -> None:
    rows = [
        _row(0, avg=0.60, samples=100, low_ratio=0.10, revision=4),
        _row(1, avg=0.40, samples=80, low_ratio=0.30, revision=5),
    ]
    s = await _summary(rows)
    assert "avg_confidence_lower" in s.metric_directions
    assert "low_confidence_ratio_higher" in s.metric_directions
    assert "samples_lower" in s.metric_directions
    assert "lower average confidence" in s.summary


async def test_unchanged_metrics_within_epsilon() -> None:
    rows = [
        _row(0, avg=0.50, samples=100, low_ratio=0.20, revision=4),
        _row(1, avg=0.51, samples=100, low_ratio=0.21, revision=5),
    ]
    s = await _summary(rows)
    assert "avg_confidence_unchanged" in s.metric_directions
    assert "low_confidence_ratio_unchanged" in s.metric_directions
    assert "samples_unchanged" in s.metric_directions


async def test_threshold_change_is_mixed_metrics_with_note() -> None:
    s = await _summary(_population_change_rows())
    assert s.comparison_status == "mixed_metrics"
    assert any("Confidence threshold changed" in n for n in s.comparability_notes)
    assert any("default → 0.10" in n for n in s.comparability_notes)
    assert "not directly comparable" in s.summary
    # The threshold-context bullet accompanies the lower average.
    assert any("weaker detections" in b for b in s.bullets)


async def test_classes_change_is_mixed_metrics_with_note() -> None:
    rows = [
        _row(0, avg=0.5, samples=100, revision=4, classes_key="all"),
        _row(1, avg=0.6, samples=100, revision=5, classes_key="person"),
    ]
    s = await _summary(rows)
    assert s.comparison_status == "mixed_metrics"
    assert any("Selected classes differ" in n for n in s.comparability_notes)


# --- FPS + warning caveats ----------------------------------------------------


async def test_fps_change_adds_cadence_caveat_and_sample_context() -> None:
    s = await _summary(_population_change_rows())
    assert s.metric_deltas.applied_processing_fps_delta == 5
    assert any("output cadence only" in c for c in s.caveats)
    assert any("5 FPS" in b and "not inference work" in b for b in s.bullets)


async def test_warning_codes_carry_into_caveats_and_groups() -> None:
    rows = [
        _row(
            0,
            avg=0.5,
            samples=100,
            revision=4,
            classes_key="person",
            warnings=["selected_classes_analytics_counts_not_filtered"],
        ),
        _row(
            1,
            avg=0.6,
            samples=100,
            revision=5,
            classes_key="person",
            warnings=["selected_classes_analytics_counts_not_filtered"],
        ),
    ]
    s = await _summary(rows)
    assert any("Line/ROI aggregate counts" in c for c in s.caveats)
    assert "selected_classes_analytics_counts_not_filtered" in s.candidate.warnings


async def test_labels_match_dashboard_format() -> None:
    s = await _summary(_population_change_rows())
    assert s.baseline.label == "All classes · default confidence · default FPS"
    assert s.candidate.label == "All classes · confidence 0.10 · 5 FPS"


# --- honesty: no judgment / causal / recommendation vocabulary -----------------


async def test_generated_text_contains_no_judgment_or_causal_words() -> None:
    """Across every scenario, the summary/bullets/notes never judge, recommend,
    or claim causation/GPU effects. (Caveats may legitimately *deny* GPU
    effects, so they are scanned for judgment words only.)"""
    scenarios = [
        [],
        [_row(0, avg=0.5, samples=200, revision=5)],
        _comparable_rows(),
        _population_change_rows(),
        [
            _row(0, avg=0.6, samples=100, low_ratio=0.1, revision=4),
            _row(1, avg=0.4, samples=80, low_ratio=0.3, revision=5),
        ],
    ]
    judgment = ("better", "worse", "best", "recommend", "optimiz", "should")
    causal = ("caused", "because of the config", "proves", "improvement due")
    for rows in scenarios:
        s = await _summary(rows)
        prose = " ".join([s.summary, *s.bullets, *s.comparability_notes]).lower()
        for word in judgment + causal:
            assert word not in prose, (word, prose)
        for caveat in s.caveats:
            for word in judgment:
                assert word not in caveat.lower(), (word, caveat)
        # GPU appears ONLY inside the denial caveat, never in summary/bullets.
        assert "gpu" not in prose


async def test_default_min_samples_constant() -> None:
    assert DEFAULT_MIN_SAMPLES == 30
