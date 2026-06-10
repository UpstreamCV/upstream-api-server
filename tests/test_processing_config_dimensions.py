"""Applied-processing-config dimension extraction (model-config M9).

Pure unit tests for ``services/processing_config_dimensions.py`` — the single
place that maps M8's ``snapshot.source.applied_processing_config`` block to the
compact dimension set the reliability tables store. The two hard properties
pinned here: extraction NEVER raises (malformed shapes degrade to the all-None
pre-M8 row shape), and the selected_classes key is canonical (order/dup
insensitive).
"""

from __future__ import annotations

from typing import Any

from upstream_api.services.processing_config_dimensions import (
    EMPTY_PROCESSING_CONFIG_DIMENSIONS,
    SELECTED_CLASSES_ANALYTICS_WARNING,
    extract_processing_config_dimensions,
    has_processing_config_warning,
    normalize_selected_classes_key,
)


def _snapshot_with_config(config_block: Any) -> dict[str, Any]:
    """A realistic M8 snapshot shell around one applied_processing_config."""
    return {
        "frame": {"width": 1280, "height": 720},
        "objects": [{"confidence": 0.8}],
        "source": {
            "runtime": "deepstream",
            "metadata_source": "pyds_pad_probe",
            "analytics_plugin": "nvdsanalytics",
            "tracker_algorithm": "iou",
            "applied_processing_config": config_block,
        },
    }


def _operator_block(**overrides: Any) -> dict[str, Any]:
    """The producer's canonical operator_saved block (M8 shape)."""
    block: dict[str, Any] = {
        "source": "operator_saved",
        "revision": 7,
        "status": "partially_applied",
        "reason": None,
        "requested": {
            "model_key": "trafficcamnet_v1",
            "selected_classes": ["person"],
            "tracker": None,
            "confidence_threshold": 0.2,
            "processing_fps": None,
        },
        "applied": {
            "model_key": "trafficcamnet_v1",
            "selected_classes": ["person"],
            "tracker": "iou",
            "confidence_threshold": 0.2,
            "processing_fps": None,
        },
        "warnings": [SELECTED_CLASSES_ANALYTICS_WARNING],
    }
    block.update(overrides)
    return block


# ---------------------------------------------------------------------------
# Missing / pre-M8 shapes -> all-None dimensions, never an exception.
# ---------------------------------------------------------------------------


def test_missing_applied_processing_config_yields_all_none() -> None:
    for snapshot in (
        {},  # pre-M8: no source at all
        {"frame": {"width": 100}, "objects": []},
        {"source": {"runtime": "deepstream"}},  # source without the block
        {"source": None},
        {"source": "deepstream"},  # source not a mapping
    ):
        dims = extract_processing_config_dimensions(snapshot)
        assert dims == EMPTY_PROCESSING_CONFIG_DIMENSIONS
        assert dims.processing_config_source is None
        assert dims.processing_config_revision is None
        assert dims.processing_config_status is None
        assert dims.applied_model_key is None
        assert dims.applied_selected_classes_key is None
        assert dims.applied_confidence_threshold is None
        assert dims.processing_config_warnings is None
        assert dims.selected_classes_analytics_counts_not_filtered is None
        assert dims.warnings_list() is None


def test_non_mapping_snapshot_yields_all_none() -> None:
    for snapshot in (None, "snap", 42, ["objects"]):
        assert extract_processing_config_dimensions(snapshot) == (
            EMPTY_PROCESSING_CONFIG_DIMENSIONS
        )


# ---------------------------------------------------------------------------
# Real producer shapes.
# ---------------------------------------------------------------------------


def test_default_lineage_extracts_source_and_status() -> None:
    """The 'default' block (no saved config): null revision, applied defaults."""
    block: dict[str, Any] = {
        "source": "default",
        "revision": None,
        "status": "applied_defaults",
        "reason": None,
        "requested": {
            "model_key": None,
            "selected_classes": None,
            "tracker": None,
            "confidence_threshold": None,
            "processing_fps": None,
        },
        "applied": {
            "model_key": "trafficcamnet_v1",
            "selected_classes": [],
            "tracker": "iou",
            "confidence_threshold": None,
            "processing_fps": None,
        },
        "warnings": [],
    }
    dims = extract_processing_config_dimensions(_snapshot_with_config(block))
    assert dims.processing_config_source == "default"
    assert dims.processing_config_revision is None
    assert dims.processing_config_status == "applied_defaults"
    assert dims.applied_model_key == "trafficcamnet_v1"
    assert dims.applied_selected_classes_key == "all"  # [] == all supported classes
    assert dims.applied_confidence_threshold is None
    assert dims.processing_config_warnings == ()
    # Metadata present, warning absent -> a real False, NOT None: distinct from
    # "no metadata at all".
    assert dims.selected_classes_analytics_counts_not_filtered is False
    assert dims.warnings_list() == []


def test_operator_saved_lineage_extracts_revision_status_and_applied() -> None:
    dims = extract_processing_config_dimensions(_snapshot_with_config(_operator_block()))
    assert dims.processing_config_source == "operator_saved"
    assert dims.processing_config_revision == 7
    assert dims.processing_config_status == "partially_applied"
    assert dims.applied_model_key == "trafficcamnet_v1"
    assert dims.applied_selected_classes_key == "person"
    assert dims.applied_confidence_threshold == 0.2
    assert dims.processing_config_warnings == (SELECTED_CLASSES_ANALYTICS_WARNING,)
    assert dims.selected_classes_analytics_counts_not_filtered is True
    assert dims.warnings_list() == [SELECTED_CLASSES_ANALYTICS_WARNING]


def test_partially_applied_with_multiple_warnings() -> None:
    block = _operator_block(
        warnings=[
            "processing_fps_requested_not_applied",
            "model_switch_requested_not_applied",
            SELECTED_CLASSES_ANALYTICS_WARNING,
        ]
    )
    dims = extract_processing_config_dimensions(_snapshot_with_config(block))
    assert dims.processing_config_status == "partially_applied"
    assert dims.processing_config_warnings == (
        "processing_fps_requested_not_applied",
        "model_switch_requested_not_applied",
        SELECTED_CLASSES_ANALYTICS_WARNING,
    )
    assert dims.selected_classes_analytics_counts_not_filtered is True


def test_warning_flag_false_when_other_warnings_only() -> None:
    block = _operator_block(warnings=["confidence_threshold_requested_not_applied"])
    dims = extract_processing_config_dimensions(_snapshot_with_config(block))
    assert dims.selected_classes_analytics_counts_not_filtered is False
    assert dims.processing_config_warnings == ("confidence_threshold_requested_not_applied",)


def test_confidence_threshold_extracts_as_numeric() -> None:
    for raw, expected in ((0.2, 0.2), (0, 0.0), (1, 1.0), (0.55, 0.55)):
        block = _operator_block()
        block["applied"]["confidence_threshold"] = raw
        dims = extract_processing_config_dimensions(_snapshot_with_config(block))
        assert dims.applied_confidence_threshold == expected
        assert isinstance(dims.applied_confidence_threshold, float)


# ---------------------------------------------------------------------------
# Malformed values degrade field-by-field, never crash.
# ---------------------------------------------------------------------------


def test_malformed_block_values_degrade_to_none() -> None:
    block = {
        "source": 42,  # not a string
        "revision": "seven",  # not an int
        "status": "",  # empty string
        "applied": {
            "model_key": ["trafficcamnet_v1"],  # not a string
            "selected_classes": "person",  # not a list
            "confidence_threshold": "0.2",  # string rejected even if numeric
        },
        "warnings": "not-a-list",
    }
    dims = extract_processing_config_dimensions(_snapshot_with_config(block))
    assert dims.processing_config_source is None
    assert dims.processing_config_revision is None
    assert dims.processing_config_status is None
    assert dims.applied_model_key is None
    assert dims.applied_selected_classes_key == "unknown"  # malformed groups visibly
    assert dims.applied_confidence_threshold is None
    assert dims.processing_config_warnings is None
    # Warnings unusable -> the flag falls back to False (metadata was present).
    assert dims.selected_classes_analytics_counts_not_filtered is False


def test_malformed_revision_and_threshold_edge_cases() -> None:
    block = _operator_block(revision=True)  # bool is an int subclass — rejected
    block["applied"]["confidence_threshold"] = 1.7  # out of [0, 1]
    dims = extract_processing_config_dimensions(_snapshot_with_config(block))
    assert dims.processing_config_revision is None
    assert dims.applied_confidence_threshold is None
    block = _operator_block(revision=-3)  # negative — rejected
    block["applied"]["confidence_threshold"] = float("nan")
    dims = extract_processing_config_dimensions(_snapshot_with_config(block))
    assert dims.processing_config_revision is None
    assert dims.applied_confidence_threshold is None


def test_applied_block_not_a_mapping_degrades_applied_fields_only() -> None:
    block = _operator_block(applied="defaults")
    dims = extract_processing_config_dimensions(_snapshot_with_config(block))
    assert dims.processing_config_source == "operator_saved"  # lineage survives
    assert dims.processing_config_revision == 7
    assert dims.applied_model_key is None
    assert dims.applied_selected_classes_key is None
    assert dims.applied_confidence_threshold is None


def test_block_not_a_mapping_yields_all_none() -> None:
    for bad in ("config", 7, ["applied"], True):
        dims = extract_processing_config_dimensions(_snapshot_with_config(bad))
        assert dims == EMPTY_PROCESSING_CONFIG_DIMENSIONS


def test_warnings_drop_non_string_entries_and_dedupe() -> None:
    block = _operator_block(
        warnings=[SELECTED_CLASSES_ANALYTICS_WARNING, 42, "", SELECTED_CLASSES_ANALYTICS_WARNING]
    )
    dims = extract_processing_config_dimensions(_snapshot_with_config(block))
    assert dims.processing_config_warnings == (SELECTED_CLASSES_ANALYTICS_WARNING,)
    assert dims.selected_classes_analytics_counts_not_filtered is True


# ---------------------------------------------------------------------------
# normalize_selected_classes_key — the canonical grouping key.
# ---------------------------------------------------------------------------


def test_selected_classes_key_all_for_empty_and_null() -> None:
    assert normalize_selected_classes_key(None) == "all"
    assert normalize_selected_classes_key([]) == "all"


def test_selected_classes_key_single_class() -> None:
    assert normalize_selected_classes_key(["person"]) == "person"


def test_selected_classes_key_canonical_sort_and_dedupe() -> None:
    assert normalize_selected_classes_key(["person", "car"]) == "car,person"
    assert normalize_selected_classes_key(["car", "person"]) == "car,person"
    assert normalize_selected_classes_key(["car", "person", "car"]) == "car,person"
    assert normalize_selected_classes_key([" person ", "car"]) == "car,person"


def test_selected_classes_key_unknown_for_malformed() -> None:
    assert normalize_selected_classes_key("person") == "unknown"
    assert normalize_selected_classes_key({"person": True}) == "unknown"
    assert normalize_selected_classes_key([42]) == "unknown"
    assert normalize_selected_classes_key(["person", ""]) == "unknown"
    assert normalize_selected_classes_key(["person", None]) == "unknown"


# ---------------------------------------------------------------------------
# has_processing_config_warning.
# ---------------------------------------------------------------------------


def test_has_processing_config_warning() -> None:
    assert has_processing_config_warning(
        [SELECTED_CLASSES_ANALYTICS_WARNING], SELECTED_CLASSES_ANALYTICS_WARNING
    )
    assert has_processing_config_warning(
        ("a", SELECTED_CLASSES_ANALYTICS_WARNING), SELECTED_CLASSES_ANALYTICS_WARNING
    )
    assert not has_processing_config_warning([], SELECTED_CLASSES_ANALYTICS_WARNING)
    assert not has_processing_config_warning(["other"], SELECTED_CLASSES_ANALYTICS_WARNING)
    assert not has_processing_config_warning(None, SELECTED_CLASSES_ANALYTICS_WARNING)
    assert not has_processing_config_warning("warnings", SELECTED_CLASSES_ANALYTICS_WARNING)


# ---------------------------------------------------------------------------
# M9.1: dedicated FPS dimensions.
# ---------------------------------------------------------------------------


def test_fps_extracted_from_valid_m7e_lineage() -> None:
    block = _operator_block(warnings=["processing_fps_output_cadence_only"])
    block["requested"]["processing_fps"] = 5
    block["applied"]["processing_fps"] = 5
    dims = extract_processing_config_dimensions(_snapshot_with_config(block))
    assert dims.requested_processing_fps == 5
    assert dims.applied_processing_fps == 5
    assert dims.processing_config_warnings == ("processing_fps_output_cadence_only",)
    # Existing dimensions are untouched by the FPS extraction.
    assert dims.applied_selected_classes_key == "person"
    assert dims.applied_confidence_threshold == 0.2


def test_fps_requested_but_not_applied_extracts_request_only() -> None:
    block = _operator_block(warnings=["processing_fps_requested_not_applied"])
    block["requested"]["processing_fps"] = 5
    block["applied"]["processing_fps"] = None
    dims = extract_processing_config_dimensions(_snapshot_with_config(block))
    assert dims.requested_processing_fps == 5
    assert dims.applied_processing_fps is None


def test_missing_fps_yields_null_dimensions() -> None:
    dims = extract_processing_config_dimensions(_snapshot_with_config(_operator_block()))
    assert dims.requested_processing_fps is None
    assert dims.applied_processing_fps is None
    # Pre-M8 snapshots stay all-None including FPS.
    legacy = extract_processing_config_dimensions({"objects": []})
    assert legacy.requested_processing_fps is None
    assert legacy.applied_processing_fps is None


def test_malformed_fps_yields_null() -> None:
    block = _operator_block()
    block["requested"]["processing_fps"] = "5"  # string rejected
    block["applied"]["processing_fps"] = [5]  # list rejected
    dims = extract_processing_config_dimensions(_snapshot_with_config(block))
    assert dims.requested_processing_fps is None
    assert dims.applied_processing_fps is None
    block = _operator_block()
    block["applied"]["processing_fps"] = True  # bool (int subclass) rejected
    assert (
        extract_processing_config_dimensions(_snapshot_with_config(block)).applied_processing_fps
        is None
    )


def test_out_of_range_fps_yields_null() -> None:
    for bad in (0, -1, 31, 1000):
        block = _operator_block()
        block["applied"]["processing_fps"] = bad
        dims = extract_processing_config_dimensions(_snapshot_with_config(block))
        assert dims.applied_processing_fps is None
    # Boundary values survive.
    for ok in (1, 30):
        block = _operator_block()
        block["applied"]["processing_fps"] = ok
        assert (
            extract_processing_config_dimensions(
                _snapshot_with_config(block)
            ).applied_processing_fps
            == ok
        )
