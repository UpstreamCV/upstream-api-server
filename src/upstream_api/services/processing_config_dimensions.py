"""Applied-processing-config dimension extraction (model-config M9).

M8 (``upstream-processor``) stamps the resolved processing-config lineage onto
every perception snapshot at ``snapshot.source.applied_processing_config``:

    {
      "source":   "operator_saved" | "default" | "fallback_default_on_error",
      "revision": <int> | null,
      "status":   "applied" | "partially_applied" | "requested_not_applied"
                  | "applied_defaults" | "fallback_default_on_error",
      "reason":   <str> | null,
      "requested": {model_key, selected_classes, tracker,
                    confidence_threshold, processing_fps},
      "applied":   {model_key, selected_classes, tracker,
                    confidence_threshold, processing_fps},
      "warnings": [<str code>, ...]
    }

M9 reduces that block to a COMPACT, QUERYABLE set of grouping dimensions for
the reliability metrics tables, so trends can be grouped/filtered by the config
that was actually applied. This module is the single place that mapping lives.

Hard rules:

- Extraction must NEVER raise. The snapshot dict is processor-produced wire
  data; anything missing or malformed degrades to ``None`` dimensions (the
  same shape an old, pre-M8 snapshot yields), never an exception — a metric
  write must keep succeeding for old and malformed snapshots alike.
- ``applied`` values are the grouping truth; ``requested`` values are
  deliberately NOT extracted (the warnings codes already record what was
  requested but not applied, and the full block stays available in
  ``latest_perception_snapshots.snapshot`` / ``observations.payload``).
- ``selected_classes`` is normalised to a canonical key string
  (``normalize_selected_classes_key``) so GROUP BY / query params never have
  to compare arrays: ``[]``/null -> "all", ["car","person"] (any order) ->
  "car,person", malformed -> "unknown".
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

# The headline M7D caveat code: selected_classes IS applied to per-object
# outputs, but line/ROI aggregate counts remain all-class. Surfaced as its own
# boolean dimension so "which metrics carry this caveat" is a trivial filter.
SELECTED_CLASSES_ANALYTICS_WARNING = "selected_classes_analytics_counts_not_filtered"

# Canonical keys for the two normalisation edge cases.
SELECTED_CLASSES_KEY_ALL = "all"
SELECTED_CLASSES_KEY_UNKNOWN = "unknown"


@dataclass(frozen=True)
class ProcessingConfigDimensions:
    """The compact M9 dimension set extracted from one snapshot.

    Every field is ``None`` when the snapshot carries no (or a malformed)
    ``source.applied_processing_config`` block — exactly the shape an old,
    pre-M8 row gets, so old and new data coexist in the same tables.
    ``processing_config_warnings`` is a tuple (immutability for the frozen
    dataclass); callers bind it to a ``text[]`` column as a list.
    """

    processing_config_source: str | None
    processing_config_revision: int | None
    processing_config_status: str | None
    applied_model_key: str | None
    applied_selected_classes_key: str | None
    applied_confidence_threshold: float | None
    processing_config_warnings: tuple[str, ...] | None
    selected_classes_analytics_counts_not_filtered: bool | None

    def warnings_list(self) -> list[str] | None:
        """The warnings as a list for asyncpg ``text[]`` binding."""
        if self.processing_config_warnings is None:
            return None
        return list(self.processing_config_warnings)


# The all-None shape: pre-M8 snapshots, and any malformed block.
EMPTY_PROCESSING_CONFIG_DIMENSIONS = ProcessingConfigDimensions(
    processing_config_source=None,
    processing_config_revision=None,
    processing_config_status=None,
    applied_model_key=None,
    applied_selected_classes_key=None,
    applied_confidence_threshold=None,
    processing_config_warnings=None,
    selected_classes_analytics_counts_not_filtered=None,
)


def _coerce_text(value: Any) -> str | None:
    """A non-empty string, else ``None`` (numbers/bools/containers rejected)."""
    if isinstance(value, str) and value:
        return value
    return None


def _coerce_revision(value: Any) -> int | None:
    """A non-negative int, else ``None``. ``bool`` is rejected explicitly
    (it is an ``int`` subclass) and so are floats — the producer's revision is
    the api-server's monotonic save counter, always an int >= 0."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int) and value >= 0:
        return value
    return None


def _coerce_threshold(value: Any) -> float | None:
    """A finite number in [0, 1], else ``None`` — same posture as the
    confidence parser in ``detection_confidence`` (strings rejected even if
    numeric, bools rejected)."""
    if isinstance(value, bool):
        return None
    if not isinstance(value, (int, float)):
        return None
    result = float(value)
    if not math.isfinite(result):
        return None
    if not 0.0 <= result <= 1.0:
        return None
    return result


def normalize_selected_classes_key(classes: Any) -> str:
    """Canonical grouping key for a selected_classes value.

    - ``[]`` or ``None`` -> ``"all"`` (the platform contract: empty selection
      means all supported classes).
    - a list of class labels -> deduped, canonically sorted, comma-joined
      (``["person", "car"]`` and ``["car", "person"]`` both -> "car,person").
    - anything malformed (not a list, non-string/empty entries) -> "unknown" —
      malformed data must group SOMEWHERE visible, never crash or silently
      masquerade as "all".
    """
    if classes is None:
        return SELECTED_CLASSES_KEY_ALL
    if not isinstance(classes, list):
        return SELECTED_CLASSES_KEY_UNKNOWN
    if not classes:
        return SELECTED_CLASSES_KEY_ALL
    labels: set[str] = set()
    for entry in classes:
        if not isinstance(entry, str) or not entry.strip():
            return SELECTED_CLASSES_KEY_UNKNOWN
        labels.add(entry.strip())
    return ",".join(sorted(labels))


def _coerce_warnings(value: Any) -> tuple[str, ...] | None:
    """The warnings codes as a tuple of strings; ``None`` when the key is
    missing or not a list. Non-string entries are dropped (a partially-usable
    list keeps its usable codes), order preserved, duplicates removed."""
    if not isinstance(value, list):
        return None
    seen: list[str] = []
    for entry in value:
        if isinstance(entry, str) and entry and entry not in seen:
            seen.append(entry)
    return tuple(seen)


def has_processing_config_warning(warnings: Any, warning_name: str) -> bool:
    """Whether ``warnings`` (any shape) contains the string code
    ``warning_name``. Defensive: a non-list / malformed value is just False."""
    if not isinstance(warnings, (list, tuple)):
        return False
    return any(entry == warning_name for entry in warnings)


def extract_processing_config_dimensions(snapshot: Any) -> ProcessingConfigDimensions:
    """Extract the M9 dimension set from one perception-snapshot dict.

    Reads ONLY ``snapshot.source.applied_processing_config``; never raises.
    A snapshot without the block (pre-M8, or any malformed shape on the way
    down) yields ``EMPTY_PROCESSING_CONFIG_DIMENSIONS`` — all-``None``, the
    backward-compatible "no config metadata" row shape.

    When the block IS present, the boolean warning dimension is a real
    ``True``/``False`` (not ``None``): "config metadata present, warning
    absent" is meaningful signal, distinct from "no metadata at all".
    """
    if not isinstance(snapshot, Mapping):
        return EMPTY_PROCESSING_CONFIG_DIMENSIONS
    source_block = snapshot.get("source")
    if not isinstance(source_block, Mapping):
        return EMPTY_PROCESSING_CONFIG_DIMENSIONS
    config_block = source_block.get("applied_processing_config")
    if not isinstance(config_block, Mapping):
        return EMPTY_PROCESSING_CONFIG_DIMENSIONS

    applied = config_block.get("applied")
    if isinstance(applied, Mapping):
        applied_model_key = _coerce_text(applied.get("model_key"))
        applied_selected_classes_key = normalize_selected_classes_key(
            applied.get("selected_classes")
        )
        applied_confidence_threshold = _coerce_threshold(applied.get("confidence_threshold"))
    else:
        applied_model_key = None
        applied_selected_classes_key = None
        applied_confidence_threshold = None

    warnings = _coerce_warnings(config_block.get("warnings"))
    return ProcessingConfigDimensions(
        processing_config_source=_coerce_text(config_block.get("source")),
        processing_config_revision=_coerce_revision(config_block.get("revision")),
        processing_config_status=_coerce_text(config_block.get("status")),
        applied_model_key=applied_model_key,
        applied_selected_classes_key=applied_selected_classes_key,
        applied_confidence_threshold=applied_confidence_threshold,
        processing_config_warnings=warnings,
        selected_classes_analytics_counts_not_filtered=has_processing_config_warning(
            warnings, SELECTED_CLASSES_ANALYTICS_WARNING
        ),
    )
