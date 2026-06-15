"""Processing package catalog — static data + lookups (P5-F).

In-process data only — NO DB table. This is a **curated API-side snapshot** of
the processor's package registry (``upstream-processor/app/packages/*.json``).
The two repos deploy independently, so the API server must NOT import processor
code or fetch it at runtime; this hand-authored mirror is the simplest safe
option (same posture as ``services/models.py``). A schema-stability test guards
the shape; when the processor packages change, update this file (a future
processor export script could generate it).

Catalog only: this does NOT assign packages to cameras (P5-G).
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from upstream_api.schemas.processing_packages import (
    ProcessingPackage,
    ProcessingPackageCategory,
)

# The 8 people-analytics observation types every analytics package emits.
_ANALYTICS_OUTPUTS = (
    "processor.line_crossing",
    "processor.line_crossing_object",
    "processor.roi_occupancy",
    "processor.zone_entered",
    "processor.zone_exited",
    "processor.dwell_started",
    "processor.dwell_updated",
    "processor.dwell_exceeded",
)
_ANALYTICS_SHAPES = ("polygon_roi", "line_segment")
_ANALYTICS_TRIGGERS = (
    "line_crossing",
    "roi_occupancy",
    "zone_entered",
    "zone_exited",
    "dwell_exceeded",
)
# The processor's genuine processing-config apply seams (M7C/M7D/M7E). Other
# fields (model/tracker) are stamp-only, so they are NOT advertised as
# overridable.
_ANALYTICS_OVERRIDES = ("confidence_threshold", "selected_classes", "processing_fps")

_CATEGORY_NAMES: dict[str, str] = {
    "people_analytics": "People Analytics",
    "diagnostic": "Diagnostic Probes",
    "mock": "Mock",
}

_CATALOG_WARNINGS = (
    "Catalog only: packages cannot yet be assigned to cameras (P5-G adds "
    "requested/applied package state).",
)


def _pkg(
    *,
    package_id: str,
    display_name: str,
    description: str | None,
    category: str,
    status: str,
    runtime: str = "deepstream",
    model_id: str | None = None,
    tracker_profile_id: str | None = None,
    analytics_profile_id: str | None = None,
    supported_classes: Sequence[str] = (),
    supported_outputs: Sequence[str] = (),
    supported_shapes: Sequence[str] = (),
    supported_triggers: Sequence[str] = (),
    default_settings: dict[str, Any] | None = None,
    allowed_overrides: Sequence[str] = (),
    compute_tier: str | None = None,
    pricing_tier: str | None = "free_probe",
    observation_volume_risk: str | None = None,
    warnings: Sequence[str] = (),
    limitations: Sequence[str] = (),
    best_for: Sequence[str] = (),
    not_recommended_for: Sequence[str] = (),
    production_ready: bool = False,
    test_mode_safe: bool = True,
    readiness_notes: str | None = None,
    related_packages: Sequence[str] = (),
) -> ProcessingPackage:
    """Build a ProcessingPackage, deriving ``requires_geometry`` (analytics
    packages need ROI/line geometry) and ``requires_restart`` (DeepStream
    packages take effect on the next worker start)."""
    return ProcessingPackage(
        package_id=package_id,
        display_name=display_name,
        description=description,
        category=category,
        status=status,
        production_ready=production_ready,
        test_mode_safe=test_mode_safe,
        runtime=runtime,
        model_id=model_id,
        tracker_profile_id=tracker_profile_id,
        analytics_profile_id=analytics_profile_id,
        supported_classes=list(supported_classes),
        supported_outputs=list(supported_outputs),
        supported_shapes=list(supported_shapes),
        supported_triggers=list(supported_triggers),
        default_settings=dict(default_settings or {}),
        allowed_overrides=list(allowed_overrides),
        compute_tier=compute_tier,
        pricing_tier=pricing_tier,
        observation_volume_risk=observation_volume_risk,
        warnings=list(warnings),
        limitations=list(limitations),
        best_for=list(best_for),
        not_recommended_for=list(not_recommended_for),
        requires_geometry=analytics_profile_id is not None,
        requires_restart=runtime == "deepstream",
        readiness_notes=readiness_notes,
        related_packages=list(related_packages),
    )


# Curated mirror of upstream-processor/app/packages/package_registry.json.
PACKAGE_CATALOG: tuple[ProcessingPackage, ...] = (
    _pkg(
        package_id="mock.detect.v0",
        display_name="Mock Detection",
        description="Synthetic detector used to prove processor handoff. Not a real model.",
        category="mock",
        status="probe",
        runtime="mock",
        supported_outputs=(
            "processor.mock_frame_seen",
            "processor.mock_heartbeat",
            "processor.mock_detection",
        ),
        compute_tier="low",
        limitations=("Outputs are synthetic; for development/testing only.",),
        best_for=("handoff_testing",),
        not_recommended_for=("any_real_analytics",),
    ),
    _pkg(
        package_id="deepstream.decode_probe.v0",
        display_name="DeepStream Decode Probe",
        description="Minimal RTSP decode probe (no inference). Diagnostic only.",
        category="diagnostic",
        status="probe",
        compute_tier="low",
        limitations=("Decode only; emits no observations.",),
    ),
    _pkg(
        package_id="deepstream.inference_probe.v0",
        display_name="DeepStream Inference Probe",
        description="TrafficCamNet inference probe; detection evidence is status/log-only.",
        category="diagnostic",
        status="probe",
        model_id="trafficcamnet_v1",
        compute_tier="low",
        limitations=("No metadata extraction, no observations.",),
    ),
    _pkg(
        package_id="deepstream.metadata_probe.v0",
        display_name="DeepStream Metadata Probe",
        description="pyds metadata probe (per-frame counters in the status payload).",
        category="diagnostic",
        status="probe",
        model_id="trafficcamnet_v1",
        allowed_overrides=("confidence_threshold", "selected_classes"),
        default_settings={"confidence_threshold": 0.4},
        compute_tier="low",
        limitations=("Status/debug evidence only; no observation writes.",),
    ),
    _pkg(
        package_id="deepstream.tracker_probe.v0",
        display_name="DeepStream Tracker Probe",
        description="Metadata probe with the IOU tracker enabled; tracker-id stability evidence.",
        category="diagnostic",
        status="probe",
        model_id="trafficcamnet_v1",
        tracker_profile_id="deepstream_iou_fast_v1",
        allowed_overrides=("confidence_threshold", "selected_classes"),
        default_settings={"confidence_threshold": 0.4},
        compute_tier="low",
        limitations=("Status/debug evidence only; no observation writes.",),
    ),
    _pkg(
        package_id="deepstream.analytics_probe.v0",
        display_name="People Analytics — TrafficCamNet",
        description=(
            "Full people-analytics chain (TrafficCamNet + IOU tracker + nvdsanalytics): "
            "line/ROI/zone/dwell observations from operator-drawn geometry."
        ),
        category="people_analytics",
        status="probe",
        model_id="trafficcamnet_v1",
        tracker_profile_id="deepstream_iou_fast_v1",
        analytics_profile_id="deepstream_nvdsanalytics_probe_v1",
        supported_outputs=_ANALYTICS_OUTPUTS,
        supported_shapes=_ANALYTICS_SHAPES,
        supported_triggers=_ANALYTICS_TRIGGERS,
        allowed_overrides=_ANALYTICS_OVERRIDES,
        default_settings={"confidence_threshold": 0.4, "target_fps": 30},
        compute_tier="low",
        observation_volume_risk="medium",
        warnings=(
            "TrafficCamNet can misclassify people as cars in non-traffic scenes — "
            "prefer the YOLOX people-analytics package for person analytics.",
        ),
        best_for=("traffic_scenes", "probe_baselines"),
        not_recommended_for=("person_analytics_in_non_traffic_scenes", "production_sla"),
        related_packages=("deepstream.yolox_analytics_probe.v0",),
    ),
    _pkg(
        package_id="deepstream.yolox_probe.v0",
        display_name="YOLOX Metadata Probe",
        description="pyds metadata probe running YOLOX-S (COCO). No tracker, no analytics.",
        category="diagnostic",
        status="probe",
        model_id="yolox_s_v1",
        allowed_overrides=("confidence_threshold", "selected_classes"),
        default_settings={"confidence_threshold": 0.4},
        compute_tier="medium",
        limitations=("Status/debug evidence only; no observation writes.",),
    ),
    _pkg(
        package_id="deepstream.yolox_analytics_probe.v0",
        display_name="People Analytics — YOLOX",
        description=(
            "People-analytics chain with YOLOX-S (reliable person detection) + IOU tracker + "
            "nvdsanalytics. The recommended people-analytics probe today."
        ),
        category="people_analytics",
        status="probe",
        model_id="yolox_s_v1",
        tracker_profile_id="deepstream_iou_fast_v1",
        analytics_profile_id="deepstream_nvdsanalytics_probe_v1",
        supported_classes=("person",),
        supported_outputs=_ANALYTICS_OUTPUTS,
        supported_shapes=_ANALYTICS_SHAPES,
        supported_triggers=_ANALYTICS_TRIGGERS,
        allowed_overrides=_ANALYTICS_OVERRIDES,
        default_settings={"confidence_threshold": 0.4, "target_fps": 30},
        compute_tier="medium",
        observation_volume_risk="medium",
        warnings=(
            "line_crossing / zone_exited / line_crossing_object are not yet live-verified "
            "under the IOU tracker (track fragmentation); see the NvDCF variant.",
        ),
        best_for=("person_entry_exit", "zone_presence", "dwell_monitoring"),
        not_recommended_for=("dense_crowds", "non_person_classes", "production_sla"),
        related_packages=("deepstream.yolox_analytics_nvdcf_probe.v0",),
    ),
    _pkg(
        package_id="deepstream.yolox_analytics_nvdcf_probe.v0",
        display_name="People Analytics — YOLOX + NvDCF (experimental)",
        description=(
            "Same YOLOX-S people-analytics chain as the IOU package, with the NvDCF tracker "
            "in place of IOU — the tracker-expansion A/B counterpart."
        ),
        category="people_analytics",
        status="experimental",
        model_id="yolox_s_v1",
        tracker_profile_id="deepstream_nvdcf_balanced_v1",
        analytics_profile_id="deepstream_nvdsanalytics_probe_v1",
        supported_classes=("person",),
        supported_outputs=_ANALYTICS_OUTPUTS,
        supported_shapes=_ANALYTICS_SHAPES,
        supported_triggers=_ANALYTICS_TRIGGERS,
        allowed_overrides=_ANALYTICS_OVERRIDES,
        default_settings={"confidence_threshold": 0.4, "target_fps": 30},
        compute_tier="high",
        observation_volume_risk="medium",
        warnings=(
            "Experimental. A representative person A/B (P5-F) found NvDCF did NOT reduce track "
            "fragmentation vs the IOU package — prefer the IOU YOLOX analytics package today.",
        ),
        best_for=("occlusion_robustness", "track_continuity"),
        not_recommended_for=("production_sla", "people_analytics_vs_iou_today"),
        readiness_notes=(
            "P5-F representative A/B (45s people clip, YOLOX detector): the reconciled NvDCF "
            "perf config produced ~83% more track IDs than IOU (more fragmentation), so it was "
            "NOT promoted. Remains experimental pending people-specific tuning or a different "
            "tracker; the weak analytics events may be a lifecycle-policy issue, not a tracker one."
        ),
        related_packages=("deepstream.yolox_analytics_probe.v0",),
    ),
)


def _visible(pkg: ProcessingPackage) -> bool:
    """Conservative account-visibility gate. Today: hide only retired packages
    (none are deprecated). Stub seam for future production/account entitlement —
    it never hides a probe/experimental/free package."""
    return pkg.status != "deprecated"


def list_packages(
    *,
    category: str | None = None,
    status: str | None = None,
    runtime: str | None = None,
    test_mode_safe: bool | None = None,
) -> list[ProcessingPackage]:
    """Visible packages, optionally filtered, in declaration order."""
    out: list[ProcessingPackage] = []
    for pkg in PACKAGE_CATALOG:
        if not _visible(pkg):
            continue
        if category is not None and pkg.category != category:
            continue
        if status is not None and pkg.status != status:
            continue
        if runtime is not None and pkg.runtime != runtime:
            continue
        if test_mode_safe is not None and pkg.test_mode_safe != test_mode_safe:
            continue
        out.append(pkg)
    return out


def get_package(package_id: str) -> ProcessingPackage | None:
    """The visible package for ``package_id``, or ``None`` if unknown/hidden."""
    for pkg in PACKAGE_CATALOG:
        if pkg.package_id == package_id and _visible(pkg):
            return pkg
    return None


def categories(packages: Sequence[ProcessingPackage]) -> list[ProcessingPackageCategory]:
    """Distinct categories present in ``packages``, in first-seen order."""
    seen: list[str] = []
    for pkg in packages:
        if pkg.category not in seen:
            seen.append(pkg.category)
    return [
        ProcessingPackageCategory(id=cat, display_name=_CATEGORY_NAMES.get(cat, cat))
        for cat in seen
    ]


def catalog_warnings() -> list[str]:
    return list(_CATALOG_WARNINGS)
