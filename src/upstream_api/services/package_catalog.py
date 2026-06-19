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
    "vehicle_analytics": "Vehicle Analytics",
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
    license_class: str = "permissive",
    first_party: bool = True,
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
    packages take effect on the next worker start). ``license_class`` /
    ``first_party`` mirror the backing model's P5-K provenance (default:
    permissive + first-party)."""
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
        license_class=license_class,
        first_party=first_party,
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
        license_class="commercial_agreement",
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
    # --- P5-K: YOLOX-S + IOU people variants (reuse the verified pipeline) ---
    _pkg(
        package_id="deepstream.yolox_people_fast_probe.v0",
        display_name="People Analytics — YOLOX (Fast)",
        description=(
            "YOLOX-S people analytics tuned for low compute / many cameras: higher "
            "confidence (0.5), 15 fps. Same verified IOU pipeline; only the defaults differ."
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
        default_settings={"confidence_threshold": 0.5, "target_fps": 15},
        compute_tier="low",
        observation_volume_risk="low",
        warnings=(
            "A lower frame rate can increase IOU track fragmentation for fast-moving people.",
        ),
        best_for=("low_compute", "many_cameras"),
        not_recommended_for=("fast_movers", "dense_crowds", "production_sla"),
        related_packages=("deepstream.yolox_analytics_probe.v0",),
    ),
    _pkg(
        package_id="deepstream.yolox_people_high_recall_probe.v0",
        display_name="People Analytics — YOLOX (High Recall)",
        description=(
            "YOLOX-S people analytics tuned to catch more detections (confidence 0.25) "
            "for crowded / don't-miss scenes. Same verified IOU pipeline; higher volume."
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
        default_settings={"confidence_threshold": 0.25, "target_fps": 30},
        compute_tier="high",
        observation_volume_risk="high",
        warnings=(
            "Lower confidence threshold increases false positives and observation "
            "volume; not separately live-tuned.",
        ),
        best_for=("crowded_scenes", "high_recall"),
        not_recommended_for=("low_compute", "observation_volume_sensitive", "production_sla"),
        related_packages=("deepstream.yolox_analytics_probe.v0",),
    ),
    _pkg(
        package_id="deepstream.yolox_people_line_crossing_probe.v0",
        display_name="People Line Crossing (experimental)",
        description=(
            "Directional people counting across a line: YOLOX-S + IOU + nvdsanalytics "
            "narrowed to line-crossing. Draw a line (no ROI needed)."
        ),
        category="people_analytics",
        status="experimental",
        model_id="yolox_s_v1",
        tracker_profile_id="deepstream_iou_fast_v1",
        analytics_profile_id="deepstream_nvdsanalytics_probe_v1",
        supported_classes=("person",),
        supported_outputs=("processor.line_crossing", "processor.line_crossing_object"),
        supported_shapes=("line_segment",),
        supported_triggers=("line_crossing",),
        allowed_overrides=_ANALYTICS_OVERRIDES,
        default_settings={"confidence_threshold": 0.4, "target_fps": 30},
        compute_tier="medium",
        observation_volume_risk="medium",
        warnings=(
            "Experimental — line-crossing emission via the IOU tracker is not yet "
            "live-confirmed (suspected track fragmentation during walking motion).",
        ),
        best_for=("line_crossing", "directional_counting"),
        not_recommended_for=("production_sla",),
        related_packages=("deepstream.yolox_analytics_probe.v0",),
    ),
    _pkg(
        package_id="deepstream.yolox_people_occupancy_probe.v0",
        display_name="People Occupancy",
        description=(
            "People occupancy / capacity / presence in a polygon ROI: YOLOX-S + IOU + "
            "nvdsanalytics ROI occupancy. Draw an ROI."
        ),
        category="people_analytics",
        status="probe",
        model_id="yolox_s_v1",
        tracker_profile_id="deepstream_iou_fast_v1",
        analytics_profile_id="deepstream_nvdsanalytics_probe_v1",
        supported_classes=("person",),
        supported_outputs=(
            "processor.roi_occupancy",
            "processor.zone_entered",
            "processor.zone_exited",
        ),
        supported_shapes=("polygon_roi",),
        supported_triggers=("roi_occupancy",),
        allowed_overrides=_ANALYTICS_OVERRIDES,
        default_settings={"confidence_threshold": 0.4, "target_fps": 30},
        compute_tier="medium",
        observation_volume_risk="medium",
        warnings=(
            "zone_exited is inferred when a track disappears (grace window), not a true "
            "geometric exit.",
        ),
        best_for=("occupancy", "capacity", "presence"),
        not_recommended_for=("dense_crowds", "production_sla"),
        related_packages=("deepstream.yolox_analytics_probe.v0",),
    ),
    _pkg(
        package_id="deepstream.yolox_people_dwell_probe.v0",
        display_name="People Dwell / Loitering",
        description=(
            "People dwell / loitering monitoring in a polygon ROI: YOLOX-S + IOU + "
            "nvdsanalytics dwell events. Draw an ROI."
        ),
        category="people_analytics",
        status="probe",
        model_id="yolox_s_v1",
        tracker_profile_id="deepstream_iou_fast_v1",
        analytics_profile_id="deepstream_nvdsanalytics_probe_v1",
        supported_classes=("person",),
        supported_outputs=(
            "processor.dwell_started",
            "processor.dwell_updated",
            "processor.dwell_exceeded",
            "processor.zone_entered",
            "processor.zone_exited",
        ),
        supported_shapes=("polygon_roi",),
        supported_triggers=("dwell_exceeded",),
        allowed_overrides=_ANALYTICS_OVERRIDES,
        default_settings={"confidence_threshold": 0.4, "target_fps": 30},
        compute_tier="medium",
        observation_volume_risk="medium",
        best_for=("dwell", "dwell_monitoring", "loitering"),
        not_recommended_for=("dense_crowds", "production_sla"),
        related_packages=("deepstream.yolox_analytics_probe.v0",),
    ),
    # --- P5-K: TrafficCamNet vehicle (new vehicle_analytics category) --------
    _pkg(
        package_id="deepstream.trafficcamnet_vehicle_probe.v0",
        display_name="Vehicle / Traffic Analytics (experimental)",
        description=(
            "Vehicle counting / occupancy via TrafficCamNet (car/bicycle) + IOU + "
            "nvdsanalytics. Experimental — vehicle detection is not yet live-verified."
        ),
        category="vehicle_analytics",
        status="experimental",
        model_id="trafficcamnet_v1",
        tracker_profile_id="deepstream_iou_fast_v1",
        analytics_profile_id="deepstream_nvdsanalytics_probe_v1",
        supported_classes=("car", "bicycle"),
        supported_outputs=(
            "processor.line_crossing",
            "processor.line_crossing_object",
            "processor.roi_occupancy",
            "processor.zone_entered",
            "processor.zone_exited",
        ),
        supported_shapes=_ANALYTICS_SHAPES,
        supported_triggers=("roi_occupancy", "line_crossing"),
        allowed_overrides=_ANALYTICS_OVERRIDES,
        default_settings={"confidence_threshold": 0.4, "target_fps": 30},
        compute_tier="low",
        observation_volume_risk="medium",
        license_class="commercial_agreement",
        warnings=("Experimental — vehicle detection is not yet live-verified in Upstream.",),
        best_for=("vehicle_counting", "traffic_scenes", "parking_occupancy"),
        not_recommended_for=("person_analytics", "production_sla"),
        readiness_notes=(
            "TrafficCamNet is the only currently-bundled traffic detector (NVIDIA "
            "TAO/NGC EULA — bundled, never redistributed; review before 'supported')."
        ),
    ),
)


# P5-K: the shared shape of a per-new-model "People Analytics — <model>" package.
# Each new model is experimental (no GPU verification this milestone); Ultralytics
# models are restrictive / BYOM (first_party=False) — see the licensing policy.
def _model_people_packages() -> tuple[ProcessingPackage, ...]:
    byom_warning = (
        "BYOM / requires your own Ultralytics license — not auto-provided as a "
        "first-party package in production until legal approval. Detection parser "
        "authored but not yet GPU-validated."
    )
    prep_warning = (
        "Experimental — this model is prepared in-repo but not yet live-verified on "
        "the GPU host (promote to probe after a verification run)."
    )

    def _mp(
        *,
        package_id: str,
        display_name: str,
        model_id: str,
        compute_tier: str,
        license_class: str = "permissive",
        first_party: bool = True,
        warnings: tuple[str, ...] = (),
    ) -> ProcessingPackage:
        return _pkg(
            package_id=package_id,
            display_name=display_name,
            description=(
                f"People analytics on {display_name.split('—')[-1].strip()} + IOU + "
                "nvdsanalytics. P5-K: this model is experimental (prepared in-repo; "
                "not yet live-verified)."
            ),
            category="people_analytics",
            status="experimental",
            model_id=model_id,
            tracker_profile_id="deepstream_iou_fast_v1",
            analytics_profile_id="deepstream_nvdsanalytics_probe_v1",
            supported_classes=("person",),
            supported_outputs=_ANALYTICS_OUTPUTS,
            supported_shapes=_ANALYTICS_SHAPES,
            supported_triggers=_ANALYTICS_TRIGGERS,
            allowed_overrides=_ANALYTICS_OVERRIDES,
            default_settings={"confidence_threshold": 0.4, "target_fps": 30},
            compute_tier=compute_tier,
            observation_volume_risk="medium",
            license_class=license_class,
            first_party=first_party,
            warnings=warnings or (prep_warning,),
            best_for=("person_entry_exit", "zone_presence", "dwell_monitoring"),
            not_recommended_for=("production_sla",),
            related_packages=("deepstream.yolox_analytics_probe.v0",),
        )

    return (
        _mp(
            package_id="deepstream.yolox_nano_people_probe.v0",
            display_name="People Analytics — YOLOX-Nano",
            model_id="yolox_nano_v1",
            compute_tier="low",
        ),
        _mp(
            package_id="deepstream.yolox_tiny_people_probe.v0",
            display_name="People Analytics — YOLOX-Tiny",
            model_id="yolox_tiny_v1",
            compute_tier="low",
        ),
        _mp(
            package_id="deepstream.yolox_m_people_probe.v0",
            display_name="People Analytics — YOLOX-M",
            model_id="yolox_m_v1",
            compute_tier="high",
        ),
        _mp(
            package_id="deepstream.yolo11n_people_probe.v0",
            display_name="People Analytics — YOLO11-N",
            model_id="yolo11n_v1",
            compute_tier="low",
            license_class="restrictive",
            first_party=False,
            warnings=(byom_warning,),
        ),
        _mp(
            package_id="deepstream.yolo11s_people_probe.v0",
            display_name="People Analytics — YOLO11-S",
            model_id="yolo11s_v1",
            compute_tier="medium",
            license_class="restrictive",
            first_party=False,
            warnings=(byom_warning,),
        ),
        _mp(
            package_id="deepstream.yolo11m_people_probe.v0",
            display_name="People Analytics — YOLO11-M",
            model_id="yolo11m_v1",
            compute_tier="high",
            license_class="restrictive",
            first_party=False,
            warnings=(byom_warning,),
        ),
        _mp(
            package_id="deepstream.yolov8n_people_probe.v0",
            display_name="People Analytics — YOLOv8-N",
            model_id="yolov8n_v1",
            compute_tier="low",
            license_class="restrictive",
            first_party=False,
            warnings=(byom_warning,),
        ),
        _mp(
            package_id="deepstream.peoplenet_people_probe.v0",
            display_name="People Analytics — PeopleNet",
            model_id="peoplenet_v1",
            compute_tier="low",
            license_class="commercial_agreement",
            warnings=(
                "Experimental — NVIDIA PeopleNet (NGC model EULA); prepared in-repo, "
                "not yet live-verified.",
            ),
        ),
    )


# Appended after the helper is defined; the per-model packages reuse the
# module-level _pkg/_ANALYTICS_* and round out the catalog.
PACKAGE_CATALOG = PACKAGE_CATALOG + _model_people_packages()


def _visible(pkg: ProcessingPackage, *, first_party_only: bool = False) -> bool:
    """Account/catalog-visibility gate. Always hides retired (deprecated)
    packages. When ``first_party_only`` is set (the production go-live switch,
    ``catalog_first_party_only``), it also hides non-first-party packages —
    AGPL/restrictive models Upstream may not auto-provide until legal approval
    (P5-K). Default false: dev/demo see everything (with a BYOM badge)."""
    if pkg.status == "deprecated":
        return False
    if first_party_only:
        return pkg.first_party
    return True


def list_packages(
    *,
    category: str | None = None,
    status: str | None = None,
    runtime: str | None = None,
    test_mode_safe: bool | None = None,
    first_party_only: bool = False,
) -> list[ProcessingPackage]:
    """Visible packages, optionally filtered, in declaration order."""
    out: list[ProcessingPackage] = []
    for pkg in PACKAGE_CATALOG:
        if not _visible(pkg, first_party_only=first_party_only):
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


def get_package(package_id: str, *, first_party_only: bool = False) -> ProcessingPackage | None:
    """The visible package for ``package_id``, or ``None`` if unknown/hidden.

    ``first_party_only`` mirrors the catalog gate for the detail endpoint;
    internal validation callers (e.g. camera package requests) omit it so an
    explicitly-chosen BYOM package still resolves."""
    for pkg in PACKAGE_CATALOG:
        if pkg.package_id == package_id and _visible(pkg, first_party_only=first_party_only):
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
