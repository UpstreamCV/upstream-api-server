"""Model registry — static catalog + lookups.

In-process data only — there is NO DB table behind this. The registry is
the source of truth the per-camera processing config validates against
(see ``schemas/processing_config_camera.py``).

Import direction is one-way: this module is imported by the camera
processing-config schema; it must NOT import that schema (avoids a cycle).

The processor does NOT read this registry yet — it runs a hard-coded
detector. The catalog exists so the dashboard can offer a model picker and
the API can reject configs that reference unknown/disabled models.
"""

from __future__ import annotations

from upstream_api.schemas.models import ModelRegistryEntry

# The catalog. A tuple so the default contents are effectively read-only;
# ``list_models()`` / ``get_model()`` resolve against the module-level
# ``MODEL_REGISTRY`` name, so a test can ``monkeypatch.setattr`` it to a new
# tuple (e.g. one whose ``mock_detect_v0`` entry has ``enabled=False``) to
# exercise the disabled-model rejection path.
MODEL_REGISTRY: tuple[ModelRegistryEntry, ...] = (
    ModelRegistryEntry(
        model_key="trafficcamnet_v1",
        display_name="TrafficCamNet",
        description="NVIDIA TrafficCamNet object detector for road scenes.",
        task_type="object_detection",
        runtime="deepstream",
        default_confidence_threshold=0.4,
        supported_classes=["car", "bicycle", "person", "roadsign"],
        supported_trackers=["iou"],
        model_version=None,
        detector_version=None,
        resource_profile="gpu_light",
        enabled=True,
        notes=(
            "The processor runs this detector today but does not yet read "
            "this registry or per-camera config."
        ),
        limitations=[
            "Fixed to the 4 TrafficCamNet classes.",
            "Can misclassify people as cars in some scenes.",
        ],
    ),
    ModelRegistryEntry(
        model_key="mock_detect_v0",
        display_name="Mock Detector",
        description=None,
        task_type="object_detection",
        runtime="mock",
        default_confidence_threshold=None,
        supported_classes=["person", "car"],
        supported_trackers=["none"],
        model_version=None,
        detector_version=None,
        resource_profile="cpu",
        enabled=True,
        notes="Development/testing detector; outputs are synthetic.",
        limitations=["Not a real detector."],
    ),
)


def list_models() -> list[ModelRegistryEntry]:
    """Every model in the registry, in declaration order."""
    return list(MODEL_REGISTRY)


def get_model(model_key: str) -> ModelRegistryEntry | None:
    """The registry entry for ``model_key``, or ``None`` if unknown.

    Resolves against the module-level ``MODEL_REGISTRY`` by attribute, so a
    test can ``monkeypatch.setattr`` the registry to inject a disabled entry.
    """
    for entry in MODEL_REGISTRY:
        if entry.model_key == model_key:
            return entry
    return None
