-- Shared-inference execution mode: per-account entitlement gate.
--
-- execution_mode (per_session | shared_inference) is a per-camera processing-
-- config field stored in camera_processing_configs.config (JSONB).
-- shared_inference is the mode where a camera's counting worker draws
-- detections from a shared per-GPU inference sidecar instead of its own
-- in-process model — mandatory at the ~100-counter fleet scale, but a platform
-- capability not every account should self-select.
--
-- This boolean is the authorization: a processing-config write may only set
-- execution_mode = 'shared_inference' when the owning account has the flag set.
-- It mirrors the session_ttl_seconds gate pattern — read at write time and
-- enforced in the router (the pydantic validators have no DB access). Default
-- false, so every existing account keeps per_session only until deliberately
-- enabled.

alter table account_limits
  add column shared_inference_enabled boolean not null default false;

comment on column account_limits.shared_inference_enabled is
  'Per-account entitlement for execution_mode = shared_inference on camera '
  'processing configs. false = per_session only (per-worker in-process '
  'models). Set true for fleet accounts running the shared per-GPU inference '
  'sidecar (e.g. the CrowdCount counter fleet).';
