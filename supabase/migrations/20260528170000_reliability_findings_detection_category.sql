-- Reliability M2: add 'detection' to the reliability_findings category check.
--
-- M0 defined the category vocabulary as stream / processor / scene_quality /
-- geometry_config. M2 adds the first "is the model actually producing reliable
-- detections?" rule (unstable_detection_confidence); that belongs to its own
-- category so the dashboard can surface it independently of workflow / scene
-- issues, and so future detection-quality rules slot in without re-categorising.
--
-- Drop + re-add the named check. No data backfill — no row in production today
-- carries 'detection', and the new value only widens the constraint.

alter table reliability_findings
  drop constraint reliability_findings_category_check;

alter table reliability_findings
  add constraint reliability_findings_category_check
  check (category in (
    'stream', 'processor', 'scene_quality', 'geometry_config', 'detection'
  ));
