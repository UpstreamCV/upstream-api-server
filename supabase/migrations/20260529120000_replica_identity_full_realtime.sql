-- REPLICA IDENTITY FULL for Realtime-published, RLS-protected tables.
--
-- Supabase Realtime will NOT deliver UPDATE/DELETE `postgres_changes` events for
-- an RLS-protected table unless the table is REPLICA IDENTITY FULL. To authorize
-- a row change it evaluates the table's RLS policy against the record, but with
-- the default replica identity the WAL only carries the PRIMARY KEY for the old
-- tuple — so a policy that references other columns (here `account_id`, used by
-- `latest_perception_snapshots_member_select` / `stream_sessions_member_select`)
-- cannot be evaluated and the event is silently dropped.
--
-- `latest_perception_snapshots` is UPSERTed (one row per camera, ON CONFLICT
-- (camera_id)), so after the first INSERT every write is an UPDATE — which means
-- the live detection overlay's subscription was SUBSCRIBED but received zero
-- events ("Fallback polling" forever). `stream_sessions` has the same shape: the
-- camera-detail Realtime listener depends on its state-transition UPDATEs
-- (approved -> streaming -> ended), which were being dropped the same way.
--
-- REPLICA IDENTITY FULL logs the entire old row to the WAL so RLS can be
-- evaluated on UPDATE/DELETE. Extra WAL volume is negligible for these
-- one-row-per-camera / per-session tables. Idempotent: re-running is a no-op.

alter table latest_perception_snapshots replica identity full;
alter table stream_sessions replica identity full;
