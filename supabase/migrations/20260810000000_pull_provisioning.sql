-- Pull-based MediaMTX path provisioning.
--
-- Motivation (incident 2026-08-05 → 2026-08-09): session start pushed a
-- provisioning call from the api-server INTO the streaming box
-- (`mediamtx_api_base_url`, tcp/19997). That inbound path depends on three
-- things that all broke at once when the box's WAN IP lease rolled: a
-- current `ingest.upstreamcv.com` A record, a router port-forward, and a
-- stable WAN IP. Every session start failed with
-- `mediamtx_provision_failed / reason=transport status=None` for four days.
--
-- The box already polls the api-server OUTBOUND for eligible sessions. This
-- migration is the schema half of moving provisioning onto that same
-- outbound poll, so the api-server never needs to reach into the network.
--
-- Deliberately NOT a new `stream_session_state` enum value: Postgres cannot
-- use a value added by `ALTER TYPE ... ADD VALUE` in the same transaction,
-- which makes it awkward inside a migration that also references it. A
-- nullable timestamp carries the same information, is backwards compatible
-- with every existing row, and needs no enum surgery.
--
--   provisioned_at IS NULL  + state='approved'  -> awaiting provisioning
--   provisioned_at IS NOT NULL                  -> path exists on the box
--
-- Rows created before this migration are backfilled to `created_at` so they
-- are never re-offered to a provisioner as pending work.

alter table stream_sessions
  add column if not exists provisioned_at timestamptz;

comment on column stream_sessions.provisioned_at is
  'When the streaming box confirmed the MediaMTX path exists. NULL on an '
  'approved row means provisioning is still pending. Set by '
  'POST /v1/internal/processor/pending-provisions/{id}/ack. Always NULL in '
  'push mode (the api-server provisions inline and never records an ack).';

-- Backfill: everything that already existed predates pull mode and must not
-- surface as pending work. Uses created_at rather than now() so the value
-- stays historically meaningful.
update stream_sessions
   set provisioned_at = created_at
 where provisioned_at is null;

-- The pending-provisions poll runs every ~1s per box, so it must never scan.
-- Partial index keeps it to the handful of genuinely-pending rows.
create index if not exists stream_sessions_pending_provision_idx
    on stream_sessions (created_at)
 where provisioned_at is null and state = 'approved';
