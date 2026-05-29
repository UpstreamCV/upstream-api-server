# Troubleshooting

Symptoms grouped by where you see them. If a symptom isn't here, add it (with the fix) in the same commit that figures it out.

## Server boot

### `pydantic_settings.exceptions.SettingsError: error parsing value for field "allowed_origins"`
Your `UPSTREAM_ALLOWED_ORIGINS` env var was malformed. Use plain CSV — not JSON:
```
UPSTREAM_ALLOWED_ORIGINS=http://localhost:3000,https://app.example.com
```
The field is parsed as a `str` in env and exposed as `list[str]` via a property; pydantic-settings does not JSON-decode it.

### `ValidationError: 1 validation error … database_url field required`
One of the required `UPSTREAM_*` env vars is missing. Copy `.env.example` to `.env` and fill in values. For local dev:
```
supabase status -o env
```
gives you `DB_URL`, `JWT_SECRET`, `ANON_KEY`, `SERVICE_ROLE_KEY`. Map them to the `UPSTREAM_*` names.

### `ConnectionRefusedError: [Errno 61] Connect call failed ('127.0.0.1', 54322)`
The local Supabase stack isn't running. Run:
```
supabase start
```
(requires Docker.) Note this only happens when the lifespan opens the DB pool — unit tests bypass the lifespan and don't need a DB.

### `/readyz` returns 503 in production
The DB pool can't reach Postgres. Check `UPSTREAM_DATABASE_URL`, network/firewall rules, and that the Supabase project isn't paused. The readiness probe uses a 500ms timeout.

## Migrations

### `Skipping migration .gitkeep…`
Harmless. The Supabase CLI logs this when it encounters `migrations/.gitkeep`. It only runs files matching `<timestamp>_<name>.sql`.

### `NOTICE (42710): extension "pgcrypto" already exists, skipping`
Harmless. Our `create extension if not exists` runs on every migration; on a hosted project that already has it, Postgres just notes it.

### `permission denied for schema auth`
You're applying RLS policies that reference `auth.uid()` against a database without the Supabase `auth` schema. This only works on Supabase. If you need to run migrations against a vanilla Postgres for a test, drop the `auth.uid()`-referencing policies or stub `auth.uid()` as a function returning NULL.

### A migration won't apply on the hosted DB
- Confirm the filename matches `<timestamp>_<name>.sql`. The Supabase GitHub integration skips other names.
- Check the integration's Action logs in GitHub for the actual psql error.
- For ordering issues, remember files are applied in lexical order; new files must have a timestamp greater than the last applied one (`select max(version) from supabase_migrations.schema_migrations`).

## Agent calls

### `401 invalid_token` from `current_agent`
- The token plaintext doesn't match `ups_<env>_<id>_<secret>` (four underscore-separated segments).
- The prefix isn't in the `api_tokens` table.
- The argon2 hash check failed (wrong secret).

To inspect a plaintext token:
```
python -c "from upstream_api.security.api_tokens import split_token; print(split_token('$TOKEN'))"
```
To inspect the DB:
```
docker exec supabase_db_upstream-api-server psql -U postgres -c "select prefix, last_four, revoked_at, expires_at from api_tokens;"
```

### `401 token_revoked` / `401 token_expired`
`revoked_at` is set or `expires_at` is past. Issue a new token via `POST /v1/api-tokens` (dashboard auth) or via `scripts/seed_dev_token.py` locally.

### `403 limit_exceeded.max_cameras`
The account hit `account_limits.max_cameras`. Either disable an unused camera (`update cameras set status='disabled' where …`) or raise the limit (`update account_limits set max_cameras = N where account_id = …`).

### `409 camera_already_streaming`
There's an active session (`state in ('approved','streaming')`) for that camera. Stop it via `POST /v1/stream-sessions/{id}/stop` or let `expires_at` pass (and re-implement the sweeper).

### `404 camera_not_found` for a camera that should exist
The `current_agent` dep binds `account_id` from the API token. The camera's `account_id` must match. Common cause: testing with two different accounts' tokens.

## Behavior that's correct but surprised someone

### `record_event` silently swallows missing-table errors
Until Phase 5 ships the `usage_events` table, every call to `record_event` catches `asyncpg.UndefinedTableError`. The insert is wrapped in `async with conn.transaction()` (a SAVEPOINT), so the swallow rolls back only the savepoint, not the parent transaction. There's a `_logger.debug("usage_events_table_missing")` you can watch for.

If you migrate the table and the swallow stays, delete the try/except — leaving it would hide real future errors.

### `POST /v1/stream-sessions/start` returns 200 but no row exists
This was a bug, now fixed. If it returns, the parent transaction is aborting silently — most likely because some code in the same transaction caught an asyncpg error without a SAVEPOINT and left the transaction in an aborted state. Search for bare `except` blocks inside `async with conn.transaction():`.

### Two tokens for the same account share a prefix
Should be impossible. The token format is `ups_<env>_<8 random base32>_<22 random base32>` and the DB has a unique constraint on `prefix`. If you see a collision, `split_token` or `issue_token` has regressed; see `tests/test_api_tokens.py::test_issue_token_returns_unique_plaintext_and_prefix`.

## Local stack

### `supabase start` hangs or fails pulling images
Docker isn't running, or a previous start left containers in a half-state. Try:
```
supabase stop
docker ps -a | grep supabase | awk '{print $1}' | xargs -r docker rm -f
supabase start
```

### Port 4000 already in use
A previous `uvicorn` is still running.
```
lsof -i :4000 -P -n | grep LISTEN | awk '{print $2}' | xargs kill
```

### Tests fail with the same `ConnectionRefusedError` as the server
A test is using `with TestClient(app) as client:` which triggers the lifespan and opens the DB pool. Switch to `client = TestClient(app)` (no context manager) — the lifespan won't run and `/healthz` still responds.

## Realtime (dashboard push)

### A Realtime channel is `SUBSCRIBED` but no `postgres_changes` events arrive (the dashboard overlay "works" via polling but never via push)
This was a multi-day debugging arc; it has several distinct causes that present almost identically. The API reading the table fine tells you **nothing** — the API uses the service role (RLS-exempt), while Realtime evaluates as the `authenticated` role. See `architecture.md` → "Supabase Realtime for dashboard push" for the full requirements. Work the decision tree in order:

1. **Per-record `Error 401: Unauthorized` in the payload (and `keys=[]`)** → the Realtime *socket* isn't presenting a valid user JWT. The browser must call `realtime.setAuth(access_token)` **before** `.subscribe()`, and the token must not be expired (a fresh login mints a new one). Client-side fix in the dashboard.
2. **No 401, but no events at all** → almost always the table is **missing `GRANT SELECT ... TO authenticated`**. Postgres rejects with `42501` *before* RLS runs, so Realtime delivers nothing silently. Check + fix:
   ```sql
   select has_table_privilege('authenticated','public.<table>','select');  -- expect true
   ```
   If false, `grant select on public.<table> to authenticated;` (and follow the rule in architecture.md so new tables get it at creation).
3. **Grant is present, still no events** → check the table is actually published and the RLS policy resolves:
   ```sql
   select * from pg_publication_tables where pubname='supabase_realtime' and tablename='<table>';
   select policyname, cmd, qual from pg_policies where tablename='<table>';
   ```
   A member-select policy that subqueries `account_members` only works if `account_members` is itself granted + readable by `authenticated` (it is here, via `account_members_self_select`).
4. **`UPDATE` events specifically don't arrive (INSERTs do), or a `camera_id` filter drops everything** → the table needs `alter table <table> replica identity full;`. With default identity the old tuple is PK-only, so UPDATE events are dropped / a non-PK `filter:` can't match. Our snapshot/session tables are UPSERTed → all UPDATEs, so this is required.
