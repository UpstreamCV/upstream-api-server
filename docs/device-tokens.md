# Device-scoped API tokens (appliance fleets)

Added 2026-07-06 (CrowdCount migration Phase 0). One token per physical
device, bound to a single agent, individually revocable.

## Why

The original `agent` scope is account-wide: any token can act as any agent
in its account. For an appliance fleet — N SD cards sitting at customer
venues — that means one compromised card compromises the whole fleet, with
no per-device revocation. A `device`-scoped token instead locks itself to
exactly one `agents` row and may only act as that agent.

## Semantics

- **Minting.** `POST /v1/api-tokens` with `scopes: ["device"]` (owner/admin
  dashboard user), or the fleet CLI `scripts/mint_device_tokens.py`
  (direct-DB, prints `name<TAB>plaintext` once per device — pipe into image
  customization). Convention: token `name` = the device's stable identifier
  (e.g. a CrowdCount device id like `T1234`).
- **Trust-on-first-use bind.** The token is minted unbound
  (`api_tokens.agent_id IS NULL` — the agents row doesn't exist until the
  device first boots). The first `POST /v1/agents/register` through the
  token locks it to the upserted agent, permanently, and writes an
  `api_token.bound` audit event. The device's agent `name` must therefore
  be stable across boots (it is: firmware derives it from the device id).
- **Anti-hijack rule.** At most one ACTIVE bound token per agent (partial
  unique index `api_tokens_one_active_per_agent_idx`): an unbound token
  cannot adopt an agent that already has a live device token — registering
  under another device's name gets `403 device_token_agent_conflict`.
  Residual TOFU caveat: an agent with NO active bound token (e.g. one
  registered via an account-scoped token pre-migration) is adoptable by
  any unbound device token on the account; bind fleet agents promptly and
  watch `api_token.bound` audit events during rollout.
- **Enforcement.** After binding, every agent-authed write path checks the
  binding: agent re-register, camera register/claim, session start/stop,
  heartbeats, snapshot upload. Mismatch → `403 device_token_agent_mismatch`.
  An unbound device token can only register (and read bootstrap) —
  everything else → `403 device_token_unbound`.
- **Bootstrap scoping.** A bound device token's `GET /v1/agents/bootstrap`
  returns only its own agent's cameras/sessions, not the whole fleet's.
- **Revocation.** Per-device: `DELETE /v1/api-tokens/{id}` (find the id by
  token name / bound agent in `GET /v1/api-tokens`), or
  `scripts/mint_device_tokens.py --revoke <device-name>`. Auth rejects
  revoked tokens on the next request; nothing else in the fleet is
  affected.
- **Deleting an agent** with a bound token is blocked by the FK (NO
  ACTION) — revoke the token first. Deliberate: agent deletion must never
  silently unbind a token back to account-wide power.

## Fleet runbook (new device)

1. Mint: `uv run python scripts/mint_device_tokens.py --account-slug <fleet> T1234`
2. Write the plaintext into the device's identity store at image time
   (CrowdCount: `device-identity.conf` on the boot-FAT, next to
   `API_PASSWORD`).
3. First boot: firmware calls `/v1/agents/register` → token binds.
4. Verify: `GET /v1/api-tokens` shows the token with `agent_id` set.

## Fleet runbook (compromised / retired device)

1. `scripts/mint_device_tokens.py --account-slug <fleet> --revoke T1234`
   (or dashboard → API tokens → revoke).
2. Device's next API call fails `401 token_revoked`; the rest of the fleet
   is untouched.
3. If the device returns to service: mint a fresh token, deliver it to the
   device's identity store (for deployed CrowdCount units: over the
   Tailscale ops channel), reboot. The fresh token re-binds to the same
   agent on register (name is unchanged).
