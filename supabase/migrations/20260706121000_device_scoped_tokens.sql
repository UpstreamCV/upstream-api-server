-- Device-scoped API tokens: bind a token to a single agent.
--
-- The existing 'agent' scope is account-wide: any token can act as any agent
-- in its account. For appliance fleets that means one stolen SD card
-- compromises every device on the account. A 'device'-scoped token instead
-- binds to exactly one agents row and may only act as that agent.
--
-- Binding is trust-on-first-use: the token is minted with agent_id NULL
-- (the agents row doesn't exist until the device first registers); the
-- first POST /v1/agents/register locks the token to the upserted agent,
-- and every later agent-authed write path enforces the binding.
--
-- No ON DELETE action (default NO ACTION / restrict): deleting an agent
-- must not silently unbind its token back to account-wide power — revoke
-- the token first, then delete the agent.

alter table api_tokens
  add column agent_id uuid references agents(id);

comment on column api_tokens.agent_id is
  'For device-scoped tokens: the single agent this token may act as. NULL '
  'on account-scoped (agent) tokens, and on device tokens that have not '
  'yet completed their trust-on-first-use bind via /v1/agents/register.';

-- Revocation runbook / fleet views look tokens up by their bound agent.
create index api_tokens_agent_idx
  on api_tokens(agent_id)
  where agent_id is not null;

-- One ACTIVE bound token per agent, enforced at the DB. This is the
-- anti-hijack rule for trust-on-first-use: an unbound device token cannot
-- adopt an agent that already has a live device token (registering under
-- another device's name). The legit token-rotation flow is unaffected —
-- it revokes the old token first, which drops it out of this partial
-- index. Two concurrent first-registers racing to bind the same agent
-- resolve here too: one wins, the loser's UPDATE raises unique_violation
-- and its register is rejected.
create unique index api_tokens_one_active_per_agent_idx
  on api_tokens(agent_id)
  where agent_id is not null and revoked_at is null;
