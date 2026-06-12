"""Agent version policy — static, env-configured facts (update M2).

The server owns WHAT versions exist (latest / minimum supported / where
to read more); the agent owns the comparison and its consequences (see
``upstream-agent/upstream_agent/update_check.py``). Mirrors the
``services/plans.py`` / ``services/models.py`` static-catalog pattern:
no DB table — the policy changes by redeploying env config.

Returns ``None`` (field omitted from bootstrap) unless at least one of
latest/minimum is configured, so deployments without a policy are
byte-identical to before.
"""

from __future__ import annotations

from upstream_api.config import Settings
from upstream_api.schemas.bootstrap import AgentVersionPolicy


def build_agent_version_policy(settings: Settings) -> AgentVersionPolicy | None:
    if not settings.agent_latest_version and not settings.agent_min_supported_version:
        return None
    return AgentVersionPolicy(
        latest_version=settings.agent_latest_version,
        minimum_supported_version=settings.agent_min_supported_version,
        severity_hint=settings.agent_update_severity_hint,
        release_notes_url=settings.agent_release_notes_url,
        install_hint=settings.agent_install_hint,
    )
