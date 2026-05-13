from __future__ import annotations

from uuid import UUID, uuid4

import pytest

from upstream_api.errors import ForbiddenError
from upstream_api.security.deps import AgentAuth, CallerAuth, UserAuth


def _user(memberships: dict[UUID, str]) -> UserAuth:
    return UserAuth(
        user_id=uuid4(),
        memberships=memberships,
        aal="aal1",
        amr=(),
    )


def _agent() -> AgentAuth:
    return AgentAuth(account_id=uuid4(), token_id=uuid4(), scopes=("agent",))


def test_agent_resolve_ignores_hint() -> None:
    agent = _agent()
    caller = CallerAuth(agent=agent, user=None)
    assert caller.resolve_account(None) == agent.account_id
    # Hint is ignored for agents because their token is single-account-scoped.
    assert caller.resolve_account(uuid4()) == agent.account_id


def test_user_with_single_membership_resolves_implicitly() -> None:
    acc = uuid4()
    caller = CallerAuth(agent=None, user=_user({acc: "owner"}))
    assert caller.resolve_account(None) == acc


def test_user_with_multiple_memberships_requires_hint() -> None:
    caller = CallerAuth(agent=None, user=_user({uuid4(): "owner", uuid4(): "viewer"}))
    with pytest.raises(ForbiddenError) as exc:
        caller.resolve_account(None)
    assert exc.value.code == "account_id_required"


def test_user_passing_unknown_account_is_rejected() -> None:
    caller = CallerAuth(agent=None, user=_user({uuid4(): "owner"}))
    with pytest.raises(ForbiddenError) as exc:
        caller.resolve_account(uuid4())
    assert exc.value.code == "forbidden_account"


def test_user_passing_owned_account_is_accepted() -> None:
    acc = uuid4()
    caller = CallerAuth(agent=None, user=_user({acc: "owner", uuid4(): "viewer"}))
    assert caller.resolve_account(acc) == acc
