from __future__ import annotations

import pytest

from upstream_api.security.api_tokens import (
    issue_token,
    split_token,
    verify_secret,
)


def test_issue_token_returns_unique_plaintext_and_prefix() -> None:
    a = issue_token()
    b = issue_token()
    assert a.plaintext != b.plaintext
    assert a.hash != b.hash
    # CRITICAL: prefixes must differ so the DB unique constraint is respected.
    assert a.prefix != b.prefix
    assert a.prefix.startswith("ups_live_")
    assert len(a.prefix.split("_")) == 3
    assert len(a.last_four) == 4


def test_split_token_recovers_prefix_and_secret() -> None:
    issued = issue_token(environment="test")
    prefix, secret = split_token(issued.plaintext)
    assert prefix == issued.prefix
    assert prefix.startswith("ups_test_")
    assert issued.plaintext == f"{prefix}_{secret}"


@pytest.mark.parametrize(
    "bad",
    [
        "",
        "abc",
        "ups_",
        "ups_live",
        "ups_live_",
        "ups_live_abc",  # only 3 segments — missing secret
        "ups__abc_xyz",  # empty env segment
        "_live_abc_xyz",  # empty leading segment
    ],
)
def test_split_token_rejects_invalid_shapes(bad: str) -> None:
    with pytest.raises(ValueError):
        split_token(bad)


def test_verify_secret_round_trip() -> None:
    issued = issue_token()
    _, secret = split_token(issued.plaintext)
    assert verify_secret(secret, issued.hash) is True
    assert verify_secret(secret + "x", issued.hash) is False
    assert verify_secret("wrong", issued.hash) is False
