from __future__ import annotations

from upstream_api.security.shared_secret import verify_shared_secret


def test_verify_shared_secret_matches() -> None:
    assert verify_shared_secret("hello", "hello") is True


def test_verify_shared_secret_rejects_mismatch() -> None:
    assert verify_shared_secret("hello", "world") is False


def test_verify_shared_secret_rejects_none() -> None:
    assert verify_shared_secret(None, "hello") is False


def test_verify_shared_secret_rejects_length_mismatch() -> None:
    assert verify_shared_secret("hello", "hello!") is False
