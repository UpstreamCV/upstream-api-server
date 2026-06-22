"""Agent API tokens: opaque, argon2id-hashed, prefix-indexed for O(1) lookup.

Plaintext format: ``ups_<env>_<id>_<secret>`` where ``id`` is 8 random base32
chars and ``secret`` is 22 random base32 chars. The ``prefix`` column stores
``ups_<env>_<id>`` (unique by birthday across ~5e11 tokens per env) and is the
unique lookup index. Only the ``secret`` portion is argon2id-hashed.
"""

from __future__ import annotations

import asyncio
import secrets
from dataclasses import dataclass

from argon2 import PasswordHasher
from argon2 import exceptions as argon2_exc

# Tuned for ~50ms verify on a modern laptop core. Adjust upward as hw improves.
_HASHER = PasswordHasher(time_cost=2, memory_cost=19456, parallelism=1)

_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567"  # base32 without 01OI ambiguity
_ID_LEN = 8
_SECRET_LEN = 22


def _random_chars(length: int) -> str:
    return "".join(secrets.choice(_ALPHABET) for _ in range(length))


# Scopes a dashboard user is allowed to request when minting a new token.
# Open vocabulary enforced here (not in the DB). ``events:read`` (Module 6 M6.2)
# grants an API token read access to the canonical event ledger; ``agent`` is
# the edge-agent credential and does NOT imply event read access. The
# ``reliability:*`` scopes (Reliability M1a) gate the Upstream Collector ingest
# (``reliability:ingest``), issue reads (``reliability:read``), and issue
# mutations such as resolve (``reliability:write``); each is explicit — none
# implies another.
ALLOWED_SCOPES: frozenset[str] = frozenset(
    {
        "agent",
        "events:read",
        "destinations:read",
        "destinations:write",
        "reliability:ingest",
        "reliability:read",
        "reliability:write",
    }
)


@dataclass(frozen=True, slots=True)
class IssuedToken:
    plaintext: str  # full token shown to the user exactly once
    prefix: str  # e.g. "ups_live_AB12CDEF" — unique per token
    hash: str  # argon2id encoded
    last_four: str  # display: "...AB12"


def issue_token(*, environment: str = "live") -> IssuedToken:
    """Generate a fresh token. Caller stores everything except ``plaintext``."""
    token_id = _random_chars(_ID_LEN)
    secret = _random_chars(_SECRET_LEN)
    prefix = f"ups_{environment}_{token_id}"
    plaintext = f"{prefix}_{secret}"
    return IssuedToken(
        plaintext=plaintext,
        prefix=prefix,
        hash=_HASHER.hash(secret),
        last_four=secret[-4:],
    )


def split_token(plaintext: str) -> tuple[str, str]:
    """Split a plaintext token into ``(prefix, secret)``.

    Raises ``ValueError`` if the format is not ``ups_<env>_<id>_<secret>``.
    """
    parts = plaintext.split("_", 3)
    if len(parts) != 4 or parts[0] != "ups" or not all(parts):
        raise ValueError("Invalid token format.")
    prefix = "_".join(parts[:3])
    return prefix, parts[3]


def verify_secret(secret: str, encoded_hash: str) -> bool:
    """Return True if ``secret`` matches ``encoded_hash``. False otherwise."""
    try:
        _HASHER.verify(encoded_hash, secret)
    except argon2_exc.VerifyMismatchError:
        return False
    except argon2_exc.InvalidHashError:
        return False
    return True


def needs_rehash(encoded_hash: str) -> bool:
    return _HASHER.check_needs_rehash(encoded_hash)


def rehash(secret: str) -> str:
    return _HASHER.hash(secret)


# Argon2 is CPU-bound (~50 ms tuned). Off-load to a worker thread so a single
# verify/issue doesn't block the event loop for the whole worker process.
async def verify_secret_async(secret: str, encoded_hash: str) -> bool:
    return await asyncio.to_thread(verify_secret, secret, encoded_hash)


async def issue_token_async(*, environment: str = "live") -> IssuedToken:
    return await asyncio.to_thread(issue_token, environment=environment)
