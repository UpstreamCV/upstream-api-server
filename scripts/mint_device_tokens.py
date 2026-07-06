"""Fleet tooling: mint device-scoped API tokens, one per appliance.

Mints ``device``-scoped tokens directly against the database (same posture
as ``seed_dev_account.py`` — reads ``UPSTREAM_*`` from ``.env``), so it
works from any machine with a DB connection string: the manufacturing
laptop, the api-server host, or CI. Each token is trust-on-first-use: it
binds to the first agent that registers through it (see
``supabase/migrations/20260706121000_device_scoped_tokens.sql``).

Plaintext tokens print exactly once, one ``name<TAB>token`` line per device
on stdout (everything else goes to stderr) — pipe into your image
customization step, e.g.:

    uv run python scripts/mint_device_tokens.py \
        --account-slug crowdcount-fleet T1234 T1235 T1236 > tokens.tsv

Revocation is per-device: find the token by its device name (or bound
agent) in GET /v1/api-tokens and DELETE /v1/api-tokens/{id} — or use
--revoke here with the device name.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from typing import Any
from uuid import UUID

import asyncpg

from upstream_api.config import get_settings
from upstream_api.security.api_tokens import DEVICE_SCOPE, issue_token


def _eprint(*args: Any, **kwargs: Any) -> None:
    print(*args, file=sys.stderr, **kwargs)


async def _connect_db() -> asyncpg.Connection:
    settings = get_settings()
    return await asyncpg.connect(settings.database_url.get_secret_value())


async def _resolve_account(
    conn: asyncpg.Connection, *, account_id: str | None, account_slug: str | None
) -> UUID:
    if account_id is not None:
        row = await conn.fetchrow("select id from accounts where id = $1", UUID(account_id))
    else:
        row = await conn.fetchrow("select id from accounts where slug = $1", account_slug)
    if row is None:
        raise SystemExit(f"Account not found: {account_id or account_slug}")
    return row["id"]


async def _mint(args: argparse.Namespace) -> None:
    settings = get_settings()
    env_label = "live" if settings.is_production else "test"
    conn = await _connect_db()
    try:
        account_pk = await _resolve_account(
            conn, account_id=args.account_id, account_slug=args.account_slug
        )
        for device_name in args.devices:
            existing = await conn.fetchval(
                """
                select count(*) from api_tokens
                 where account_id = $1 and name = $2 and revoked_at is null
                """,
                account_pk,
                device_name,
            )
            if existing and not args.allow_duplicate:
                _eprint(
                    f"SKIP {device_name}: an active token with this name already "
                    "exists (revoke it first, or pass --allow-duplicate)."
                )
                continue
            issued = issue_token(environment=env_label)
            await conn.execute(
                """
                insert into api_tokens
                  (account_id, name, prefix, hash, last_four, scopes, expires_at)
                values ($1, $2, $3, $4, $5, $6, $7)
                """,
                account_pk,
                device_name,
                issued.prefix,
                issued.hash,
                issued.last_four,
                [DEVICE_SCOPE],
                args.expires_at,
            )
            # The ONLY stdout output: name<TAB>plaintext, shown exactly once.
            print(f"{device_name}\t{issued.plaintext}")
            _eprint(f"minted {device_name} ({issued.prefix}, scope={DEVICE_SCOPE})")
    finally:
        await conn.close()


async def _revoke(args: argparse.Namespace) -> None:
    conn = await _connect_db()
    try:
        account_pk = await _resolve_account(
            conn, account_id=args.account_id, account_slug=args.account_slug
        )
        for device_name in args.devices:
            result = await conn.execute(
                """
                update api_tokens
                   set revoked_at = now()
                 where account_id = $1 and name = $2 and revoked_at is null
                """,
                account_pk,
                device_name,
            )
            count = int(result.split()[-1])
            if count:
                _eprint(f"revoked {count} token(s) named {device_name!r}")
            else:
                _eprint(f"SKIP {device_name}: no active token with this name")
    finally:
        await conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    account = parser.add_mutually_exclusive_group(required=True)
    account.add_argument("--account-id", help="Account UUID to mint under.")
    account.add_argument("--account-slug", help="Account slug to mint under.")
    parser.add_argument(
        "devices",
        nargs="+",
        metavar="DEVICE",
        help="Device names (e.g. crowdcount device ids like T1234); used as the token name.",
    )
    parser.add_argument(
        "--revoke",
        action="store_true",
        help="Revoke the named devices' active tokens instead of minting.",
    )
    parser.add_argument(
        "--expires-at",
        default=None,
        help="Optional ISO-8601 expiry for minted tokens (default: no expiry).",
    )
    parser.add_argument(
        "--allow-duplicate",
        action="store_true",
        help="Mint even when an active token with the same name exists.",
    )
    args = parser.parse_args()
    if args.expires_at is not None:
        from datetime import datetime

        args.expires_at = datetime.fromisoformat(args.expires_at)
    asyncio.run(_revoke(args) if args.revoke else _mint(args))


if __name__ == "__main__":
    main()
