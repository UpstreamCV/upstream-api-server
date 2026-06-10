"""Add (or remove) a platform admin by email.

Looks up the Supabase Auth user for the given email and upserts a
``platform_admins`` row, granting access to the /v1/admin/* surface consumed
by upstream-admin-panel. If the user doesn't exist yet, optionally creates
one via the Supabase Admin API (``--create --password ...``).

Usage:
    make admin-seed EMAIL=you@example.com
    uv run python scripts/seed_platform_admin.py --email you@example.com
    uv run python scripts/seed_platform_admin.py --email you@example.com \\
        --create --password 'a-strong-password'
    uv run python scripts/seed_platform_admin.py --email you@example.com --remove
    uv run python scripts/seed_platform_admin.py --list

Reads ``UPSTREAM_*`` env vars from ``.env`` (pydantic-settings), so it works
against local Supabase CLI or hosted Supabase identically. For production,
point ``.env`` at the hosted URL + service_role key (or run the equivalent
insert in the Supabase SQL editor).
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from typing import Any
from uuid import UUID

import asyncpg
from supabase import create_client

from upstream_api.config import get_settings


def _eprint(*args: Any, **kwargs: Any) -> None:
    print(*args, file=sys.stderr, **kwargs)


async def _connect_db() -> asyncpg.Connection:
    settings = get_settings()
    return await asyncpg.connect(settings.database_url.get_secret_value())


def _create_user_via_admin(email: str, password: str) -> UUID:
    settings = get_settings()
    client = create_client(
        settings.supabase_url,
        settings.supabase_service_role_key.get_secret_value(),
    )
    response = client.auth.admin.create_user(
        {"email": email, "password": password, "email_confirm": True}
    )
    user = getattr(response, "user", None) or response
    user_id = getattr(user, "id", None) or user.get("id")  # type: ignore[union-attr]
    if user_id is None:
        raise RuntimeError(f"create_user returned no id: {response!r}")
    return UUID(str(user_id))


async def _list_admins(conn: asyncpg.Connection) -> int:
    rows = await conn.fetch(
        """
        select p.user_id, p.note, p.created_at, u.email
          from public.platform_admins p
          left join auth.users u on u.id = p.user_id
         order by p.created_at
        """
    )
    if not rows:
        print("No platform admins.")
        return 0
    for row in rows:
        print(f"  {row['user_id']}  {row['email'] or '<unknown>'}  ({row['note'] or '-'})")
    return 0


async def main_async(args: argparse.Namespace) -> int:
    conn = await _connect_db()
    try:
        if args.list:
            return await _list_admins(conn)

        user = await conn.fetchrow(
            "select id from auth.users where email = $1",
            args.email,
        )
        if user is None:
            if not args.create:
                _eprint(
                    f"[error] no auth user with email {args.email!r}. "
                    "Sign up first (dashboard or admin panel /login), or pass "
                    "--create --password '...' to create one now."
                )
                return 2
            if not args.password:
                _eprint("[error] --create requires --password.")
                return 2
            user_id = _create_user_via_admin(args.email, args.password)
            _eprint(f"[ok] created auth user {user_id}")
        else:
            user_id = UUID(str(user["id"]))

        if args.remove:
            deleted = await conn.execute(
                "delete from public.platform_admins where user_id = $1",
                user_id,
            )
            print(f"Removed platform admin {args.email} ({user_id}): {deleted}")
            return 0

        await conn.execute(
            """
            insert into public.platform_admins (user_id, note)
            values ($1, $2)
            on conflict (user_id) do update set note = excluded.note
            """,
            user_id,
            args.note or f"seeded via script for {args.email}",
        )
        print(f"Platform admin granted: {args.email} ({user_id})")
        return 0
    finally:
        await conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Manage the platform_admins allowlist.")
    parser.add_argument("--email", help="Email of the Supabase Auth user.")
    parser.add_argument("--note", help="Optional note stored on the allowlist row.")
    parser.add_argument(
        "--create",
        action="store_true",
        help="Create the auth user if missing (requires --password).",
    )
    parser.add_argument("--password", help="Password when creating a new auth user.")
    parser.add_argument(
        "--remove",
        action="store_true",
        help="Remove the user from the allowlist instead of adding.",
    )
    parser.add_argument("--list", action="store_true", help="List current platform admins.")
    args = parser.parse_args()
    if not args.list and not args.email:
        parser.error("--email is required unless --list is given")
    sys.exit(asyncio.run(main_async(args)))


if __name__ == "__main__":
    main()
