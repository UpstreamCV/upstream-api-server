"""One-command dev account + fixtures seeder.

Creates a stable Supabase Auth user ``dev@upstream.test`` (password
``upstream-dev-password``) via the Admin API, lets the existing
``handle_new_user`` trigger auto-provision the tenant, then populates
the account with sample data so the dashboard isn't empty during
manual testing: one agent, one camera, one active stream session,
three heartbeats, one passing validation, one snapshot, one API token.

Usage:
    make dev-account                # create or reuse the dev account
    make dev-account-reset          # wipe + recreate (re-seed fixtures)
    uv run python scripts/seed_dev_account.py --token-only  # just mint a token

The script reads ``UPSTREAM_*`` env vars from ``.env`` (pydantic-settings)
so it works against local Supabase CLI or hosted Supabase identically.
Hosted: set ``.env`` to the hosted URL + service_role key first.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import asyncpg
from supabase import create_client

from upstream_api.config import get_settings
from upstream_api.security.api_tokens import issue_token
from upstream_api.security.session_jwt import (
    SessionLimits,
    mint_session_token,
)
from upstream_api.security.streamid import build_streamid

DEV_EMAIL = "dev@upstream.test"
DEV_PASSWORD = "upstream-dev-password"
DEV_WORKSPACE = "Upstream Dev"

# Smallest valid JPEG: a 1x1 black pixel, ~134 bytes.
_TINY_JPEG = bytes.fromhex(
    "ffd8ffe000104a46494600010100000100010000ffdb004300080606070605"
    "08070707090908090a0c140e0c0b0b0c191213130f141d1a1f1e1d1a1c1c20"
    "242e2720222c231c1c2837292c30313434341f27393d38323c2e333432ffdb"
    "0043010909090c0b0c180d0d1832211c213232323232323232323232323232"
    "32323232323232323232323232323232323232323232323232323232323232"
    "3232323232323232323232ffc00011080001000103012200021101031101ff"
    "c4001f0000010501010101010100000000000000000102030405060708090a"
    "0bffc400b5100002010303020403050504040000017d01020300041105122131"
    "410613516107227114328191a1082342b1c11552d1f02433627282090a161718"
    "191a25262728292a3435363738393a434445464748494a535455565758595a63"
    "6465666768696a737475767778797a838485868788898a92939495969798999a"
    "a2a3a4a5a6a7a8a9aab2b3b4b5b6b7b8b9bac2c3c4c5c6c7c8c9cad2d3d4d5d6"
    "d7d8d9dae1e2e3e4e5e6e7e8e9eaf1f2f3f4f5f6f7f8f9faffc4001f01000301"
    "0101010101010101010000000000000102030405060708090a0bffc400b51100"
    "020102040403040705040400010277000102031104052131061241510761711"
    "3223281081442911321a1b1c109233352f0156272d10a162434e125f117181"
    "91a262728292a35363738393a434445464748494a535455565758595a636465"
    "666768696a737475767778797a82838485868788898a92939495969798999aa2"
    "a3a4a5a6a7a8a9aab2b3b4b5b6b7b8b9bac2c3c4c5c6c7c8c9cad2d3d4d5d6d7"
    "d8d9dae2e3e4e5e6e7e8e9eaf2f3f4f5f6f7f8f9faffda000c03010002110311"
    "003f00fbfcffd9"
)


def _eprint(*args: Any, **kwargs: Any) -> None:
    print(*args, file=sys.stderr, **kwargs)


async def _connect_db() -> asyncpg.Connection:
    settings = get_settings()
    return await asyncpg.connect(settings.database_url.get_secret_value())


def _admin_client() -> Any:
    settings = get_settings()
    return create_client(
        settings.supabase_url,
        settings.supabase_service_role_key.get_secret_value(),
    )


async def _find_user(conn: asyncpg.Connection, email: str) -> dict[str, Any] | None:
    return await conn.fetchrow(  # type: ignore[no-any-return]
        "select id from auth.users where email = $1",
        email,
    )


async def _wipe_dev_user(conn: asyncpg.Connection, email: str) -> None:
    """Delete the dev user. Cascades through account_members → accounts →
    account_limits → agents → cameras → stream_sessions → etc via FKs.
    """
    user = await _find_user(conn, email)
    if user is None:
        return
    # auth.users cascade isn't wired to public.account_members (no FK on
    # user_id). Wipe membership first; the account + its dependent tables
    # then cascade away via the accounts FK chain.
    await conn.execute("delete from public.account_members where user_id = $1", user["id"])
    await conn.execute(
        """
        delete from public.accounts
         where id in (
           select a.id from public.accounts a
           left join public.account_members m on m.account_id = a.id
           where m.account_id is null
         )
        """,
    )
    # Now the auth user can go.
    client = _admin_client()
    client.auth.admin.delete_user(str(user["id"]))


def _create_user_via_admin() -> UUID:
    client = _admin_client()
    response = client.auth.admin.create_user(
        {
            "email": DEV_EMAIL,
            "password": DEV_PASSWORD,
            "email_confirm": True,
            "user_metadata": {"workspace_name": DEV_WORKSPACE},
        }
    )
    user = getattr(response, "user", None) or response
    user_id = getattr(user, "id", None) or user.get("id")  # type: ignore[union-attr]
    if user_id is None:
        raise RuntimeError(f"create_user returned no id: {response!r}")
    return UUID(str(user_id))


async def _wait_for_membership(conn: asyncpg.Connection, user_id: UUID) -> UUID:
    """Trigger fires synchronously; one fetch should suffice. Belt-and-braces
    retry once after a tiny sleep in case Supabase Auth runs the insert in a
    different transaction than the trigger.
    """
    for _ in range(2):
        row = await conn.fetchrow(
            "select account_id from public.account_members where user_id = $1",
            user_id,
        )
        if row is not None:
            return UUID(str(row["account_id"]))
        await asyncio.sleep(0.5)
    raise RuntimeError(
        "auth.users row exists but no account_members row. "
        "Is the handle_new_user trigger applied to this database?"
    )


async def _seed_fixtures(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    skip_snapshot_upload: bool = False,
) -> dict[str, Any]:
    """Populate one agent + camera + session + heartbeats + validation + snapshot."""

    settings = get_settings()
    now = datetime.now(tz=UTC)

    agent_id = uuid4()
    await conn.execute(
        """
        insert into public.agents (
            id, account_id, name, hostname, version, os, python_version,
            capabilities, last_seen_at, status
        )
        values ($1, $2, 'dev-agent', 'laptop.dev', '0.1.0',
                'darwin', '3.11.5', '{}'::jsonb, now(), 'active')
        """,
        agent_id,
        account_id,
    )

    camera_id = uuid4()
    await conn.execute(
        """
        insert into public.cameras (
            id, account_id, agent_id, name, external_id, device_path,
            default_width, default_height, default_fps, default_codec,
            default_bitrate_kbps, default_latency_mode, status, metadata
        )
        values ($1, $2, $3, 'dev-camera', 'dev-1', '/dev/video0',
                1280, 720, 30, 'h264', 3000, 'balanced', 'streaming',
                '{}'::jsonb)
        """,
        camera_id,
        account_id,
        agent_id,
    )

    session_id = uuid4()
    jti = uuid4()
    encoded_jwt, _claims = mint_session_token(
        secret=settings.session_jwt_secret.get_secret_value(),
        account_id=account_id,
        agent_id=agent_id,
        camera_id=camera_id,
        session_id=session_id,
        jti=jti,
        ttl_seconds=settings.session_token_ttl_seconds,
        limits=SessionLimits(width=1280, height=720, fps=30, codec="h264", bitrate_kbps=3000),
    )
    streamid = build_streamid(encoded_jwt)
    srt_url = f"srt://ingest.upstreamcv.com:8890?streamid={streamid}&latency=200"

    await conn.execute(
        """
        insert into public.stream_sessions (
            id, account_id, agent_id, camera_id, state,
            req_width, req_height, req_fps, req_codec, req_bitrate_kbps,
            req_latency_mode,
            lim_width, lim_height, lim_fps, lim_codec, lim_bitrate_kbps,
            lim_latency_mode,
            streamid, srt_url, session_token_jti,
            expires_at, started_at
        )
        values ($1, $2, $3, $4, 'streaming',
                1280, 720, 30, 'h264', 3000, 'balanced',
                1280, 720, 30, 'h264', 3000, 'balanced',
                $5, $6, $7, $8, now())
        """,
        session_id,
        account_id,
        agent_id,
        camera_id,
        streamid,
        srt_url,
        jti,
        now + timedelta(seconds=settings.session_token_ttl_seconds),
    )

    # 3 heartbeats over the last 10 seconds
    for offset in (0, 5, 10):
        await conn.execute(
            """
            insert into public.heartbeats (
                account_id, agent_id, camera_id, session_id, status,
                fps, bitrate_kbps, width, height, codec, agent_version,
                reported_at
            )
            values ($1, $2, $3, $4, 'streaming',
                    29.97, 2980, 1280, 720, 'h264', '0.1.0', now() - ($5::int * interval '1 second'))
            """,
            account_id,
            agent_id,
            camera_id,
            session_id,
            offset,
        )

    # One passing validation
    observed = {
        "codec": "h264",
        "width": 1280,
        "height": 720,
        "fps": 29.97,
        "bitrate_kbps": 2980,
    }
    expected = {
        "codec": "h264",
        "width": 1280,
        "height": 720,
        "fps": 30,
        "bitrate_kbps": 3000,
    }
    await conn.execute(
        """
        insert into public.stream_validations (
            session_id, passed, observed, expected, mismatch_fields
        )
        values ($1, true, $2::jsonb, $3::jsonb, '{}')
        """,
        session_id,
        json.dumps(observed),
        json.dumps(expected),
    )

    # Snapshot — upload the tiny JPEG via service role then insert the row.
    snapshot_storage_key: str | None = None
    if not skip_snapshot_upload:
        snapshot_id = uuid4()
        day = now.strftime("%Y%m%d")
        snapshot_storage_key = f"{account_id}/{camera_id}/{day}/{snapshot_id}.jpg"
        try:
            client = _admin_client()
            client.storage.from_(settings.supabase_storage_bucket).upload(
                snapshot_storage_key,
                _TINY_JPEG,
                {"content-type": "image/jpeg", "upsert": "true"},
            )
            await conn.execute(
                """
                insert into public.snapshots (
                    id, account_id, agent_id, camera_id, session_id,
                    storage_key, content_type, width, height, bytes,
                    captured_at
                )
                values ($1, $2, $3, $4, $5, $6, 'image/jpeg', 1, 1, $7, now())
                """,
                snapshot_id,
                account_id,
                agent_id,
                camera_id,
                session_id,
                snapshot_storage_key,
                len(_TINY_JPEG),
            )
        except Exception as exc:  # pragma: no cover — network-dependent
            _eprint(
                f"[warn] snapshot upload skipped — bucket '{settings.supabase_storage_bucket}' "
                f"may not exist on this Supabase project. ({exc})"
            )
            snapshot_storage_key = None

    return {
        "agent_id": str(agent_id),
        "camera_id": str(camera_id),
        "session_id": str(session_id),
        "snapshot_storage_key": snapshot_storage_key,
        "srt_url": srt_url,
    }


async def _issue_api_token(
    conn: asyncpg.Connection,
    *,
    account_id: UUID,
    name: str = "dev-token",
) -> str:
    issued = issue_token(environment="test")
    await conn.execute(
        """
        insert into public.api_tokens
            (account_id, name, prefix, hash, last_four, scopes)
        values ($1, $2, $3, $4, $5, '{agent}'::text[])
        """,
        account_id,
        name,
        issued.prefix,
        issued.hash,
        issued.last_four,
    )
    return issued.plaintext


def _print_summary(
    *,
    account_id: UUID,
    user_id: UUID,
    token_plaintext: str | None,
    fixtures: dict[str, Any] | None,
) -> None:
    settings = get_settings()
    print()
    print("=" * 72)
    print(" Dev account ready")
    print("=" * 72)
    print(f" Dashboard:  http://localhost:3000")
    print(f" Email:      {DEV_EMAIL}")
    print(f" Password:   {DEV_PASSWORD}")
    print(f" Workspace:  {DEV_WORKSPACE}")
    print(f" User id:    {user_id}")
    print(f" Account id: {account_id}")
    print()
    if token_plaintext:
        print(" API token (save this — won't print again on re-run):")
        print(f"   {token_plaintext}")
        print()
        print(" Sample curl against the api:")
        print(
            f'   curl -H "Authorization: Bearer {token_plaintext}" '
            f"{settings.supabase_url.replace('supabase.co', 'upstreamcv.com').replace('127.0.0.1:54321', 'localhost:4000')}/v1/agents"
        )
        print()
    if fixtures:
        print(" Fixture data populated:")
        print(f"   agent_id:   {fixtures['agent_id']}")
        print(f"   camera_id:  {fixtures['camera_id']}")
        print(f"   session_id: {fixtures['session_id']}")
        if fixtures.get("snapshot_storage_key"):
            print(f"   snapshot:   {fixtures['snapshot_storage_key']}")
        else:
            print("   snapshot:   (skipped — bucket missing)")
        print()
    print("=" * 72)


async def main_async(args: argparse.Namespace) -> int:
    conn = await _connect_db()
    try:
        existing = await _find_user(conn, DEV_EMAIL)

        if args.reset and existing is not None:
            _eprint("[info] --reset: wiping dev user + cascade")
            await _wipe_dev_user(conn, DEV_EMAIL)
            existing = None

        if existing is None:
            _eprint("[info] creating dev user via Supabase Admin API")
            user_id = _create_user_via_admin()
            account_id = await _wait_for_membership(conn, user_id)
            _eprint(f"[ok] user {user_id} provisioned account {account_id}")
            fixtures = await _seed_fixtures(conn, account_id=account_id)
            token_plaintext = await _issue_api_token(conn, account_id=account_id)
        else:
            user_id = UUID(str(existing["id"]))
            membership = await conn.fetchrow(
                "select account_id from public.account_members where user_id = $1",
                user_id,
            )
            if membership is None:
                _eprint(
                    "[warn] dev user exists but no membership — trigger may not have fired"
                )
                return 2
            account_id = UUID(str(membership["account_id"]))

            if args.token_only:
                _eprint("[info] --token-only: issuing fresh API token")
                token_plaintext = await _issue_api_token(
                    conn, account_id=account_id, name=f"dev-token-{int(datetime.now(tz=UTC).timestamp())}"
                )
                fixtures = None
            else:
                _eprint(f"[info] dev account already exists (account {account_id}); skipping")
                _eprint("[info] use --reset to wipe + re-seed, or --token-only to mint a fresh token")
                token_plaintext = None
                fixtures = None

        _print_summary(
            account_id=account_id,
            user_id=user_id,
            token_plaintext=token_plaintext,
            fixtures=fixtures,
        )
        return 0
    finally:
        await conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed the dev account + fixtures.")
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Delete the dev user (cascades through account) and re-create.",
    )
    parser.add_argument(
        "--token-only",
        action="store_true",
        help="Mint a fresh API token for the existing dev account; do nothing else.",
    )
    args = parser.parse_args()
    sys.exit(asyncio.run(main_async(args)))


if __name__ == "__main__":
    main()
