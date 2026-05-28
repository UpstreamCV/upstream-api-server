"""End-to-end smoke test for the API server.

Walks the happy path: register an agent, register a camera, start a stream
session, decode the JWT returned at the top level, and assert the claims
match what we asked for. The SRT streamid itself carries an opaque
``sess_<hex>`` key, not the JWT.

Usage:
    UPSTREAM_API_URL=http://localhost:4000 \\
    UPSTREAM_DEV_TOKEN=ups_test_... \\
    uv run python scripts/smoke_local.py

The session JWT secret is read from .env via the normal Settings loader.
"""

from __future__ import annotations

import os
import sys
from uuid import UUID

import httpx

from upstream_api.config import get_settings
from upstream_api.security.session_jwt import verify_session_token
from upstream_api.security.streamid import extract_stream_key

API_URL = os.environ.get("UPSTREAM_API_URL", "http://localhost:4000")
TOKEN = os.environ.get("UPSTREAM_DEV_TOKEN")


def die(msg: str) -> None:
    print(f"FAIL: {msg}", file=sys.stderr)
    sys.exit(1)


def main() -> None:
    if not TOKEN:
        die("Set UPSTREAM_DEV_TOKEN to a plaintext API token issued via seed_dev_token.py.")

    headers = {"Authorization": f"Bearer {TOKEN}"}

    with httpx.Client(base_url=API_URL, timeout=10.0, headers=headers) as client:
        # 1. Register the agent
        r = client.post(
            "/v1/agents/register",
            json={"name": "smoke-agent", "version": "0.1.0", "os": "darwin"},
        )
        if r.status_code != 200:
            die(f"agent register: {r.status_code} {r.text}")
        agent_id = UUID(r.json()["agent"]["id"])
        print(f"  ok  agent_id={agent_id}")

        # 2. Register a camera
        r = client.post(
            "/v1/cameras/register",
            json={
                "agent_id": str(agent_id),
                "name": "smoke-cam",
                "external_id": "smoke-1",
                "default_width": 1280,
                "default_height": 720,
                "default_fps": 30,
                "default_codec": "h264",
                "default_bitrate_kbps": 3000,
            },
        )
        if r.status_code != 200:
            die(f"camera register: {r.status_code} {r.text}")
        camera_id = UUID(r.json()["id"])
        print(f"  ok  camera_id={camera_id}")

        # 3. Start a stream session
        r = client.post(
            "/v1/stream-sessions/start",
            json={
                "camera_id": str(camera_id),
                "requested": {
                    "width": 1280,
                    "height": 720,
                    "fps": 30,
                    "codec": "h264",
                    "bitrate_kbps": 3000,
                },
            },
        )
        if r.status_code != 200:
            die(f"session start: {r.status_code} {r.text}")
        body = r.json()

        assert body["session"]["state"] == "approved", body
        assert body["srt_url"].startswith("srt://"), body
        assert "streamid=publish:sess_" in body["srt_url"], body
        print(f"  ok  srt_url={body['srt_url'][:80]}…")

        # 4. The streamid is now an opaque short key — sanity-check its shape.
        stream_key = extract_stream_key(body["session"]["streamid"])
        assert stream_key.startswith("sess_"), stream_key
        assert len(body["session"]["streamid"]) < 64, body["session"]["streamid"]
        print(f"  ok  stream_key={stream_key}")

        # 5. The signed JWT still rides at the top level for non-ingest callers.
        settings = get_settings()
        claims = verify_session_token(
            body["session_token"], secret=settings.session_jwt_secret.get_secret_value()
        )
        assert str(claims.camera_id) == str(camera_id), claims
        assert str(claims.agent_id) == str(agent_id), claims
        assert claims.limits.width == 1280, claims
        assert claims.limits.fps == 30, claims
        print(f"  ok  jwt claims session_id={claims.session_id}")

        # 5. Stop the session (cleanup)
        session_id = body["session"]["id"]
        r = client.post(f"/v1/stream-sessions/{session_id}/stop")
        if r.status_code != 200:
            die(f"session stop: {r.status_code} {r.text}")
        assert r.json()["state"] == "ended", r.json()
        print("  ok  session stopped")

    print("PASS")


if __name__ == "__main__":
    main()
