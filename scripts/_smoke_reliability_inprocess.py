"""In-process smoke test for the reliability slice (M0/M1-A).

Drives the assembled FastAPI app via TestClient against a fake asyncpg pool —
the same shape the unit tests use, but stitched together into one end-to-end
walk through every behavior the slice promises:

    healthy → unknown → needs_review → critical → dedupe → resolve → 404 scope

This complements (does not replace) the unit tests in tests/. It exists as a
script so the user can rerun it against the real database after the migration
applies (swap the FakePool import for a real pool with seeded data).

The script is intentionally not collected by pytest — it's a verification
walk, not a regression suite.
"""

from __future__ import annotations

import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

# Make tests/ importable so we can reuse the FakeReliabilityConn from the
# endpoint test file — it already understands every query the router/evaluator
# emits, so we don't reimplement the dispatch table here.
_HERE = Path(__file__).resolve().parent
_REPO_ROOT = _HERE.parent
sys.path.insert(0, str(_REPO_ROOT))
sys.path.insert(0, str(_REPO_ROOT / "tests"))

# Conftest sets the env vars create_app() needs at import time.
import tests.conftest  # noqa: F401  # pyright: ignore[reportMissingImports]
from fastapi.testclient import TestClient  # noqa: E402

from upstream_api.app import create_app  # noqa: E402
from upstream_api.db.pool import get_pool  # noqa: E402
from upstream_api.security.deps import AgentAuth, CallerAuth, current_caller  # noqa: E402
from tests.test_reliability_endpoints import (  # noqa: E402
    FakePool,
    FakeReliabilityConn,
)

GREEN = "\033[32m"
RED = "\033[31m"
DIM = "\033[2m"
RESET = "\033[0m"


def ok(label: str, detail: str = "") -> None:
    print(f"  {GREEN}ok{RESET}   {label}{DIM} {detail}{RESET}")


def fail(label: str, detail: str = "") -> None:
    print(f"  {RED}FAIL{RESET} {label} {detail}")
    sys.exit(1)


def client_for(
    conn: Any,
    *,
    account_id: UUID,
    authenticated: bool = True,
) -> TestClient:
    app = create_app()
    app.dependency_overrides[get_pool] = lambda: FakePool(conn)
    if authenticated:
        app.dependency_overrides[current_caller] = lambda: CallerAuth(
            agent=AgentAuth(account_id=account_id, token_id=uuid4(), scopes=("agent",)),
            user=None,
        )
    return TestClient(app)


def session(*, state: str = "streaming", age_seconds: float = 5) -> dict[str, Any]:
    now = datetime.now(tz=UTC)
    return {
        "id": uuid4(),
        "state": state,
        "created_at": now - timedelta(seconds=age_seconds + 30),
        "started_at": now - timedelta(seconds=age_seconds + 30),
        "ended_at": None,
        "expires_at": now + timedelta(seconds=3600),
    }


def snapshot(*, age_seconds: float) -> dict[str, Any]:
    now = datetime.now(tz=UTC)
    return {
        "id": uuid4(),
        "captured_at": now - timedelta(seconds=age_seconds),
        "created_at": now - timedelta(seconds=age_seconds),
        "session_id": None,
    }


def processor_status(
    *,
    age_seconds: float = 5,
    applied_geometry: dict[str, Any] | None = None,
    error_code: str | None = None,
) -> dict[str, Any]:
    now = datetime.now(tz=UTC)
    payload: dict[str, Any] = {}
    if applied_geometry is not None:
        payload["applied_geometry"] = applied_geometry
    return {
        "id": uuid4(),
        "camera_id": uuid4(),
        "stream_session_id": uuid4(),
        "processor_id": "smoke-proc",
        "runtime": "python",
        "package_id": "pkg",
        "status": "processing",
        "fps_input": 30.0,
        "fps_processed": 29.8,
        "last_frame_at": now - timedelta(seconds=age_seconds),
        "last_observation_at": now - timedelta(seconds=age_seconds),
        "error_code": error_code,
        "error_message": None,
        "payload": payload,
        "created_at": now - timedelta(seconds=age_seconds + 60),
        "updated_at": now - timedelta(seconds=age_seconds),
    }


def hit(client: TestClient, kind: str, camera_id: UUID) -> Any:
    """Helper: GET /v1/cameras/{id}/reliability/{kind} and assert 200."""
    r = client.get(f"/v1/cameras/{camera_id}/reliability/{kind}")
    if r.status_code != 200:
        fail(f"{kind} unexpected {r.status_code}", r.text[:200])
    return r.json()


def main() -> None:
    print("Reliability slice — in-process smoke")
    print(f"{DIM}fake asyncpg pool, TestClient against the real FastAPI app{RESET}")
    print()

    # ----- Test 1: Healthy state ------------------------------------------
    print("Test 1 — Healthy")
    account, camera = uuid4(), uuid4()
    conn = FakeReliabilityConn(
        camera_account_id=account,
        session_row=session(state="streaming"),
        snapshot_row=snapshot(age_seconds=5),
        observation_at=datetime.now(tz=UTC) - timedelta(seconds=5),
        processor_status_row=processor_status(
            age_seconds=5,
            applied_geometry={
                "source": "operator_saved",
                "revision": 9,
                "line_count": 2,
                "roi_count": 1,
            },
        ),
        has_saved_geometry=True,
    )
    client = client_for(conn, account_id=account)
    summary = hit(client, "summary", camera)
    assert summary["status"] == "healthy", summary
    assert summary["open_findings_count"] == 0, summary
    assert summary["top_findings"] == [], summary
    assert summary["recommended_next_action"] is None, summary
    assert summary["highest_severity"] is None, summary
    ok("status=healthy", "no findings, no recommended action")
    findings = hit(client, "findings", camera)
    assert findings["findings"] == [], findings
    ok("findings endpoint", "empty list, last_evaluated_at populated")

    # ----- Test 2: Unknown / insufficient data ----------------------------
    print()
    print("Test 2 — Unknown / insufficient data")
    account, camera = uuid4(), uuid4()
    conn = FakeReliabilityConn(camera_account_id=account)  # no facts at all
    client = client_for(conn, account_id=account)
    summary = hit(client, "summary", camera)
    assert summary["status"] == "unknown", summary
    assert summary["open_findings_count"] == 0, summary
    ok("status=unknown", "missing facts don't fabricate findings")

    # ----- Test 3: latest_frame_stale -------------------------------------
    print()
    print("Test 3 — latest_frame_stale")
    account, camera = uuid4(), uuid4()
    conn = FakeReliabilityConn(
        camera_account_id=account,
        session_row=session(state="streaming"),
        snapshot_row=snapshot(age_seconds=120),  # > 60s threshold
        observation_at=datetime.now(tz=UTC) - timedelta(seconds=5),
        processor_status_row=processor_status(age_seconds=5),
    )
    client = client_for(conn, account_id=account)
    summary = hit(client, "summary", camera)
    assert summary["status"] == "needs_review", summary
    assert summary["highest_severity"] == "warning", summary
    assert summary["open_findings_count"] >= 1, summary
    top = summary["top_findings"][0]
    assert top["finding_type"] == "latest_frame_stale", top
    assert top["category"] == "stream", top
    assert top["severity"] == "warning", top
    assert top["status"] == "open", top
    assert top["recommended_action"], top
    ev = top["evidence"]
    assert ev["latest_frame_age_seconds"] >= 60, ev
    assert ev["threshold_seconds"] == 60, ev
    assert "stream_session_id" in ev, ev
    ok("status=needs_review", f"top={top['finding_type']}, age={ev['latest_frame_age_seconds']}s")
    ok("evidence carries age + threshold + session, no secrets", str(ev))

    # ----- Test 3b: dedupe (repeated evaluation) --------------------------
    open_count_before = sum(
        1 for f in conn.open_findings.values() if f["status"] == "open"
    )
    hit(client, "summary", camera)
    hit(client, "summary", camera)
    open_count_after = sum(
        1 for f in conn.open_findings.values() if f["status"] == "open"
    )
    assert open_count_after == open_count_before, (
        open_count_before,
        open_count_after,
    )
    keys = [
        f["dedupe_key"] for f in conn.open_findings.values() if f["status"] == "open"
    ]
    assert len(keys) == len(set(keys)), keys
    ok("dedupe stable across repeated reads", f"open={open_count_after}, unique keys")

    # ----- Test 4: stream_valid_processor_silent --------------------------
    print()
    print("Test 4 — stream_valid_processor_silent")
    account, camera = uuid4(), uuid4()
    # Stream live + recent snapshot, but processor status is very stale and no
    # observations at all.
    sess = session(state="streaming")
    conn = FakeReliabilityConn(
        camera_account_id=account,
        session_row=sess,
        snapshot_row=snapshot(age_seconds=5),
        observation_at=None,
        processor_status_row=processor_status(age_seconds=600),
    )
    client = client_for(conn, account_id=account)
    summary = hit(client, "summary", camera)
    assert summary["status"] == "critical", summary
    assert summary["highest_severity"] == "critical", summary
    top = summary["top_findings"][0]
    assert top["finding_type"] == "stream_valid_processor_silent", top
    assert top["severity"] == "critical", top
    assert top["category"] == "stream", top
    assert "should not be trusted" in top["message"], top
    assert top["recommended_action"], top
    ev = top["evidence"]
    assert ev["stream_state"] == "streaming", ev
    assert ev["processor_status_age_seconds"] >= 60, ev
    assert "stream_session_id" in ev, ev
    ok("status=critical", f"top={top['finding_type']}")
    ok("evidence includes ages + stream_state", str(ev))
    # Critical's recommended action surfaces in recommended_next_action.
    assert summary["recommended_next_action"] == top["recommended_action"]
    ok("recommended_next_action mirrors critical's action")

    # ----- Test 5: processor_using_default_geometry -----------------------
    print()
    print("Test 5 — processor_using_default_geometry")

    # 5a — source=default with no saved geometry → info
    account, camera = uuid4(), uuid4()
    conn = FakeReliabilityConn(
        camera_account_id=account,
        session_row=session(state="streaming"),
        snapshot_row=snapshot(age_seconds=5),
        observation_at=datetime.now(tz=UTC) - timedelta(seconds=5),
        processor_status_row=processor_status(
            age_seconds=5,
            applied_geometry={
                "source": "default",
                "revision": None,
                "line_count": 1,
                "roi_count": 1,
            },
        ),
        has_saved_geometry=False,
    )
    client = client_for(conn, account_id=account)
    summary = hit(client, "summary", camera)
    assert summary["status"] == "info", summary
    assert summary["highest_severity"] == "info", summary
    top = summary["top_findings"][0]
    assert top["finding_type"] == "processor_using_default_geometry", top
    assert top["severity"] == "info", top
    ok("source=default, no saved geometry → info", top["finding_type"])

    # 5b — source=default with saved geometry → warning
    account, camera = uuid4(), uuid4()
    conn = FakeReliabilityConn(
        camera_account_id=account,
        session_row=session(state="streaming"),
        snapshot_row=snapshot(age_seconds=5),
        observation_at=datetime.now(tz=UTC) - timedelta(seconds=5),
        processor_status_row=processor_status(
            age_seconds=5,
            applied_geometry={
                "source": "default",
                "revision": None,
                "line_count": 1,
                "roi_count": 1,
            },
        ),
        has_saved_geometry=True,
    )
    client = client_for(conn, account_id=account)
    summary = hit(client, "summary", camera)
    top = summary["top_findings"][0]
    assert top["severity"] == "warning", top
    ok("source=default, saved geometry exists → warning", "")

    # 5c — source=fallback_default_on_error → warning + error_reason in evidence
    account, camera = uuid4(), uuid4()
    conn = FakeReliabilityConn(
        camera_account_id=account,
        session_row=session(state="streaming"),
        snapshot_row=snapshot(age_seconds=5),
        observation_at=datetime.now(tz=UTC) - timedelta(seconds=5),
        processor_status_row=processor_status(
            age_seconds=5,
            applied_geometry={
                "source": "fallback_default_on_error",
                "revision": 9,
                "error_reason": "geometry envelope has no enabled rois",
            },
        ),
    )
    client = client_for(conn, account_id=account)
    summary = hit(client, "summary", camera)
    top = summary["top_findings"][0]
    assert top["severity"] == "warning", top
    assert top["evidence"]["error_reason"], top["evidence"]
    ok("source=fallback_default_on_error → warning", top["evidence"]["error_reason"])

    # 5d — error_code=geometry_invalid → warning + error_message in evidence
    account, camera = uuid4(), uuid4()
    conn = FakeReliabilityConn(
        camera_account_id=account,
        session_row=session(state="streaming"),
        snapshot_row=snapshot(age_seconds=5),
        observation_at=datetime.now(tz=UTC) - timedelta(seconds=5),
        processor_status_row=processor_status(
            age_seconds=5,
            error_code="geometry_invalid",
        ),
    )
    # error_message is set on the row builder side; smoke uses defaults.
    conn.processor_status_row["error_message"] = "invalid geometry envelope"
    client = client_for(conn, account_id=account)
    summary = hit(client, "summary", camera)
    top = summary["top_findings"][0]
    assert top["severity"] == "warning", top
    assert top["evidence"]["error_code"] == "geometry_invalid", top["evidence"]
    ok("error_code=geometry_invalid → warning", top["evidence"]["error_code"])

    # 5e — applied_geometry block missing → no finding (insufficient data)
    account, camera = uuid4(), uuid4()
    conn = FakeReliabilityConn(
        camera_account_id=account,
        session_row=session(state="streaming"),
        snapshot_row=snapshot(age_seconds=5),
        observation_at=datetime.now(tz=UTC) - timedelta(seconds=5),
        processor_status_row=processor_status(age_seconds=5),  # payload empty
    )
    client = client_for(conn, account_id=account)
    summary = hit(client, "summary", camera)
    geom_types = [
        f["finding_type"] for f in summary["top_findings"]
        if f["finding_type"] == "processor_using_default_geometry"
    ]
    assert not geom_types, summary
    ok("missing applied_geometry block → no finding", "matches 'missing_applied_geometry' panel state")

    # ----- Test 6: Resolution ---------------------------------------------
    print()
    print("Test 6 — Resolution returns finding to healthy")
    account, camera = uuid4(), uuid4()
    conn = FakeReliabilityConn(
        camera_account_id=account,
        session_row=session(state="streaming"),
        snapshot_row=snapshot(age_seconds=120),
        observation_at=datetime.now(tz=UTC) - timedelta(seconds=5),
        processor_status_row=processor_status(age_seconds=5),
    )
    client = client_for(conn, account_id=account)
    summary = hit(client, "summary", camera)
    assert any(
        f["finding_type"] == "latest_frame_stale" for f in summary["top_findings"]
    )
    ok("warning opened (latest_frame_stale)", "open_count=%d" % summary["open_findings_count"])

    # Heal the snapshot and re-evaluate.
    conn.snapshot_row = snapshot(age_seconds=5)
    summary = hit(client, "summary", camera)
    assert summary["status"] == "healthy", summary
    assert summary["open_findings_count"] == 0, summary
    # And the underlying row is now status=resolved with a reason stamp.
    resolved_rows = [
        f for f in conn.open_findings.values()
        if f["finding_type"] == "latest_frame_stale"
        and f["status"] == "resolved"
    ]
    assert resolved_rows, [r["status"] for r in conn.open_findings.values()]
    assert resolved_rows[0]["resolved_at"] is not None, resolved_rows[0]
    assert resolved_rows[0]["resolved_reason"] == "healthy", resolved_rows[0]
    ok("status flipped back to healthy", "open finding moved to status=resolved")
    ok("resolved_at + resolved_reason stamped", f"reason={resolved_rows[0]['resolved_reason']}")

    # ----- Test 7: Account / camera scoping -------------------------------
    print()
    print("Test 7 — Account / camera scoping")

    # 7a — unauthenticated → 401
    conn = FakeReliabilityConn(camera_account_id=uuid4())
    client = client_for(conn, account_id=uuid4(), authenticated=False)
    r = client.get(f"/v1/cameras/{uuid4()}/reliability/summary")
    assert r.status_code == 401, (r.status_code, r.text)
    ok("unauthenticated → 401", r.json()["error"]["code"])

    # 7b — unknown camera → 404 camera_not_found
    account = uuid4()
    conn = FakeReliabilityConn(camera_account_id=None)
    client = client_for(conn, account_id=account)
    r = client.get(f"/v1/cameras/{uuid4()}/reliability/summary")
    assert r.status_code == 404, (r.status_code, r.text)
    assert r.json()["error"]["code"] == "camera_not_found", r.json()
    ok("unknown camera → 404 camera_not_found")

    # 7c — cross-account camera → 404 camera_not_found (not 403; deny enumeration)
    caller_account, other_owner = uuid4(), uuid4()
    conn = FakeReliabilityConn(camera_account_id=other_owner)
    client = client_for(conn, account_id=caller_account)
    r = client.get(f"/v1/cameras/{uuid4()}/reliability/summary")
    assert r.status_code == 404, (r.status_code, r.text)
    assert r.json()["error"]["code"] == "camera_not_found", r.json()
    ok("cross-account camera → 404 camera_not_found", "no enumeration leak")

    # 7d — same for findings endpoint
    r = client.get(f"/v1/cameras/{uuid4()}/reliability/findings")
    assert r.status_code == 404, (r.status_code, r.text)
    ok("findings endpoint also 404s cross-account")

    print()
    print(f"{GREEN}PASS{RESET} — reliability slice smoke complete")


if __name__ == "__main__":
    os.environ.setdefault("UPSTREAM_ENVIRONMENT", "development")
    main()
