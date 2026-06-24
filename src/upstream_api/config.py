from functools import lru_cache
from typing import Literal

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="UPSTREAM_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    port: int = 4000
    environment: Literal["development", "staging", "production"] = "development"
    log_level: str = "INFO"

    database_url: SecretStr

    supabase_url: str
    supabase_anon_key: SecretStr
    supabase_service_role_key: SecretStr
    supabase_jwt_secret: SecretStr

    session_jwt_secret: SecretStr
    streamid_hmac_key: SecretStr

    mediamtx_webhook_secret: SecretStr
    processor_webhook_secret: SecretStr
    # M9.3: optional — when unset, GET /metrics 404s (the endpoint does not
    # exist for that deployment). Deployed envs keep it set.
    prometheus_auth_token: SecretStr | None = None
    # Module 6 M6.3: symmetric key used to pgcrypto-encrypt webhook destination
    # signing secrets at rest. Optional so the server still boots without it
    # (additive); creating a webhook destination requires it (503 otherwise).
    # The future delivery worker (M6.4) decrypts with the same key to sign.
    webhook_secret_enc_key: SecretStr | None = None

    @field_validator("prometheus_auth_token", mode="before")
    @classmethod
    def _empty_prometheus_auth_token_is_unset(cls, v: object) -> object:
        """``UPSTREAM_PROMETHEUS_AUTH_TOKEN=""`` must behave as unset (404)
        rather than configuring a token no request can ever match — the
        bearer check rejects empty credentials, so an empty-string token
        would make /metrics permanently 401. Same posture as the empty
        ``fanout_token_secret`` fix in routers/cameras.py."""
        if isinstance(v, SecretStr):
            return v if v.get_secret_value() else None
        return v if v else None

    # Comma-separated list in env (UPSTREAM_ALLOWED_ORIGINS=a,b,c).
    # Surfaced as ``allowed_origins: list[str]`` below.
    allowed_origins_csv: str = Field(default="http://localhost:3000", alias="ALLOWED_ORIGINS")

    sentry_dsn: str | None = None

    @property
    def allowed_origins(self) -> list[str]:
        return [s.strip() for s in self.allowed_origins_csv.split(",") if s.strip()]

    session_token_ttl_seconds: int = 14400
    db_pool_min_size: int = 5
    db_pool_max_size: int = 20

    # Workflow Engine M4: interval for the in-process time-based rule
    # evaluator loop (camera_offline / no_detections_for). 0 disables the
    # loop entirely — the internal endpoint
    # (POST /v1/internal/workflow-evaluator/run) still works for external
    # cron. Single-instance deployments only; with multiple api-server
    # replicas, disable the loop on all but one (the NOT-EXISTS firing
    # guard makes overlap safe, but redundant evaluation is wasted work).
    workflow_evaluator_interval_seconds: int = 60

    # Admin-panel health history: tick interval for the in-process sampler
    # loop (services/health_sampler.py) that persists component health into
    # system_health_samples. 0 disables. Single-instance posture, same as
    # the workflow evaluator above.
    health_sample_interval_seconds: int = 60

    # Reliability M2b: interval + batch size for the in-process source-liveness
    # sweep (services/reliability_source_liveness.py) that detects silent
    # external sources / offline collectors and opens/resolves cv_issues. 0
    # disables the loop — the internal endpoint
    # (POST /v1/internal/reliability/source-liveness/run) still works for cron.
    # Same gated single-instance posture as the workflow evaluator; OFF in tests
    # because TestClient doesn't run the lifespan.
    source_liveness_sweep_interval_seconds: int = 60
    source_liveness_sweep_limit: int = 500

    # Module 6 M6.4 — in-process webhook delivery worker. Defaults OFF so the
    # outbound capability is a deliberate per-deploy opt-in (set
    # UPSTREAM_WEBHOOK_DELIVERY_ENABLED=true in prod). The durable
    # event_delivery_attempts rows are the queue; the loop claims due rows with
    # FOR UPDATE SKIP LOCKED, so it is safe under multiple API workers. The
    # internal drain endpoint works regardless of the loop for cron/tests.
    webhook_delivery_enabled: bool = False
    webhook_delivery_interval_seconds: int = 2
    webhook_delivery_batch_size: int = 25
    webhook_delivery_timeout_seconds: float = 5.0
    webhook_delivery_max_attempts: int = 5

    # Module 7 M2 — Stripe Checkout + Customer Portal. ALL optional: with
    # stripe_secret_key unset, /v1/billing/* and /v1/webhooks/stripe return
    # 503 billing_not_configured and the dashboard shows its "coming soon"
    # fallback — local/dev deployments need no Stripe account. Env names:
    # UPSTREAM_STRIPE_SECRET_KEY, UPSTREAM_STRIPE_WEBHOOK_SECRET,
    # UPSTREAM_STRIPE_PRICE_STARTER_MONTHLY, UPSTREAM_STRIPE_PRICE_PRO_MONTHLY,
    # UPSTREAM_DASHBOARD_BASE_URL.
    stripe_secret_key: SecretStr | None = None
    stripe_webhook_secret: SecretStr | None = None
    stripe_price_starter_monthly: str | None = None
    stripe_price_pro_monthly: str | None = None
    # Where Checkout/Portal send the user back to (no trailing slash).
    dashboard_base_url: str = "http://localhost:3000"

    supabase_storage_bucket: str = "snapshots"
    snapshot_max_bytes: int = 5 * 1024 * 1024  # 5 MB; Caddy must allow >= this on /v1/snapshots

    # SRT ingest endpoint advertised back to the agent in `srt_url`. Defaults
    # point at the public ingest hostname; override per env (dev/staging can
    # set these to a raw IP for a local streaming server).
    # --- Agent version policy (update M2, notify-only) -------------------
    # Server-owned facts the agent compares itself against; shipped on
    # GET /v1/agents/bootstrap as ``agent_version_policy``. ALL optional:
    # with latest+minimum both unset the field is omitted entirely and
    # agents behave exactly as before. Strings are display facts — the
    # agent never executes install_hint.
    agent_latest_version: str | None = None
    agent_min_supported_version: str | None = None
    agent_update_severity_hint: str | None = None
    agent_release_notes_url: str | None = None
    agent_install_hint: str | None = None

    srt_ingest_host: str = "ingest.upstreamcv.com"
    srt_ingest_port: int = 18890

    # MediaMTX base URL for the WHEP read-path proxy. The api-server forwards
    # SDP offers from authenticated dashboard users to this server's WHEP
    # endpoint. MUST be reachable only from the api-server in production —
    # MediaMTX's read path is otherwise unauthenticated. Local dev points at
    # the host-mapped port in upstream-streaming-server/docker-compose.yml.
    mediamtx_base_url: str = "http://localhost:8889"

    # MediaMTX HTTP API base URL for per-session path provisioning (Phase 2).
    # Used by `services/mediamtx_provisioning.py` to PATCH a per-path
    # `srtPublishPassphrase` at session start and DELETE the path at session
    # end. Reaches MediaMTX's `:9997` admin port — must be unreachable from
    # the public internet (firewall + `authHTTPExclude` carve-out cover this
    # in production; the streaming-server only accepts `api` actions from the
    # AWS api-server's Elastic IP).
    mediamtx_api_base_url: str = "http://localhost:9997"

    # M8.0 — base of the internal MediaMTX RTSP read path advertised to a
    # processor in `GET /v1/internal/processor/eligible-sessions`. The full
    # URL is built as ``{mediamtx_rtsp_base}/{stream_key}`` where stream_key
    # is the session's MediaMTX path (the `publish:` prefix stripped from
    # `streamid`). This is the *read* surface, NOT the SRT publish URL —
    # `srt_url` is never returned to a processor. Like `mediamtx_base_url`
    # this must be reachable only from trusted internal hosts (MediaMTX has
    # no auth on the RTSP read path; it is firewalled in production).
    #   local/container: rtsp://host.docker.internal:8554
    #   same docker net: rtsp://mediamtx:8554
    #   deployed:        rtsp://<internal-streaming-host>:8554
    mediamtx_rtsp_base: str = "rtsp://host.docker.internal:8554"

    # Perception fan-out hot path (live detection overlays at 10-15 Hz).
    # Both optional: with either unset,
    # POST /v1/cameras/{id}/perception/stream-token returns 503
    # perception_stream_not_configured (same pattern as
    # billing_not_configured) and the dashboard silently stays on its
    # Realtime + polling fallback.
    #
    # fanout_token_secret must MATCH the sidecar's
    # UPSTREAM_FANOUT_TOKEN_SECRET (upstream-streaming-server) — this
    # server mints subscriber tokens, the sidecar verifies them.
    # fanout_public_ws_url is the browser-reachable base of the sidecar;
    # the WS subscribe URL is advertised as
    # ``{fanout_public_ws_url}/v1/subscribe`` in the token response so
    # the dashboard needs no NEXT_PUBLIC_* env of its own.
    #   prod:      wss://ingest.upstreamcv.com/perception
    #   local dev: ws://127.0.0.1:18990
    fanout_token_secret: SecretStr | None = None
    fanout_public_ws_url: str | None = None
    fanout_token_ttl_seconds: int = 600

    # P5-K — processing-package catalog gate. When true, the package catalog
    # hides non-first-party packages (AGPL/restrictive models that Upstream may
    # not auto-provide as first-party hosted packages until legal/commercial
    # approval). Default false so dev/demo can use them; flip true at go-live.
    catalog_first_party_only: bool = False

    @property
    def is_production(self) -> bool:
        return self.environment == "production"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
