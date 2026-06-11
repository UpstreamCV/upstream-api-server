# Deployment

Where this service runs in production and how to reach it. Keep this file in sync with reality — if you destroy or rebuild infra, update here in the same commit.

## Current production target

| Resource | Value |
|---|---|
| Cloud | AWS |
| Region | `us-east-2` (Ohio) |
| Account | `<aws-account-id>` |
| Instance | `<instance-id>` (t4g.small, ARM64) |
| AMI | `<ami-id>` — `al2023-ami-2023.11.20260509.0-kernel-6.1-arm64` |
| Public IP (Elastic) | `<api-ip>` |
| DNS (default) | `<ec2-public-dns>` |
| Custom domain | `api.upstreamcv.com` → `<api-ip>` (A record) |
| Availability zone | `us-east-2b` |
| Root volume | 16 GiB gp3, encrypted, delete-on-termination |
| Security group | `<security-group-id>` (`upstream-api-sg`): 22 / 80 / 443 from `0.0.0.0/0` |
| Key pair (AWS) | `upstream-api` (ed25519) |
| Key pair (local private) | `<ssh-key>` (mode 0400) |
| Elastic IP allocation | `<eip-allocation-id>` |
| Tags | `Name=upstream-api`, `Project=upstream` |

Estimated monthly cost: ~$13 (instance ~$12 + EBS ~$1.30 + EIP free while attached).

## SSH

```sh
ssh -i <ssh-key> ec2-user@<api-ip>
```

User is `ec2-user`. Password authentication is disabled by default on AL2023.

## What is on the box

Installed as of 2026-05-12:
- `git` (2.50.1)
- `docker` (25.0.14) — daemon enabled at boot
- `docker compose` plugin v2 — installed at `/usr/local/lib/docker/cli-plugins/docker-compose`
- `ec2-user` added to the `docker` group
- Repo cloned to `~ec2-user/upstream-api-server` via deploy key (GitHub: read-only)

Deploy config (also tracked in the repo at `deploy/`):
- `~ec2-user/upstream-api-server/deploy/docker-compose.yml` — `api` (FastAPI built from `Dockerfile`) + `caddy` (TLS termination, ports 80/443)
- `~ec2-user/upstream-api-server/deploy/Caddyfile` — reverse-proxies `api.upstreamcv.com` to the `api` container on port 4000; auto Let's Encrypt
- `~ec2-user/upstream-api-server/deploy/.env` — production secrets, mode 0600, **not in git**

Not yet installed (planned):
- systemd unit so `docker compose` comes up on reboot (today: manual `docker compose up -d`)
- AWS Systems Manager Parameter Store for secrets (today: secrets live in the `.env` file on disk)
- Automatic deploys on push to `main` (today: manual `git pull && docker compose up -d --build`)

## Bringing the stack up the first time

After `deploy/.env` has all real values (random secrets are pre-filled; Supabase values come from the dashboard):

```sh
ssh -i <ssh-key> ec2-user@<api-ip>
cd ~/upstream-api-server/deploy
docker compose build api
docker compose up -d
docker compose logs -f
```

In a separate shell:

```sh
# DNS check
dig +short api.upstreamcv.com  # → <api-ip>

# TLS cert is acquired automatically on the first https request
curl -v https://api.upstreamcv.com/healthz
```

## Updating the running service

```sh
cd ~/upstream-api-server
git pull
cd deploy
docker compose up -d --build api
```

Caddy keeps running; only the `api` container restarts.

## Pointing a domain

`api.upstreamcv.com` is currently pointed at `<api-ip>` via an A record. Until Caddy is installed on the box, that hostname will fail to connect on ports 80/443 — that's expected, not a DNS issue.

To verify DNS propagation:
```sh
dig +short api.upstreamcv.com   # should return <api-ip>
```

When Caddy is set up, its Caddyfile will reference `api.upstreamcv.com` and Caddy will auto-fetch a Let's Encrypt cert on first request to port 80.

## Operational notes

- The Elastic IP is what makes the public IP stable across stop/start cycles. **Do not release the EIP unless you also remove the DNS record** — otherwise the next account to allocate that IP could MITM your domain.
- `HttpTokens=required` is set on the instance — IMDSv2-only. Old SDKs / SSRF probes that hit IMDSv1 will fail; this is intentional.
- The account is currently signed in as the **root user**, which is an AWS antipattern. Follow-up: create an IAM user `upstream-deploy` with scoped permissions and rotate the credentials in `~/.aws/`.

## Destroying / rebuilding

```sh
# Stop billing — keeps disk + EIP, can restart later
aws ec2 stop-instances --instance-ids <instance-id>

# Full teardown — releases EIP, terminates instance, deletes volume
aws ec2 terminate-instances --instance-ids <instance-id>
aws ec2 release-address --allocation-id <eip-allocation-id>
aws ec2 delete-security-group --group-id <security-group-id>
aws ec2 delete-key-pair --key-name upstream-api
rm <ssh-key>
```

After teardown, update this file (and `commit-log.md`) so the next agent doesn't ssh into a dead IP.

## Streaming server (separate, not yet provisioned)

When `upstream-streaming-server` lands it will need its own EC2 instance with **UDP** ports open in the security group for SRT ingest. Plan for a second t4g.small (or larger, depending on bitrate × camera count) — do not bolt it onto this box, the concerns are different and the resource needs scale differently.

## Stripe (Module 7 M2)

Optional — with no Stripe env vars the billing endpoints return 503 and
the dashboard shows "Billing management coming soon". To enable:

1. In the Stripe Dashboard create two recurring monthly prices (Starter,
   Pro) and copy their `price_...` IDs.
2. Set on the api-server:
   `UPSTREAM_STRIPE_SECRET_KEY`, `UPSTREAM_STRIPE_PRICE_STARTER_MONTHLY`,
   `UPSTREAM_STRIPE_PRICE_PRO_MONTHLY`,
   `UPSTREAM_DASHBOARD_BASE_URL=https://<your dashboard origin>`.
3. Add a webhook endpoint in Stripe (Developers → Webhooks) pointing at
   `https://<api host>/v1/webhooks/stripe` with events:
   `checkout.session.completed`, `customer.subscription.created`,
   `customer.subscription.updated`, `customer.subscription.deleted`,
   `invoice.payment_failed`. Copy the signing secret into
   `UPSTREAM_STRIPE_WEBHOOK_SECRET`.
4. Apply the `account_billing` migration (`supabase db push`) and deploy.
5. Customer Portal: enable it once at
   Stripe Dashboard → Settings → Billing → Customer portal.

Local testing: `stripe listen --forward-to localhost:4000/v1/webhooks/stripe`
prints a `whsec_...` to use as the webhook secret; pay with card
4242 4242 4242 4242 in test mode. Caddy note: the webhook path must pass
the raw body through untouched (no buffering rewrites) — default reverse
proxying is fine.
