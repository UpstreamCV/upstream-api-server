# Deployment

Where this service runs in production and how to reach it. Keep this file in sync with reality — if you destroy or rebuild infra, update here in the same commit.

## Current production target

| Resource | Value |
|---|---|
| Cloud | AWS |
| Region | `us-east-2` (Ohio) |
| Account | `768125641607` |
| Instance | `i-07434302e4b0785e8` (t4g.small, ARM64) |
| AMI | `ami-0dfd8ee32c49fce7e` — `al2023-ami-2023.11.20260509.0-kernel-6.1-arm64` |
| Public IP (Elastic) | `18.225.104.21` |
| DNS (default) | `ec2-18-225-104-21.us-east-2.compute.amazonaws.com` |
| Custom domain | `api.upstreamcv.com` → `18.225.104.21` (A record) |
| Availability zone | `us-east-2b` |
| Root volume | 16 GiB gp3, encrypted, delete-on-termination |
| Security group | `sg-0236c95fe7b168503` (`upstream-api-sg`): 22 / 80 / 443 from `0.0.0.0/0` |
| Key pair (AWS) | `upstream-api` (ed25519) |
| Key pair (local private) | `~/.ssh/upstream-api.pem` (mode 0400) |
| Elastic IP allocation | `eipalloc-00c601da170d052ae` |
| Tags | `Name=upstream-api`, `Project=upstream` |

Estimated monthly cost: ~$13 (instance ~$12 + EBS ~$1.30 + EIP free while attached).

## SSH

```sh
ssh -i ~/.ssh/upstream-api.pem ec2-user@18.225.104.21
```

User is `ec2-user`. Password authentication is disabled by default on AL2023.

## What is on the box

_Nothing yet besides the AL2023 base image._ Next steps (not yet executed):

1. Install Docker + docker-compose plugin.
2. Install Caddy for TLS termination.
3. Add the FastAPI service via `docker-compose.yml` + a Caddyfile.
4. Pull `UPSTREAM_*` secrets from AWS Systems Manager Parameter Store on boot.
5. Add a systemd unit to start the stack at boot.

Once installed, this section will list the systemd units, the Caddyfile location, the Parameter Store paths, and the deploy command.

## Pointing a domain

`api.upstreamcv.com` is currently pointed at `18.225.104.21` via an A record. Until Caddy is installed on the box, that hostname will fail to connect on ports 80/443 — that's expected, not a DNS issue.

To verify DNS propagation:
```sh
dig +short api.upstreamcv.com   # should return 18.225.104.21
```

When Caddy is set up, its Caddyfile will reference `api.upstreamcv.com` and Caddy will auto-fetch a Let's Encrypt cert on first request to port 80.

## Operational notes

- The Elastic IP is what makes the public IP stable across stop/start cycles. **Do not release the EIP unless you also remove the DNS record** — otherwise the next account to allocate that IP could MITM your domain.
- `HttpTokens=required` is set on the instance — IMDSv2-only. Old SDKs / SSRF probes that hit IMDSv1 will fail; this is intentional.
- The account is currently signed in as the **root user**, which is an AWS antipattern. Follow-up: create an IAM user `upstream-deploy` with scoped permissions and rotate the credentials in `~/.aws/`.

## Destroying / rebuilding

```sh
# Stop billing — keeps disk + EIP, can restart later
aws ec2 stop-instances --instance-ids i-07434302e4b0785e8

# Full teardown — releases EIP, terminates instance, deletes volume
aws ec2 terminate-instances --instance-ids i-07434302e4b0785e8
aws ec2 release-address --allocation-id eipalloc-00c601da170d052ae
aws ec2 delete-security-group --group-id sg-0236c95fe7b168503
aws ec2 delete-key-pair --key-name upstream-api
rm ~/.ssh/upstream-api.pem
```

After teardown, update this file (and `commit-log.md`) so the next agent doesn't ssh into a dead IP.

## Streaming server (separate, not yet provisioned)

When `upstream-streaming-server` lands it will need its own EC2 instance with **UDP** ports open in the security group for SRT ingest. Plan for a second t4g.small (or larger, depending on bitrate × camera count) — do not bolt it onto this box, the concerns are different and the resource needs scale differently.
