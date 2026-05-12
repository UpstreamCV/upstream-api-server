# Caddy with the official rate-limit plugin baked in.
# https://github.com/mholt/caddy-ratelimit
FROM caddy:2-builder AS builder
RUN xcaddy build \
    --with github.com/mholt/caddy-ratelimit

FROM caddy:2-alpine
COPY --from=builder /usr/bin/caddy /usr/bin/caddy
