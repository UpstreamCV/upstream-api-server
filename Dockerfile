FROM python:3.11-slim AS builder

ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never

COPY --from=ghcr.io/astral-sh/uv:0.5 /uv /uvx /bin/

WORKDIR /app
COPY pyproject.toml uv.lock* ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --no-dev

COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev


FROM python:3.11-slim AS runtime

RUN groupadd --system --gid 1001 app && \
    useradd --system --uid 1001 --gid app --home-dir /app --shell /sbin/nologin app

WORKDIR /app
COPY --from=builder --chown=app:app /app /app

# WORKDIR creates /app owned by root before COPY runs, and COPY --chown only
# affects copied entries, not the parent dir. gunicorn 26+ writes a control
# socket at $HOME/.gunicorn (HOME=/app for the `app` user), so /app itself
# must be writable by `app`.
RUN chown app:app /app

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

USER app
EXPOSE 4000

CMD ["gunicorn", "upstream_api.main:app", \
     "--worker-class", "uvicorn.workers.UvicornWorker", \
     "--workers", "2", \
     "--bind", "0.0.0.0:4000", \
     "--access-logfile", "-", \
     "--error-logfile", "-"]
