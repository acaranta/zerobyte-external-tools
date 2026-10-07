# syntax=docker/dockerfile:1

# --- build: resolve the locked dependencies into a self-contained venv ----------
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS build

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/app/.venv

WORKDIR /src
# Dependencies first so that code changes do not invalidate this layer.
COPY pyproject.toml uv.lock .python-version ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project

COPY README.md ./
COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable

# --- runtime: slim Python, no uv, no build tools, non-root -----------------------
FROM python:3.12-slim-bookworm

ENV PATH=/app/.venv/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    TZ=Europe/Paris \
    HEALTH_PORT=8080

COPY --from=build /app/.venv /app/.venv

RUN groupadd --system --gid 10001 zbsched \
    && useradd --system --uid 10001 --gid zbsched --no-create-home --shell /usr/sbin/nologin zbsched

USER 10001:10001
WORKDIR /app

HEALTHCHECK --interval=60s --timeout=5s --start-period=30s --retries=3 \
    CMD ["python", "-c", "import os,sys,urllib.request; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:%s/healthz' % os.environ.get('HEALTH_PORT', '8080'), timeout=4).status == 200 else 1)"]

ENTRYPOINT ["python", "-m", "zbsched"]
