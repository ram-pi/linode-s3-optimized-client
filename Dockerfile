# syntax=docker/dockerfile:1.7
# Multi-stage build for the S3 optimized client.
# Uses uv for dependency management and a slim final image.

ARG PYTHON_VERSION=3.12

# ---------------------------------------------------------------------------
# Build stage: install uv, sync dependencies into a venv
# ---------------------------------------------------------------------------
FROM python:${PYTHON_VERSION}-slim AS builder

ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_PYTHON_DOWNLOADS=never \
    PYTHONUNBUFFERED=1

# Install uv (pinned via astral-sh keyless setup).
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

WORKDIR /app

# Install dependencies first (better layer caching).
COPY pyproject.toml uv.lock* ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --no-editable || uv sync --no-install-project --no-editable

# Copy the source and install the project itself (non-editable so the package
# is copied into the venv and survives into the final image).
COPY src/ ./src/
COPY README.md ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-editable || uv sync --no-editable

# ---------------------------------------------------------------------------
# Final stage: minimal runtime image
# ---------------------------------------------------------------------------
FROM python:${PYTHON_VERSION}-slim AS final

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/opt/venv/bin:${PATH}"

# Create a non-root user.
RUN groupadd --system --gid 1001 app \
    && useradd --system --uid 1001 --gid app --home-dir /home/app --create-home app

# Copy the venv from the builder.
COPY --from=builder --chown=app:app /opt/venv /opt/venv

WORKDIR /home/app

# Default download directory (writable by the non-root user).
RUN mkdir -p /home/app/downloads

# Entrypoint: as root, chown mounted volumes so the non-root user can write,
# then exec the tool as user "app" using runuser (preserves args correctly).
COPY <<'EOF' /usr/local/bin/entrypoint.sh
#!/bin/sh
set -e

# Chown common output mount points so the non-root "app" user can write.
for dir in /tmp/downloads /home/app/downloads /data /output; do
    [ -d "$dir" ] && chown -R app:app "$dir" 2>/dev/null || true
done

# Parse --output / -o from args and chown its parent directory.
prev=""
for arg in "$@"; do
    if [ "$prev" = "--output" ] || [ "$prev" = "-o" ]; then
        parent=$(dirname "$arg")
        [ -d "$parent" ] && chown -R app:app "$parent" 2>/dev/null || true
    fi
    prev="$arg"
done

exec runuser -u app -- python -m s3_optimized_client "$@"
EOF
RUN chmod +x /usr/local/bin/entrypoint.sh

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["--help"]