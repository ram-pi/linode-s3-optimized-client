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

USER app
WORKDIR /home/app

# Default download directory (writable by the non-root user).
RUN mkdir -p /home/app/downloads

ENTRYPOINT ["python", "-m", "s3_optimized_client"]
CMD ["--help"]