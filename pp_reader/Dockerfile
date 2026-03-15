# Multi-stage Dockerfile for PP Reader
# Uses COPY + BUILD_VERSION ARG for cache-busting in HA's DinD environment

# ── Cache-buster: change BUILD_VERSION to force a full rebuild ──
ARG BUILD_VERSION=dev

# ============================================================================
# Stage 1: Node.js frontend builder
# ============================================================================
FROM node:20-slim AS frontend-builder

WORKDIR /app/frontend

# Copy dependency spec first (cached unless deps change)
COPY frontend/package.json ./

# Install Node dependencies (cached unless package.json changes)
RUN npm install

# Copy all frontend source (last — changes frequently, small layer)
COPY frontend/ ./

# Build frontend
RUN npm run build

# ============================================================================
# Stage 2: Python backend builder
# ============================================================================
FROM python:3.13-slim AS python-builder

ARG BUILD_VERSION=dev
LABEL io.hacs.version="${BUILD_VERSION}"

WORKDIR /app/backend

# Install system dependencies (rarely changes — cached)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency spec first (cached unless deps change)
COPY backend/pyproject.toml ./pyproject.toml

# Install Python dependencies (cached unless pyproject.toml changes)
RUN pip install --no-cache-dir tomli 2>/dev/null; \
    pip install --no-cache-dir \
    "fastapi>=0.104.0" \
    "uvicorn[standard]>=0.24.0" \
    "pydantic>=2.0" \
    "pydantic-settings>=2.0" \
    "asyncpg>=0.29.0" \
    "sqlalchemy[asyncio]>=2.0" \
    "alembic>=1.13.0" \
    "protobuf>=4.25.0" \
    "yahooquery>=2.3.0" \
    "aiohttp>=3.9.0" \
    "python-dateutil>=2.8.2" \
    "grpcio-tools>=1.59.0"

# Copy proto schema and generate stubs
COPY proto/ /app/proto/
RUN mkdir -p app/generated && \
    python -m grpc_tools.protoc -I/app/proto --python_out=app/generated /app/proto/client.proto

# Copy all backend source (last — changes frequently, small layer)
COPY backend/ ./

# Install package in editable mode (now with all source present)
RUN pip install --no-cache-dir -e .

# ============================================================================
# Stage 3: Runtime image
# ============================================================================
FROM python:3.13-slim

ARG BUILD_VERSION=dev
LABEL io.hacs.version="${BUILD_VERSION}"

WORKDIR /app

# Install runtime dependencies (curl for HEALTHCHECK, postgresql for local DB mode)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    postgresql \
    && rm -rf /var/lib/apt/lists/*

# Copy Python environment from builder
COPY --from=python-builder /usr/local/lib/python3.13/site-packages /usr/local/lib/python3.13/site-packages
COPY --from=python-builder /usr/local/bin /usr/local/bin

# Copy Python app directory structure from builder (includes generated proto stubs)
COPY --from=python-builder /app/backend /app/backend

# Copy frontend build artifacts to static serving path
COPY --from=frontend-builder /app/frontend/dist /app/static

# Copy entrypoint script
COPY docker/run.sh /run.sh
RUN chmod +x /run.sh

WORKDIR /app/backend

# Health check (start-period=90s to allow first-run initdb ~15-20s overhead)
HEALTHCHECK --interval=30s --timeout=10s --start-period=90s --retries=3 \
    CMD curl -f http://localhost:8000/healthz || exit 1

# Entrypoint: starts local PostgreSQL or assembles DATABASE_URL, then execs uvicorn
CMD ["/run.sh"]
