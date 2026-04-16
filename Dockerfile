# =============================================================================
# Multi-Source ETL Pipeline — Dockerfile
# =============================================================================
# Builds a reproducible batch-runner image for the Python ETL pipeline.
#
# Build:
#   docker build -t etl-pipeline .
#   docker compose build etl-pipeline       # via Compose
#   make docker-build                        # via Makefile
#
# Run (full refresh):
#   make docker-run
#   docker compose run --rm etl-pipeline --full-refresh
#
# Run a single stage:
#   docker compose run --rm etl-pipeline --stage silver
#
# Open a shell inside the container:
#   make docker-exec
# =============================================================================

# ---------------------------------------------------------------------------
# Build-time ARGs
# ---------------------------------------------------------------------------
# PYTHON_VERSION — controls the base image tag in all stages.
#   Default: 3.12 (latest stable; satisfies pyproject.toml requires >=3.10,
#   ships smaller than 3.10-slim, and provides optimised wheels for all deps).
#   Override: docker build --build-arg PYTHON_VERSION=3.11 .
#
# UID — host UID for the non-root container user.
#   Default: 1000 (primary user on most Linux systems).
#   Override to match your host UID to avoid bind-mount ownership issues:
#     docker build --build-arg UID=$(id -u) .
ARG PYTHON_VERSION=3.12
ARG UID=1000


# =============================================================================
# Stage 1 — deps
# Install Python wheels into an isolated prefix.  This layer is cached
# independently from source code so that editing main.py or src/ does NOT
# invalidate the expensive pip install step.  Any change to requirements.txt
# will bust this layer and trigger a full reinstall.
# =============================================================================
FROM python:${PYTHON_VERSION}-slim AS deps

WORKDIR /install

# Copy only the dependency manifest — tight cache key.
COPY requirements.txt .

# Upgrade pip for the fastest modern resolver, then install all packages
# into /install so they can be copied cleanly into the runtime stage.
# --no-cache-dir prevents pip from writing its download cache to disk,
# keeping the layer size minimal (saves ~200–400 MB for this dep set).
RUN pip install --upgrade pip --no-cache-dir \
 && pip install \
        --no-cache-dir \
        --prefix=/install \
        -r requirements.txt


# =============================================================================
# Stage 2 — runtime
# Minimal final image: Python interpreter + installed wheels + app source.
# No pip, no build tools, no test fixtures.
# =============================================================================
FROM python:${PYTHON_VERSION}-slim AS runtime

# Re-declare ARG after FROM — build ARGs do not cross stage boundaries.
ARG UID=1000

# ---------------------------------------------------------------------------
# System packages
# libpq5       — shared library required by psycopg2-binary at runtime
# ca-certificates — required for HTTPS calls to Open-Meteo, Frankfurter,
#                   Kaggle.  The slim base strips these by default.
# Cleaned in the same RUN layer to avoid caching the apt lists in the image.
# ---------------------------------------------------------------------------
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        libpq5 \
        ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# ---------------------------------------------------------------------------
# Non-root user
# Running as root inside a container violates CIS Docker Benchmark L1.
# A fixed UID makes bind-mount file ownership predictable on the host side.
# ---------------------------------------------------------------------------
RUN groupadd --gid ${UID} etl \
 && useradd --uid ${UID} --gid etl --no-create-home --shell /sbin/nologin etl

# ---------------------------------------------------------------------------
# Application layout
# ---------------------------------------------------------------------------
WORKDIR /app

# Copy installed packages from the deps stage.
# This preserves cache: source changes skip pip entirely on rebuild.
COPY --from=deps /install /usr/local

# Copy application source.  data/ and logs/ arrive via bind mounts at runtime.
COPY main.py  ./
COPY src/     ./src/
COPY sql/     ./sql/

# Pre-create runtime directories so file writes never raise FileNotFoundError
# even when no volume is mounted (e.g., during a quick smoke test).
RUN mkdir -p \
        data/bronze/olist \
        data/bronze/weather \
        data/bronze/fx \
        data/bronze/manual \
        data/silver \
        data/gold/dimensions \
        data/gold/facts \
        logs \
 && chown -R etl:etl /app

# Switch to the non-root user for the remainder of the image and at runtime.
USER etl

# ---------------------------------------------------------------------------
# Runtime environment
# ---------------------------------------------------------------------------
# PYTHONUNBUFFERED=1        — flush stdout/stderr immediately so docker logs
#                             shows pipeline output in real time.
# PYTHONDONTWRITEBYTECODE=1 — suppress __pycache__ inside the container.
# PYTHONPATH=/app           — makes `src/` importable as a top-level namespace
#                             without an editable install or setup.py.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app

# ---------------------------------------------------------------------------
# Health check
# The pipeline is a batch runner, not a server, so there is no HTTP endpoint.
# We verify that core dependencies imported correctly at build time.
# ---------------------------------------------------------------------------
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=1 \
    CMD python -c "import pandas; import sqlalchemy; import loguru" || exit 1

# ---------------------------------------------------------------------------
# Entry point
# ENTRYPOINT locks the executable; CMD provides the default argument set.
# Pass different flags by overriding CMD:
#   docker compose run --rm etl-pipeline --stage silver
#   docker compose run --rm etl-pipeline --incremental
# ---------------------------------------------------------------------------
ENTRYPOINT ["python", "main.py"]
CMD ["--full-refresh"]
