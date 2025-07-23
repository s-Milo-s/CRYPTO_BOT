# ──────────────────────────────────────────────────────────────
# Dockerfile (cached‑deps, two‑stage build)
# ──────────────────────────────────────────────────────────────

##############################
# 1 – “deps” stage
##############################
FROM python:3.11-slim AS deps
WORKDIR /deps

# Keep pip’s wheel cache + bump timeouts (helps flaky mirrors)
ENV PIP_NO_CACHE_DIR=0 \
    PIP_DEFAULT_TIMEOUT=120 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Copy *only* requirements so Docker can cache this layer
COPY requirements.txt .

# Install deps once; subsequent builds reuse this layer
RUN python -m pip install --upgrade pip wheel && \
    python -m pip install --only-binary=:all: -r requirements.txt

##############################
# 2 – final runtime image
##############################
FROM python:3.11-slim
WORKDIR /app

# ── keep original env flags
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# ── copy pre‑installed site‑packages from deps stage
COPY --from=deps /usr/local /usr/local

# ── copy project code
COPY . .

# ── create non‑root user (unchanged)
RUN adduser -u 5678 --disabled-password --gecos "" appuser && \
    chown -R appuser /app
USER appuser

# ── expose FastAPI/Gunicorn port (unchanged)
EXPOSE 8000

# ── default run command (unchanged)
CMD ["gunicorn", "--bind", "0.0.0.0:8000", \
     "-k", "uvicorn.workers.UvicornWorker", "app.main:app"]