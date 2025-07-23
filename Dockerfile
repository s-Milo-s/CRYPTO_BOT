# -----------------------------------------------------------------------------
# Dockerfile  
# -----------------------------------------------------------------------------

# ➊  Use Python 3.11 so redbeat 2.x is compatible
FROM python:3.11-slim

# ➋  Expose FastAPI / Gunicorn port
EXPOSE 8000

# ➌  Prevent .pyc files + enable unbuffered logs
ENV PYTHONDONTWRITEBYTECODE=1\
    PYTHONUNBUFFERED=1\
    PIP_DEFAULT_TIMEOUT=60 \
    PIP_RETRIES=10\
    PIP_INDEX_URL=https://pypi.python.org/simple

# ➍  Install Python deps
COPY requirements.txt .
RUN python -m pip install --upgrade pip wheel && \
    python -m pip install -v --prefer-binary --use-feature=fast-deps -r requirements.txt

# ➎  Copy project code
WORKDIR /app
COPY . /app

# ➏  Create non‑root user 
RUN adduser -u 5678 --disabled-password --gecos "" appuser \
    && chown -R appuser /app
USER appuser

# ➐  Default run command 
CMD ["gunicorn", "--bind", "0.0.0.0:8000", "-k", "uvicorn.workers.UvicornWorker", "app.main:app"]
