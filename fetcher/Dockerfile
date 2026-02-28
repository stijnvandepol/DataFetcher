FROM python:3.12.8-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# System deps (p7zip needed at runtime for extraction)
RUN apt-get update && \
    apt-get install -y --no-install-recommends p7zip-full && \
    rm -rf /var/lib/apt/lists/*

# Python deps — pinned to minor version
RUN pip install --no-cache-dir \
    flask==3.1.* \
    requests==2.32.* \
    orjson==3.10.* \
    psycopg2-binary==2.9.* \
    gunicorn==23.*

# Non-root user with writable temp directory for 7z extraction
RUN groupadd -r fetcher && useradd -r -g fetcher -d /app -s /sbin/nologin fetcher && \
    mkdir -p /tmp/fetcher && chown fetcher:fetcher /tmp/fetcher

COPY --chown=fetcher:fetcher fetcher.py .

USER fetcher

EXPOSE 8000

HEALTHCHECK --interval=10s --timeout=3s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/healthz')" || exit 1

CMD ["python", "fetcher.py"]
