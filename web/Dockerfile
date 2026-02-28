FROM python:3.12.8-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Python deps — pinned to minor version
RUN pip install --no-cache-dir \
    flask==3.1.* \
    psycopg2-binary==2.9.* \
    gunicorn==23.* \
    requests==2.32.*

# Non-root user
RUN groupadd -r webapp && useradd -r -g webapp -d /app -s /sbin/nologin webapp

COPY --chown=webapp:webapp app.py ./
COPY --chown=webapp:webapp templates/ templates/

USER webapp

EXPOSE 5000

HEALTHCHECK --interval=10s --timeout=3s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:5000/healthz')" || exit 1

CMD ["gunicorn", "-b", "0.0.0.0:5000", "-w", "2", "--threads", "4", "--timeout", "120", "app:app"]
