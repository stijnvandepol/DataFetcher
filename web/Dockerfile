FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends p7zip-full && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir flask psycopg2-binary gunicorn requests orjson

COPY app.py fetcher_lib.py ./
COPY templates/ templates/

EXPOSE 5000

CMD ["gunicorn", "-b", "0.0.0.0:5000", "-w", "1", "--threads", "4", "--timeout", "600", "app:app"]
