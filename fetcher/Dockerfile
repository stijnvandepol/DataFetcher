FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends p7zip-full && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir flask requests orjson psycopg2-binary gunicorn

COPY fetcher.py .

CMD ["python", "fetcher.py"]
