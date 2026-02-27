FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir flask psycopg2-binary gunicorn

COPY app.py .
COPY templates/ templates/

EXPOSE 5000

CMD ["gunicorn", "-b", "0.0.0.0:5000", "-w", "4", "--timeout", "30", "app:app"]
