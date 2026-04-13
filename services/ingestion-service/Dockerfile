FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir paho-mqtt==2.1.0 psycopg2-binary==2.9.9
COPY ingestion.py .
CMD ["python", "ingestion.py"]
