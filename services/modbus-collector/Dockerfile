FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir pymodbus==3.7.4 paho-mqtt==2.1.0
COPY collector.py .
CMD ["python", "collector.py"]
