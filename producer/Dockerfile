FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY acc_producer.py .

# Exposed port for UDP telemetry (needs mapping in K8s)
EXPOSE 9003/udp

CMD ["python", "acc_producer.py"]
