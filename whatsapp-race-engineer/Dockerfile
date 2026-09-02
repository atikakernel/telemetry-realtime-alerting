FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ ./app/

# Generated charts/diagrams are written here at runtime
RUN mkdir -p /app/generated

EXPOSE 8000

# PORT is overridable at runtime (e.g. -e PORT=8001 when 8000 is taken)
CMD ["sh", "-c", "uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000}"]
