FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY minio_consumer.py .
CMD ["python", "-u", "minio_consumer.py"]
