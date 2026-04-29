FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY imu_producer.py .
CMD ["python", "-u", "imu_producer.py"]
