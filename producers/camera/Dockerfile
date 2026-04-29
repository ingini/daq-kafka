# producers/camera/Dockerfile
# Tegra gw5300 (UYVY) + GStreamer + appsink 방식
FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    # GStreamer 코어 + V4L2 플러그인
    gstreamer1.0-tools \
    gstreamer1.0-plugins-base \
    gstreamer1.0-plugins-good \
    gstreamer1.0-plugins-bad \
    gstreamer1.0-libav \
    libgstreamer1.0-dev \
    libgstreamer-plugins-base1.0-dev \
    # Python GObject (gi.repository)
    python3-gi \
    python3-gst-1.0 \
    gir1.2-gstreamer-1.0 \
    gir1.2-gst-plugins-base-1.0 \
    # OpenCV 의존성
    libgl1 libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY cam_producer.py .

CMD ["python", "-u", "cam_producer.py"]
