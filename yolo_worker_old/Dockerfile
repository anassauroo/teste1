FROM python:3.11-slim

# Dependências do OpenCV/ffmpeg para RTSP
RUN apt-get update && apt-get install -y --no-install-recommends \
    libglib2.0-0 libgl1 libsm6 libxext6 libxrender1 ffmpeg \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY utils/ utils/
COPY processors/ processors/
COPY trackers/ trackers/
COPY app.py .

ENV KAFKA_BROKER=kafka:9092
ENV WORKER_ID=yolo-wkr-001
ENV MAX_CONCURRENCY=1
ENV CAP_FAMILIES=ultralytics
ENV CAP_YOLO_VERSIONS=v8
ENV CAP_ACCEL=cpu
ENV ARCH=amd64

CMD ["python", "app.py"]
