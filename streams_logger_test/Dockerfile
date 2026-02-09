FROM python:3.11-slim

# Instala ffmpeg para capturar frame via RTSP
RUN apt-get update && apt-get install -y ffmpeg && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONPATH=/app
ENV KAFKA_BROKER=kafka:9092

# Garante que a pasta de frames exista dentro do container
RUN mkdir -p /app/frames

CMD ["python", "app.py"]
