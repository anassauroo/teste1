FROM python:3.11-slim

# Evita bytecode e força stdout imediato
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Dependências do sistema (inclui ffmpeg)
RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

# usuário não-root
RUN useradd -m appuser
USER appuser

ENV KAFKA_BROKER=kafka:9092 \
    TOPIC=streams_ativos \
    GROUP_ID=streams-logger

ENTRYPOINT ["python", "app.py"]