import os
import json
import time
import signal
import socket
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import asyncio
from aiokafka import AIOKafkaConsumer
import subprocess
from pathlib import Path

MEDIAMTX_HOST = os.getenv("MEDIAMTX_HOST", "mediamtx")
MEDIAMTX_RTSP_PORT = int(os.getenv("MEDIAMTX_RTSP_PORT", "8554"))

FRAMES_DIR = Path("/app/frames")
FRAMES_DIR.mkdir(parents=True, exist_ok=True)

# Guarda quais streams já tiveram frame capturado
captured_streams = set()

BROKERS = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = os.getenv("TOPIC", "streams_ativos")
GROUP_ID = os.getenv("GROUP_ID", "streams-logger")
CLIENT_ID = os.getenv("CLIENT_ID", socket.gethostname())

shutdown = False

def handle_signal(signum, frame):
    global shutdown
    shutdown = True
    print("\n[consumer] encerrando...")

signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)

def connect_consumer(retries=30, delay=3):
    last_err = None
    for attempt in range(1, retries+1):
        try:
            print(f"[consumer] tentando conectar ao Kafka ({BROKERS}) – tentativa {attempt}/{retries}")
            c = KafkaConsumer(
                TOPIC,
                bootstrap_servers=[b.strip() for b in BROKERS.split(",") if b.strip()],
                group_id=GROUP_ID,
                client_id=CLIENT_ID,
                enable_auto_commit=True,
                auto_offset_reset=os.getenv("AUTO_OFFSET_RESET", "latest"),  # 'earliest' p/ histórico
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                key_deserializer=lambda k: k.decode("utf-8") if k else None,
                consumer_timeout_ms=1000
            )
            print("[consumer] conectado")
            return c
        except NoBrokersAvailable as e:
            last_err = e
            print(f"[consumer] broker indisponível: {e}. aguardando {delay}s…")
            time.sleep(delay)
    raise RuntimeError(f"não consegui conectar ao Kafka após {retries} tentativas: {last_err}")

def summarize_streams(streams):
    """
    Aceita:
      - lista de strings: ["cam-a","cam-b"]
      - lista de dicts: [{"nome":..., "source_id":...}, ...]
    """
    if not streams:
        return "∅ sem streams"
    if isinstance(streams[0], str):
        names = streams
    elif isinstance(streams[0], dict):
        # tenta usar nome; se não tiver, cai para source_id
        names = [s.get("nome") or s.get("source_id") or "<?>"
                 for s in streams]
    else:
        names = [str(s) for s in streams]
    preview = ", ".join(names[:8]) + (" …" if len(names) > 8 else "")
    return f"{len(names)} stream(s): {preview}"

def manage_processing(streams):
    print("\n" + "="*60)
    print(f"[{datetime.utcnow().isoformat()}Z] mensagem recebida")

def capture_first_frame(stream_path: str, ts=None):
    """
    Usa ffmpeg pra capturar o primeiro frame de um stream RTSP
    exposto pelo MediaMTX e salvar em /app/frames.
    """
    rtsp_url = f"rtsp://{MEDIAMTX_HOST}:{MEDIAMTX_RTSP_PORT}/{stream_path}"
    safe_name = stream_path.replace("/", "_")

    if isinstance(ts, (int, float)):
        ts_suffix = datetime.utcfromtimestamp(ts).strftime("%Y%m%dT%H%M%SZ")
        filename = f"{safe_name}_{ts_suffix}.jpg"
    else:
        filename = f"{safe_name}_frame1.jpg"

    output_file = FRAMES_DIR / filename
    print(f"[streams-logger-test] Capturando primeiro frame de {rtsp_url} -> {output_file}")

    cmd = [
        "ffmpeg",
        "-y",             # sobrescreve se já existir
        "-i", rtsp_url,
        "-frames:v", "1", # só 1 frame
        "-q:v", "2",      # qualidade boa
        str(output_file),
    ]

    try:
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
        print(f"[streams-logger-test] Frame salvo em {output_file}")
    except subprocess.CalledProcessError as e:
        print(f"[streams-logger-test] ERRO ao capturar frame de {rtsp_url}: {e}")

    

async def consume(topico, group_id):
    consumer = AIOKafkaConsumer(
        topico,
        bootstrap_servers=BROKERS,
        group_id=group_id,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        auto_offset_reset="latest",
        enable_auto_commit=True
    )
    await consumer.start()
    print("[streams-logger-test] Conectado ao Kafka, aguardando mensagens em", TOPIC)
    try:
        async for msg in consumer:
            if shutdown:
                break
            payload = msg.value
            ts = payload.get("timestamp")
            ts_str = datetime.utcfromtimestamp(ts).isoformat()+"Z" if isinstance(ts, (int, float)) else "n/a"
            streams = payload.get("streams", [])
            summary = summarize_streams(streams)
            # todo evaluate processing status
            print(f"  tópico/partição/offset: {msg.topic}/{msg.partition}/{msg.offset}")
            print(f"  timestamp publisher   : {ts_str}")
            print(f"  resumo                : {summary}")
            # JSON bruto:
            print(json.dumps(payload, indent=2, ensure_ascii=False))
            for s in streams:
                path = None

                if isinstance(s, str):
                    path = s
                elif isinstance(s, dict):
                    for key in ("path", "name", "stream", "id"):
                        if key in s:
                            path = str(s[key])
                            break

                if not path:
                    continue

                key = (path, ts)

                if key not in captured_streams:
                    capture_first_frame(path, ts)
                    captured_streams.add(key)
                    
            manage_processing(streams)
    finally:
        await consumer.stop()

async def main_async():
    await asyncio.gather(
        consume(TOPIC, GROUP_ID),
        #consume("streams_eventos", "grp-eventos")
    )

asyncio.run(main_async())

