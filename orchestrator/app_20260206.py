import os
import json
import signal
import socket
import re
import subprocess
import shutil
from pathlib import Path
from datetime import datetime
import asyncio
import time

import numpy as np
import cv2

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer


# =========================
# Config Kafka (consumer + optional producer)
# =========================
BROKERS = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = os.getenv("TOPIC", "streams_ativos")
GROUP_ID = os.getenv("GROUP_ID", "streams-logger")
CLIENT_ID = os.getenv("CLIENT_ID", socket.gethostname())

ALERTS_ENABLED = os.getenv("ALERTS_ENABLED", "1").strip() not in ("0", "false", "False", "FALSE")
ALERTS_TOPIC = os.getenv("ALERTS_TOPIC", "alerts_movimento")


# =========================
# MediaMTX / Frames
# =========================
MEDIAMTX_HOST = os.getenv("MEDIAMTX_HOST", "mediamtx")
MEDIAMTX_RTSP_PORT = int(os.getenv("MEDIAMTX_RTSP_PORT", "8554"))

FRAMES_DIR = Path(os.getenv("FRAMES_DIR", "/app/frames"))
FRAMES_DIR.mkdir(parents=True, exist_ok=True)


# =========================
# Captura periódica
# =========================
CAPTURE_INTERVAL_S = float(os.getenv("CAPTURE_INTERVAL_S", "1.0"))
CAPTURE_TIMEOUT_S = int(os.getenv("CAPTURE_TIMEOUT_S", "20"))
CAPTURE_JPEG_Q = os.getenv("CAPTURE_JPEG_Q", "2")
CAPTURE_ENABLED = os.getenv("CAPTURE_ENABLED", "1").strip() not in ("0", "false", "False", "FALSE")


# =========================
# Detecção de movimento
# =========================
MOTION_ENABLED = os.getenv("MOTION_ENABLED", "1").strip() not in ("0", "false", "False", "FALSE")

# motion_ratio ~ fração de pixels "ativos" após thresholding
MOTION_THRESHOLD = float(os.getenv("MOTION_THRESHOLD", "0.03"))  # 3% é um bom ponto de partida
MOTION_DIFF_THRESHOLD = int(os.getenv("MOTION_DIFF_THRESHOLD", "25"))  # limiar do absdiff para binarização
MOTION_BLUR = int(os.getenv("MOTION_BLUR", "5"))  # 5 ou 7 ajudam contra ruído

# Evita gerar evento a cada frame
MOTION_MIN_INTERVAL_S = float(os.getenv("MOTION_MIN_INTERVAL_S", "5"))

# Se 1, salva TODO frame periodicamente (além de eventos). Se 0, salva só eventos.
SAVE_ALL_FRAMES = os.getenv("SAVE_ALL_FRAMES", "0").strip() not in ("0", "false", "False", "FALSE")

# Logar ratio em todo frame pode poluir; deixe 0 para logar só eventos
MOTION_LOG_EVERY_FRAME = os.getenv("MOTION_LOG_EVERY_FRAME", "0").strip() not in ("0", "false", "False", "FALSE")


# =========================
# Controle / estado
# =========================
shutdown = False

# tasks por stream
capture_tasks: dict[str, asyncio.Task] = {}

# estado por stream
last_gray: dict[str, np.ndarray] = {}
last_event_ts: dict[str, float] = {}

# producer (optional)
producer: AIOKafkaProducer | None = None


def handle_signal(signum, frame):
    global shutdown
    shutdown = True
    print("\n[consumer] encerrando...")

signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)


def _sanitize_path(p: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]+", "_", p.replace("/", "_"))

def _date_dir() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d")

def _ts_filename() -> str:
    return datetime.utcnow().strftime("%H-%M-%S_%f")[:-3]

def _extract_stream_names(streams):
    if not streams:
        return []

    if isinstance(streams[0], str):
        return [s for s in streams if isinstance(s, str) and s.strip()]

    if isinstance(streams[0], dict):
        names = []
        for s in streams:
            if not isinstance(s, dict):
                continue
            n = s.get("nome") or s.get("source_id")
            if isinstance(n, str) and n.strip():
                names.append(n)
        return names

    return [str(s) for s in streams]


def capture_one_frame_to_tmp(stream_path: str) -> Path | None:
    """
    Captura 1 frame via RTSP e salva em um arquivo temporário (sobrescreve):
      /app/frames/<stream>/tmp/latest.jpg
    Retorna Path do tmp, ou None se falhar.
    """
    stream_dir = FRAMES_DIR / _sanitize_path(stream_path)
    tmp_dir = stream_dir / "tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)

    out_file = tmp_dir / "latest.jpg"
    rtsp_url = f"rtsp://{MEDIAMTX_HOST}:{MEDIAMTX_RTSP_PORT}/{stream_path}"

    cmd = [
        "ffmpeg",
        "-y",
        "-loglevel", "error",
        "-rtsp_transport", "tcp",
        "-i", rtsp_url,
        "-frames:v", "1",
        "-q:v", str(CAPTURE_JPEG_Q),
        str(out_file),
    ]

    try:
        subprocess.run(cmd, check=True, timeout=CAPTURE_TIMEOUT_S)
        return out_file
    except FileNotFoundError:
        print("[frames] ffmpeg não encontrado no container (verifique Dockerfile).")
    except subprocess.TimeoutExpired:
        print(f"[frames] timeout ao capturar 1 frame de '{stream_path}' ({CAPTURE_TIMEOUT_S}s)")
    except subprocess.CalledProcessError as e:
        print(f"[frames] falha ao capturar 1 frame de '{stream_path}': {e}")

    return None


def save_frame(stream_path: str, src: Path, prefix: str = "frame") -> Path:
    """
    Copia o frame tmp para o diretório "final" por dia:
      /app/frames/<stream>/<YYYY-MM-DD>/<prefix>_<HH-MM-SS_mmm>.jpg
    """
    out_dir = FRAMES_DIR / _sanitize_path(stream_path) / _date_dir()
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"{prefix}_{_ts_filename()}.jpg"
    shutil.copy2(src, out_file)
    return out_file


def compute_motion_ratio(stream_path: str, frame_path: Path) -> float | None:
    """
    Retorna motion_ratio (0..1) usando absdiff entre frames consecutivos do mesmo stream.
    """
    img = cv2.imread(str(frame_path))
    if img is None:
        return None

    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

    # blur para reduzir ruído (deve ser ímpar)
    k = MOTION_BLUR if MOTION_BLUR % 2 == 1 else MOTION_BLUR + 1
    gray = cv2.GaussianBlur(gray, (k, k), 0)

    prev = last_gray.get(stream_path)
    last_gray[stream_path] = gray

    if prev is None or prev.shape != gray.shape:
        # primeiro frame (não dá pra comparar)
        return 0.0

    diff = cv2.absdiff(gray, prev)
    _, th = cv2.threshold(diff, MOTION_DIFF_THRESHOLD, 255, cv2.THRESH_BINARY)

    motion_ratio = float(np.mean(th > 0))
    return motion_ratio


async def publish_alert(stream_path: str, motion_ratio: float, saved_path: Path):
    """
    Publica alerta no Kafka (se habilitado).
    """
    global producer
    if not ALERTS_ENABLED or producer is None:
        return

    payload = {
        "stream": stream_path,
        "timestamp": time.time(),
        "motion_ratio": motion_ratio,
        "frame_path": str(saved_path),
        "date_utc": _date_dir(),
    }

    try:
        await producer.send_and_wait(ALERTS_TOPIC, json.dumps(payload).encode("utf-8"))
    except Exception as e:
        print(f"[alerts] falha ao publicar alerta: {e}")


async def capture_loop(stream_path: str):
    """
    Loop por stream:
      - captura 1 frame (tmp/latest.jpg)
      - calcula motion_ratio
      - salva frames conforme política:
          SAVE_ALL_FRAMES=1 -> salva sempre
          SAVE_ALL_FRAMES=0 -> salva apenas eventos (motion >= threshold + cooldown)
      - opcional: publica alerta Kafka em alerts_movimento
    """
    print(f"[frames] iniciando loop para '{stream_path}' (intervalo={CAPTURE_INTERVAL_S}s)")

    # inicializa cooldown
    last_event_ts.setdefault(stream_path, 0.0)

    try:
        while not shutdown:
            tmp = await asyncio.to_thread(capture_one_frame_to_tmp, stream_path)
            if tmp is None:
                await asyncio.sleep(CAPTURE_INTERVAL_S)
                continue

            # opcional: salvar tudo
            if SAVE_ALL_FRAMES:
                saved = await asyncio.to_thread(save_frame, stream_path, tmp, "frame")
                print(f"[frames] salvo: {saved}")

            # movimento
            if MOTION_ENABLED:
                ratio = await asyncio.to_thread(compute_motion_ratio, stream_path, tmp)
                if ratio is None:
                    await asyncio.sleep(CAPTURE_INTERVAL_S)
                    continue

                now = time.time()
                can_emit = (now - last_event_ts.get(stream_path, 0.0)) >= MOTION_MIN_INTERVAL_S
                is_motion = ratio >= MOTION_THRESHOLD

                if is_motion and can_emit:
                    # salva evento
                    # inclui ratio no nome para facilitar inspeção
                    out_dir = FRAMES_DIR / _sanitize_path(stream_path) / _date_dir()
                    out_dir.mkdir(parents=True, exist_ok=True)
                    event_file = out_dir / f"event_{_ts_filename()}_r{ratio:.4f}.jpg"
                    shutil.copy2(tmp, event_file)

                    last_event_ts[stream_path] = now
                    print(f"[motion] EVENT '{stream_path}' ratio={ratio:.4f} saved={event_file}")

                    await publish_alert(stream_path, ratio, event_file)

                else:
                    if MOTION_LOG_EVERY_FRAME:
                        print(f"[motion] '{stream_path}' ratio={ratio:.4f} < {MOTION_THRESHOLD:.4f} (no-event)")

            await asyncio.sleep(CAPTURE_INTERVAL_S)

    except asyncio.CancelledError:
        print(f"[frames] loop cancelado para '{stream_path}'")
        raise
    except Exception as e:
        print(f"[frames] erro inesperado no loop de '{stream_path}': {e}")
    finally:
        print(f"[frames] finalizando loop de '{stream_path}'")
        # limpeza de estado por stream (opcional)
        last_gray.pop(stream_path, None)
        last_event_ts.pop(stream_path, None)


def summarize_streams(streams):
    names = _extract_stream_names(streams)
    if not names:
        return "∅ sem streams"
    preview = ", ".join(names[:8]) + (" …" if len(names) > 8 else "")
    return f"{len(names)} stream(s): {preview}"


def manage_processing(streams):
    """
    Start/stop do loop por stream baseado no payload do tópico streams_ativos.
    """
    if not CAPTURE_ENABLED:
        return

    names = _extract_stream_names(streams)
    active_now = set(names)
    active_prev = set(capture_tasks.keys())

    # iniciar novos
    for stream_path in (active_now - active_prev):
        capture_tasks[stream_path] = asyncio.create_task(capture_loop(stream_path))

    # cancelar os que saíram
    for stream_path in (active_prev - active_now):
        task = capture_tasks.pop(stream_path, None)
        if task:
            task.cancel()


async def consume(topico, group_id):
    consumer = AIOKafkaConsumer(
        topico,
        bootstrap_servers=BROKERS,
        group_id=group_id,
        client_id=CLIENT_ID,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        auto_offset_reset="latest",
        enable_auto_commit=True
    )

    await consumer.start()
    try:
        async for msg in consumer:
            if shutdown:
                break

            payload = msg.value
            ts = payload.get("timestamp")
            ts_str = datetime.utcfromtimestamp(ts).isoformat() + "Z" if isinstance(ts, (int, float)) else "n/a"
            streams = payload.get("streams", [])
            summary = summarize_streams(streams)

            print("\n" + "=" * 60)
            print(f"[{datetime.utcnow().isoformat()}Z] mensagem recebida")
            print(f"  tópico/partição/offset: {msg.topic}/{msg.partition}/{msg.offset}")
            print(f"  timestamp publisher   : {ts_str}")
            print(f"  resumo                : {summary}")
            # opcional: comentar para reduzir log
            # print(json.dumps(payload, indent=2, ensure_ascii=False))

            manage_processing(streams)

    finally:
        await consumer.stop()


async def shutdown_tasks():
    if capture_tasks:
        for t in capture_tasks.values():
            t.cancel()
        await asyncio.gather(*capture_tasks.values(), return_exceptions=True)
        capture_tasks.clear()


async def main_async():
    global producer

    # producer (opcional)
    if ALERTS_ENABLED:
        producer = AIOKafkaProducer(bootstrap_servers=BROKERS)
        await producer.start()
        print(f"[alerts] producer ativo (topic={ALERTS_TOPIC})")

    try:
        await consume(TOPIC, GROUP_ID)
    finally:
        await shutdown_tasks()
        if producer is not None:
            await producer.stop()
            producer = None


asyncio.run(main_async())
