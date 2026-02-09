# app.py
import os, time, json, threading, queue, uuid, cv2
from typing import Optional, Dict, Any, Tuple
from kafka import KafkaProducer, KafkaConsumer
from utils.loader import load_class

# ---------- Config ----------
BROKER          = os.getenv("KAFKA_BROKER", "kafka:9092")
WORKER_ID       = os.getenv("WORKER_ID", f"yolo-wkr-{uuid.uuid4().hex[:4]}")
MAX_CONCURRENCY = int(os.getenv("MAX_CONCURRENCY", "1"))

# capabilities (anúncio)
CAP_FAMILIES    = os.getenv("CAP_FAMILIES", "ultralytics").split(",")
CAP_YOLO_VERS   = os.getenv("CAP_YOLO_VERSIONS", "v8").split(",")
CAP_ACCEL       = os.getenv("CAP_ACCEL", "cpu").split(",")
ARCH            = os.getenv("ARCH", "amd64")

TOPIC_ANNOUNCE  = os.getenv("TOPIC_ANNOUNCE", "workers.announce")
TOPIC_STATUS    = os.getenv("TOPIC_STATUS", "workers.status")
TOPIC_RESULTS   = os.getenv("TOPIC_RESULTS", "results.detections")
TOPIC_JOBS_PREF = os.getenv("TOPIC_PREFIX_JOBS", "jobs.")

# ingest
RTSP_READ_TIMEOUT_SEC = int(os.getenv("RTSP_READ_TIMEOUT_SEC", "5"))
TARGET_FPS            = float(os.getenv("TARGET_FPS", "0"))  # 0 = sem throttling
JPEG_QUALITY          = int(os.getenv("JPEG_QUALITY", "85")) # caso queira enfileirar frame para slow path

# ---------- Kafka ----------
producer = KafkaProducer(
    bootstrap_servers=[BROKER],
    acks=1, linger_ms=0, batch_size=16*1024,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def announce_loop(running_ref):
    while True:
        msg = {
            "worker_id": WORKER_ID,
            "ts": int(time.time()),
            "capabilities": {
                "families": CAP_FAMILIES,
                "yolo_versions": CAP_YOLO_VERS,
                "accel": CAP_ACCEL,
                "arch": ARCH,
                "max_concurrency": MAX_CONCURRENCY
            },
            "load": {"running": running_ref[0]}
        }
        producer.send(TOPIC_ANNOUNCE, msg)
        time.sleep(5)

# ---------- Tracking Loader ----------
def get_tracker(tracking_spec: Optional[Dict[str,Any]]):
    if not tracking_spec or not tracking_spec.get("enabled", False):
        from trackers.passthrough import PassThrough
        return PassThrough()
    algo = tracking_spec.get("algorithm", "passthrough").lower()
    try:
        # ex: trackers.bytetrack.ByteTrack
        cls_name = tracking_spec.get("class") or f"trackers.{algo}.{algo.capitalize()}Track"
        TCls = load_class(cls_name)
        return TCls(**tracking_spec.get("kwargs", {}))
    except Exception:
        from trackers.passthrough import PassThrough
        return PassThrough()

# ---------- Processor Loader ----------
_processors_cache: Dict[Tuple[str,Tuple], Any] = {}

def get_processor(proc_spec: Dict[str,Any]):
    cls_name = proc_spec["class"]
    kwargs   = proc_spec.get("kwargs", {})
    key = (cls_name, tuple(sorted(kwargs.items())))
    if key in _processors_cache:
        return _processors_cache[key]
    Cls = load_class(cls_name)
    inst = Cls(**kwargs)
    if hasattr(inst, "warmup"): inst.warmup()
    _processors_cache[key] = inst
    return inst

# ---------- Ingest com fila de 1 (latência baixa) ----------
class FramePump:
    def __init__(self, rtsp_url: str):
        self.rtsp_url = rtsp_url
        self.q = queue.Queue(maxsize=1)
        self._stop = threading.Event()
        self._t = threading.Thread(target=self._loop, daemon=True)

    def start(self): self._t.start()
    def stop(self):
        self._stop.set()
        try: self.q.put_nowait((None, 0.0))
        except: pass
        self._t.join(timeout=1.0)

    def get(self, timeout=1.0) -> Tuple[Optional[Any], float]:
        try:
            return self.q.get(timeout=timeout)
        except queue.Empty:
            return (None, 0.0)

    def _loop(self):
        # dicas ffmpeg p/ latência (nem todos builds usam):
        cap = cv2.VideoCapture(self.rtsp_url, cv2.CAP_FFMPEG)
        cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
        last_push = 0.0
        while not self._stop.is_set():
            ok, frame = cap.read()
            ts = time.time()
            if not ok:
                # pequeno backoff
                time.sleep(0.05)
                continue
            if TARGET_FPS > 0:
                min_delta = 1.0 / TARGET_FPS
                if ts - last_push < min_delta:
                    continue
            last_push = ts
            if self.q.full():
                try: self.q.get_nowait()
                except: pass
            self.q.put((frame, ts))
        cap.release()

# ---------- Job handler ----------
running_count = [0]

def send_status(job_id: str, stream: str, state: str, error: Optional[str]=None, progress: Optional[dict]=None):
    producer.send(TOPIC_STATUS, {
        "job_id": job_id,
        "worker_id": WORKER_ID,
        "stream": stream,
        "state": state,
        "error": error,
        "progress": progress or {},
        "ts": time.time()
    })

def publish_detections(job_id: str, stream: str, worker_id: str, ts: float, frame_shape, dets):
    h, w = frame_shape[:2]
    # envia um evento por frame com a lista de objetos (pode trocar para 1 evento por objeto se quiser)
    event = {
        "job_id": job_id,
        "stream": stream,
        "event_time": ts,
        "frame_id": f"f_{int(ts*1000)}",
        "frame": {"w": int(w), "h": int(h), "url": None},
        "detections": dets,
        "worker_id": worker_id,
        "producer_ts": time.time()
    }
    producer.send(TOPIC_RESULTS, key=stream.encode(), value=event)

def process_job(job: Dict[str,Any]):
    running_count[0] += 1
    job_id = job["job_id"]
    stream_name = job["stream"]["name"]
    rtsp_url = job["stream"]["rtsp"]
    proc_spec = job["processor"]
    tracking_spec = job.get("tracking")

    send_status(job_id, stream_name, "running")
    alive = True
    hb = threading.Thread(target=_heartbeat, args=(job_id, stream_name, lambda: alive), daemon=True)
    hb.start()

    try:
        processor = get_processor(proc_spec)
        tracker   = get_tracker(tracking_spec)

        pump = FramePump(rtsp_url)
        pump.start()

        last_seen = time.time()
        while True:
            frame, ts = pump.get(timeout=1.0)
            if frame is None:
                # timeout de leitura?
                if time.time() - last_seen > RTSP_READ_TIMEOUT_SEC:
                    raise RuntimeError("timeout lendo RTSP")
                continue
            last_seen = ts

            dets = processor.process_frame(frame, ts)
            dets = tracker.update(dets, frame, ts)
            publish_detections(job_id, stream_name, WORKER_ID, ts, frame.shape, dets)

            # aqui o job roda contínuo; se quiser “por amostra”, adicione condição de saída

    except Exception as e:
        send_status(job_id, stream_name, "error", error=str(e))
    finally:
        alive = False
        running_count[0] -= 1
        send_status(job_id, stream_name, "done")

def _heartbeat(job_id: str, stream: str, alive_fn):
    while alive_fn():
        send_status(job_id, stream, "running", progress={})
        time.sleep(5)

# ---------- Main ----------
def main():
    # announce
    threading.Thread(target=announce_loop, args=(running_count,), daemon=True).start()

    # consumer dedicado a este worker
    topic = f"{TOPIC_JOBS_PREF}{WORKER_ID}"
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=[BROKER],
        group_id=f"g-{WORKER_ID}",
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )
    print(f"[worker {WORKER_ID}] ouvindo {topic} em {BROKER}")

    for msg in consumer:
        job = msg.value
        # simples: um job de cada vez (MAX_CONCURRENCY=1)
        # se quiser N, crie um pool de threads e respeite running_count < MAX_CONCURRENCY
        process_job(job)

if __name__ == "__main__":
    main()
