import os

BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")

TOPIC_STREAMS   = os.getenv("TOPIC_STREAMS", "streams_ativos")
TOPIC_ANNOUNCE  = os.getenv("TOPIC_ANNOUNCE", "workers.announce")
TOPIC_STATUS    = os.getenv("TOPIC_STATUS", "workers.status")
TOPIC_PREFIX_JOBS = os.getenv("TOPIC_PREFIX_JOBS", "jobs.")

LEASE_TTL_SEC   = int(os.getenv("LEASE_TTL_SEC", "60"))
HEARTBEAT_TTL   = int(os.getenv("HEARTBEAT_TTL", "15"))  # se não anunciar em 15s, worker é suspeito

# default requirements (caso não exista regra por stream)
DEFAULT_PROC = {
    "class": os.getenv("DEFAULT_PROC_CLASS", "processors.ultralytics_yolov8.YOLOv8Processor"),
    "kwargs": {
        "weights_uri": os.getenv("DEFAULT_WEIGHTS", "file:///weights/yolov8n.pt"),
        "accel": os.getenv("DEFAULT_ACCEL", "cpu"),
        "conf": float(os.getenv("DEFAULT_CONF", "0.25")),
        "iou": float(os.getenv("DEFAULT_IOU", "0.45"))
    }
}
DEFAULT_TRACKING = {
    "enabled": os.getenv("DEFAULT_TRACKING", "false").lower()=="true",
    "algorithm": os.getenv("DEFAULT_TRACK_ALGO", "bytetrack"),
    "kwargs": {}
}
