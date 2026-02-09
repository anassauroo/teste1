from dataclasses import dataclass, field
from typing import Dict, Any, Optional
import time

@dataclass
class WorkerCaps:
    families: list
    yolo_versions: list
    accel: list
    arch: str
    max_concurrency: int

@dataclass
class Worker:
    worker_id: str
    caps: WorkerCaps
    running: int = 0
    last_seen: float = field(default_factory=lambda: time.time())

@dataclass
class Lease:
    job_id: str
    worker_id: str
    stream: str
    deadline: float
