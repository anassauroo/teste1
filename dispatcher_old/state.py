import time, heapq
from typing import Dict, Tuple, Any, List, Optional, Set
from models import Worker, Lease

class State:
    def __init__(self):
        self.active_streams: Dict[str, dict] = {}   
        self.open_streams: Set[str] = set()         # <- último snapshot recebido
        self.workers: Dict[str, Worker] = {}
        self.leases: Dict[str, Lease] = {}
        self.queue: List[Tuple[int, float, str, dict]] = []
        self._counter = 0
        self.rules: Dict[str, dict] = {}
        self.req_map: Dict[str, dict] = {}

    def update_open_streams(self, names: List[str]):
        """Atualiza o snapshot atual de streams abertos."""
        self.open_streams = set(names)

    def processing_streams(self) -> Set[str]:
        """Streams que, do ponto de vista do dispatcher, estão sob processamento (via leases)."""
        return set(l.stream for l in self.leases.values())

    def streams_diff(self) -> Tuple[Set[str], Set[str]]:
        """Retorna (to_start, to_stop)."""
        open_now = self.open_streams
        processing = self.processing_streams()
        to_start = open_now - processing
        to_stop  = processing - open_now
        return to_start, to_stop

    def upsert_worker(self, w: Worker):
        self.workers[w.worker_id] = w

    def mark_seen(self, worker_id: str):
        if worker_id in self.workers:
            self.workers[worker_id].last_seen = time.time()

    def add_stream(self, name: str, meta: dict):
        self.active_streams[name] = meta

    def remove_stream(self, name: str):
        self.active_streams.pop(name, None)

    def enqueue(self, stream: str, req: dict, priority: int = 10):
        self._counter += 1
        heapq.heappush(self.queue, (priority, time.time(), stream, req))

    def pop_queue(self) -> Optional[Tuple[str, dict]]:
        if not self.queue: return None
        _, _, s, r = heapq.heappop(self.queue)
        return (s, r)
