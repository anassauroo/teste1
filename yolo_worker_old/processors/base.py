# processors/base.py
from typing import Any, Dict, List
import numpy as np

Detection = Dict[str, Any]

class Processor:
    def __init__(self, **kwargs): ...
    def warmup(self): pass
    def process_frame(self, frame: np.ndarray, ts: float) -> List[Detection]:
        raise NotImplementedError
    def close(self): pass
