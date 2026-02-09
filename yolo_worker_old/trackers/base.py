# trackers/base.py
from typing import List, Dict, Any
import numpy as np

class Tracker:
    def update(self, dets: List[Dict[str,Any]], frame: np.ndarray, ts: float) -> List[Dict[str,Any]]:
        raise NotImplementedError
    def reset(self): pass
