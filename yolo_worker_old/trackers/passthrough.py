# trackers/passthrough.py
from .base import Tracker

class PassThrough(Tracker):
    def __init__(self, **kwargs): pass
    def update(self, dets, frame, ts):
        # não altera, só repassa (stub)
        return dets
