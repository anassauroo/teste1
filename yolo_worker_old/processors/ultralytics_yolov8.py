# processors/ultralytics_yolov8.py
from .base import Processor, Detection
from ultralytics import YOLO
import numpy as np

class YOLOv8Processor(Processor):
    def __init__(self, weights_uri: str, accel: str="cpu", conf: float=0.25, iou: float=0.45, imgsz: int = 640, **_):
        self.model = YOLO(weights_uri)
        self.conf = conf
        self.iou = iou
        self.imgsz = imgsz
        self.accel = accel

    def warmup(self):
        # opcional: mover p/ cuda se disponível
        if self.accel == "cuda":
            try:
                import torch
                if torch.cuda.is_available():
                    self.model.to("cuda")
            except Exception:
                pass

    def process_frame(self, frame: np.ndarray, ts: float):
        # retorno padrão do worker: lista de detecções
        res = self.model.predict(frame, conf=self.conf, iou=self.iou, imgsz=self.imgsz, verbose=False)[0]
        dets = []
        names = getattr(self.model, "names", {})
        if hasattr(res, "boxes") and res.boxes is not None:
            for b in res.boxes:
                xyxy = b.xyxy[0].tolist()
                conf = float(b.conf[0])
                cls  = int(b.cls[0])
                dets.append({
                    "ts": ts,
                    "bbox": xyxy,        # [x1,y1,x2,y2]
                    "conf": conf,
                    "cls": cls,
                    "label": names.get(cls, str(cls))
                })
        return dets

    def close(self):
        pass
