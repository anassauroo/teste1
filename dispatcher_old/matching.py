from typing import Optional, Dict
from models import Worker

def match_worker(req: Dict, workers: Dict[str, Worker]) -> Optional[str]:
    # política simples: primeiro disponível que atende requisitos
    preferred_accel = req["kwargs"].get("accel", "cpu")
    family = req.get("family", None) or _family_from_class(req["class"])
    yolo_version = req["kwargs"].get("yolo_version", None)  # opcional

    # ordena por (capacidade livre decrescente, aceleração preferida primeiro)
    candidates = []
    for wid, w in workers.items():
        if w.running < w.caps.max_concurrency:
            if family in w.caps.families:
                if (not yolo_version) or (yolo_version in w.caps.yolo_versions):
                    # pontua aceleração
                    score = 0
                    if preferred_accel in w.caps.accel: score += 10
                    free = w.caps.max_concurrency - w.running
                    score += free
                    candidates.append((score, wid))
    if not candidates:
        return None
    candidates.sort(reverse=True)
    return candidates[0][1]

def _family_from_class(qualified_name: str) -> str:
    # ex: "processors.ultralytics_yolov8.YOLOv8Processor" -> "ultralytics"
    parts = qualified_name.split(".")
    # heurística simples:
    for p in parts:
        if "ultralytics" in p: return "ultralytics"
        if "onnx" in p: return "onnx"
        if "openvino" in p: return "openvino"
        if "tensorrt" in p: return "tensorrt"
    return "ultralytics"
