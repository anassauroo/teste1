# utils/loader.py
import importlib

def load_class(qualified_name: str):
    mod, cls = qualified_name.rsplit(".", 1)
    return getattr(importlib.import_module(mod), cls)