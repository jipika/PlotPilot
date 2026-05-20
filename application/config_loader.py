"""应用配置加载 — 集中读取 config/*.yaml。"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import yaml

_CONFIG: Dict[str, Any] | None = None
_ROOT = Path(__file__).resolve().parents[1]


def load_app_config(reload: bool = False) -> Dict[str, Any]:
    global _CONFIG
    if _CONFIG is not None and not reload:
        return _CONFIG
    merged: Dict[str, Any] = {}
    config_dir = _ROOT / "config"
    for name in ("app.yaml", "performance.yaml"):
        path = config_dir / name
        if path.is_file():
            with path.open(encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            if isinstance(data, dict):
                merged.update(data)
    _CONFIG = merged
    return _CONFIG


def get_config(key: str, default: Any = None) -> Any:
    return load_app_config().get(key, default)
