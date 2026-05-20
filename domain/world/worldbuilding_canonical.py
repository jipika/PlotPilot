"""世界观权威读路径 — 仅 worldbuilding 表（含 extensions_json）。"""
from __future__ import annotations

from typing import Any, Dict

from application.world.worldbuilding_storage import entity_to_canonical_slices


def read_canonical_worldbuilding_slices(wb: Any, bible: Any = None) -> Dict[str, Dict[str, str]]:
    """读取完整五维切片；bible 参数保留签名兼容，不再参与合并。"""
    _ = bible
    return entity_to_canonical_slices(wb)
