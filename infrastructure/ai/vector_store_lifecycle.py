"""向量存储生命周期 — 统一创建与关闭。"""
from __future__ import annotations

import logging
from typing import Optional

logger = logging.getLogger(__name__)

_store: Optional[object] = None


def get_vector_store_singleton():
    """懒加载 ChromaDB/FAISS 向量存储（与 dependencies.get_vector_store 对齐）。"""
    global _store
    if _store is None:
        from interfaces.api.dependencies import get_vector_store

        _store = get_vector_store()
    return _store


def shutdown_vector_store() -> None:
    global _store
    if _store is None:
        return
    close = getattr(_store, "close", None)
    if callable(close):
        try:
            close()
            logger.info("向量存储已关闭")
        except Exception as e:
            logger.warning("关闭向量存储失败: %s", e)
    _store = None
