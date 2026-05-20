"""持久化队列适配器 — 已收敛为 SQLite V2，本模块仅 re-export 统一门面。"""
from application.engine.services.persistence_queue_unified import (
    UnifiedPersistenceQueue,
    get_unified_persistence_queue,
)

PersistenceQueueAdapter = UnifiedPersistenceQueue


def get_persistence_queue_adapter() -> UnifiedPersistenceQueue:
    return get_unified_persistence_queue()
