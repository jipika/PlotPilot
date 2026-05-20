"""持久化队列统一门面 — SQLite V2 为唯一实现（替代 mp.Queue V1）。"""
from __future__ import annotations

import logging
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


class UnifiedPersistenceQueue:
    """V2 SQLite 队列的 V1 兼容 API 门面。"""

    def __init__(self) -> None:
        self._v2 = None

    def _get_v2(self):
        if self._v2 is None:
            from application.engine.services.persistence_queue_v2 import (
                get_persistent_queue_v2,
                initialize_persistent_queue_v2,
            )
            from infrastructure.persistence.database.connection import get_connection_pool

            try:
                self._v2 = get_persistent_queue_v2()
            except RuntimeError:
                self._v2 = initialize_persistent_queue_v2(get_connection_pool())
        return self._v2

    def initialize(self):
        """启动时初始化 V2（替代 mp.Queue）。"""
        self._get_v2()
        logger.info("✅ 持久化队列已初始化 (SQLite V2)")
        return None

    def inject_queue(self, queue) -> None:
        """已废弃：V2 通过共享 DB 跨进程，无需注入 mp.Queue。"""
        if queue is not None:
            logger.debug("inject_queue 已忽略（使用 SQLite V2 跨进程队列）")

    def get_queue(self):
        return None

    def register_handler(self, command_type: str, handler: Callable) -> None:
        self._get_v2().register_handler(command_type, handler)

    def push(self, command_type: str, payload: Dict[str, Any], **kwargs) -> bool:
        try:
            self._get_v2().push(command_type, payload, **kwargs)
            return True
        except Exception as e:
            logger.error("推入持久化队列失败: %s %s", command_type, e)
            return False

    def push_batch(self, commands: List[tuple]) -> bool:
        if not commands:
            return True
        batch_payload = [{"command_type": ct, "payload": p} for ct, p in commands]
        from application.engine.services.persistence_command_types import PersistenceCommandType

        return self.push(PersistenceCommandType.BATCH.value, {"commands": batch_payload})

    def is_consumer_running(self) -> bool:
        v2 = self._get_v2()
        t = v2._consumer_thread
        return t is not None and t.is_alive()

    def start_consumer(self) -> None:
        self._get_v2().start_consumer()

    def stop_consumer(self) -> None:
        self._get_v2().stop_consumer()

    def get_stats(self) -> Dict:
        return self._get_v2().get_stats()


_unified_queue: Optional[UnifiedPersistenceQueue] = None


def get_unified_persistence_queue() -> UnifiedPersistenceQueue:
    global _unified_queue
    if _unified_queue is None:
        _unified_queue = UnifiedPersistenceQueue()
    return _unified_queue
