"""Autopilot 持久化桥 — CQRS 写入通道（从 AutopilotDaemon 抽取）。"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict

from domain.novel.value_objects.novel_id import NovelId

logger = logging.getLogger(__name__)


class AutopilotPersistenceBridge:
    """守护进程侧 SQL / 补丁 推入持久化队列。"""

    def push_command(self, command_type: str, payload: Dict[str, Any]) -> bool:
        try:
            from application.engine.services.persistence_queue import get_persistence_queue

            return get_persistence_queue().push(command_type, payload)
        except Exception as e:
            logger.debug("持久化队列不可用: %s", e)
            return False

    def queue_sql(self, sql: str, params: tuple | list = ()) -> bool:
        from application.engine.services.persistence_command_types import PersistenceCommandType

        params_list = list(params) if params else []
        return self.push_command(
            PersistenceCommandType.EXECUTE_SQL.value,
            {"sql": sql, "params": params_list},
        )

    def patch_novel_fields(self, novel_id: NovelId, fields: Dict[str, Any]) -> bool:
        from domain.novel.entities.novel import AutopilotStatus as _APS, NovelStage as _NS

        if not fields:
            return True

        processed: Dict[str, Any] = {}
        for key, value in fields.items():
            if isinstance(value, _APS):
                processed[key] = value.value
            elif isinstance(value, _NS):
                processed[key] = value.value
            elif isinstance(value, bool):
                processed[key] = 1 if value else 0
            elif isinstance(value, (dict, list)):
                processed[key] = json.dumps(value, ensure_ascii=False)
            else:
                processed[key] = value

        processed["updated_at"] = datetime.now(timezone.utc).isoformat()
        set_clauses = [f"{key} = ?" for key in processed.keys()]
        values = list(processed.values()) + [novel_id.value]
        sql = f"UPDATE novels SET {', '.join(set_clauses)} WHERE id = ?"
        return self.queue_sql(sql, values)
