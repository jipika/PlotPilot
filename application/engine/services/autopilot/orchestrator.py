"""Autopilot 编排器 — 仅状态机路由，不含生成细节（P1-1）。"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, List

if TYPE_CHECKING:
    from domain.novel.entities.novel import Novel

logger = logging.getLogger(__name__)


class AutopilotOrchestrator:
    """轮询活跃小说并委托 AutopilotDaemon._process_novel。"""

    def __init__(self, daemon):
        self._daemon = daemon

    def poll_once(self) -> int:
        novels: List["Novel"] = self._daemon._get_active_novels()
        for novel in novels:
            import asyncio

            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.create_task(self._daemon._process_novel(novel))
            else:
                loop.run_until_complete(self._daemon._process_novel(novel))
        return len(novels)
