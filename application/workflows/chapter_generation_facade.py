"""章节生成统一入口 — HTTP 与 Autopilot 共用（P1-1b）。"""
from __future__ import annotations

from typing import Any, Dict, Optional


class ChapterGenerationFacade:
    """薄门面：委托 AutoNovelGenerationWorkflow，避免双轨实现。"""

    def __init__(self, workflow=None):
        self._workflow = workflow

    def _wf(self):
        if self._workflow is None:
            from application.workflows.auto_novel_generation_workflow import (
                AutoNovelGenerationWorkflow,
            )

            self._workflow = AutoNovelGenerationWorkflow()
        return self._workflow

    async def generate_chapter(
        self,
        novel_id: str,
        chapter_number: int,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        wf = self._wf()
        if hasattr(wf, "generate_chapter_for_novel"):
            return await wf.generate_chapter_for_novel(novel_id, chapter_number, **kwargs)
        raise NotImplementedError("Workflow 未暴露 generate_chapter_for_novel")


def get_chapter_generation_facade() -> ChapterGenerationFacade:
    return ChapterGenerationFacade()
