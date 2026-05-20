"""Novel → engine Story 投影 — P0-4 渐进迁移入口。"""
from __future__ import annotations

from typing import Optional

from engine.core.entities.story import Story, StoryId, StoryPhase


def novel_progress(novel) -> float:
    target = getattr(novel, "target_chapters", 0) or 0
    current = getattr(novel, "current_auto_chapters", 0) or 0
    if target <= 0:
        return 0.0
    return min(1.0, max(0.0, current / target))


def story_phase_for_novel(novel) -> StoryPhase:
    if hasattr(novel, "get_story_phase"):
        phase = novel.get_story_phase()
        if isinstance(phase, StoryPhase):
            return phase
        return StoryPhase(str(getattr(phase, "value", phase)))
    return StoryPhase.from_progress(novel_progress(novel))


def project_novel_to_story(novel) -> Story:
    """将 Novel 聚合根投影为 engine Story（只读叙事模型）。"""
    nid = novel.novel_id.value if hasattr(novel.novel_id, "value") else str(novel.novel_id)
    return Story(
        story_id=StoryId(nid),
        title=getattr(novel, "title", "") or "",
        premise=getattr(novel, "premise", "") or getattr(novel, "description", "") or "",
        target_chapters=getattr(novel, "target_chapters", 0) or 0,
        current_chapter=getattr(novel, "current_auto_chapters", 0) or 0,
        story_phase=story_phase_for_novel(novel),
    )
