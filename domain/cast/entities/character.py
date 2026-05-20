"""兼容层 — Cast Character 基于 engine.core 统一模型。"""
from __future__ import annotations

from typing import List

from engine.core.entities.character import Character as EngineCharacter, CharacterId
from domain.cast.entities.story_event import StoryEvent


class Character(EngineCharacter):
    """Cast 图谱角色 — 在统一模型上扩展 traits / note / story_events。"""

    def __init__(
        self,
        id: CharacterId,
        name: str,
        aliases: List[str] | None = None,
        role: str = "",
        traits: str = "",
        note: str = "",
        story_events: List[StoryEvent] | None = None,
    ):
        if not name or not name.strip():
            raise ValueError("Character name cannot be empty")
        super().__init__(
            character_id=id,
            name=name,
            aliases=list(aliases or []),
            role=role or "",
        )
        self.traits = traits or ""
        self.note = note or ""
        self.story_events = list(story_events or [])

    @property
    def id(self) -> CharacterId:
        return self.character_id

    def add_story_event(self, event: StoryEvent) -> None:
        existing_ids = {e.id for e in self.story_events}
        if event.id in existing_ids:
            self.story_events = [
                e if e.id != event.id else event for e in self.story_events
            ]
        else:
            self.story_events.append(event)

    def remove_story_event(self, event_id: str) -> None:
        self.story_events = [e for e in self.story_events if e.id != event_id]
