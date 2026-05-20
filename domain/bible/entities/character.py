"""兼容层 — Bible Character 基于 engine.core 统一模型。"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from engine.core.entities.character import (
    Character as EngineCharacter,
    CharacterId,
    VoiceStyle,
    Wound,
)
from domain.shared.exceptions import InvalidOperationError


class Character(EngineCharacter):
    """Bible 角色实体 — 统一 engine.core 模型，保留旧构造与方法签名。"""

    def __init__(
        self,
        id: CharacterId,
        name: str,
        description: str,
        relationships: List[Any] | None = None,
        public_profile: str = "",
        hidden_profile: str = "",
        reveal_chapter: int | None = None,
        mental_state: str = "NORMAL",
        mental_state_reason: str = "",
        verbal_tic: str = "",
        idle_behavior: str = "",
        core_belief: str = "",
        moral_taboos: Optional[List[str]] = None,
        voice_profile: Optional[Dict[str, Any]] = None,
        active_wounds: Optional[List[Dict[str, str]]] = None,
    ):
        vp = VoiceStyle()
        if voice_profile:
            for key, val in voice_profile.items():
                if hasattr(vp, key):
                    setattr(vp, key, val)

        wounds: List[Wound] = []
        for item in active_wounds or []:
            if isinstance(item, dict):
                wounds.append(
                    Wound(
                        description=item.get("description", ""),
                        trigger=item.get("trigger", ""),
                        effect=item.get("effect", ""),
                    )
                )

        super().__init__(
            character_id=id,
            name=name,
            description=description,
            relationships=list(relationships or []),
            public_profile=public_profile,
            hidden_profile=hidden_profile,
            reveal_chapter=reveal_chapter,
            mental_state=mental_state or "NORMAL",
            verbal_tic=verbal_tic or "",
            idle_behavior=idle_behavior or "",
            core_belief=core_belief or "",
            moral_taboos=list(moral_taboos or []),
            voice_profile=vp,
            active_wounds=wounds,
        )
        self.mental_state_reason = mental_state_reason or ""

    @property
    def id(self) -> str:
        return self.character_id.value

    def add_relationship(self, relationship: Any) -> None:
        if relationship in self.relationships:
            raise InvalidOperationError(f"Relationship already exists: {relationship}")
        self.relationships.append(relationship)

    def remove_relationship(self, relationship: str) -> None:
        if relationship not in self.relationships:
            raise InvalidOperationError(f"Relationship not found: {relationship}")
        self.relationships.remove(relationship)

    def update_description(self, description: str) -> None:
        if not description or not description.strip():
            raise ValueError("Description cannot be empty")
        self.description = description
