"""Bible Character 序列化辅助 — 兼容 engine VoiceStyle / Wound 值对象。"""
from __future__ import annotations

from typing import Any, Dict, List


def voice_profile_to_dict(voice_profile: Any) -> Dict[str, Any]:
    if voice_profile is None:
        return {}
    if hasattr(voice_profile, "to_dict"):
        return voice_profile.to_dict()
    if isinstance(voice_profile, dict):
        return dict(voice_profile)
    return {}


def active_wounds_to_list(active_wounds: Any) -> List[Dict[str, Any]]:
    if not active_wounds:
        return []
    result: List[Dict[str, Any]] = []
    for item in active_wounds:
        if hasattr(item, "to_dict"):
            result.append(item.to_dict())
        elif isinstance(item, dict):
            result.append(dict(item))
    return result
