"""世界观单存储：worldbuilding 表列 + extensions_json，不再与 Bible 双写合并。"""
from __future__ import annotations

import json
from typing import Any, Dict, Optional

from application.world.worldbuilding_merge import (
    WORLD_BUILDING_DIMENSION_KEYS,
    _LEGACY_KEYS_BY_DIMENSION,
    empty_worldbuilding_slices,
    project_slices_to_legacy_api_shape,
    worldbuilding_slices_nonempty,
)

# 维度字段 → ORM 平面属性
_FIELD_MAP: Dict[str, Dict[str, str]] = {
    "core_rules": {
        "power_system": "power_system",
        "physics_rules": "physics_rules",
        "magic_tech": "magic_tech",
    },
    "geography": {
        "terrain": "terrain",
        "climate": "climate",
        "resources": "resources",
        "ecology": "ecology",
    },
    "society": {
        "politics": "politics",
        "economy": "economy",
        "class_system": "class_system",
    },
    "culture": {
        "history": "history",
        "religion": "religion",
        "taboos": "taboos",
    },
    "daily_life": {
        "food_clothing": "food_clothing",
        "language_slang": "language_slang",
        "entertainment": "entertainment",
    },
}


def parse_extensions_json(raw: Any) -> Dict[str, Dict[str, str]]:
    if not raw:
        return empty_worldbuilding_slices()
    if isinstance(raw, dict):
        data = raw
    else:
        try:
            data = json.loads(str(raw))
        except (json.JSONDecodeError, TypeError):
            return empty_worldbuilding_slices()
    out = empty_worldbuilding_slices()
    for dim in WORLD_BUILDING_DIMENSION_KEYS:
        blk = data.get(dim)
        if isinstance(blk, dict):
            out[dim] = {k: str(v).strip() for k, v in blk.items() if str(v).strip()}
    return out


def entity_to_canonical_slices(wb: Any) -> Dict[str, Dict[str, str]]:
    """从 worldbuilding 实体读取完整五维（列 + extensions_json）。"""
    if wb is None:
        return empty_worldbuilding_slices()
    slices = empty_worldbuilding_slices()
    for dim, mapping in _FIELD_MAP.items():
        for key, attr in mapping.items():
            val = str(getattr(wb, attr, "") or "").strip()
            if val:
                slices[dim][key] = val
    extras = parse_extensions_json(getattr(wb, "extensions_json", None))
    for dim in WORLD_BUILDING_DIMENSION_KEYS:
        for k, v in (extras.get(dim) or {}).items():
            if v:
                slices[dim][k] = v
    return slices


def apply_slices_to_entity(wb: Any, slices: Dict[str, Any]) -> None:
    """将五维 dict 写回实体：经典 15 字段落列，其余进 extensions_json。"""
    extras = empty_worldbuilding_slices()
    legacy_sets = {dim: frozenset(keys) for dim, keys in _LEGACY_KEYS_BY_DIMENSION.items()}

    for dim in WORLD_BUILDING_DIMENSION_KEYS:
        blk = slices.get(dim) or {}
        if not isinstance(blk, dict):
            continue
        mapping = _FIELD_MAP.get(dim, {})
        legacy = legacy_sets.get(dim, frozenset())
        for key, value in blk.items():
            s = "" if value is None else str(value).strip()
            if not s:
                continue
            if key in legacy and key in mapping:
                setattr(wb, mapping[key], s)
            else:
                extras[dim][key] = s

    wb.extensions_json = json.dumps(extras, ensure_ascii=False)


def apply_dimension_to_entity(wb: Any, dim_key: str, dim_data: Dict[str, Any]) -> None:
    """SSE 单维度增量写入。"""
    if dim_key not in WORLD_BUILDING_DIMENSION_KEYS:
        return
    current = entity_to_canonical_slices(wb)
    blk = dict(current.get(dim_key) or {})
    for k, v in (dim_data or {}).items():
        s = "" if v is None else str(v).strip()
        if s:
            blk[k] = s
    current[dim_key] = blk
    apply_slices_to_entity(wb, current)


def canonical_slices_for_api(wb: Any) -> Dict[str, Dict[str, str]]:
    """API 展示用：完整切片压成 15 经典字段形状。"""
    return project_slices_to_legacy_api_shape(entity_to_canonical_slices(wb))


__all__ = [
    "entity_to_canonical_slices",
    "apply_slices_to_entity",
    "apply_dimension_to_entity",
    "canonical_slices_for_api",
    "worldbuilding_slices_nonempty",
]
