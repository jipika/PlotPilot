"""知识三元组核心模型 — 与膨胀的 KnowledgeTriple 分离（P2-7）。"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class TripleCore:
    subject: str
    predicate: str
    object: str
    chapter_id: Optional[int] = None
    provenance: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class TripleMetadata:
    """索引/标签/置信度等附属信息。"""
    entity_type: str = ""
    importance: str = "normal"
    tags: List[str] = field(default_factory=list)
    confidence: float = 1.0
    location_type: Optional[str] = None
