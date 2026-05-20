"""潜台词台账 — 从 ForeshadowingRegistry 拆出的子域（P2-6）。"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List


@dataclass
class SubtextEntry:
    id: str
    novel_id: str
    chapter_number: int
    text: str
    tags: List[str] = field(default_factory=list)


@dataclass
class SubtextLedger:
    """仅管理潜台词/伏笔外的叙事潜层记录。"""

    entries: List[SubtextEntry] = field(default_factory=list)

    def add(self, entry: SubtextEntry) -> None:
        self.entries.append(entry)

    def for_chapter(self, chapter_number: int) -> List[SubtextEntry]:
        return [e for e in self.entries if e.chapter_number == chapter_number]
