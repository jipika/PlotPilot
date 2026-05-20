"""一次性脚本：将 AutopilotDaemon 写作阶段方法抽到 chapter_writing_mixin.py。"""
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DAEMON = ROOT / "application" / "engine" / "services" / "autopilot_daemon.py"
MIXIN = ROOT / "application" / "engine" / "services" / "autopilot" / "chapter_writing_mixin.py"

# 1-based inclusive line ranges to move
RANGES = [
    (1387, 2397),
    (2399, 2470),
    (3416, 4089),
]

HEADER = '''"""自动驾驶 — 写作阶段（从 AutopilotDaemon 下沉的 Mixin）。"""
import time
import logging
import asyncio
from dataclasses import replace
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from domain.novel.entities.novel import Novel, NovelStage, AutopilotStatus
from domain.novel.entities.chapter import ChapterStatus
from domain.novel.value_objects.novel_id import NovelId
from domain.novel.value_objects.chapter_id import ChapterId
from domain.novel.value_objects.word_count import WordCount
from domain.novel.value_objects.generation_preferences import GenerationPreferences
from domain.ai.services.llm_service import GenerationConfig
from domain.ai.value_objects.prompt import Prompt
from domain.structure.story_node import StoryNode
from application.ai.llm_output_sanitize import strip_reasoning_artifacts
from application.ai.prose_fragment_aggregator import aggregate_inline_prose_fragments
from application.ai.llm_retry_policy import LLM_MAX_TOTAL_ATTEMPTS
from application.workflows.beat_continuation import format_prior_draft_for_prompt
from application.ai.llm_retry_policy import LLM_MAX_TOTAL_ATTEMPTS as VOICE_REWRITE_MAX_ATTEMPTS

logger = logging.getLogger(__name__)

VOICE_REWRITE_THRESHOLD = 0.68
VOICE_WARNING_THRESHOLD_FALLBACK = 0.75


def _coerce_word_count_to_int(wc: Any) -> int:
    if wc is None:
        return 0
    if isinstance(wc, WordCount):
        return wc.value
    return int(wc)


class ChapterWritingMixin:
    """写作阶段状态机与流式节拍生成（由 AutopilotDaemon 继承）。"""

'''

FOOTER = "\n"


def main() -> None:
    lines = DAEMON.read_text(encoding="utf-8").splitlines()
    chunks: list[str] = []
    for start, end in RANGES:
        chunks.extend(lines[start - 1 : end])
    body = "\n".join(chunks)
    MIXIN.write_text(HEADER + body + FOOTER, encoding="utf-8")

    # 从 daemon 删除（自下而上）
    new_lines = lines[:]
    for start, end in reversed(RANGES):
        del new_lines[start - 1 : end]

    text = "\n".join(new_lines) + "\n"
    import_block = "from application.engine.services.autopilot.chapter_writing_mixin import ChapterWritingMixin\n"
    if "ChapterWritingMixin" not in text:
        marker = "logger = logging.getLogger(__name__)\n\n\n"
        text = text.replace(
            marker,
            marker,
        )
        text = text.replace(
            "class AutopilotDaemon:",
            import_block + "\nclass AutopilotDaemon(ChapterWritingMixin):",
            1,
        )
    DAEMON.write_text(text, encoding="utf-8")
    print(f"Wrote {MIXIN} ({len(chunks)} lines)")
    print(f"Updated {DAEMON} ({len(new_lines)} lines)")


if __name__ == "__main__":
    main()
