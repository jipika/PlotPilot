"""自动驾驶状态与审计快照 — 从 novels 主表拆出的侧表读写（P2-1）。"""
from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Dict, Optional

from domain.novel.entities.novel import AutopilotStatus, Novel, NovelStage
from domain.novel.value_objects.novel_id import NovelId
from infrastructure.persistence.database.connection import DatabaseConnection

logger = logging.getLogger(__name__)


class NovelAutopilotStateRepository:
    """novel_autopilot_states + novel_audit_snapshots 双表持久化。"""

    def __init__(self, db: DatabaseConnection):
        self.db = db

    def upsert_from_novel(self, novel: Novel) -> None:
        novel_id = novel.novel_id.value if hasattr(novel, "novel_id") else novel.id
        now = datetime.utcnow().isoformat()

        _ap = getattr(novel, "autopilot_status", "stopped")
        autopilot_status = _ap.value if isinstance(_ap, AutopilotStatus) else _ap
        _cs = getattr(novel, "current_stage", "planning")
        current_stage = _cs.value if isinstance(_cs, NovelStage) else _cs

        self.db.execute(
            """
            INSERT INTO novel_autopilot_states (
                novel_id, autopilot_status, auto_approve_mode, current_stage,
                current_act, current_chapter_in_act, max_auto_chapters,
                current_auto_chapters, last_chapter_tension, consecutive_error_count,
                current_beat_index, beats_completed, target_words_per_chapter, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(novel_id) DO UPDATE SET
                autopilot_status = excluded.autopilot_status,
                auto_approve_mode = excluded.auto_approve_mode,
                current_stage = excluded.current_stage,
                current_act = excluded.current_act,
                current_chapter_in_act = excluded.current_chapter_in_act,
                max_auto_chapters = excluded.max_auto_chapters,
                current_auto_chapters = excluded.current_auto_chapters,
                last_chapter_tension = excluded.last_chapter_tension,
                consecutive_error_count = excluded.consecutive_error_count,
                current_beat_index = excluded.current_beat_index,
                beats_completed = excluded.beats_completed,
                target_words_per_chapter = excluded.target_words_per_chapter,
                updated_at = excluded.updated_at
            """,
            (
                novel_id,
                autopilot_status,
                1 if getattr(novel, "auto_approve_mode", False) else 0,
                current_stage,
                getattr(novel, "current_act", 0),
                getattr(novel, "current_chapter_in_act", 0),
                getattr(novel, "max_auto_chapters", 9999),
                getattr(novel, "current_auto_chapters", 0),
                getattr(novel, "last_chapter_tension", 0),
                getattr(novel, "consecutive_error_count", 0),
                getattr(novel, "current_beat_index", 0),
                1 if getattr(novel, "beats_completed", False) else 0,
                getattr(novel, "target_words_per_chapter", 2500),
                now,
            ),
        )

        laqs = getattr(novel, "last_audit_quality_scores", {}) or {}
        lai = getattr(novel, "last_audit_issues", []) or []
        self.db.execute(
            """
            INSERT INTO novel_audit_snapshots (
                novel_id, last_audit_chapter_number, last_audit_similarity,
                last_audit_drift_alert, last_audit_narrative_ok, last_audit_at,
                last_audit_vector_stored, last_audit_foreshadow_stored,
                last_audit_triples_extracted, last_audit_quality_scores,
                last_audit_issues, audit_progress, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(novel_id) DO UPDATE SET
                last_audit_chapter_number = excluded.last_audit_chapter_number,
                last_audit_similarity = excluded.last_audit_similarity,
                last_audit_drift_alert = excluded.last_audit_drift_alert,
                last_audit_narrative_ok = excluded.last_audit_narrative_ok,
                last_audit_at = excluded.last_audit_at,
                last_audit_vector_stored = excluded.last_audit_vector_stored,
                last_audit_foreshadow_stored = excluded.last_audit_foreshadow_stored,
                last_audit_triples_extracted = excluded.last_audit_triples_extracted,
                last_audit_quality_scores = excluded.last_audit_quality_scores,
                last_audit_issues = excluded.last_audit_issues,
                audit_progress = excluded.audit_progress,
                updated_at = excluded.updated_at
            """,
            (
                novel_id,
                getattr(novel, "last_audit_chapter_number", None),
                getattr(novel, "last_audit_similarity", None),
                1 if getattr(novel, "last_audit_drift_alert", False) else 0,
                1 if getattr(novel, "last_audit_narrative_ok", True) else 0,
                getattr(novel, "last_audit_at", None),
                1 if getattr(novel, "last_audit_vector_stored", False) else 0,
                1 if getattr(novel, "last_audit_foreshadow_stored", False) else 0,
                1 if getattr(novel, "last_audit_triples_extracted", False) else 0,
                json.dumps(laqs) if laqs else None,
                json.dumps(lai) if lai else None,
                getattr(novel, "audit_progress", None),
                now,
            ),
        )

    def load_into_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """若侧表有数据，覆盖 novels 行中的自动驾驶/审计字段（读路径归一）。"""
        novel_id = row.get("id")
        if not novel_id:
            return row

        ap = self.db.fetch_one(
            "SELECT * FROM novel_autopilot_states WHERE novel_id = ?",
            (novel_id,),
        )
        if ap:
            for key in (
                "autopilot_status",
                "auto_approve_mode",
                "current_stage",
                "current_act",
                "current_chapter_in_act",
                "max_auto_chapters",
                "current_auto_chapters",
                "last_chapter_tension",
                "consecutive_error_count",
                "current_beat_index",
                "beats_completed",
                "target_words_per_chapter",
            ):
                if key in ap.keys() and ap[key] is not None:
                    row[key] = ap[key]

        aus = self.db.fetch_one(
            "SELECT * FROM novel_audit_snapshots WHERE novel_id = ?",
            (novel_id,),
        )
        if aus:
            for key in (
                "last_audit_chapter_number",
                "last_audit_similarity",
                "last_audit_drift_alert",
                "last_audit_narrative_ok",
                "last_audit_at",
                "last_audit_vector_stored",
                "last_audit_foreshadow_stored",
                "last_audit_triples_extracted",
                "last_audit_quality_scores",
                "last_audit_issues",
                "audit_progress",
            ):
                if key in aus.keys() and aus[key] is not None:
                    row[key] = aus[key]
        return row
