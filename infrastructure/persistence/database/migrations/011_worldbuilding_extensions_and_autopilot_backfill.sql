-- 011: 世界观扩展字段单表存储 + 自动驾驶侧表回填

ALTER TABLE worldbuilding ADD COLUMN extensions_json TEXT NOT NULL DEFAULT '{}';

-- 从 novels 回填侧表（幂等）
INSERT OR IGNORE INTO novel_autopilot_states (
    novel_id, autopilot_status, auto_approve_mode, current_stage,
    current_act, current_chapter_in_act, max_auto_chapters,
    current_auto_chapters, last_chapter_tension, consecutive_error_count,
    current_beat_index, beats_completed, target_words_per_chapter, updated_at
)
SELECT
    id, COALESCE(autopilot_status, 'stopped'), COALESCE(auto_approve_mode, 0),
    COALESCE(current_stage, 'planning'), COALESCE(current_act, 0),
    COALESCE(current_chapter_in_act, 0), COALESCE(max_auto_chapters, 9999),
    COALESCE(current_auto_chapters, 0), COALESCE(last_chapter_tension, 0),
    COALESCE(consecutive_error_count, 0), COALESCE(current_beat_index, 0),
    COALESCE(beats_completed, 0), COALESCE(target_words_per_chapter, 2500),
    COALESCE(updated_at, CURRENT_TIMESTAMP)
FROM novels;

INSERT OR IGNORE INTO novel_audit_snapshots (
    novel_id, last_audit_chapter_number, last_audit_similarity,
    last_audit_drift_alert, last_audit_narrative_ok, last_audit_at,
    last_audit_vector_stored, last_audit_foreshadow_stored,
    last_audit_triples_extracted, last_audit_quality_scores,
    last_audit_issues, audit_progress, updated_at
)
SELECT
    id, last_audit_chapter_number, last_audit_similarity,
    COALESCE(last_audit_drift_alert, 0), COALESCE(last_audit_narrative_ok, 1),
    last_audit_at, COALESCE(last_audit_vector_stored, 0),
    COALESCE(last_audit_foreshadow_stored, 0), COALESCE(last_audit_triples_extracted, 0),
    last_audit_quality_scores, last_audit_issues, audit_progress,
    COALESCE(updated_at, CURRENT_TIMESTAMP)
FROM novels;
