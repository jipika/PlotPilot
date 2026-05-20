-- 012: 将 novels 既有自动驾驶/审计列回填到侧表
INSERT OR IGNORE INTO novel_autopilot_states (
    novel_id, autopilot_status, auto_approve_mode, current_stage,
    current_act, current_chapter_in_act, max_auto_chapters,
    current_auto_chapters, last_chapter_tension, consecutive_error_count,
    current_beat_index, beats_completed, target_words_per_chapter, updated_at
)
SELECT
    id, autopilot_status, auto_approve_mode, current_stage,
    current_act, current_chapter_in_act, max_auto_chapters,
    current_auto_chapters, last_chapter_tension, consecutive_error_count,
    current_beat_index, beats_completed, target_words_per_chapter, updated_at
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
    last_audit_drift_alert, last_audit_narrative_ok, last_audit_at,
    last_audit_vector_stored, last_audit_foreshadow_stored,
    last_audit_triples_extracted, last_audit_quality_scores,
    last_audit_issues, audit_progress, updated_at
FROM novels;
