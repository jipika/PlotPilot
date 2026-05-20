"""持久化命令类型 — V1/V2 唯一枚举定义（架构治理 P1-2）。"""
from enum import Enum


class PersistenceCommandType(Enum):
    """持久化命令类型"""

    UPSERT_CHAPTER = "upsert_chapter"
    UPDATE_CHAPTER_STATUS = "update_chapter_status"
    UPDATE_CHAPTER_TENSION = "update_chapter_tension"
    UPDATE_CHAPTER_WORD_COUNT = "update_chapter_word_count"

    PATCH_NOVEL = "patch_novel"
    SAVE_NOVEL = "save_novel"
    UPDATE_NOVEL_STATE = "update_novel_state"

    UPSERT_KNOWLEDGE = "upsert_knowledge"
    SAVE_STORY_NODE = "save_story_node"
    UPDATE_FORESHADOWS = "update_foreshadows"
    UPDATE_STORYLINES = "update_storylines"
    UPDATE_PLOT_ARC = "update_plot_arc"
    UPDATE_CHRONICLES = "update_chronicles"
    UPDATE_KNOWLEDGE = "update_knowledge"
    UPDATE_BIBLE = "update_bible"
    UPDATE_TRIPLES = "update_triples"
    UPDATE_SNAPSHOTS = "update_snapshots"

    EXECUTE_SQL = "execute_sql"
    EXECUTE_SQL_TXN_BATCH = "execute_sql_txn_batch"
    DELETE_CHAPTER = "delete_chapter"

    BATCH = "batch"
