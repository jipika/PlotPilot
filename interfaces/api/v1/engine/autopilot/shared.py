"""Autopilot 路由共享状态与构建逻辑（v2：含审阅确认 + SSE 生成流）。"""

# control / streams / system 使用 `from ...shared import *`；须显式列出以 _ 开头的符号
__all__ = [
    "APIRouter",
    "AutopilotStatus",
    "HTTPException",
    "NovelId",
    "NovelStage",
    "Optional",
    "PER_NOVEL_FAILURE_THRESHOLD",
    "Query",
    "StartRequest",
    "StreamingResponse",
    "asyncio",
    "datetime",
    "get_chapter_repository",
    "get_db_path",
    "get_novel_repository",
    "install_autopilot_log_ring_handler",
    "iter_new_for_novel",
    "json",
    "logger",
    "os",
    "read_incremental_log_file_lines",
    "router",
    "shorten_log_message",
    "time",
    "_SHARED_STATE_CACHE",
    "_SSE_MAX_LIFETIME_SECONDS",
    "_SSE_THREAD_POOL",
    "_audit_event_message",
    "_autopilot_events_tick_sync",
    "_autopilot_status_zh",
    "_chapter_stream_chunks_sync",
    "_chapter_stream_tick_sync",
    "_clamp_autopilot_target_chapters",
    "_clamp_autopilot_words_per_chapter",
    "_get_shared_state_for_novel",
    "_get_shared_state_for_novel_cached",
    "_has_chapter_nodes_under_current_act",
    "_is_client_disconnected",
    "_log_stream_boot_meta_sync",
    "_log_stream_file_cursor_init_sync",
    "_log_stream_io_tick_sync",
    "_log_stream_replay_sync",
    "_persist_autopilot_running_sync",
    "_rm",
    "_stage_name_zh",
    "_stage_needs_human_review",
]

import asyncio
import json
import logging
import os
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from typing import Any, Dict, List, Optional, Tuple
from domain.novel.entities.novel import AutopilotStatus, NovelStage
from domain.novel.entities.chapter import ChapterStatus
from domain.novel.value_objects.novel_id import NovelId
from domain.novel.value_objects.word_count import WordCount
from interfaces.api.dependencies import get_novel_repository, get_chapter_repository
from application.paths import get_db_path
from application.core.chapter_target_limits import (
    CHAPTER_TARGET_WORDS_MAX,
    CHAPTER_TARGET_WORDS_MIN,
    clamp_chapter_target_words,
)
from infrastructure.persistence.database.story_node_repository import StoryNodeRepository
from application.engine.services.autopilot_log_ring import (
    file_end_offset,
    initial_snapshot_offset,
    install_autopilot_log_ring_handler,
    iter_new_for_novel,
    read_incremental_log_file_lines,
    shorten_log_message,
    snapshot_for_novel,
)

logger = logging.getLogger(__name__)


def _chapter_status_str(c) -> str:
    return c.status.value if hasattr(c.status, "value") else c.status


def resolve_autopilot_current_chapter_number(chapters) -> Optional[int]:
    """与 SSE 日志、进度条一致：有内容的 draft 取最大章号；否则取最大 completed+1（预测下一章）。

    注意：幕级规划时会创建空的 draft 记录，需要忽略内容为空的 draft。
    """
    if not chapters:
        return None
    try:
        # 只考虑有实际内容的 draft（字数 > 0）
        def has_content(c) -> bool:
            wc = c.word_count
            if hasattr(wc, 'value'):
                wc = wc.value
            # 也检查 content 长度（兼容 word_count 为空的情况）
            content_len = len(c.content) if hasattr(c, 'content') and c.content else 0
            return (wc or 0) > 0 or content_len > 0

        drafts_with_content = [
            c for c in chapters
            if _chapter_status_str(c) == "draft" and has_content(c)
        ]
        if drafts_with_content:
            return max(int(c.number) for c in drafts_with_content)

        completed = [c for c in chapters if _chapter_status_str(c) == "completed"]
        if completed:
            return max(int(c.number) for c in completed) + 1
    except Exception:
        return None
    return None


def _has_chapter_nodes_under_current_act(novel_id: str, current_act_zero_based: int) -> bool:
    """当前幕（0-based）下是否已有章节结构节点。有则确认审阅后应直接 WRITING，避免再次跑幕级规划并重复弹确认。"""
    repo = StoryNodeRepository(get_db_path())
    target_act_number = (current_act_zero_based or 0) + 1
    all_nodes = repo.get_by_novel_sync(novel_id)
    act_nodes = sorted(
        [
            n
            for n in all_nodes
            if (n.node_type.value if hasattr(n.node_type, "value") else str(n.node_type)) == "act"
        ],
        key=lambda n: n.number,
    )
    target = next((n for n in act_nodes if n.number == target_act_number), None)
    if not target:
        return False
    for ch in repo.get_children_sync(target.id):
        t = ch.node_type.value if hasattr(ch.node_type, "value") else str(ch.node_type)
        if t == "chapter":
            return True
    return False


def _stage_after_review(novel) -> NovelStage:
    """审阅确认后的下一阶段：幕下已有章节点 → 写作；否则 → 幕级规划（含宏观审阅后尚未规划章节的情况）。"""
    nid = novel.novel_id.value if hasattr(novel.novel_id, "value") else str(novel.novel_id)
    ca = getattr(novel, "current_act", 0) or 0
    if _has_chapter_nodes_under_current_act(nid, ca):
        return NovelStage.WRITING
    return NovelStage.ACT_PLANNING


def _persist_autopilot_running_sync(
    novel_id: str,
    *,
    max_auto_chapters: int,
    target_chapters: int,
    target_words_per_chapter: int,
) -> None:
    """将 RUNNING 写入 DB 并等待持久化队列落盘。

    守护进程仅按 DB autopilot_status=running 捞书；全量 save() 易与首页改篇幅等
    并发写回 stopped，故用 patch + 兜底 UPDATE。
    """
    from application.engine.services.persistence_queue import get_persistence_queue
    from infrastructure.persistence.database.connection import get_database

    repo = get_novel_repository()
    novel = repo.get_by_id(NovelId(novel_id))
    if not novel:
        return

    fresh_stages_obj = {NovelStage.PLANNING, NovelStage.MACRO_PLANNING}
    if novel.current_stage in fresh_stages_obj:
        patch_stage = NovelStage.MACRO_PLANNING
    elif novel.current_stage == NovelStage.PAUSED_FOR_REVIEW:
        patch_stage = _stage_after_review(novel)
    else:
        patch_stage = novel.current_stage

    repo.patch(
        NovelId(novel_id),
        autopilot_status=AutopilotStatus.RUNNING,
        max_auto_chapters=max_auto_chapters,
        current_auto_chapters=novel.current_auto_chapters or 0,
        consecutive_error_count=0,
        target_chapters=target_chapters,
        target_words_per_chapter=target_words_per_chapter,
        current_stage=patch_stage,
    )

    pq = get_persistence_queue()
    if pq is not None:
        pq.wait_until_idle(timeout=5.0)

    row = get_database().fetch_one(
        "SELECT autopilot_status FROM novels WHERE id = ?",
        (novel_id,),
    )
    ap = (row or {}).get("autopilot_status") if row else None
    if ap != "running":
        logger.warning(
            "autopilot persist: novel_id=%s DB 仍为 %r，兜底 UPDATE running",
            novel_id,
            ap,
        )
        get_database().execute(
            """UPDATE novels SET autopilot_status = 'running', updated_at = CURRENT_TIMESTAMP
               WHERE id = ?""",
            (novel_id,),
        )
        get_database().commit()
        if pq is not None:
            pq.wait_until_idle(timeout=3.0)


def _stage_needs_human_review(stage: Optional[str]) -> bool:
    """是否与人工审阅闸门对齐（须调用 /resume）。

    「reviewing」为历史/兼容舞台值；闸门主路径使用 paused_for_review。两者均需展示确认按钮。
    """
    s = (stage or "").strip().lower()
    return s in ("paused_for_review", "reviewing")


router = APIRouter(prefix="/autopilot", tags=["autopilot"])

# ── 使用统一资源管理器管理线程池和缓存 ──
from application.engine.services.resource_manager import (
    ResourceManager, ThreadPoolResource, CacheResource, create_cache
)

# 初始化资源管理器
_rm = ResourceManager()

# SSE 专用线程池（通过资源管理器管理）
_SSE_THREAD_POOL = ThreadPoolResource(
    ThreadPoolExecutor(max_workers=12, thread_name_prefix="sse-io"),
    name="sse-executor"
)
_rm.register(_SSE_THREAD_POOL)

# 共享状态缓存（带 TTL 过期清理）
_SHARED_STATE_CACHE = CacheResource(
    name="shared_state",
    ttl_seconds=1.0,  # 1 秒 TTL
    max_size=1000
)
_rm.register(_SHARED_STATE_CACHE)

# SSE 连接最大存活时间（秒）：超时后自动断开，避免悬空连接累积
_SSE_MAX_LIFETIME_SECONDS = 7200  # 2 小时

# 与 AutopilotDaemon 中单本挂起阈值一致；守护进程内另有全局 CircuitBreaker（独立进程，API 不可见）
PER_NOVEL_FAILURE_THRESHOLD = 3


class _LightChapter:
    """轻量章节代理对象（SSE 流用，不加载 content 字段，减少 DB IO 和内存）"""
    __slots__ = ('id', 'number', 'title', 'status', 'word_count', 'content')

    def __init__(self, id=None, number=0, title="", status=None):
        self.id = id
        self.number = number
        self.title = title
        self.status = status or ChapterStatus.DRAFT
        self.word_count = WordCount(0)
        self.content = None


async def _is_client_disconnected() -> bool:
    """检测 SSE 客户端是否已断开连接。

    通过短暂让出事件循环控制权，让 uvicorn 检测底层 socket 状态。
    如果客户端已断开，后续 yield 会触发 CancelledError 或 ConnectionReset。
    """
    try:
        await asyncio.sleep(0)
    except asyncio.CancelledError:
        return True
    return False


def _stage_name_zh(stage: str) -> str:
    """阶段枚举值 → 中文（与前端驾驶舱一致）"""
    m = {
        "planning": "宏观规划",
        "macro_planning": "宏观规划",
        "act_planning": "幕级规划",
        "writing": "正文撰写",
        "auditing": "章节审计",
        "reviewing": "待审阅确认",
        "paused_for_review": "待审阅确认",
        "completed": "全书完成",
    }
    return m.get(stage, stage)


def _autopilot_status_zh(status: str) -> str:
    return {
        "stopped": "已停止",
        "running": "运行中",
        "error": "异常挂起",
        "completed": "已完成",
    }.get(status, status)


def _audit_event_message(event_type: str, data: Dict[str, Any]) -> str:
    """生成审计事件的消息文本"""
    messages = {
        "audit_start": lambda d: f"🔍 开始审计第 {d.get('chapter_number', '?')} 章（{d.get('word_count', 0)} 字）",
        "audit_voice_check": lambda d: f"📊 文风预检中...",
        "audit_voice_result": lambda d: (
            f"📊 文风相似度: {d.get('similarity_score'):.1%}" + (" ⚠️ 偏离告警" if d.get('drift_alert') else "")
            if d.get('similarity_score') is not None
            else "📊 文风相似度: 指纹样本不足（需 ≥10 个采血样本）"
        ),
        "audit_aftermath": lambda d: f"🔄 章后管线处理中...",
        "audit_tension": lambda d: f"⚡ 张力打分中...",
        "audit_tension_result": lambda d: f"⚡ 张力值: {d.get('tension', 'N/A')}/10",
        "audit_complete": lambda d: f"✅ 第 {d.get('chapter_number', '?')} 章审计完成" + (" 🎉全书完成！" if d.get('is_completed') else ""),
    }
    return messages.get(event_type, lambda d: f"审计事件: {event_type}")(data)


def _build_fallback_status(novel) -> Dict[str, Any]:
    """DB 被锁时的降级状态响应：只返回 novels 表中的字段，不含章节统计。

    关键作用：审计期间守护进程持写锁时，/status 仍能返回基本状态，
    前端不会卡死（至少能看到「审计中」和 audit_progress）。
    """
    target = novel.target_chapters or 1
    twpc = getattr(novel, "target_words_per_chapter", None) or 2500
    lacn = getattr(novel, "last_audit_chapter_number", None)
    last_tension = int(getattr(novel, "last_chapter_tension", 0) or 0)
    last_chapter_audit = None
    if lacn is not None:
        last_chapter_audit = {
            "chapter_number": int(lacn),
            "tension": last_tension,
            "drift_alert": bool(getattr(novel, "last_audit_drift_alert", False)),
            "similarity_score": getattr(novel, "last_audit_similarity", None),
            "narrative_sync_ok": bool(getattr(novel, "last_audit_narrative_ok", True)),
            "at": getattr(novel, "last_audit_at", None),
            "vector_stored": bool(getattr(novel, "last_audit_vector_stored", False)),
            "foreshadow_stored": bool(getattr(novel, "last_audit_foreshadow_stored", False)),
            "triples_extracted": bool(getattr(novel, "last_audit_triples_extracted", False)),
            "quality_scores": getattr(novel, "last_audit_quality_scores", {}) or {},
            "issues": getattr(novel, "last_audit_issues", []) or [],
        }
    return {
        "autopilot_status": novel.autopilot_status.value if hasattr(novel.autopilot_status, "value") else novel.autopilot_status,
        "current_stage": novel.current_stage.value if hasattr(novel.current_stage, "value") else novel.current_stage,
        "current_act": getattr(novel, "current_act", 0),
        "current_chapter_in_act": getattr(novel, "current_chapter_in_act", 0),
        "current_beat_index": getattr(novel, "current_beat_index", 0),
        "current_auto_chapters": getattr(novel, "current_auto_chapters", 0),
        "max_auto_chapters": getattr(novel, "max_auto_chapters", 9999),
        "target_chapters": novel.target_chapters,
        "target_words_per_chapter": twpc,
        "target_plan_total_words": target * twpc,
        "last_chapter_tension": last_tension,
        "consecutive_error_count": getattr(novel, "consecutive_error_count", 0),
        "total_words": 0,  # 降级：无法统计
        "completed_chapters": 0,  # 降级
        "progress_pct": 0.0,  # 降级
        "manuscript_chapters": 0,  # 降级
        "progress_pct_manuscript": 0.0,  # 降级
        "current_chapter_number": None,
        "needs_review": _stage_needs_human_review(
            novel.current_stage.value if hasattr(novel.current_stage, "value") else str(novel.current_stage)
        ),
        "auto_approve_mode": getattr(novel, "auto_approve_mode", False),
        "last_chapter_audit": last_chapter_audit,
        "audit_progress": getattr(novel, "audit_progress", None),
        "_degraded": True,  # 前端可据此显示「数据同步中」提示
    }


# ── SSE / 高频接口：同步仓储与文件 IO 放入线程池，避免阻塞 asyncio 事件循环（否则会拖死全站 API）──


def _get_shared_state_for_novel(novel_id: str) -> Optional[Dict[str, Any]]:
    """从跨进程共享内存读取小说实时状态（零 DB IO，纳秒级响应）。

    架构原则：状态走内存，数据走磁盘。守护进程写入共享字典，API 进程直接读取。
    """
    try:
        from interfaces.main import get_shared_novel_state
        return get_shared_novel_state(novel_id)
    except Exception:
        return None


def _build_autopilot_status_sync(novel_id: str) -> Optional[Dict[str, Any]]:
    """get_autopilot_status 的同步实现（供 asyncio.to_thread 调用）。

    共享内存提供阶段、审计进度、张力等；完稿/书稿/总字数以短超时 SQLite 聚合为准
   （_build_status_with_shared），再与共享字段合并。DB 被锁或异常时降级为纯共享内存
    或占位响应。

    修复：曾经 _cached_completed_chapters=0 因 `is not None` 走纯内存导致永久 0/0/总字数 0。
    """
    # ── 第一层：共享内存（阶段）+ DB 校准（章节聚合）──
    shared = _get_shared_state_for_novel(novel_id)
    if shared and shared.get("_updated_at"):
        # 共享状态存在且有效（30 秒内更新过）
        age = time.time() - shared["_updated_at"]
        if age < 60.0:  # 🔥 放宽到60秒，避免LLM调用期间误判过期
            logger.debug("status 共享内存+DB 校准 novel=%s age=%.1fs", novel_id, age)
            return _build_status_with_shared(novel_id, shared)

    # ── 第二层：经 DatabaseConnection 只读（与消费者共用 WAL 通道）──
    import sqlite3
    from application.paths import get_db_path
    from infrastructure.persistence.database.connection import get_database

    novel: Any = None

    try:
        db = get_database(get_db_path())

        row = db.fetch_one(
            "SELECT * FROM novels WHERE id = ?",
            (novel_id,),
        )
        if not row:
            return None
        novel = dict(row)

        agg_rows = db.fetch_all(
            "SELECT status, SUM(LENGTH(COALESCE(content,''))) as total_wc FROM chapters WHERE novel_id = ? GROUP BY status",
            (novel_id,),
        )
        completed_count = 0
        in_manuscript_count = 0
        total_words = 0
        for r in agg_rows:
            s = r["status"] or ""
            wc = r["total_wc"] or 0
            total_words += wc
            if s == "completed":
                completed_count += 1
                in_manuscript_count += 1
            elif s == "draft":
                in_manuscript_count += 1

        draft_row = db.fetch_one(
            "SELECT MAX(number) as max_num FROM chapters WHERE novel_id = ? AND status = 'draft' AND COALESCE(LENGTH(content),0) > 0",
            (novel_id,),
        )
        if draft_row and draft_row["max_num"]:
            current_chapter_number = draft_row["max_num"]
        else:
            completed_max = db.fetch_one(
                "SELECT MAX(number) as max_num FROM chapters WHERE novel_id = ? AND status = 'completed'",
                (novel_id,),
            )
            current_chapter_number = (
                (completed_max["max_num"] + 1)
                if (completed_max and completed_max["max_num"])
                else None
            )

    except sqlite3.OperationalError as e:
        if "database is locked" in str(e).lower() or "busy" in str(e).lower():
            logger.debug("status DB 被锁，降级到共享内存 novel=%s", novel_id)
            # 🔥 关键修复：DB 被锁时不再查 DB（novel_repo.get_by_id 也会被锁住！），
            # 改用共享内存构建降级状态。这是之前线程池耗尽的直接原因之一：
            # DB 锁 → 降级查 novel_repo → 也被锁 → 线程池线程被占满 → 所有 API 卡死
            if shared and shared.get("_updated_at"):
                return _build_status_pure_memory(novel_id, shared)
            return _build_fallback_from_shared(novel_id, shared)
        raise
    except Exception:
        # 🔥 同上：任何 DB 异常都优先用共享内存，不再查 DB
        logger.debug("status DB 异常，降级到共享内存 novel=%s", novel_id)
        if shared and shared.get("_updated_at"):
            return _build_status_pure_memory(novel_id, shared)
        return _build_fallback_from_shared(novel_id, shared)

    # 合并共享内存中的实时状态（如果存在）
    if shared:
        novel["current_stage"] = shared.get("current_stage", novel.get("current_stage"))
        novel["audit_progress"] = shared.get("audit_progress", novel.get("audit_progress"))
        novel["last_chapter_tension"] = shared.get("last_chapter_tension", novel.get("last_chapter_tension"))
        novel["last_audit_similarity"] = shared.get("last_audit_similarity", novel.get("last_audit_similarity"))
        novel["last_audit_drift_alert"] = shared.get("last_audit_drift_alert", novel.get("last_audit_drift_alert"))

    target = (novel.get("target_chapters") if isinstance(novel, dict) else novel.target_chapters) or 1
    twpc = (novel.get("target_words_per_chapter") if isinstance(novel, dict) else getattr(novel, "target_words_per_chapter", None)) or 2500

    lacn = novel.get("last_audit_chapter_number") if isinstance(novel, dict) else getattr(novel, "last_audit_chapter_number", None)
    last_tension = int((novel.get("last_chapter_tension") if isinstance(novel, dict) else getattr(novel, "last_chapter_tension", 0)) or 0)
    last_chapter_audit = None
    if lacn is not None:
        last_chapter_audit = {
            "chapter_number": int(lacn),
            "tension": last_tension,
            "drift_alert": bool((novel.get("last_audit_drift_alert") if isinstance(novel, dict) else getattr(novel, "last_audit_drift_alert", False))),
            "similarity_score": novel.get("last_audit_similarity") if isinstance(novel, dict) else getattr(novel, "last_audit_similarity", None),
            "narrative_sync_ok": bool((novel.get("last_audit_narrative_ok") if isinstance(novel, dict) else getattr(novel, "last_audit_narrative_ok", True))),
            "at": novel.get("last_audit_at") if isinstance(novel, dict) else getattr(novel, "last_audit_at", None),
            "vector_stored": bool((novel.get("last_audit_vector_stored") if isinstance(novel, dict) else getattr(novel, "last_audit_vector_stored", False))),
            "foreshadow_stored": bool((novel.get("last_audit_foreshadow_stored") if isinstance(novel, dict) else getattr(novel, "last_audit_foreshadow_stored", False))),
            "triples_extracted": bool((novel.get("last_audit_triples_extracted") if isinstance(novel, dict) else getattr(novel, "last_audit_triples_extracted", False))),
            "quality_scores": (novel.get("last_audit_quality_scores") if isinstance(novel, dict) else getattr(novel, "last_audit_quality_scores", {})) or {},
            "issues": (novel.get("last_audit_issues") if isinstance(novel, dict) else getattr(novel, "last_audit_issues", [])) or [],
        }

    _ap_status = novel.get("autopilot_status") if isinstance(novel, dict) else novel.autopilot_status
    _ap_status_str = _ap_status if isinstance(_ap_status, str) else (_ap_status.value if hasattr(_ap_status, "value") else str(_ap_status))
    _stage = novel.get("current_stage") if isinstance(novel, dict) else novel.current_stage
    _stage_str = _stage if isinstance(_stage, str) else (_stage.value if hasattr(_stage, "value") else str(_stage))

    # 🔥 读取守护进程心跳（判断后端是否存活）
    daemon_heartbeat = None
    daemon_alive = False
    try:
        from interfaces.main import _get_shared_state
        g_state = _get_shared_state()
        daemon_heartbeat = g_state.get("_daemon_heartbeat")
        if daemon_heartbeat:
            daemon_alive = (time.time() - daemon_heartbeat) < 60.0  # 60 秒内有心跳视为存活
    except Exception:
        pass

    return {
        "autopilot_status": _ap_status_str,
        "current_stage": _stage_str,
        "current_act": novel.get("current_act") if isinstance(novel, dict) else novel.current_act,
        "current_chapter_in_act": novel.get("current_chapter_in_act") if isinstance(novel, dict) else novel.current_chapter_in_act,
        "current_beat_index": novel.get("current_beat_index") if isinstance(novel, dict) else getattr(novel, "current_beat_index", 0),
        "current_auto_chapters": novel.get("current_auto_chapters") if isinstance(novel, dict) else getattr(novel, "current_auto_chapters", 0),
        "max_auto_chapters": novel.get("max_auto_chapters") if isinstance(novel, dict) else getattr(novel, "max_auto_chapters", 9999),
        "target_chapters": novel.get("target_chapters") if isinstance(novel, dict) else novel.target_chapters,
        "target_words_per_chapter": twpc,
        "target_plan_total_words": target * twpc,
        "last_chapter_tension": last_tension,
        "consecutive_error_count": novel.get("consecutive_error_count") if isinstance(novel, dict) else getattr(novel, "consecutive_error_count", 0),
        "total_words": total_words,
        "completed_chapters": completed_count,
        "progress_pct": round(completed_count / target * 100, 1) if target else 0,
        "manuscript_chapters": in_manuscript_count,
        "progress_pct_manuscript": round(in_manuscript_count / target * 100, 1) if target else 0,
        "current_chapter_number": current_chapter_number,
        "needs_review": _stage_needs_human_review(_stage_str),
        "auto_approve_mode": novel.get("auto_approve_mode") if isinstance(novel, dict) else getattr(novel, "auto_approve_mode", False),
        "last_chapter_audit": last_chapter_audit,
        "audit_progress": novel.get("audit_progress") if isinstance(novel, dict) else getattr(novel, "audit_progress", None),
        "daemon_alive": daemon_alive,
        "daemon_heartbeat_at": daemon_heartbeat,
    }


def _build_fallback_from_shared(novel_id: str, shared: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """🔥 DB 不可用且共享内存数据不全时的兜底状态。

    与 _build_fallback_status 不同：此方法不查 DB，完全基于共享内存。
    即使共享内存数据不全，也返回一个基本可用的状态，前端不会卡死。
    """
    if not shared:
        # 完全没有共享内存数据：返回最小状态
        return {
            "autopilot_status": "running",
            "current_stage": "syncing",
            "current_act": None,
            "current_chapter_in_act": None,
            "current_beat_index": 0,
            "current_auto_chapters": 0,
            "max_auto_chapters": 9999,
            "target_chapters": 0,
            "target_words_per_chapter": 2500,
            "target_plan_total_words": 0,
            "last_chapter_tension": 0,
            "consecutive_error_count": 0,
            "total_words": 0,
            "completed_chapters": 0,
            "progress_pct": 0,
            "manuscript_chapters": 0,
            "progress_pct_manuscript": 0,
            "current_chapter_number": None,
            "needs_review": False,
            "auto_approve_mode": False,
            "last_chapter_audit": None,
            "audit_progress": None,
            "_degraded": True,
            "_message": "数据同步中，请稍候...",
        }

    # 有共享内存但可能不完整
    return _build_status_pure_memory(novel_id, shared)


def _build_status_pure_memory(novel_id: str, shared: Dict[str, Any]) -> Dict[str, Any]:
    """🔥 纯共享内存路径：完全跳过 DB，1ms 返回。

    这是最关键的架构优化：当守护进程在写作/审计期间持有 DB 写锁时，
    /status 请求完全不碰 DB，只读共享内存，实现"状态与数据分离"。

    前提：守护进程在每次更新共享状态时缓存了统计信息（_cached_* 字段）。
    """
    # 读取守护进程心跳
    daemon_heartbeat = None
    daemon_alive = False
    try:
        from interfaces.main import _get_shared_state
        g_state = _get_shared_state()
        daemon_heartbeat = g_state.get("_daemon_heartbeat")
        if daemon_heartbeat:
            daemon_alive = (time.time() - daemon_heartbeat) < 60.0
    except Exception:
        pass

    # 构建 last_chapter_audit
    lacn = shared.get("last_audit_chapter_number")
    last_tension = int(shared.get("last_chapter_tension", 0) or 0)
    last_chapter_audit = None
    if lacn is not None:
        last_chapter_audit = {
            "chapter_number": int(lacn),
            "tension": last_tension,
            "drift_alert": bool(shared.get("last_audit_drift_alert", False)),
            "similarity_score": shared.get("last_audit_similarity"),
            "narrative_sync_ok": bool(shared.get("last_audit_narrative_ok", True)),
            "at": shared.get("last_audit_at"),
            "vector_stored": bool(shared.get("last_audit_vector_stored", False)),
            "foreshadow_stored": bool(shared.get("last_audit_foreshadow_stored", False)),
            "triples_extracted": bool(shared.get("last_audit_triples_extracted", False)),
            "quality_scores": shared.get("last_audit_quality_scores", {}) or {},
            "issues": shared.get("last_audit_issues", []) or [],
        }

    completed_count = shared.get("_cached_completed_chapters", 0)
    manuscript_count = shared.get("_cached_manuscript_chapters", 0)
    total_words = shared.get("_cached_total_words", 0)
    target = shared.get("target_chapters", 1) or 1
    twpc = shared.get("target_words_per_chapter", 2500) or 2500
    stage = shared.get("current_stage", "writing")

    return {
        "autopilot_status": shared.get("autopilot_status", "running"),
        "current_stage": stage,
        "current_act": shared.get("current_act"),
        "current_act_title": shared.get("current_act_title"),
        "current_act_description": shared.get("current_act_description"),
        "current_chapter_in_act": shared.get("current_chapter_in_act"),
        "current_beat_index": shared.get("current_beat_index", 0),
        "current_auto_chapters": shared.get("current_auto_chapters", 0),
        "max_auto_chapters": shared.get("max_auto_chapters", 9999),
        "target_chapters": target,
        "target_words_per_chapter": twpc,
        "target_plan_total_words": target * twpc,
        "last_chapter_tension": last_tension,
        "consecutive_error_count": shared.get("consecutive_error_count", 0),
        "total_words": total_words,
        "completed_chapters": completed_count,
        "progress_pct": round(completed_count / target * 100, 1) if target else 0,
        "manuscript_chapters": manuscript_count,
        "progress_pct_manuscript": round(manuscript_count / target * 100, 1) if target else 0,
        "current_chapter_number": shared.get("_cached_current_chapter_number"),
        "needs_review": _stage_needs_human_review(stage),
        "auto_approve_mode": shared.get("auto_approve_mode", False),
        "last_chapter_audit": last_chapter_audit,
        "audit_progress": shared.get("audit_progress"),
        "_from_shared_memory": True,
        "daemon_alive": daemon_alive,
        "daemon_heartbeat_at": daemon_heartbeat,
        "writing_substep": shared.get("writing_substep", ""),
        "writing_substep_label": shared.get("writing_substep_label", ""),
        "total_beats": shared.get("total_beats", 0),
        "beat_focus": shared.get("beat_focus", ""),
        "beat_target_words": shared.get("beat_target_words", 0),
        "accumulated_words": shared.get("accumulated_words", 0),
        "chapter_target_words": shared.get("chapter_target_words", 0),
        "context_tokens": shared.get("context_tokens", 0),
        "beat_hard_cap": shared.get("beat_hard_cap", 0),
        "beat_phase": shared.get("beat_phase", ""),
        "beat_max_words_hint": shared.get("beat_max_words_hint", 0),
        "beat_remaining_budget": shared.get("beat_remaining_budget", 0),
        "last_smart_truncate": shared.get("last_smart_truncate"),
        "planned_micro_beats": shared.get("planned_micro_beats") or [],
        "outline_plan_mode": shared.get("outline_plan_mode", ""),
    }


def _build_status_with_shared(novel_id: str, shared: Dict[str, Any]) -> Dict[str, Any]:
    """合并共享内存（阶段、审计进度等）与 SQLite 章节聚合（完稿/书稿/总字数）。

    聚合经 `get_database` 只读路径；失败时用共享内存 _cached_* 与 novels 行字段兜底，
    避免在守护进程持锁时长阻塞 /status。
    """
    from application.paths import get_db_path
    from infrastructure.persistence.database.connection import get_database

    db_path = get_db_path()
    completed_count = 0
    in_manuscript_count = 0
    total_words = 0
    current_chapter_number = None
    target = 1
    twpc = 2500

    try:
        db = get_database(db_path)

        agg_rows = db.fetch_all(
            "SELECT status, SUM(LENGTH(COALESCE(content,''))) as total_wc FROM chapters WHERE novel_id = ? GROUP BY status",
            (novel_id,),
        )
        for r in agg_rows:
            s = r["status"] or ""
            wc = r["total_wc"] or 0
            total_words += wc
            if s == "completed":
                completed_count += 1
                in_manuscript_count += 1
            elif s == "draft":
                in_manuscript_count += 1

        draft_row = db.fetch_one(
            "SELECT MAX(number) as max_num FROM chapters WHERE novel_id = ? AND status = 'draft' AND COALESCE(LENGTH(content),0) > 0",
            (novel_id,),
        )
        if draft_row and draft_row["max_num"]:
            current_chapter_number = draft_row["max_num"]
        else:
            completed_max = db.fetch_one(
                "SELECT MAX(number) as max_num FROM chapters WHERE novel_id = ? AND status = 'completed'",
                (novel_id,),
            )
            current_chapter_number = (
                (completed_max["max_num"] + 1)
                if (completed_max and completed_max["max_num"])
                else None
            )

        row = db.fetch_one(
            "SELECT target_chapters, target_words_per_chapter, autopilot_status, auto_approve_mode, consecutive_error_count FROM novels WHERE id = ?",
            (novel_id,),
        )
        if row:
            target = row["target_chapters"] or 1
            twpc = row["target_words_per_chapter"] or 2500
            autopilot_status = row["autopilot_status"] or "stopped"
            auto_approve_mode = bool(row["auto_approve_mode"])
            consecutive_error_count = row["consecutive_error_count"] or 0
        else:
            autopilot_status = "stopped"
            auto_approve_mode = False
            consecutive_error_count = 0

    except Exception as e:
        logger.debug("共享内存模式 DB 统计查询失败 novel=%s: %s，使用共享内存缓存值", novel_id, e)
        # 🔥 关键修复：DB 查询失败时，从共享内存读取缓存值，而不是返回 0
        # 守护进程每次更新共享状态时会写入缓存统计
        autopilot_status = shared.get("autopilot_status", "running")
        auto_approve_mode = shared.get("auto_approve_mode", False)
        consecutive_error_count = shared.get("consecutive_error_count", 0)
        target = shared.get("target_chapters", 1) or 1
        twpc = shared.get("target_words_per_chapter", 2500) or 2500
        completed_count = shared.get("_cached_completed_chapters", 0)
        in_manuscript_count = shared.get("_cached_manuscript_chapters", 0)
        total_words = shared.get("_cached_total_words", 0)
        current_chapter_number = shared.get("_cached_current_chapter_number")

    # 构建 last_chapter_audit
    lacn = shared.get("last_audit_chapter_number")
    last_tension = int(shared.get("last_chapter_tension", 0) or 0)
    last_chapter_audit = None
    if lacn is not None:
        last_chapter_audit = {
            "chapter_number": int(lacn),
            "tension": last_tension,
            "drift_alert": bool(shared.get("last_audit_drift_alert", False)),
            "similarity_score": shared.get("last_audit_similarity"),
            "narrative_sync_ok": bool(shared.get("last_audit_narrative_ok", True)),
            "at": shared.get("last_audit_at"),
            "vector_stored": bool(shared.get("last_audit_vector_stored", False)),
            "foreshadow_stored": bool(shared.get("last_audit_foreshadow_stored", False)),
            "triples_extracted": bool(shared.get("last_audit_triples_extracted", False)),
            "quality_scores": shared.get("last_audit_quality_scores", {}) or {},
            "issues": shared.get("last_audit_issues", []) or [],
        }

    stage = shared.get("current_stage", "writing")

    # 🔥 读取守护进程心跳
    daemon_heartbeat = None
    daemon_alive = False
    try:
        from interfaces.main import _get_shared_state
        g_state = _get_shared_state()
        daemon_heartbeat = g_state.get("_daemon_heartbeat")
        if daemon_heartbeat:
            daemon_alive = (time.time() - daemon_heartbeat) < 60.0
    except Exception:
        pass

    return {
        "autopilot_status": autopilot_status,
        "current_stage": stage,
        "current_act": shared.get("current_act"),
        "current_act_title": shared.get("current_act_title"),
        "current_act_description": shared.get("current_act_description"),
        "current_chapter_in_act": shared.get("current_chapter_in_act"),
        "current_beat_index": shared.get("current_beat_index", 0),
        "current_auto_chapters": shared.get("current_auto_chapters", 0),
        "max_auto_chapters": shared.get("max_auto_chapters", 9999),
        "target_chapters": target,
        "target_words_per_chapter": twpc,
        "target_plan_total_words": target * twpc,
        "last_chapter_tension": last_tension,
        "consecutive_error_count": consecutive_error_count,
        "total_words": total_words,
        "completed_chapters": completed_count,
        "progress_pct": round(completed_count / target * 100, 1) if target else 0,
        "manuscript_chapters": in_manuscript_count,
        "progress_pct_manuscript": round(in_manuscript_count / target * 100, 1) if target else 0,
        "current_chapter_number": current_chapter_number,
        "needs_review": _stage_needs_human_review(stage),
        "auto_approve_mode": auto_approve_mode,
        "last_chapter_audit": last_chapter_audit,
        "audit_progress": shared.get("audit_progress"),
        "_from_shared_memory": True,  # 前端可据此显示「实时同步中」提示
        "daemon_alive": daemon_alive,
        "daemon_heartbeat_at": daemon_heartbeat,
        # ★ V9 细化字段
        "writing_substep": shared.get("writing_substep", ""),
        "writing_substep_label": shared.get("writing_substep_label", ""),
        "total_beats": shared.get("total_beats", 0),
        "beat_focus": shared.get("beat_focus", ""),
        "beat_target_words": shared.get("beat_target_words", 0),
        "accumulated_words": shared.get("accumulated_words", 0),
        "chapter_target_words": shared.get("chapter_target_words", 0),
        "context_tokens": shared.get("context_tokens", 0),
        "beat_hard_cap": shared.get("beat_hard_cap", 0),
        "beat_phase": shared.get("beat_phase", ""),
        "beat_max_words_hint": shared.get("beat_max_words_hint", 0),
        "beat_remaining_budget": shared.get("beat_remaining_budget", 0),
        "last_smart_truncate": shared.get("last_smart_truncate"),
        "planned_micro_beats": shared.get("planned_micro_beats") or [],
        "outline_plan_mode": shared.get("outline_plan_mode", ""),
    }


def _chapter_stream_poll_sync(novel_repo, chapter_repo, novel_id: str):
    """章节 SSE：单轮 DB 读（写作阶段才拉全章节列表）。

    🔥 关键优化：写作阶段也改用轻量 SQL 聚合查询，不再全量加载章节对象。
    chapter_repo.list_by_novel 会加载所有章节的 content 字段（可能数百KB），
    审计期间 DB 被守护进程写锁持有时会阻塞线程池 5 秒以上。
    前端只需要知道当前章节号和状态，不需要全部章节对象。
    """
    novel = novel_repo.get_by_id(NovelId(novel_id))
    if not novel:
        return None, None
    chapters = None
    if novel.current_stage.value == "writing":
        # 🔥 轻量查询：只获取 draft 章节的编号和基本信息，不加载 content
        try:
            db = chapter_repo.db if hasattr(chapter_repo, 'db') else None
            if db is not None:
                rows = db.fetch_all(
                    "SELECT id, number, title, status FROM chapters WHERE novel_id = ? AND status = 'draft' ORDER BY number",
                    (novel_id,)
                )
                if rows:
                    chapters = []
                    for r in rows:
                        lc = _LightChapter(
                            id=r['id'],
                            number=r['number'],
                            title=r['title'],
                            status=ChapterStatus(r['status']) if r['status'] else ChapterStatus.DRAFT,
                        )
                        chapters.append(lc)
        except Exception:
            # DB 被锁时跳过，前端通过 /status 获取进度
            pass
    return novel, chapters


def _chapter_stream_chunks_sync(novel_id: str, max_chunks: int) -> List[str]:
    from application.engine.services.streaming_bus import streaming_bus

    return streaming_bus.get_chunks_batch(novel_id, max_chunks=max_chunks)


def _chapter_stream_tick_sync(novel_repo, chapter_repo, novel_id: str, max_chunks: int):
    """单次轮询：DB 读取 + chunks 获取合并在同一线程池任务中，减少 asyncio.to_thread 调用次数。"""
    novel, chapters = _chapter_stream_poll_sync(novel_repo, chapter_repo, novel_id)
    chunks = _chapter_stream_chunks_sync(novel_id, max_chunks) if novel else []
    return novel, chapters, chunks


def _autopilot_events_tick_sync(novel_repo, chapter_repo, novel_id: str) -> Tuple[Optional[Dict[str, Any]], bool]:
    """返回 (payload, 本轮 yield 后是否应结束流)；novel 不存在时 (None, True)。

    若共享里章节缓存非全零则走内存快路径；否则用与 /status 一致的 SQLite 聚合。
    """
    # 读共享快照（可能仅含阶段信息；计数见下方分支）
    shared = _get_shared_state_for_novel_cached(novel_id)

    if shared and shared.get("_updated_at") and shared.get("_cached_completed_chapters") is not None:
        cc = int(shared.get("_cached_completed_chapters") or 0)
        mw = shared.get("_cached_manuscript_chapters")
        mw_i = int(mw) if mw is not None else 0
        tw = shared.get("_cached_total_words")
        tw_i = int(tw) if tw is not None else 0
        # 与 /status 一致：三连 0 的快路径不可靠（历史上常为占位），改走 DB 聚合
        cache_looks_populated = cc > 0 or mw_i > 0 or tw_i > 0
        if cache_looks_populated:
            tgt = shared.get("target_chapters", 1) or 1
            data = {
                "autopilot_status": shared.get("autopilot_status", "stopped"),
                "current_stage": shared.get("current_stage", "writing"),
                "current_act": shared.get("current_act"),
                "current_act_title": shared.get("current_act_title"),
                "current_act_description": shared.get("current_act_description"),
                "current_beat_index": shared.get("current_beat_index", 0) or 0,
                "current_auto_chapters": shared.get("current_auto_chapters", 0) or 0,
                "target_chapters": tgt,
                "progress_pct": round((shared.get("_cached_completed_chapters", 0) or 0) / tgt * 100, 1) if tgt else 0,
                "total_words": shared.get("_cached_total_words", 0) or 0,
                "completed_chapters": shared.get("_cached_completed_chapters", 0) or 0,
                "current_chapter_number": shared.get("_cached_current_chapter_number"),
                "audit_progress": shared.get("audit_progress"),
                "last_chapter_tension": shared.get("last_chapter_tension", 0) or 0,
            }
            terminal_states = {"stopped", "error", "completed"}
            should_break = data["autopilot_status"] in terminal_states
            return data, should_break

    # 慢路径：novel + SQLite 聚合（共享无可用缓存或缓存不可信时）
    novel = novel_repo.get_by_id(NovelId(novel_id))
    if not novel:
        return None, True

    # 轻量 SQL 聚合
    try:
        db = chapter_repo.db if hasattr(chapter_repo, 'db') else None
        if db is not None:
            agg_rows = db.fetch_all(
                "SELECT status, SUM(LENGTH(COALESCE(content,''))) as total_wc FROM chapters WHERE novel_id = ? GROUP BY status",
                (novel_id,)
            )
            ev_completed = 0
            ev_in_manuscript = 0
            ev_total_words = 0
            for row in agg_rows:
                s = row['status'] or ''
                wc = row['total_wc'] or 0
                ev_total_words += wc
                if s == 'completed':
                    ev_completed += 1
                    ev_in_manuscript += 1
                elif s == 'draft':
                    ev_in_manuscript += 1

            draft_row = db.fetch_one(
                "SELECT MAX(number) as max_num FROM chapters WHERE novel_id = ? AND status = 'draft' AND COALESCE(LENGTH(content),0) > 0",
                (novel_id,)
            )
            if draft_row and draft_row['max_num']:
                ev_chapter_number = draft_row['max_num']
            else:
                completed_max = db.fetch_one(
                    "SELECT MAX(number) as max_num FROM chapters WHERE novel_id = ? AND status = 'completed'",
                    (novel_id,)
                )
                ev_chapter_number = (completed_max['max_num'] + 1) if (completed_max and completed_max['max_num']) else None
        else:
            raise RuntimeError("no db handle")
    except Exception:
        # DB 查询失败时用共享内存降级
        shared_ev = _get_shared_state_for_novel_cached(novel_id)
        ev_total_words = int(shared_ev.get("_cached_total_words", 0)) if shared_ev else 0
        ev_completed = shared_ev.get("_cached_completed_chapters", 0) if shared_ev else 0
        ev_in_manuscript = shared_ev.get("_cached_manuscript_chapters", 0) if shared_ev else 0
        ev_chapter_number = shared_ev.get("_cached_current_chapter_number") if shared_ev else None

    tgt = novel.target_chapters or 1
    data = {
        "autopilot_status": novel.autopilot_status.value,
        "current_stage": novel.current_stage.value,
        "current_act": novel.current_act,
        "current_act_title": getattr(novel, "current_act_title", None) or getattr(novel, "_current_act_title", None),
        "current_act_description": getattr(novel, "current_act_description", None) or getattr(novel, "_current_act_description", None),
        "current_beat_index": getattr(novel, "current_beat_index", 0),
        "current_chapter_number": ev_chapter_number,
        "completed_chapters": ev_completed,
        "manuscript_chapters": ev_in_manuscript,
        "progress_pct": round(ev_completed / tgt * 100, 1) if tgt else 0,
        "progress_pct_manuscript": round(ev_in_manuscript / tgt * 100, 1) if tgt else 0,
        "total_words": ev_total_words,
        "target_chapters": novel.target_chapters,
        "needs_review": _stage_needs_human_review(novel.current_stage.value),
        "consecutive_error_count": getattr(novel, "consecutive_error_count", 0),
    }
    terminal_states = {"stopped", "error", "completed"}
    should_break = (
        novel.autopilot_status.value in terminal_states
        and not _stage_needs_human_review(novel.current_stage.value)
    )
    return data, should_break


def _log_stream_replay_sync(novel_id: str, after_seq: int, last_seq_cursor: int) -> Tuple[List[str], int]:
    """历史快照重放：返回待 yield 的完整 SSE 行与更新后的 last_seq_cursor。"""
    out: List[str] = []
    last = last_seq_cursor
    if after_seq == 0:
        for snap in snapshot_for_novel(novel_id, limit=400):
            ev = {
                "type": "log_line",
                "message": shorten_log_message(snap.message),
                "timestamp": snap.timestamp_iso,
                "metadata": {
                    "seq": snap.seq,
                    "level": snap.level,
                    "logger": snap.logger_name,
                    "replay": True,
                },
            }
            out.append(f"data: {json.dumps(ev, ensure_ascii=False)}\n\n")
            last = max(last, snap.seq)
    return out, last


# ── 共享内存读取缓存（使用资源管理器的 CacheResource）──
# 缓存逻辑已移至 _SHARED_STATE_CACHE，以下函数为便捷封装


def _get_shared_state_for_novel_cached(novel_id: str) -> Optional[Dict[str, Any]]:
    """带缓存的共享内存读取（1 秒 TTL），减少 Manager.dict 代理 IPC 开销。

    multiprocessing.Manager.dict() 的每次 .get() 都是一次跨进程 IPC 调用（~0.1-1ms），
    SSE 每 2 秒轮询一次 + /status 每 3-5 秒轮询一次，积少成多。
    加 1 秒本地缓存后，同一秒内的多次读取只做一次 IPC。
    """
    # 使用资源管理器的缓存
    cached = _SHARED_STATE_CACHE.get(novel_id)
    if cached is not None:
        return cached

    # 缓存过期或不存在，从共享内存读取
    data = _get_shared_state_for_novel(novel_id)
    if data is not None:
        _SHARED_STATE_CACHE.set(novel_id, data)
    return data


def _log_stream_io_tick_sync(
    novel_repo,
    chapter_repo,
    novel_id: str,
    log_file_path: str,
    file_cursor: int,
    last_seq_cursor: int,
):
    """日志 SSE 单轮：读库 + tail 日志文件 + 内存环。novel 不存在时 novel 为 None。

    🔥 架构优化：优先从共享内存读取，避免 DB 锁竞争。
    """
    # 🔥 优先从共享内存读取状态（零 DB IO）
    shared = _get_shared_state_for_novel_cached(novel_id)

    # 构造一个轻量 novel 代理对象
    class _LightNovel:
        def __init__(self, shared_data):
            self._shared = shared_data or {}
            self.current_stage = type('obj', (object,), {'value': self._shared.get('current_stage', 'writing')})()
            self.autopilot_status = type('obj', (object,), {'value': self._shared.get('autopilot_status', 'stopped')})()
            # 🔥 添加缺失的属性
            self.current_act = self._shared.get('current_act')
            self.current_chapter_in_act = self._shared.get('current_chapter_in_act')
            self.current_beat_index = self._shared.get('current_beat_index', 0)
            self.target_chapters = self._shared.get('target_chapters', 0)
            self.title = self._shared.get('title', '')

    novel = _LightNovel(shared)

    # 🔥 只在共享内存没有数据时才查 DB（降级路径）
    if not shared or not shared.get("_updated_at"):
        db_novel = novel_repo.get_by_id(NovelId(novel_id))
        if not db_novel:
            return None, None, None, file_cursor, []
        novel = db_novel

    # 🔥 写作阶段：始终从数据库查询实时统计（缓存只在章节完成时更新）
    chapters_stats = None
    if novel.current_stage.value == "writing":
        try:
            db = chapter_repo.db if hasattr(chapter_repo, 'db') else None
            if db is not None:
                # 获取当前章节号
                draft_row = db.fetch_one(
                    "SELECT MAX(number) as max_num FROM chapters WHERE novel_id = ? AND status = 'draft' AND COALESCE(LENGTH(content),0) > 0",
                    (novel_id,)
                )
                current_ch = None
                if draft_row and draft_row['max_num']:
                    current_ch = draft_row['max_num']
                # 聚合统计
                agg_rows = db.fetch_all(
                    "SELECT status, SUM(LENGTH(COALESCE(content,''))) as total_wc FROM chapters WHERE novel_id = ? GROUP BY status",
                    (novel_id,)
                )
                completed_cnt = 0
                total_wc = 0
                for r in agg_rows:
                    s = r['status'] or ''
                    wc = r['total_wc'] or 0
                    total_wc += wc
                    if s == 'completed':
                        completed_cnt += 1
                chapters_stats = {
                    'current_chapter_number': current_ch,
                    'completed_count': completed_cnt,
                    'total_words': total_wc,
                }
        except Exception:
            # DB 被锁时使用共享内存缓存
            if shared:
                chapters_stats = {
                    'current_chapter_number': shared.get("_cached_current_chapter_number"),
                    'completed_count': shared.get("_cached_completed_chapters", 0),
                    'total_words': shared.get("_cached_total_words", 0),
                }

    file_lines, new_cursor = read_incremental_log_file_lines(log_file_path, novel_id, file_cursor)
    ring_batch = list(iter_new_for_novel(novel_id, last_seq_cursor, limit=200))

    # 🔥 获取审计事件
    from application.engine.services.streaming_bus import streaming_bus
    stream_data = streaming_bus.get_chunks_and_events_batch(novel_id, max_chunks=200)
    audit_events = stream_data.get("audit_events", [])

    return novel, chapters_stats, file_lines, new_cursor, ring_batch, audit_events


def _log_stream_boot_meta_sync(novel_repo, novel_id: str) -> Dict[str, Any]:
    novel_boot = novel_repo.get_by_id(NovelId(novel_id))
    init_meta: Dict[str, Any] = {}
    if novel_boot:
        init_meta = {
            "stage": novel_boot.current_stage.value,
            "stage_label": _stage_name_zh(novel_boot.current_stage.value),
            "autopilot_status": novel_boot.autopilot_status.value,
            "autopilot_status_label": _autopilot_status_zh(novel_boot.autopilot_status.value),
        }
    return init_meta


def _log_stream_file_cursor_init_sync(log_file_path: str, after_seq: int) -> int:
    if after_seq == 0:
        return initial_snapshot_offset(log_file_path)
    return file_end_offset(log_file_path)


def _clamp_autopilot_target_chapters(tc: int) -> int:
    return max(1, min(9999, int(tc)))


def _clamp_autopilot_words_per_chapter(w: int) -> int:
    return clamp_chapter_target_words(int(w))


class StartRequest(BaseModel):
    max_auto_chapters: Optional[int] = 9999  # 保护上限，默认几乎无限制，由 target_chapters 控制实际完成点
    target_chapters: Optional[int] = Field(
        default=None,
        ge=1,
        le=9999,
        description="本次启动采用的目标总章数（与前端向导一致时可原子落库，避免与 PUT /novels 竞态）",
    )
    target_words_per_chapter: Optional[int] = Field(
        default=None,
        ge=CHAPTER_TARGET_WORDS_MIN,
        le=CHAPTER_TARGET_WORDS_MAX,
        description="每章目标字数（与 chapter_target_limits 上限对齐）",
    )


