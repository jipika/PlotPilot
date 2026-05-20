"""自动驾驶守护进程 v2 - 全托管写作引擎（事务最小化 + 节拍幂等）

核心设计：
1. 死循环轮询数据库，捞出所有 autopilot_status=RUNNING 的小说
2. 根据 current_stage 执行对应的状态机逻辑
3. 事务最小化：DB 写操作只在读状态和更新状态两个瞬间，LLM 请求期间不持有锁
4. 节拍级幂等：每写完一个节拍立刻落库，断点续写从 current_beat_index 恢复
5. 熔断保护：连续失败 3 次挂起单本小说，全局熔断器防止 API 雪崩
"""
import time
import logging
import asyncio
from dataclasses import replace
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from domain.novel.entities.novel import Novel, NovelStage, AutopilotStatus
from domain.novel.entities.chapter import ChapterStatus
from domain.novel.value_objects.novel_id import NovelId
from domain.novel.repositories.novel_repository import NovelRepository
from domain.ai.services.llm_service import LLMService, GenerationConfig
from domain.ai.value_objects.prompt import Prompt
from application.engine.services.context_builder import ContextBuilder
from application.engine.services.background_task_service import BackgroundTaskService, TaskType
from application.workflows.auto_novel_generation_workflow import AutoNovelGenerationWorkflow
from application.engine.services.chapter_aftermath_pipeline import ChapterAftermathPipeline
from application.engine.services.style_constraint_builder import build_style_summary
from application.ai.llm_output_sanitize import strip_reasoning_artifacts
from application.ai.prose_fragment_aggregator import aggregate_inline_prose_fragments
from application.ai.llm_retry_policy import LLM_MAX_TOTAL_ATTEMPTS
from application.workflows.beat_continuation import format_prior_draft_for_prompt
from domain.novel.value_objects.chapter_id import ChapterId
from domain.novel.value_objects.word_count import WordCount
from domain.novel.value_objects.generation_preferences import GenerationPreferences
from domain.structure.story_node import StoryNode

logger = logging.getLogger(__name__)


def _coerce_word_count_to_int(wc: Any) -> int:
    """章节 word_count 可能为 int 或 WordCount 值对象。"""
    if wc is None:
        return 0
    if isinstance(wc, WordCount):
        return wc.value
    return int(wc)

# 定向修文：单章内 LLM 修文轮数上限（与全局一致）
VOICE_REWRITE_MAX_ATTEMPTS = LLM_MAX_TOTAL_ATTEMPTS
VOICE_REWRITE_THRESHOLD = 0.68
VOICE_WARNING_THRESHOLD_FALLBACK = 0.75


from application.engine.services.autopilot.chapter_writing_mixin import ChapterWritingMixin

class AutopilotDaemon(ChapterWritingMixin):
    """自动驾驶守护进程（v2 完整实现）"""

    def __init__(
        self,
        novel_repository,
        llm_service,
        context_builder,
        background_task_service,
        planning_service,
        story_node_repo,
        chapter_repository,
        poll_interval: int = 5,
        voice_drift_service=None,
        circuit_breaker=None,
        chapter_workflow: Optional[AutoNovelGenerationWorkflow] = None,
        aftermath_pipeline: Optional[ChapterAftermathPipeline] = None,
        volume_summary_service=None,
        foreshadowing_repository=None,
        knowledge_service=None,
    ):
        self.novel_repository = novel_repository
        self.llm_service = llm_service
        self.context_builder = context_builder
        self.background_task_service = background_task_service
        self.planning_service = planning_service
        self.story_node_repo = story_node_repo
        self.chapter_repository = chapter_repository
        self.poll_interval = poll_interval
        self.voice_drift_service = voice_drift_service
        self.circuit_breaker = circuit_breaker
        self.chapter_workflow = chapter_workflow
        self.aftermath_pipeline = aftermath_pipeline
        self.volume_summary_service = volume_summary_service
        self.foreshadowing_repository = foreshadowing_repository
        self.knowledge_service = knowledge_service

        # 章节"节拍耗尽但字数不足"重写计数器，key=(novel_id, chapter_num)
        # 防止清除重写陷入新的无限循环
        self._beat_exhausted_rewrite_count: Dict[tuple, int] = {}

        #: 本章写作阶段产生的 Beat 快照，供章后叙事同步写入 micro_beats（非章纲句读切分）
        self._pending_chapter_micro_beats: Dict[Tuple[str, int], List[Dict[str, Any]]] = {}

        # 惰性初始化 VolumeSummaryService
        if not self.volume_summary_service and llm_service and story_node_repo:
            from application.blueprint.services.volume_summary_service import VolumeSummaryService
            self.volume_summary_service = VolumeSummaryService(
                llm_service=llm_service,
                story_node_repository=story_node_repo,
                chapter_repository=chapter_repository,
                foreshadowing_repository=foreshadowing_repository,
            )

    def _push_persistence_command(self, command_type: str, payload: Dict) -> bool:
        """推送持久化命令到队列（CQRS 单一写入者模式）。"""
        if not hasattr(self, "_persistence_bridge"):
            from application.engine.services.autopilot.persistence_bridge import (
                AutopilotPersistenceBridge,
            )

            self._persistence_bridge = AutopilotPersistenceBridge()
        return self._persistence_bridge.push_command(command_type, payload)

    def run_forever(self):
        """守护进程主循环（事务最小化原则）"""
        logger.info("=" * 80)
        logger.info("🚀 Autopilot Daemon Started")
        logger.info(f"   Poll Interval: {self.poll_interval}s")
        logger.info(f"   Circuit Breaker: {'Enabled' if self.circuit_breaker else 'Disabled'}")
        logger.info(f"   Voice Drift Service: {'Enabled' if self.voice_drift_service else 'Disabled'}")
        logger.info(f"   Volume Summary Service: {'Enabled' if self.volume_summary_service else 'Disabled'}")
        logger.info("=" * 80)

        # 创建持久化事件循环（避免每个小说都 asyncio.run() 创建/销毁循环）
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        loop_count = 0
        while True:
            loop_count += 1
            loop_start = time.time()

            # 🔥 心跳：每轮循环写入共享内存，让前端能判断守护进程是否存活
            # 即使 LLM 调用卡住，心跳仍会定期更新（因为 _call_with_timeout 会超时释放）
            self._write_daemon_heartbeat()

            # 熔断器检查
            if self.circuit_breaker and self.circuit_breaker.is_open():
                wait = self.circuit_breaker.wait_seconds()
                logger.warning(f"⚠️  熔断器打开，暂停 {wait:.0f}s")
                time.sleep(min(wait, self.poll_interval))
                continue

            try:
                # 消费 mp.Queue 中的停止信号消息（设置本地 threading.Event）
                try:
                    from application.engine.services.streaming_bus import streaming_bus
                    streaming_bus.consume_stop_signals()
                except Exception:
                    pass

                active_novels = self._get_active_novels()  # 快速只读查询

                # 🔥 关键修复：清理已恢复为 RUNNING 但本地停止信号仍残留的小说
                # 场景：用户点"停止"→ threading.Event.set() → 用户点"开始"→ DB 改回 RUNNING
                # 但 mp.Queue 的 start_signal 可能还没被消费，threading.Event 仍为 set
                # 导致 _is_still_running() 永远返回 False，小说无法继续处理
                if active_novels:
                    self._cleanup_stale_stop_signals(active_novels)

                if loop_count % 10 == 1:  # 每10轮（约50秒）记录一次状态
                    logger.info(f"🔄 Loop #{loop_count}: 发现 {len(active_novels)} 本活跃小说")

                if active_novels:
                    for novel in active_novels:
                        novel_start = time.time()
                        loop.run_until_complete(self._process_novel(novel))
                        novel_elapsed = time.time() - novel_start
                        logger.debug(f"   [{novel.novel_id}] 处理耗时: {novel_elapsed:.2f}s")

            except Exception as e:
                logger.error(f"❌ Daemon 顶层异常: {e}", exc_info=True)

            loop_elapsed = time.time() - loop_start
            if loop_elapsed > self.poll_interval * 2:
                logger.warning(f"⏱️  Loop #{loop_count} 耗时过长: {loop_elapsed:.2f}s")

            time.sleep(self.poll_interval)

    def _get_active_novels(self) -> List[Novel]:
        """获取所有活跃小说（DB + 共享内存，避免 DB 与前端状态短暂不一致时漏捞）"""
        running = self.novel_repository.find_by_autopilot_status(
            AutopilotStatus.RUNNING.value
        )
        seen = {n.novel_id.value for n in running}

        try:
            from application.engine.services.shared_state_repository import (
                get_shared_state_repository,
            )

            shared_repo = get_shared_state_repository()
            for nid in shared_repo.get_all_novel_ids():
                if nid in seen:
                    continue
                state = shared_repo.get_novel_state(nid)
                if not state or state.autopilot_status != AutopilotStatus.RUNNING.value:
                    continue
                novel = self.novel_repository.get_by_id(NovelId(nid))
                if novel is None:
                    continue
                novel.autopilot_status = AutopilotStatus.RUNNING
                running.append(novel)
                seen.add(nid)
                logger.info(
                    "[%s] 共享内存为 running、DB 未同步，已纳入守护进程处理队列",
                    nid,
                )
        except Exception as e:
            logger.debug("合并共享内存 running 小说失败（可忽略）: %s", e)

        return running

    def _write_daemon_heartbeat(self) -> None:
        """写入守护进程心跳到共享内存，让前端判断后端是否存活。

        成熟方案做法：
        - 守护进程每轮循环写入时间戳（~5s 一次）
        - API 进程的 /status 读取心跳时间戳
        - 前端若连续 60s 未看到心跳更新，可显示"后端忙碌或网络延迟"

        🔥 改进：同时更新所有活跃小说的共享内存 _updated_at，
        避免 LLM 调用期间共享状态过期导致前端显示"后端处理中"。
        """
        now = time.time()
        try:
            import sys
            shared = sys.modules.get("__shared_state")
            if shared is not None:
                shared["_daemon_heartbeat"] = now
                # 🔥 同时刷新所有活跃小说的 _updated_at，防止共享状态过期
                for key in list(shared.keys()):
                    if key.startswith("novel:") and isinstance(shared[key], dict):
                        shared[key]["_updated_at"] = now
                return
        except Exception:
            pass
        # 降级：通过主进程模块
        try:
            from interfaces.main import update_shared_novel_state
            # 用特殊 key 写入心跳（非小说级别，而是全局级别）
            from interfaces.main import _get_shared_state
            state = _get_shared_state()
            state["_daemon_heartbeat"] = now
        except Exception:
            pass

    def _save_novel_ephemeral(self, novel: Novel) -> bool:
        """🔥 用独立短连接保存 novel 状态到 DB（替代 novel_repository.save()）。

        核心问题：novel_repository.save() 使用线程本地长连接，在守护进程
        （独立进程）中会长时间持有 SQLite 写锁，阻塞 API 进程。
        改为独立短连接：打开 → 写入 → 提交 → 关闭，写锁持有时间极短。

        字段必须与 SqliteNovelRepository.save() 的 ON CONFLICT UPDATE 完全一致，
        否则会遗漏更新导致数据丢失（如张力、审计快照等）。
        """
        import json as _json

        fields = {
            "autopilot_status": novel.autopilot_status.value if isinstance(novel.autopilot_status, AutopilotStatus) else str(novel.autopilot_status),
            "current_stage": novel.current_stage.value if hasattr(novel.current_stage, 'value') else str(novel.current_stage),
            "current_act": novel.current_act or 0,
            "current_chapter_in_act": novel.current_chapter_in_act or 0,
            "current_beat_index": novel.current_beat_index or 0,
            "current_auto_chapters": novel.current_auto_chapters or 0,
            "target_chapters": novel.target_chapters or 0,
            "target_words_per_chapter": novel.target_words_per_chapter or 2500,
            "consecutive_error_count": novel.consecutive_error_count or 0,
            "last_chapter_tension": novel.last_chapter_tension or 0,
            # 🔥 needs_review 是计算字段（由 current_stage == paused_for_review 推导），
            # novels 表无此列，不能写入 DB，否则会导致 "no such column: needs_review" 错误
            # 使整条审计落盘失败。前端通过 current_stage 自行推导 needs_review。
            "beats_completed": getattr(novel, 'beats_completed', 0) or 0,
            # 审计快照字段（与 SqliteNovelRepository.save() 对齐）
            "auto_approve_mode": 1 if getattr(novel, 'auto_approve_mode', False) else 0,
            "last_audit_chapter_number": getattr(novel, 'last_audit_chapter_number', None),
            "last_audit_similarity": getattr(novel, 'last_audit_similarity', None),
            "last_audit_drift_alert": 1 if getattr(novel, 'last_audit_drift_alert', False) else 0,
            "last_audit_narrative_ok": 1 if getattr(novel, 'last_audit_narrative_ok', True) else 0,
            "last_audit_at": getattr(novel, 'last_audit_at', None),
            "last_audit_vector_stored": 1 if getattr(novel, 'last_audit_vector_stored', False) else 0,
            "last_audit_foreshadow_stored": 1 if getattr(novel, 'last_audit_foreshadow_stored', False) else 0,
            "last_audit_triples_extracted": 1 if getattr(novel, 'last_audit_triples_extracted', False) else 0,
            "last_audit_quality_scores": _json.dumps(getattr(novel, 'last_audit_quality_scores', None)) if getattr(novel, 'last_audit_quality_scores', None) else None,
            "last_audit_issues": _json.dumps(getattr(novel, 'last_audit_issues', None)) if getattr(novel, 'last_audit_issues', None) else None,
            "audit_progress": getattr(novel, 'audit_progress', None),
        }

        set_clauses = [f"{k} = ?" for k in fields.keys()]
        set_clauses.append("updated_at = CURRENT_TIMESTAMP")
        sql = f"UPDATE novels SET {', '.join(set_clauses)} WHERE id = ?"
        params = list(fields.values()) + [novel.novel_id.value]

        # CQRS：全部由 API 消费者串行落库，守护进程不再直连写第二路
        ok = self._queue_sql(sql, params)
        if not ok:
            logger.warning("持久化队列写入失败 novel=%s（无短连接兜底）", novel.novel_id.value)
        return ok

    def _save_chapter_ephemeral(self, novel_id: str, chapter_number: int,
                                 content: str = None, status: str = None,
                                 word_count: int = None,
                                 tension_score: float = None,
                                 plot_tension: float = None,
                                 emotional_tension: float = None,
                                 pacing_tension: float = None) -> bool:
        """🔥 保存章节状态——CQRS 统一写入通道。

        默认推持久化队列（零锁竞争）；
        仅 completed 状态为关键路径（需同步落库），用短连接直接写。
        """
        set_parts = []
        params = []

        if content is not None:
            set_parts.append("content = ?")
            params.append(content)
        if status is not None:
            set_parts.append("status = ?")
            params.append(status)
        if word_count is not None:
            set_parts.append("word_count = ?")
            params.append(word_count)
        if tension_score is not None:
            set_parts.append("tension_score = ?")
            params.append(tension_score)
        if plot_tension is not None:
            set_parts.append("plot_tension = ?")
            params.append(plot_tension)
        if emotional_tension is not None:
            set_parts.append("emotional_tension = ?")
            params.append(emotional_tension)
        if pacing_tension is not None:
            set_parts.append("pacing_tension = ?")
            params.append(pacing_tension)

        if not set_parts:
            return True

        set_parts.append("updated_at = CURRENT_TIMESTAMP")
        sql = f"UPDATE chapters SET {', '.join(set_parts)} WHERE novel_id = ? AND number = ?"
        params.extend([novel_id, chapter_number])

        # 全部经由持久化队列；completed 亦不直连 DB，杜绝第二写者。
        return self._queue_sql(sql, params)

    def _queue_sql(self, sql: str, params: tuple | list = ()) -> bool:
        """CQRS 统一写入通道 — 见 AutopilotPersistenceBridge.queue_sql。"""
        if not hasattr(self, "_persistence_bridge"):
            from application.engine.services.autopilot.persistence_bridge import (
                AutopilotPersistenceBridge,
            )

            self._persistence_bridge = AutopilotPersistenceBridge()
        return self._persistence_bridge.queue_sql(sql, params)

    def _patch_novel_ephemeral(
        self,
        novel_id: NovelId,
        fields: Dict[str, Any],
        **kwargs: Any,
    ) -> bool:
        """增量 UPDATE novels——统一持久化队列，与单写者内核一致。"""
        from domain.novel.entities.novel import AutopilotStatus as _APS, NovelStage as _NS

        _ = kwargs

        if not fields:
            return True

        processed: Dict[str, Any] = {}
        for key, value in fields.items():
            if isinstance(value, _APS):
                processed[key] = value.value
            elif isinstance(value, _NS):
                processed[key] = value.value
            elif isinstance(value, bool):
                processed[key] = 1 if value else 0
            elif isinstance(value, (dict, list)):
                import json as _json

                processed[key] = _json.dumps(value, ensure_ascii=False)
            else:
                processed[key] = value

        processed["updated_at"] = datetime.now(timezone.utc).isoformat()

        set_clauses = [f"{key} = ?" for key in processed.keys()]
        values = list(processed.values()) + [novel_id.value]

        sql = f"UPDATE novels SET {', '.join(set_clauses)} WHERE id = ?"
        return self._queue_sql(sql, values)

    def _push_patch_to_queue(self, novel_id: NovelId, fields: Dict[str, Any]) -> None:
        """将增量更新推入持久化队列——CQRS 统一写入通道的兼容入口。

        现在底层统一走 _queue_sql → EXECUTE_SQL，此方法保留作为调用点兼容，
        处理枚举/bool/JSON 转换后构建 SQL 并推队列。
        """
        from domain.novel.entities.novel import AutopilotStatus as _APS, NovelStage as _NS
        from application.engine.services.persistence_queue import PersistenceCommandType

        # 枚举转换（与 _patch_novel_ephemeral 一致）
        processed = {}
        for key, value in fields.items():
            if isinstance(value, _APS):
                processed[key] = value.value
            elif isinstance(value, _NS):
                processed[key] = value.value
            elif isinstance(value, bool):
                processed[key] = 1 if value else 0
            elif isinstance(value, (dict, list)):
                import json as _json
                processed[key] = _json.dumps(value, ensure_ascii=False)
            else:
                processed[key] = value

        processed["updated_at"] = datetime.now(timezone.utc).isoformat()
        set_clauses = [f"{key} = ?" for key in processed.keys()]
        values = list(processed.values()) + [novel_id.value]
        sql = f"UPDATE novels SET {', '.join(set_clauses)} WHERE id = ?"

        ok = self._queue_sql(sql, values)
        if ok:
            logger.debug("[novel-%s] 增量更新已推队列", novel_id.value)
        else:
            logger.warning("[novel-%s] 推队列失败，数据可能丢失", novel_id.value)

    def _read_chapter_stats_ephemeral(
        self, novel_id: str, timeout: float = 5.0
    ) -> Optional[Tuple[int, int, int]]:
        """与 /autopilot/status DB 路径一致的章节聚合（短连接只读）。

        用于在审计完成、章节落库后刷新共享内存缓存，避免 _cache_stats_to_shared_memory
        用 current_auto_chapters=0 覆盖真实统计导致前端长期显示 0/0/总字数 0。
        """
        from application.paths import get_db_path
        from infrastructure.persistence.database.connection import get_database

        try:
            db = get_database(get_db_path())
            agg_rows = db.fetch_all(
                "SELECT status, SUM(LENGTH(COALESCE(content,''))) as total_wc "
                "FROM chapters WHERE novel_id = ? GROUP BY status",
                (novel_id,),
            )
            completed_count = 0
            in_manuscript_count = 0
            total_words = 0
            for r in agg_rows:
                s = r["status"] or ""
                wc = r["total_wc"] or 0
                total_words += int(wc)
                if s == "completed":
                    completed_count += 1
                    in_manuscript_count += 1
                elif s == "draft":
                    in_manuscript_count += 1
            return (completed_count, in_manuscript_count, total_words)
        except Exception as e:
            logger.debug("章节统计短连接读取失败 novel=%s: %s", novel_id, e)
            return None

    def _read_autopilot_status_ephemeral(self, novel_id: NovelId) -> Optional[AutopilotStatus]:
        """用独立 SQLite 连接读 autopilot_status。

        主仓储连接在 asyncio 与 asyncio.to_thread、或后台线程里并发用时，同一 sqlite3 连接
        跨线程未定义行为，且长连接可能看不到他处已提交的 STOPPED。短连接每次打开可读 WAL 最新提交。

        优化：使用 WAL 模式和更短的超时，提高响应速度。
        """
        from application.paths import get_db_path
        from infrastructure.persistence.database.connection import get_database

        db = get_database(get_db_path())
        row = db.fetch_one(
            "SELECT autopilot_status FROM novels WHERE id = ?",
            (novel_id.value,),
        )
        if not row:
            return None
        raw = row["autopilot_status"]
        try:
            return AutopilotStatus(raw)
        except ValueError:
            return AutopilotStatus.STOPPED

    def _merge_autopilot_status_from_db(self, novel: Novel) -> None:
        """用户点「停止」只改 DB；写库前必须合并，否则会覆盖 STOPPED。"""
        status = self._read_autopilot_status_ephemeral(novel.novel_id)
        if status is not None:
            novel.autopilot_status = status

    def _is_still_running(self, novel: Novel) -> bool:
        """检查自动驾驶是否仍在运行（IPC 优先 + DB 降级）。

        检测优先级：
        1. 本地 threading.Event（亚微秒级，零 I/O 开销）—— 主通道
        2. DB 降级（仅本地 Event 未设置时使用，如守护进程重启后冷启动）

        无论哪个通道检测到 STOPPED，都立即返回 False。
        """
        # 通道 1：本地停止信号（亚微秒级）
        try:
            from application.engine.services.novel_stop_signal import is_novel_stopped
            if is_novel_stopped(novel.novel_id.value) or is_novel_stopped("__all__"):
                novel.autopilot_status = AutopilotStatus.STOPPED
                return False
        except Exception:
            pass  # 模块未初始化时静默降级

        # 通道 2：DB 降级（守护进程重启后冷启动时仍需要）
        self._merge_autopilot_status_from_db(novel)
        return novel.autopilot_status == AutopilotStatus.RUNNING

    def _cleanup_stale_stop_signals(self, active_novels: List[Novel]) -> None:
        """🔥 清理残留的停止信号

        当用户"停止"→"开始"后，DB 中 autopilot_status 已恢复为 RUNNING，
        但守护进程内的 threading.Event 可能仍为 set 状态（mp.Queue 的
        start_signal 还没来得及被消费）。

        这个方法在每轮主循环中执行，确保 DB 为 RUNNING 的小说
        不会被残留的本地停止信号阻塞。
        """
        try:
            from application.engine.services.novel_stop_signal import is_novel_stopped, clear_local_novel_stop
            for novel in active_novels:
                nid = novel.novel_id.value
                if is_novel_stopped(nid):
                    # DB 中是 RUNNING，但本地 Event 为 set → 清除残留信号
                    # 先确认 DB 确实是 RUNNING（避免误清真正的停止信号）
                    db_status = self._read_autopilot_status_ephemeral(novel.novel_id)
                    if db_status == AutopilotStatus.RUNNING:
                        clear_local_novel_stop(nid)
                        logger.info(
                            f"[{nid}] 🔧 清除残留停止信号（DB=RUNNING，但本地 Event 仍为 set）"
                        )
        except Exception as e:
            logger.debug(f"清理残留停止信号失败（可忽略）: {e}")

    def _novel_is_running(self, novel_id: NovelId) -> bool:
        """流式轮询用：不修改内存 novel；检查是否仍为 RUNNING（IPC 优先 + DB 降级）。

        优先检查本地 threading.Event（零 I/O 开销），仅当未设置时降级到 DB 轮询。
        """
        # 通道 1：本地停止信号
        try:
            from application.engine.services.novel_stop_signal import is_novel_stopped
            if is_novel_stopped(novel_id.value) or is_novel_stopped("__all__"):
                return False
        except Exception:
            pass

        # 通道 2：DB 降级
        return self._novel_is_running_in_db(novel_id)

    def _novel_is_running_in_db(self, novel_id: NovelId) -> bool:
        """DB 降级路径：独立连接读是否仍为 RUNNING（仅当 mp.Event 未初始化时使用）。"""
        status = self._read_autopilot_status_ephemeral(novel_id)
        return status == AutopilotStatus.RUNNING

    def _flush_novel(self, novel: Novel) -> None:
        """关键阶段立即写库，避免下一轮轮询仍读到旧 stage（重复幕级规划 / 重复日志）。

        使用 patch 增量更新（仅写入变化的字段），减少写事务持锁时间。

        🔥 CQRS 架构：_patch_novel_ephemeral 推持久化队列（EXECUTE_SQL），
        由 API 进程消费者线程串行执行，与单写者内核一致。
        同步非统计字段到共享内存，避免 /status 长期读到过时阶段信息。
        章节聚合（完稿/书稿/总字数）由章节落库与审计完成路径写入 _cached_*。
        """
        self._merge_autopilot_status_from_db(novel)
        # 关键字段增量更新（不再全量 save 30+ 字段）
        patch_fields = dict(
            autopilot_status=novel.autopilot_status,
            current_stage=novel.current_stage,
            current_act=novel.current_act,
            current_chapter_in_act=novel.current_chapter_in_act,
            current_beat_index=novel.current_beat_index,
            beats_completed=novel.beats_completed,
            consecutive_error_count=novel.consecutive_error_count,
            current_auto_chapters=novel.current_auto_chapters,
        )
        # 审计快照字段（审计阶段写入，避免丢失）
        if getattr(novel, "last_audit_chapter_number", None) is not None:
            patch_fields["last_audit_chapter_number"] = novel.last_audit_chapter_number
            patch_fields["last_audit_similarity"] = getattr(novel, "last_audit_similarity", None)
            patch_fields["last_audit_drift_alert"] = getattr(novel, "last_audit_drift_alert", False)
            patch_fields["last_audit_narrative_ok"] = getattr(novel, "last_audit_narrative_ok", True)
            patch_fields["last_audit_vector_stored"] = getattr(novel, "last_audit_vector_stored", False)
            patch_fields["last_audit_foreshadow_stored"] = getattr(novel, "last_audit_foreshadow_stored", False)
            patch_fields["last_audit_triples_extracted"] = getattr(novel, "last_audit_triples_extracted", False)
            patch_fields["last_audit_at"] = getattr(novel, "last_audit_at", None)
        # 张力值
        if getattr(novel, "last_chapter_tension", None) is not None:
            patch_fields["last_chapter_tension"] = novel.last_chapter_tension

        # 🔥 CQRS：优先推持久化队列（零锁竞争），_patch_novel_ephemeral 内部
        # 默认走 _queue_sql → EXECUTE_SQL 命令，由 API 进程消费者串行执行
        ok = self._patch_novel_ephemeral(novel.novel_id, patch_fields)

        # 同步阶段、节拍等非聚合字段到共享内存（完稿/书稿/总字数仅在落库与审计节点写入 _cached_*）
        self._cache_stats_to_shared_memory(novel)

    def _save_novel_state(self, novel: Novel) -> None:
        """与 _flush_novel 相同语义：增量 patch 替代全量 save。"""
        self._flush_novel(novel)

    def _sync_novel_current_act_from_chapter_story_node(self, novel: Novel, chapter_node: StoryNode) -> None:
        """按章节在结构树上的父幕校正 ``novel.current_act``（0-based，且约定等于 ``act.number - 1``）。

        ``_find_next_unwritten_chapter_async`` 按全书章号扫描，而 ``current_act`` 仅在幕规划/
        幕写完时推进；若章节曾错误挂到别幕（或预生成高编号幕），会出现「正在写第 23 章却
        显示第 4 幕」等割裂。写作/审计前以**真实父幕**为准同步一次。
        """
        if not chapter_node or not getattr(chapter_node, "parent_id", None):
            return
        nid = novel.novel_id.value
        try:
            all_nodes = self.story_node_repo.get_by_novel_sync(nid)
            by_id = {n.id: n for n in all_nodes}
            parent = by_id.get(chapter_node.parent_id)
            if not parent or parent.node_type.value != "act":
                return
            act_serial = int(parent.number)  # story_nodes.act.number，全书幕序号
            desired = act_serial - 1
            if novel.current_act != desired:
                logger.info(
                    f"[{nid}] 校正 current_act：{novel.current_act} → {desired} "
                    f"（第{chapter_node.number}章挂于幕 act.number={act_serial} {parent.title!r}）"
                )
                novel.current_act = desired
                try:
                    self._push_patch_to_queue(novel.novel_id, {"current_act": desired})
                except Exception as pe:
                    logger.debug(f"[{nid}] 校正 current_act 落库入队失败（可忽略）: {pe}")
        except Exception as e:
            logger.debug(f"[{nid}] 按章节校正 current_act 失败（可忽略）: {e}")

    def _sync_novel_current_act_from_chapter_number(self, novel: Novel, chapter_num: int) -> None:
        """由全局章号查找 story 节点后同步 ``current_act``。"""
        if chapter_num is None or chapter_num < 1:
            return
        try:
            all_nodes = self.story_node_repo.get_by_novel_sync(novel.novel_id.value)
            ch_node = next(
                (
                    n
                    for n in all_nodes
                    if n.node_type.value == "chapter" and int(n.number) == int(chapter_num)
                ),
                None,
            )
            if ch_node:
                self._sync_novel_current_act_from_chapter_story_node(novel, ch_node)
        except Exception as e:
            logger.debug(f"[{novel.novel_id.value}] 按章号校正 current_act 失败（可忽略）: {e}")

    def _cache_stats_to_shared_memory(self, novel: Novel) -> None:
        """将「非统计」状态同步到共享内存（节拍 / flush 高频路径）。

        注意：不要在此写入 _cached_completed_chapters / _cached_manuscript_chapters /
        _cached_total_words / _cached_current_chapter_number。本方法在每次 patch 后调用；
        若在规划或节拍阶段用不完整的 novel 字段覆盖，会把审计刚对齐的缓存冲掉，
        前端会长期显示完稿 0、书稿 0、总字数 0。

        章节聚合统计仅在章节落库后与审计完成时，通过 _read_chapter_stats_ephemeral
        显式写入共享内存。
        """
        nid = novel.novel_id.value
        # 🔥 查询当前幕的标题和描述（供前端展示）
        current_act_title = None
        current_act_description = None
        try:
            if novel.current_act is not None:
                target_act_number = novel.current_act + 1  # 1-indexed
                all_nodes = self.story_node_repo.get_by_novel_sync(nid)
                act_nodes = sorted(
                    [n for n in all_nodes if n.node_type.value == "act"],
                    key=lambda n: n.number
                )
                target_act = next((n for n in act_nodes if n.number == target_act_number), None)
                if target_act:
                    current_act_title = target_act.title
                    current_act_description = target_act.description
        except Exception as e:
            logger.debug(f"[{nid}] 查询幕标题/描述失败（可忽略）: {e}")

        try:
            self._update_shared_state(
                nid,
                current_stage=novel.current_stage.value,
                current_act=novel.current_act,
                current_act_title=current_act_title,
                current_act_description=current_act_description,
                current_chapter_in_act=novel.current_chapter_in_act,
                current_beat_index=novel.current_beat_index or 0,
                autopilot_status=novel.autopilot_status.value,
                consecutive_error_count=novel.consecutive_error_count or 0,
                target_chapters=novel.target_chapters,
                target_words_per_chapter=getattr(novel, 'target_words_per_chapter', 2500) or 2500,
                auto_approve_mode=getattr(novel, 'auto_approve_mode', False),
                last_chapter_tension=getattr(novel, 'last_chapter_tension', 0) or 0,
                current_auto_chapters=novel.current_auto_chapters,
            )
        except Exception as e:
            logger.debug(f"[{nid}] 缓存统计到共享内存失败（可忽略）: {e}")

    # ── 故事线 / 编年史 共享内存同步 ───────────────────────────────

    def _sync_storylines_to_shared_memory(self, novel_id: str) -> None:
        """🔥 宏观规划完成后重新加载故事线到共享内存，确保甘特图/故事线列表实时可见。

        纯 DB 读取 + 内存写入，毫秒级，不阻塞事件循环。
        """
        try:
            from application.engine.services.state_bootstrap import StateBootstrap
            bootstrap = StateBootstrap()
            count = len(bootstrap._load_storylines(novel_id))
            logger.debug(f"[{novel_id}] 同步故事线到共享内存: {count} 条")
        except Exception as e:
            logger.debug(f"[{novel_id}] 同步故事线失败（可忽略）: {e}")

    async def _extract_chapter_bridge(self, novel_id: str, chapter_number: int, content: str) -> None:
        """🔗 衔接引擎：审计完成后提取章节桥段（5 维衔接锚点），供下一章首段衔接使用。

        策略：
        - 用 LLM 从章节末尾 ~1500 字提取：悬念钩子、情感余韵、场景状态、角色位置、未完成动作
        - 持久化到 chapter_bridges 表
        - 下一章写作时由 context_budget_allocator 的 T0 层自动注入衔接指令
        - 约 ~300 token 输入 + ~200 token 输出，成本极低
        """
        try:
            from application.engine.services.chapter_bridge_service import ChapterBridgeService
            from application.paths import get_db_path

            svc = ChapterBridgeService(
                llm_service=self.llm_service,
                db_path=str(get_db_path()),
            )
            bridge = await svc.extract_bridge(novel_id, chapter_number, content)
            logger.info(
                f"[{novel_id}] 🔗 桥段提取完成 ch={chapter_number} "
                f"hook={'有' if bridge.suspense_hook else '无'} "
                f"emotion={'有' if bridge.emotional_residue else '无'} "
                f"scene={'有' if bridge.scene_state else '无'}"
            )
        except Exception as e:
            logger.warning(f"[{novel_id}] 桥段提取失败（不影响主流程）ch={chapter_number}: {e}")

    async def _run_anti_ai_audit(
        self,
        novel_id: str,
        chapter_number: int,
        content: str,
    ) -> Any:
        """🛡️ Anti-AI 审计管线：对生成的章节进行 AI 味检测与审计。

        执行流程：
        1. 使用 ClicheScanner 扫描 AI 味模式
        2. 使用 AntiAIAuditor 生成审计报告
        3. 使用 AntiAIMetricsService 计算指标快照
        4. 使用 AntiAILearningService 学习新模式
        5. 将审计结果持久化到日志

        此方法为异步包装，实际扫描为同步操作。
        失败不影响主流程，返回 None；成功返回报告对象（供章末闸门判定）。
        """
        try:
            import asyncio
            loop = asyncio.get_event_loop()

            # 在线程池中执行同步扫描
            report = await loop.run_in_executor(
                None,
                self._sync_anti_ai_audit,
                novel_id,
                chapter_number,
                content,
            )

            if report:
                logger.info(
                    f"[{novel_id}] 🛡️ Anti-AI 审计完成 ch={chapter_number} "
                    f"score={report.metrics.severity_score} "
                    f"assessment={report.metrics.overall_assessment} "
                    f"hits={report.metrics.total_hits} "
                    f"critical={report.metrics.critical_hits}"
                )

                # 如果严重级别过高，记录警告
                if report.metrics.overall_assessment in ("中等", "严重"):
                    logger.warning(
                        f"[{novel_id}] 🛡️ 章节 {chapter_number} AI味过重 "
                        f"(score={report.metrics.severity_score}, "
                        f"assessment={report.metrics.overall_assessment})，"
                        f"建议：{'; '.join(report.recommendations[:2])}"
                    )
            return report

        except Exception as e:
            logger.warning(f"[{novel_id}] Anti-AI 审计失败（不影响主流程）ch={chapter_number}: {e}")
        return None

    def _sync_anti_ai_audit(
        self,
        novel_id: str,
        chapter_number: int,
        content: str,
    ):
        """同步执行 Anti-AI 审计。"""
        from application.audit.services.anti_ai_audit import get_anti_ai_auditor
        from application.audit.services.anti_ai_metrics import get_anti_ai_metrics_service
        from application.audit.services.anti_ai_learning import get_anti_ai_learning_service

        # 1. 审计扫描
        auditor = get_anti_ai_auditor()
        chapter_id = f"ch-{chapter_number}"
        report = auditor.scan_chapter(chapter_id, content)

        # 2. 计算指标
        metrics_svc = get_anti_ai_metrics_service()
        snapshot = metrics_svc.compute_snapshot(
            chapter_id=chapter_id,
            chapter_number=chapter_number,
            content=content,
            hits=report.hits,
        )

        # 3. 学习新模式
        learning_svc = get_anti_ai_learning_service()
        learning_svc.analyze_chapter_audit(
            novel_id=novel_id,
            chapter_number=chapter_number,
            content=content,
            hits=report.hits,
        )

        # 4. 持久化审计结果到数据库
        try:
            from infrastructure.persistence.database.sqlite_anti_ai_audit_repository import SqliteAntiAiAuditRepository
            from infrastructure.persistence.database.connection import get_database

            db = get_database()
            repo = SqliteAntiAiAuditRepository(db)
            repo.upsert(
                novel_id=novel_id,
                chapter_number=chapter_number,
                total_hits=report.metrics.total_hits,
                critical_hits=report.metrics.critical_hits,
                warning_hits=report.metrics.warning_hits,
                info_hits=report.metrics.info_hits,
                severity_score=report.metrics.severity_score,
                overall_assessment=report.metrics.overall_assessment,
                hit_density=snapshot.hit_density,
                critical_density=snapshot.critical_density,
                category_distribution=report.metrics.category_distribution,
                top_patterns=report.metrics.top_patterns,
                recommendations=report.recommendations,
                improvement_suggestions=report.improvement_suggestions,
                hits_detail=[
                    {
                        "pattern": h.pattern,
                        "text": h.text,
                        "start": h.start,
                        "end": h.end,
                        "severity": h.severity,
                        "category": h.category,
                        "replacement_hint": h.replacement_hint,
                    }
                    for h in report.hits
                ],
            )
            logger.debug(
                f"[{novel_id}] 🛡️ Anti-AI 审计结果已持久化 ch={chapter_number}"
            )
        except Exception as persist_err:
            logger.warning(
                f"[{novel_id}] Anti-AI 审计结果持久化失败（不影响主流程）ch={chapter_number}: {persist_err}"
            )

        return report

    async def _continuity_self_check(
        self,
        novel_id: str,
        chapter_number: int,
        content: str,
    ) -> str:
        """🔗 衔接自检：检查章节首段与前章桥段的衔接度，低于阈值则自动修整。

        仅在非第 1 章时触发。约 ~200 token 的轻量 LLM 调用。
        如果衔接度 < 0.6，自动修整首段（最多 2 轮）。
        """
        try:
            from application.engine.services.chapter_bridge_service import ChapterBridgeService
            from application.paths import get_db_path

            svc = ChapterBridgeService(
                llm_service=self.llm_service,
                db_path=str(get_db_path()),
            )
            prev_bridge = svc.get_prev_chapter_bridge(novel_id, chapter_number)
            if not prev_bridge:
                logger.debug(f"[{novel_id}] 无前章桥段，跳过衔接自检 ch={chapter_number}")
                return content

            result = await svc.check_continuity(novel_id, chapter_number, content, prev_bridge)
            logger.info(
                f"[{novel_id}] 🔗 衔接自检 ch={chapter_number} score={result.score:.2f}"
                + (f" issues={result.issues}" if result.issues else "")
            )

            # 衔接度低于 0.6，自动修整
            if result.score < 0.6 and result.issues:
                logger.warning(
                    f"[{novel_id}] 🔗 衔接度低 ({result.score:.2f})，自动修整首段 ch={chapter_number}"
                )
                fixed_content = await svc.auto_fix_opening(
                    novel_id, chapter_number, content, prev_bridge, result, max_rounds=2
                )
                if fixed_content != content:
                    logger.info(
                        f"[{novel_id}] 🔗 首段修整完成 ch={chapter_number} "
                        f"原={len(content)}字→新={len(fixed_content)}字"
                    )
                    return fixed_content
                else:
                    logger.info(f"[{novel_id}] 🔗 首段修整未改变内容 ch={chapter_number}")
        except Exception as e:
            logger.warning(f"[{novel_id}] 衔接自检失败（不影响主流程）ch={chapter_number}: {e}")

        return content

    # ── 信息密度检测阈值（每 500 字应有 1 条新事实）──
    INFO_DENSITY_MIN_FACTS_PER_500 = 0.6   # 低于此值时补写
    INFO_DENSITY_MAX_SUPPLEMENT = 1        # 最多补写 1 次，控制时间成本

    def _estimate_info_density(self, content: str) -> float:
        """轻量估算章节信息密度（无 LLM，纯规则）。

        策略：将"可复述新事实"近似为以下句式的命中数：
        - 包含「发现」「得知」「意识到」「决定」「表示」「承认」「透露」「说」「答」「道」等动词的句子
        - 包含人名 + 动作的句子（而非景物/体感）
        这是一种快速启发式，不精确但足以识别"全章无事发生"。

        Returns:
            facts_per_500: 每 500 字的事实句估计数量
        """
        import re
        if not content or len(content) < 100:
            return 1.0  # 太短的章节不做处罚

        # 句子分割（以句号、感叹号、问号为边界）
        sentences = re.split(r'[。！？…]+', content)
        sentences = [s.strip() for s in sentences if len(s.strip()) > 5]

        fact_keywords = frozenset([
            "发现", "得知", "意识到", "决定", "表示", "承认", "透露", "说", "答", "道",
            "问", "笑", "皱眉", "叹", "沉默", "转身", "离开", "拿起", "放下", "走",
            "站", "坐", "看", "盯", "抬头", "低头", "挥手", "点头", "摇头",
            "掏出", "交给", "递", "接", "打开", "关上", "进入", "离开",
        ])
        fact_count = sum(
            1 for s in sentences
            if any(kw in s for kw in fact_keywords)
        )
        chars = max(1, len(content.replace("\n", "").replace(" ", "")))
        return fact_count / (chars / 500)

    async def _density_supplement_beat(
        self,
        novel_id: str,
        chapter_num: int,
        outline: str,
        existing_content: str,
        target_word_count: int,
        novel: Any,
    ) -> str:
        """信息密度补写：追加一个「情节推进节拍」使内容更充实。

        只在密度低于阈值时触发，最多补写 INFO_DENSITY_MAX_SUPPLEMENT 次。
        补写内容追加到原正文末尾。
        """
        supplement_words = max(400, target_word_count // 5)
        try:
            from domain.ai.value_objects.prompt import Prompt
            from domain.ai.services.llm_service import GenerationConfig
            from infrastructure.ai.prompt_keys import AUTOPILOT_INFO_DENSITY_SUPPLEMENT
            from infrastructure.ai.prompt_registry import get_prompt_registry

            variables = {
                "existing_content": existing_content[-400:],
                "supplement_words": str(supplement_words),
                "chapter_num": str(chapter_num),
                "novel_id": novel_id,
            }
            registry = get_prompt_registry()
            p = registry.render_to_prompt(AUTOPILOT_INFO_DENSITY_SUPPLEMENT, variables)
            if not p:
                from infrastructure.ai.prompt_utils import get_prompt_system
                system = get_prompt_system(AUTOPILOT_INFO_DENSITY_SUPPLEMENT)
                user_msg = (
                    f"【信息密度补写指令】\n"
                    f"本章大纲：{outline}\n\n"
                    f"本章已生成正文（末尾约400字供参考）：\n"
                    f"…{existing_content[-400:]}\n\n"
                    f"请接续已有正文，补写一段约 {supplement_words} 字的情节推进段落。\n"
                    f"要求：\n"
                    f"1. 至少包含一个角色做出具体决定或行动并产生后果\n"
                    f"2. 或引入一条新信息/线索/冲突\n"
                    f"3. 与前文情绪和场景无缝衔接，不重复已有内容\n"
                    f"4. 不要写章节标题，直接输出正文\n"
                )
                p = Prompt(system=system, user=user_msg)
            cfg = GenerationConfig(max_tokens=int(supplement_words * 1.5), temperature=0.82)
            result = await self.llm_service.generate(p, cfg)
            supplement = (result.content if hasattr(result, "content") else str(result)).strip()
            if supplement:
                logger.info(
                    "[%s] 📈 信息密度补写：ch=%d 追加 %d 字",
                    novel_id, chapter_num, len(supplement),
                )
                return existing_content.rstrip() + "\n\n" + supplement
        except Exception as exc:
            logger.warning("[%s] 信息密度补写失败（不影响主流程）ch=%d: %s", novel_id, chapter_num, exc)
        return existing_content

    def _sync_chronicles_to_shared_memory(self, novel_id: str) -> None:
        """🔥 审计完成后重新构建编年史缓存（Bible timeline_notes + snapshots + chapters），确保全息编年史实时可见。

        纯内存读取 + 聚合写入，纳秒级，不阻塞事件循环。
        """
        try:
            from application.engine.services.state_bootstrap import StateBootstrap
            bootstrap = StateBootstrap()
            count = len(bootstrap._load_chronicles(novel_id))
            logger.debug(f"[{novel_id}] 同步编年史到共享内存: {count} 行")
        except Exception as e:
            logger.debug(f"[{novel_id}] 同步编年史失败（可忽略）: {e}")

    async def _process_novel(self, novel: Novel):
        """处理单个小说（全流程）"""
        try:
            # 🔥 二次防线：处理小说前再次清理残留停止信号
            # 场景：_cleanup_stale_stop_signals 和此处之间可能有新的 stop→start 事件
            try:
                from application.engine.services.novel_stop_signal import is_novel_stopped, clear_local_novel_stop
                if is_novel_stopped(novel.novel_id.value):
                    db_status = self._read_autopilot_status_ephemeral(novel.novel_id)
                    if db_status == AutopilotStatus.RUNNING:
                        clear_local_novel_stop(novel.novel_id.value)
                        logger.info(
                            f"[{novel.novel_id}] 🔧 _process_novel: 清除残留停止信号"
                        )
            except Exception:
                pass

            if not self._is_still_running(novel):
                logger.info(f"[{novel.novel_id}] 用户已停止自动驾驶，跳过本轮")
                return

            stage_name = novel.current_stage.value
            logger.debug(f"[{novel.novel_id}] 当前阶段: {stage_name}")

            if novel.current_stage == NovelStage.MACRO_PLANNING:
                logger.info(f"[{novel.novel_id}] 📋 开始宏观规划")
                await self._handle_macro_planning(novel)
            elif novel.current_stage == NovelStage.ACT_PLANNING:
                logger.info(f"[{novel.novel_id}] 📝 开始幕级规划 (第 {novel.current_act + 1} 幕)")
                await self._handle_act_planning(novel)
            elif novel.current_stage == NovelStage.WRITING:
                logger.info(f"[{novel.novel_id}] ✍️  开始写作 (第 {novel.current_act + 1} 幕)")
                await self._handle_writing(novel)
            elif novel.current_stage == NovelStage.AUDITING:
                logger.info(f"[{novel.novel_id}] 🔍 开始审计")
                await self._handle_auditing(novel)
            elif novel.current_stage == NovelStage.PAUSED_FOR_REVIEW:
                # 全自动模式：跳过审阅，直接进入下一阶段
                if getattr(novel, 'auto_approve_mode', False):
                    logger.info(f"[{novel.novel_id}] 🚀 全自动模式：跳过人工审阅")
                    # 根据当前状态自动进入下一阶段
                    # 宏观规划完成后 -> 幕级规划
                    # 幕级规划完成后 -> 写作
                    # 写作完成后 -> 审计
                    novel.current_stage = NovelStage.ACT_PLANNING
                    self._save_novel_state(novel)
                    return
                else:
                    logger.debug(f"[{novel.novel_id}] ⏸️  等待人工审阅")
                    return  # 人工干预点：不处理，等前端确认

            # ✅ 收尾写库（合并 DB 停止标志，避免把用户「停止」写回 RUNNING）
            self._merge_autopilot_status_from_db(novel)
            if novel.autopilot_status == AutopilotStatus.RUNNING:
                if self.circuit_breaker:
                    self.circuit_breaker.record_success()
                novel.consecutive_error_count = 0
            else:
                logger.info(f"[{novel.novel_id}] 💾 本轮结束（用户已停止，不再计成功/重置熔断）")
            self._save_novel_state(novel)
            logger.debug(f"[{novel.novel_id}] 💾 状态已保存")

        except Exception as e:
            logger.error(f"❌ [{novel.novel_id}] 处理失败: {e}", exc_info=True)

            self._merge_autopilot_status_from_db(novel)
            if novel.autopilot_status != AutopilotStatus.RUNNING:
                logger.info(f"[{novel.novel_id}] 处理异常但用户已停止，不累计熔断/失败次数")
                self._save_novel_state(novel)
                return

            # 熔断器：记录失败
            if self.circuit_breaker:
                self.circuit_breaker.record_failure()
            novel.consecutive_error_count = (novel.consecutive_error_count or 0) + 1

            if novel.consecutive_error_count >= 3:
                # 单本小说连续 3 次错误 → 挂起（不影响其他小说）
                logger.error(f"🚨 [{novel.novel_id}] 连续失败 {novel.consecutive_error_count} 次，挂起等待急救")
                novel.autopilot_status = AutopilotStatus.ERROR
            else:
                logger.warning(f"⚠️  [{novel.novel_id}] 连续失败 {novel.consecutive_error_count}/3 次")
            self._save_novel_state(novel)

    async def _handle_macro_planning(self, novel: Novel):
        """处理宏观规划（规划部/卷/幕）- 使用极速模式让 AI 自主推断结构"""
        if not self._is_still_running(novel):
            return

        # ★ 子步骤状态：宏观规划
        self._update_shared_state(
            novel.novel_id.value,
            writing_substep="macro_planning",
            writing_substep_label="宏观规划",
        )

        target_chapters = novel.target_chapters or 30

        logger.info(
            "[%s] macro_planning start target_chapters=%s",
            novel.novel_id.value,
            target_chapters,
        )

        # 使用极速模式：structure_preference=None，让 AI 根据目标章节数智能决定结构
        # 这样 30 章、100 章、300 章、500 章会自动生成不同规模的叙事骨架
        result = await self.planning_service.generate_macro_plan(
            novel_id=novel.novel_id.value,
            target_chapters=target_chapters,
            structure_preference=None,
        )

        ok = bool(result.get("success"))
        n_parts = len(result.get("structure") or []) if isinstance(result.get("structure"), list) else -1
        logger.info(
            "[%s] macro_planning generate_macro_plan returned success=%s parts=%s",
            novel.novel_id.value,
            ok,
            n_parts,
        )

        if not self._is_still_running(novel):
            logger.info(f"[{novel.novel_id}] 宏观规划 LLM 返回后检测到停止，不再落库")
            return

        await self.planning_service.apply_macro_plan_from_llm_result(
            result,
            novel_id=novel.novel_id.value,
            target_chapters=target_chapters,
            minimal_fallback_on_empty=True,
        )

        # ⏸ 幕级大纲已就绪，进入人工审阅点（先落库再记日志，防止未保存导致下轮仍跑宏观规划）
        # 全自动模式：跳过审阅，直接进入幕级规划
        if getattr(novel, 'auto_approve_mode', False):
            novel.current_stage = NovelStage.ACT_PLANNING
            self._flush_novel(novel)
            # 🔥 宏观规划完成：同步故事线到共享内存（甘特图实时可见）
            self._sync_storylines_to_shared_memory(novel.novel_id.value)
            logger.info(f"[{novel.novel_id}] 🚀 全自动模式：宏观规划完成，直接进入幕级规划")
        else:
            novel.current_stage = NovelStage.PAUSED_FOR_REVIEW
            self._flush_novel(novel)
            # 🔥 宏观规划完成：同步故事线到共享内存（甘特图实时可见）
            self._sync_storylines_to_shared_memory(novel.novel_id.value)
            logger.info(f"[{novel.novel_id}] 宏观规划完成，进入审阅等待")

    async def _handle_act_planning(self, novel: Novel):
        """处理幕级规划（插入缓冲章策略 + 动态幕生成）"""
        if not self._is_still_running(novel):
            return

        # ★ 子步骤状态：幕级规划
        self._update_shared_state(
            novel.novel_id.value,
            writing_substep="act_planning",
            writing_substep_label=f"第 {novel.current_act + 1} 幕规划",
        )

        novel_id = novel.novel_id.value
        target_act_number = novel.current_act + 1  # 1-indexed

        # 提前计算结构推荐参数，供后续多处使用（避免动态幕生成失败时变量未定义）
        from application.blueprint.services.continuous_planning_service import calculate_structure_params
        target_chapters = novel.target_chapters or 100
        struct_params = calculate_structure_params(target_chapters)
        rec_chapters_per_act = struct_params["chapters_per_act"]
        rec_acts_per_volume = struct_params["acts_per_volume"]

        all_nodes = await self.story_node_repo.get_by_novel(novel_id)
        act_nodes = sorted(
            [n for n in all_nodes if n.node_type.value == "act"],
            key=lambda n: n.number
        )

        target_act = next((n for n in act_nodes if n.number == target_act_number), None)

        # 动态幕生成：超长篇可能只规划了部/卷框架，幕节点需要动态生成
        if not target_act:
            # 先尝试找到父卷节点
            volume_nodes = sorted(
                [n for n in all_nodes if n.node_type.value == "volume"],
                key=lambda n: n.number
            )

            # 🚨 安全检查：如果没有卷节点，说明宏观规划失败，重新规划
            if not volume_nodes:
                logger.error(
                    f"[{novel_id}] 宏观规划缺少卷节点！无法进行幕级规划。"
                    f"parts={len([n for n in all_nodes if n.node_type.value == 'part'])}, "
                    f"volumes=0, acts={len(act_nodes)}. "
                    f"触发重新规划..."
                )
                # 回退到宏观规划阶段重新生成
                novel.current_stage = NovelStage.MACRO_PLANNING
                novel.current_act = 0
                self._flush_novel(novel)
                return

            # 智能父卷选择：优先让当前卷填满（达到 rec_acts_per_volume 幕），再跳下一卷
            parent_volume = self._find_parent_volume_for_new_act(
                volume_nodes=volume_nodes,
                act_nodes=act_nodes,
                current_auto_chapters=novel.current_auto_chapters or 0,
                target_chapters=target_chapters,
                rec_acts_per_volume=rec_acts_per_volume,
                novel_id=novel.novel_id,
            )

            if parent_volume:
                logger.info(
                    f"[{novel.novel_id}] 🎯 动态生成第 {target_act_number} 幕"
                    f"（父卷：第 {parent_volume.number} 卷，每幕建议 {rec_chapters_per_act} 章）"
                )
                try:
                    # 使用最后一个幕作为参考（如果有）
                    last_act = act_nodes[-1] if act_nodes else None
                    if last_act:
                        await self.planning_service.create_next_act_auto(
                            novel_id=novel_id,
                            current_act_id=last_act.id
                        )
                    else:
                        # 完全没有幕节点，创建第一个幕
                        logger.info(f"[{novel.novel_id}] 创建首幕")
                        from domain.structure.story_node import StoryNode, NodeType, PlanningStatus, PlanningSource
                        first_act = StoryNode(
                            id=f"act-{novel_id}-1",
                            novel_id=novel_id,
                            parent_id=parent_volume.id,
                            node_type=NodeType.ACT,
                            number=1,
                            title="第一幕 · 开端",
                            description="故事起始，建立世界观与主角目标",
                            order_index=0,
                            planning_status=PlanningStatus.CONFIRMED,
                            planning_source=PlanningSource.AI_MACRO,
                            suggested_chapter_count=rec_chapters_per_act,
                        )
                        await self.story_node_repo.save(first_act)
                    
                    # 重新加载
                    all_nodes = await self.story_node_repo.get_by_novel(novel_id)
                    act_nodes = sorted(
                        [n for n in all_nodes if n.node_type.value == "act"],
                        key=lambda n: n.number
                    )
                    target_act = next((n for n in act_nodes if n.number == target_act_number), None)
                except Exception as e:
                    logger.warning(f"[{novel.novel_id}] 动态幕生成失败: {e}")

            if not target_act:
                logger.error(f"[{novel.novel_id}] 找不到第 {target_act_number} 幕，且动态生成失败，回退到宏观规划")
                novel.current_stage = NovelStage.MACRO_PLANNING
                novel.current_act = 0
                self._flush_novel(novel)
                return

        # 检查该幕下是否已有章节节点（避免重复规划）
        act_children = self.story_node_repo.get_children_sync(target_act.id)
        confirmed_chapters = [n for n in act_children if n.node_type.value == "chapter"]

        just_created_chapter_plan = False
        if not confirmed_chapters:
            # 使用结构计算引擎的推荐值作为 fallback（替代硬编码的 5）
            chapter_budget = target_act.suggested_chapter_count or rec_chapters_per_act
            if not target_act.suggested_chapter_count:
                logger.info(
                    f"[{novel.novel_id}] 幕 {target_act_number} 无 suggested_chapter_count，"
                    f"使用引擎推荐值 {rec_chapters_per_act}"
                )
            plan_result: Dict[str, Any] = {}
            try:
                plan_result = await self.planning_service.plan_act_chapters(
                    act_id=target_act.id,
                    custom_chapter_count=chapter_budget
                )
            except Exception as e:
                logger.warning(
                    f"[{novel.novel_id}] plan_act_chapters 未捕获异常: {e}",
                    exc_info=True,
                )
                plan_result = {}

            if not self._is_still_running(novel):
                logger.info(f"[{novel.novel_id}] 幕级规划返回后检测到停止，不再落库")
                return

            raw = plan_result.get("chapters")
            chapters_data: List[Dict[str, Any]] = raw if isinstance(raw, list) else []
            if not chapters_data:
                # 不再创建占位章节，直接报错停止
                logger.error(
                    f"[{novel.novel_id}] 幕 {target_act_number} 规划失败：未得到有效章节规划"
                )
                novel.consecutive_error_count = (novel.consecutive_error_count or 0) + 1
                if novel.consecutive_error_count >= 3:
                    novel.autopilot_status = AutopilotStatus.ERROR
                    logger.error(f"[{novel.novel_id}] 连续失败达3次，已挂起")
                self._flush_novel(novel)
                return

            await self.planning_service.confirm_act_planning(
                act_id=target_act.id,
                chapters=chapters_data
            )
            just_created_chapter_plan = True

        act_children = self.story_node_repo.get_children_sync(target_act.id)
        confirmed_chapters = [n for n in act_children if n.node_type.value == "chapter"]

        # current_act 为 0-based 幕索引（与 Novel 实体一致），勿写入 1-based 的 target_act_number
        novel.current_act = target_act_number - 1

        if not confirmed_chapters:
            logger.error(
                f"[{novel.novel_id}] 幕 {target_act_number} 仍无章节节点，下轮继续幕级规划"
            )
            novel.current_stage = NovelStage.ACT_PLANNING
            return

        # 仅在本轮「新落库」幕级章节规划时暂停审阅；用户确认后同幕已有节点则直接写作，避免反复弹审批
        # 全自动模式：跳过审阅，直接进入写作
        if just_created_chapter_plan:
            if getattr(novel, 'auto_approve_mode', False):
                novel.current_stage = NovelStage.WRITING
                self._flush_novel(novel)
                logger.info(f"[{novel.novel_id}] 🚀 全自动模式：第 {target_act_number} 幕规划完成，直接进入写作")
            else:
                novel.current_stage = NovelStage.PAUSED_FOR_REVIEW
                self._flush_novel(novel)
                logger.info(f"[{novel.novel_id}] 第 {target_act_number} 幕规划完成，进入审阅等待")
        else:
            novel.current_stage = NovelStage.WRITING
            self._flush_novel(novel)
            logger.info(
                f"[{novel.novel_id}] 第 {target_act_number} 幕章节节点已存在，进入写作"
            )



    def _latest_completed_chapter_number(self, novel_id: NovelId) -> Optional[int]:
        """已完结章节的最大章节号（与故事树全局章节号一致）。

        🔥 性能优化：使用轻量 SQL 查询，不加载章节内容。
        原来用 list_by_novel 会加载所有章节的 content 字段（可能数百 KB），
        在 DB 锁竞争时会阻塞很久。
        """
        try:
            db = self.chapter_repository.db if hasattr(self.chapter_repository, 'db') else None
            if db is not None:
                row = db.fetch_one(
                    "SELECT MAX(number) as max_num FROM chapters WHERE novel_id = ? AND status = 'completed'",
                    (novel_id.value,)
                )
                if row and row['max_num']:
                    return row['max_num']
                return None
        except Exception:
            pass  # 降级到原方法

        # 降级：原来的方法
        chapters = self.chapter_repository.list_by_novel(novel_id)
        completed = [c for c in chapters if c.status == ChapterStatus.COMPLETED]
        if not completed:
            return None
        return max(c.number for c in completed)

    def _count_completed_chapters(self, novel_id: NovelId) -> int:
        """轻量 COUNT 查询：只返回已完成章节数，不加载全部章节对象。

        用于审计阶段的全书完成检测，替代 list_by_novel() 以减少 DB 锁持有时间
        和内存开销（103 章时 list_by_novel 加载 103 个完整 Chapter 对象含正文，
        而本方法只返回一个整数）。
        """
        try:
            db = self.chapter_repository.db if hasattr(self.chapter_repository, 'db') else None
            if db is not None:
                row = db.fetch_one(
                    "SELECT COUNT(*) as cnt FROM chapters WHERE novel_id = ? AND status = 'completed'",
                    (novel_id.value,)
                )
                return row['cnt'] if row else 0
        except Exception:
            pass
        # 降级：使用原有方法
        chapters = self.chapter_repository.list_by_novel(novel_id)
        return sum(1 for c in chapters if c.status == ChapterStatus.COMPLETED)

    def _publish_audit_event(self, novel_id: str, event_type: str, data: Optional[Dict] = None) -> None:
        """发布审计事件到流式总线

        Args:
            novel_id: 小说 ID
            event_type: 事件类型
                - "audit_start": 审计开始
                - "audit_voice_check": 文风预检
                - "audit_voice_result": 文风预检结果
                - "audit_aftermath": 章后管线
                - "audit_tension": 张力打分
                - "audit_tension_result": 张力打分结果
                - "audit_complete": 审计完成
            data: 事件数据
        """
        try:
            from application.engine.services.streaming_bus import streaming_bus
            streaming_bus.publish_audit_event(novel_id, event_type, data)
        except Exception as e:
            logger.debug(f"[{novel_id}] 发布审计事件失败: {e}")

    def _update_shared_state(self, novel_id: str, **fields) -> None:
        """将实时状态写入共享内存（供 API 与其他进程读取，不经由 SQLite）。

        守护进程高频写入阶段、审计进度、张力等；关键节点再落盘 novels/chapters。
        章节聚合 _cached_* 仅在落库、审计完成时写入，供 DB 被锁时 /status 降级使用；
        正常情况下 /status 会对完稿/书稿/总字数做短超时 DB 聚合并与共享字段合并。
        """
        # 🔥 确保 novel_id 始终在数据中
        fields["novel_id"] = novel_id

        try:
            # 优先尝试从主进程注入的共享状态
            import sys
            shared = sys.modules.get("__shared_state")
            if shared is not None:
                key = f"novel:{novel_id}"
                current = dict(shared.get(key, {}))
                current.update(fields)
                current["_updated_at"] = time.time()
                # 🔥 同时更新守护进程心跳
                shared["_daemon_heartbeat"] = time.time()
                shared[key] = current
                return
        except Exception:
            pass

        # 降级：直接通过主进程模块的函数写入（开发环境单进程时）
        try:
            from interfaces.main import update_shared_novel_state
            update_shared_novel_state(novel_id, **fields)
        except Exception:
            pass

    async def _handle_auditing(self, novel: Novel):
        """处理审计（含张力打分）

        核心架构优化：
        1. 高频状态（audit_progress、stage 等）写入共享内存，避免读库
        2. 只在审计完成时统一 save 到 DB，减少写锁持有时间
        3. /status 对章节聚合走短超时读库；共享内存中的 _cached_* 用于 DB 忙时的降级
        4. 每个 LLM 调用加超时保护，避免 deepseek API 卡住导致整个守护进程挂起
        5. 🔥 流式推送审计进度，让前端实时看到审计状态
        """
        if not self._is_still_running(novel):
            return

        chapter_num = self._latest_completed_chapter_number(NovelId(novel.novel_id.value))
        if chapter_num is None:
            novel.current_stage = NovelStage.WRITING
            self._update_shared_state(
                novel.novel_id.value,
                current_stage="writing",
                audit_progress=None,
            )
            return

        chapter = self.chapter_repository.get_by_novel_and_number(
            NovelId(novel.novel_id.value), chapter_num
        )
        if not chapter:
            novel.current_stage = NovelStage.WRITING
            self._update_shared_state(
                novel.novel_id.value,
                current_stage="writing",
                audit_progress=None,
            )
            return

        content = chapter.content or ""
        self._sync_novel_current_act_from_chapter_number(novel, chapter_num)
        self._cache_stats_to_shared_memory(novel)
        chapter_id = ChapterId(chapter.id)

        # 🔥 发布审计开始事件
        self._publish_audit_event(
            novel.novel_id.value,
            "audit_start",
            {"chapter_number": chapter_num, "word_count": len(content)}
        )

        # 1. 先做文风预检；若严重偏离则定向改写，最多两轮，再执行章后管线，避免分析结果与最终正文错位
        novel.audit_progress = "voice_check"
        # 🔥 架构优化：写共享内存，零 DB IO
        self._update_shared_state(
            novel.novel_id.value,
            current_stage="auditing",
            audit_progress="voice_check",
            last_chapter_number=chapter_num,
            writing_substep="audit_voice_check",
            writing_substep_label="文风预检",
        )
        # 🔥 发布文风预检事件
        self._publish_audit_event(
            novel.novel_id.value,
            "audit_voice_check",
            {"chapter_number": chapter_num}
        )
        drift_result = await self._call_with_timeout(
            self._score_voice_only(novel.novel_id.value, chapter_num, content),
            timeout=180.0,  # 文风预检最多 3 分钟
            novel_id=novel.novel_id.value,
            label="voice_check",
            fallback={"drift_alert": False, "similarity_score": None},
        )
        content, drift_result = await self._apply_voice_rewrite_loop(
            novel,
            chapter,
            content,
            drift_result,
        )
        # 🔥 发布文风预检结果事件
        self._publish_audit_event(
            novel.novel_id.value,
            "audit_voice_result",
            {
                "similarity_score": drift_result.get("similarity_score"),
                "drift_alert": drift_result.get("drift_alert"),
            }
        )

        # 2. 统一章后管线：叙事/向量、文风（一次）、KG 推断；三元组与伏笔在叙事同步单次 LLM 中落库
        novel.audit_progress = "aftermath_pipeline"
        # 🔥 架构优化：写共享内存，零 DB IO
        self._update_shared_state(
            novel.novel_id.value,
            audit_progress="aftermath_pipeline",
            writing_substep="audit_aftermath",
            writing_substep_label="章后管线（叙事/向量/KG）",
        )
        # 🔥 发布章后管线事件
        self._publish_audit_event(
            novel.novel_id.value,
            "audit_aftermath",
            {"chapter_number": chapter_num}
        )
        if self.aftermath_pipeline:
            try:
                _mb = self._pending_chapter_micro_beats.pop(
                    (novel.novel_id.value, chapter_num), None
                )
                drift_result = await self._call_with_timeout(
                    self.aftermath_pipeline.run_after_chapter_saved(
                        novel.novel_id.value,
                        chapter_num,
                        content,
                        chapter_micro_beats=_mb,
                    ),
                    timeout=300.0,  # 章后管线最多 5 分钟（含多次 LLM）
                    novel_id=novel.novel_id.value,
                    label="aftermath_pipeline",
                    fallback={"drift_alert": False, "similarity_score": None, "narrative_sync_ok": False, "vector_stored": False, "foreshadow_stored": False, "triples_extracted": False},
                )
                logger.info(
                    f"[{novel.novel_id}] 章后管线完成: 相似度={drift_result.get('similarity_score')}, "
                    f"drift_alert={drift_result.get('drift_alert')}"
                )
            except Exception as e:
                logger.warning(f"[{novel.novel_id}] 章后管线失败（降级旧逻辑）：{e}")
                drift_result = self._legacy_auditing_tasks_and_voice(
                    novel, chapter_num, content, chapter_id
                )
        else:
            drift_result = self._legacy_auditing_tasks_and_voice(
                novel, chapter_num, content, chapter_id
            )

        # ── 停止检查：章后管线和文风预检完成后 ──
        if not self._is_still_running(novel):
            logger.info(f"[{novel.novel_id}] 用户已停止（章后管线完成后），跳过张力打分")
            return

        # 2. 张力打分（轻量 LLM 调用，~200 token）
        novel.audit_progress = "tension_scoring"
        # 🔥 架构优化：写共享内存，零 DB IO
        self._update_shared_state(
            novel.novel_id.value,
            audit_progress="tension_scoring",
            writing_substep="audit_tension",
            writing_substep_label="张力打分",
        )
        # 🔥 发布张力打分事件
        self._publish_audit_event(
            novel.novel_id.value,
            "audit_tension",
            {"chapter_number": chapter_num}
        )
        # ★ Phase 1: 统一张力刻度为 0-100（不再有损转换为 1-10）
        # 优先使用章后管线中的多维张力评分（0-100），替代旧式 _score_tension（1-10）
        tension_composite = drift_result.get("tension_composite") if drift_result else None
        if tension_composite is not None and tension_composite > 0:
            tension = int(tension_composite)  # 直接存 0-100，不再 /10 降级
            logger.info(f"[{novel.novel_id}] 章节 {chapter_num} 多维张力值：{tension}/100")
        else:
            # 降级：旧式评分（1-10），升级到 0-100 刻度
            old_scale_tension = await self._call_with_timeout(
                self._score_tension(content),
                timeout=60.0,
                novel_id=novel.novel_id.value,
                label="tension_scoring",
                fallback=5,
            )
            tension = old_scale_tension * 10  # 1-10 → 0-100
            logger.info(f"[{novel.novel_id}] 章节 {chapter_num} 旧式张力值：{old_scale_tension}/10 → {tension}/100")
        novel.last_chapter_tension = tension
        # 共享内存：供 /status 等高频读路径；章节张力另见下方 _write_tension_ephemeral
        self._update_shared_state(
            novel.novel_id.value,
            last_chapter_tension=tension,
        )
        # 同步章节张力到 chapters 表，供 /monitor/tension-curve 与「audit_tension_result」SSE 刷新一致读库
        #（章后管线可能已写过多维张力，此处幂等 UPDATE 覆盖 composite；旧式打分路径则依赖本次写入）
        try:
            from application.world.services.chapter_narrative_sync import _write_tension_ephemeral

            _write_tension_ephemeral(
                novel.novel_id.value, chapter_num, float(tension), None
            )
        except Exception as e:
            logger.debug(
                "[%s] 张力同步 chapters 表失败（非致命）: %s",
                novel.novel_id.value,
                e,
            )
        # 🔥 发布张力打分结果事件
        self._publish_audit_event(
            novel.novel_id.value,
            "audit_tension_result",
            {"tension": tension, "chapter_number": chapter_num}
        )
        logger.info(f"[{novel.novel_id}] 章节 {chapter_num} 张力值：{tension}/100（共享内存 + 章节表已对齐）")

        # 章末审阅快照（写入 novels，供 /autopilot/status 与前台「章节状态 / 章节元素」）
        previous_same_chapter_drift = (
            novel.last_audit_chapter_number == chapter_num
            and bool(novel.last_audit_drift_alert)
        )
        novel.last_audit_chapter_number = chapter_num
        novel.last_audit_similarity = drift_result.get("similarity_score")
        novel.last_audit_drift_alert = bool(drift_result.get("drift_alert", False))
        novel.last_audit_narrative_ok = bool(drift_result.get("narrative_sync_ok", True))
        novel.last_audit_vector_stored = bool(drift_result.get("vector_stored", False))
        novel.last_audit_foreshadow_stored = bool(drift_result.get("foreshadow_stored", False))
        novel.last_audit_triples_extracted = bool(drift_result.get("triples_extracted", False))
        novel.last_audit_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

        drift_too_high = bool(drift_result.get("drift_alert", False))
        similarity_score = drift_result.get("similarity_score")
        similarity_below_threshold = self._similarity_below_warning_threshold(similarity_score)
        if drift_result.get("similarity_score") is not None:
            logger.info(
                f"[{novel.novel_id}] 文风相似度：{drift_result.get('similarity_score')}，"
                f"告警：{drift_too_high}"
            )

        # 3. 文风漂移仅保留告警，不再删章回滚
        if drift_too_high and similarity_below_threshold:
            logger.warning(
                f"[{novel.novel_id}] 章节 {chapter_num} 文风仍偏离，但已完成有限次定向修正，保留本章继续推进"
            )
        elif drift_too_high and previous_same_chapter_drift:
            logger.info(
                f"[{novel.novel_id}] 同章文风告警持续存在，但已从删除回滚切换为保留并继续"
            )
        elif drift_too_high and not similarity_below_threshold:
            logger.info(
                f"[{novel.novel_id}] 文风告警来自历史窗口，当前章节相似度未低于阈值，保留本章"
            )

        # ── 停止检查：张力打分完成后 ──
        if not self._is_still_running(novel):
            logger.info(f"[{novel.novel_id}] 用户已停止（张力打分完成后），跳过落库")
            return

        # 🛡️ Anti-AI：在章末闸门判定之前执行（结果落库），以便「严重」可触发 paused_for_review
        anti_report = await self._run_anti_ai_audit(
            novel.novel_id.value, chapter_num, content
        )

        prefs = getattr(novel, "generation_prefs", None) or GenerationPreferences()
        auto = bool(getattr(novel, "auto_approve_mode", False))

        hard_narrative = not bool(drift_result.get("narrative_sync_ok", True))
        hard_voice = drift_too_high and similarity_below_threshold
        hard_fail = hard_narrative or hard_voice

        anti_assessment = None
        if anti_report is not None:
            anti_assessment = getattr(
                getattr(anti_report, "metrics", None), "overall_assessment", None
            )
        anti_ai_severe = anti_assessment == "严重"

        pause_gate = (not auto) and (
            bool(getattr(prefs, "pause_after_each_chapter_audit", False))
            or (
                bool(getattr(prefs, "audit_pause_on_hard_fail", False))
                and hard_fail
            )
            or (
                bool(getattr(prefs, "audit_pause_on_anti_ai_severe", False))
                and anti_ai_severe
            )
        )

        novel.audit_progress = None  # 审计完成，清除进度标记
        novel.current_beat_index = 0  # 🔥 重置节拍索引，下一章从节拍 0 开始
        novel.beats_completed = False  # 🔥 重置节拍完成标志

        # 5. 全书完成检测（用轻量 COUNT 查询替代 list_by_novel，减少 DB 锁持有时间）
        completed_count = self._count_completed_chapters(NovelId(novel.novel_id.value))
        book_done = completed_count >= novel.target_chapters

        if pause_gate:
            novel.current_stage = NovelStage.PAUSED_FOR_REVIEW
            logger.info(
                "[%s] 章末审阅闸门：进入 paused_for_review（每章一停=%s，硬伤停机=%s，Anti-AI严重=%s；"
                "narrative_ok=%s hard_voice=%s assessment=%s）",
                novel.novel_id.value,
                getattr(prefs, "pause_after_each_chapter_audit", False),
                bool(getattr(prefs, "audit_pause_on_hard_fail", False)) and hard_fail,
                bool(getattr(prefs, "audit_pause_on_anti_ai_severe", False))
                and anti_ai_severe,
                drift_result.get("narrative_sync_ok", True),
                hard_voice,
                anti_assessment,
            )
        else:
            novel.current_stage = NovelStage.WRITING

        if book_done and not pause_gate:
            logger.info(f"[{novel.novel_id}] 🎉 全书完成！共 {completed_count} 章")
            novel.autopilot_status = AutopilotStatus.STOPPED
            novel.current_stage = NovelStage.COMPLETED
        elif book_done and pause_gate:
            logger.info(
                "[%s] 全书已完成 %s 章，但章末闸门打开：保持待审阅，恢复后继续结束流程",
                novel.novel_id.value,
                completed_count,
            )

        # 🔥 发布审计完成事件
        self._publish_audit_event(
            novel.novel_id.value,
            "audit_complete",
            {
                "chapter_number": chapter_num,
                "tension": tension,
                "similarity_score": drift_result.get("similarity_score"),
                "completed_chapters": completed_count,
                "target_chapters": novel.target_chapters,
                "is_completed": book_done and not pause_gate,
                "paused_for_review": pause_gate,
                "hard_fail": hard_fail,
                "anti_ai_assessment": anti_assessment,
            },
        )

        # 🔥 审计完成：统一 save 到 DB（低频、一次落盘）
        # 同时更新共享内存，让前端立刻感知
        st_stats = self._read_chapter_stats_ephemeral(novel.novel_id.value)
        if st_stats:
            cc_sig, mc_sig, tw_sig = st_stats
        else:
            cc_sig, mc_sig, tw_sig = completed_count, completed_count, 0

        self._update_shared_state(
            novel.novel_id.value,
            current_stage=novel.current_stage.value,
            audit_progress=None,
            current_beat_index=0,  # 🔥 同步重置节拍索引到共享内存
            current_auto_chapters=novel.current_auto_chapters,  # 🔥 同步已完成章节数
            last_audit_chapter_number=novel.last_audit_chapter_number,
            last_audit_similarity=novel.last_audit_similarity,
            last_audit_drift_alert=novel.last_audit_drift_alert,
            last_audit_narrative_ok=novel.last_audit_narrative_ok,
            last_audit_vector_stored=novel.last_audit_vector_stored,
            last_audit_foreshadow_stored=novel.last_audit_foreshadow_stored,
            last_audit_triples_extracted=novel.last_audit_triples_extracted,
            last_audit_at=novel.last_audit_at,
            last_chapter_tension=novel.last_chapter_tension,
            _cached_completed_chapters=cc_sig,
            _cached_manuscript_chapters=mc_sig,
            _cached_total_words=tw_sig,
            target_chapters=novel.target_chapters,
            target_words_per_chapter=novel.target_words_per_chapter,
            autopilot_status=novel.autopilot_status.value,
            consecutive_error_count=novel.consecutive_error_count,
        )

        # 🔥 审计完成时：不再用旧式 _score_tension (1-10) 写入 chapters.tension_score
        # 原因：章后管线（chapter_narrative_sync）已通过 TensionScoringService 进行多维评分
        # （0-100 刻度，含 plot/emotional/pacing），并通过 _write_tension_ephemeral 写入 DB。
        # 旧式评分仅取前500字、1-10 粗粒度，会覆盖真实多维评分，导致张力图变平。
        # ★ Phase 1: novel.last_chapter_tension 已统一为 0-100 刻度，用于余韵章判断。

        # 🔥 核心修复：novel_repository.save() 改为独立短连接写入
        # 原因：repository.save() 使用线程本地长连接，写锁持有时间不可控
        # 在守护进程（multiprocessing.Process）中，这会阻塞 API 进程的所有 DB 操作
        self._save_novel_ephemeral(novel)
        logger.info(f"[{novel.novel_id}] 审计完成，状态已落盘")

        # 🔥 审计完成：同步编年史+故事线到共享内存
        # narrative_sync 会更新故事线进度到 DB，这里重新加载确保共享内存同步
        self._sync_chronicles_to_shared_memory(novel.novel_id.value)
        self._sync_storylines_to_shared_memory(novel.novel_id.value)

        # 🔗 衔接引擎：审计完成后提取章节桥段（供下一章首段衔接使用）
        await self._extract_chapter_bridge(novel.novel_id.value, chapter_num, content)

        # ── 停止检查：审计落盘完成后 ──
        if not self._is_still_running(novel):
            logger.info(f"[{novel.novel_id}] 用户已停止（审计落盘后），跳过摘要生成")
            return

        # 6. 自动触发宏观诊断（卷完结或约 6 万字间隔；静默注入，无前端提案交互）
        await self._auto_trigger_macro_diagnosis(novel, completed_count)

        # ── 停止检查：宏观诊断完成后 ──
        if not self._is_still_running(novel):
            logger.info(f"[{novel.novel_id}] 用户已停止（宏观诊断后），跳过摘要生成")
            return

        # 7. 🆕 摘要生成钩子（双轨融合 - 轨道一）
        await self._maybe_generate_summaries(novel, completed_count)

    async def _call_with_timeout(
        self,
        coro,
        timeout: float,
        novel_id: str = "",
        label: str = "",
        fallback=None,
    ):
        """为 LLM 调用加超时保护 + 停止信号响应，避免 API 卡住或用户停止后仍在等待。

        双重保护：
        1. asyncio.wait_for 超时保护——防止 LLM API 无限等待
        2. 停止信号监听——用户点击停止后，5 秒内终止当前 LLM 调用

        Args:
            coro: awaitable 协程对象
            timeout: 超时秒数
            novel_id: 小说 ID（用于写共享状态和检查停止信号）
            label: 调用标签（用于日志）
            fallback: 超时/停止时的降级返回值
        """
        # ── 并行：LLM 调用 + 停止信号监听 ──
        stop_detected = asyncio.Event()

        async def _watch_stop():
            """监听停止信号，检测到后设置事件（双通道：IPC 优先 + 队列消费）"""
            while not stop_detected.is_set():
                await asyncio.sleep(0.2)  # 200ms 检查间隔（🔥 从 100ms 放宽，减少 CPU 开销）
                # 通道 1：本地 threading.Event
                try:
                    from application.engine.services.novel_stop_signal import is_novel_stopped
                    if is_novel_stopped(novel_id) or is_novel_stopped("__all__"):
                        stop_detected.set()
                        return
                except Exception:
                    pass
                # 通道 2：主动消费 mp.Queue
                try:
                    from application.engine.services.streaming_bus import streaming_bus
                    streaming_bus.consume_control_signals(novel_id)
                except Exception:
                    pass

        watch_task = None
        if novel_id:
            watch_task = asyncio.create_task(_watch_stop())

        try:
            result = await asyncio.wait_for(coro, timeout=timeout)

            # LLM 调用正常完成，但检查是否在等待期间收到了停止信号
            if stop_detected.is_set():
                logger.info(f"[{novel_id}] 🛑 {label} 完成但停止信号已触发，丢弃结果")
                return fallback

            return result

        except asyncio.TimeoutError:
            logger.warning(
                f"[{novel_id}] ⏱️ {label} 超时（{timeout}s），使用降级值: {fallback}"
            )
            if novel_id:
                self._update_shared_state(
                    novel_id,
                    _last_timeout_label=label,
                    _last_timeout_at=time.time(),
                )
            return fallback
        except Exception as e:
            logger.warning(f"[{novel_id}] {label} 异常: {e}，使用降级值")
            return fallback
        finally:
            stop_detected.set()
            if watch_task is not None:
                watch_task.cancel()
                try:
                    await watch_task
                except asyncio.CancelledError:
                    pass

    def _get_voice_service(self):
        """优先复用章后管线里的 voice service，避免配置分叉。"""
        if self.aftermath_pipeline and getattr(self.aftermath_pipeline, "_voice", None):
            return getattr(self.aftermath_pipeline, "_voice")
        return self.voice_drift_service

    def _similarity_below_warning_threshold(self, similarity_score: Any) -> bool:
        """展示告警阈值：宽松，用于提示。"""
        if similarity_score is None:
            return False
        try:
            from application.analyst.services.voice_drift_service import DRIFT_ALERT_THRESHOLD
            return float(similarity_score) < float(DRIFT_ALERT_THRESHOLD)
        except Exception:
            return float(similarity_score) < VOICE_WARNING_THRESHOLD_FALLBACK

    def _should_attempt_voice_rewrite(self, drift_result: Dict[str, Any]) -> bool:
        """自动修文阈值：严格，仅对明显偏离的当前章触发。"""
        similarity = drift_result.get("similarity_score")
        if similarity is None:
            return False
        try:
            return float(similarity) < VOICE_REWRITE_THRESHOLD
        except Exception:
            return False

    async def _score_voice_only(
        self,
        novel_id: str,
        chapter_number: int,
        content: str,
    ) -> Dict[str, Any]:
        """仅做文风评分，用于决定是否先修文。"""
        voice_service = self._get_voice_service()
        if not voice_service or not content or not str(content).strip():
            return {"drift_alert": False, "similarity_score": None}

        try:
            if getattr(voice_service, "use_llm_mode", False):
                return await voice_service.score_chapter_async(
                    novel_id=novel_id,
                    chapter_number=chapter_number,
                    content=content,
                )
            return voice_service.score_chapter(
                novel_id=novel_id,
                chapter_number=chapter_number,
                content=content,
            )
        except Exception as e:
            logger.warning("[%s] 文风预检失败，跳过自动修文：%s", novel_id, e)
            return {"drift_alert": False, "similarity_score": None}

    def _build_voice_rewrite_prompt(
        self,
        novel: Novel,
        chapter,
        content: str,
        similarity_score: float,
        attempt: int,
    ) -> Prompt:
        """构建定向修正文风的改写提示。"""
        style_summary = ""
        voice_anchors = ""
        voice_service = self._get_voice_service()
        try:
            fingerprint_repo = getattr(voice_service, "fingerprint_repo", None)
            if fingerprint_repo:
                fingerprint = fingerprint_repo.get_by_novel(novel.novel_id.value, None)
                style_summary = build_style_summary(fingerprint)
        except Exception as e:
            logger.debug("[%s] style_summary 获取失败: %s", novel.novel_id, e)

        if self.context_builder:
            try:
                voice_anchors = self.context_builder.build_voice_anchor_system_section(
                    novel.novel_id.value
                )
            except Exception as e:
                logger.debug("[%s] voice anchors 获取失败: %s", novel.novel_id, e)

        style_block = style_summary.strip() or "暂无明确统计摘要，优先保持既有作者语气与句式节奏。"
        anchor_block = voice_anchors.strip() or "无额外角色声线锚点。"
        outline = (getattr(chapter, "outline", "") or "").strip() or "无单独大纲，必须严格保留现有剧情事实。"

        # CPMS render
        from infrastructure.ai.prompt_keys import VOICE_REWRITE
        from infrastructure.ai.prompt_registry import get_prompt_registry

        variables = {
            "style_fingerprint": style_block,
            "anchor_block": anchor_block,
            "chapter_number": str(chapter.number),
            "attempt": str(attempt),
            "similarity_score": f"{similarity_score:.4f}",
            "threshold": f"{VOICE_REWRITE_THRESHOLD:.2f}",
            "outline": outline,
            "content": content,
        }
        registry = get_prompt_registry()
        p = registry.render_to_prompt(VOICE_REWRITE, variables)
        if p:
            return p

        # Fallback
        from infrastructure.ai.prompt_utils import get_prompt_system
        system = get_prompt_system(VOICE_REWRITE)
        user = f"""当前为第 {chapter.number} 章，第 {attempt} 次文风定向修正。

当前相似度：{similarity_score:.4f}
自动修文触发阈值：{VOICE_REWRITE_THRESHOLD:.2f}

章节大纲：
{outline}

请在不改变剧情事实的前提下，修订以下正文的叙述语气、句式节奏与措辞，使其更贴近既有文风：

{content}
"""
        return Prompt(system=system, user=user)

    async def _rewrite_chapter_for_voice(
        self,
        novel: Novel,
        chapter,
        content: str,
        similarity_score: float,
        attempt: int,
    ) -> Optional[str]:
        """执行一次定向修文。"""
        if not self.llm_service:
            return None

        prompt = self._build_voice_rewrite_prompt(
            novel,
            chapter,
            content,
            similarity_score,
            attempt,
        )
        config = GenerationConfig(
            max_tokens=max(4096, min(8192, int(len(content) * 1.5))),
            temperature=0.35,
        )
        try:
            result = await self.llm_service.generate(prompt, config)
        except Exception as e:
            logger.warning("[%s] 文风定向修文失败（attempt=%d）：%s", novel.novel_id, attempt, e)
            return None

        rewritten = strip_reasoning_artifacts((result.content or "").strip())
        if not rewritten:
            return None
        return rewritten

    async def _apply_voice_rewrite_loop(
        self,
        novel: Novel,
        chapter,
        content: str,
        initial_drift_result: Dict[str, Any],
    ) -> tuple[str, Dict[str, Any]]:
        """严重漂移时做有限次定向修文，并即时复评分。"""
        current_content = content
        current_result = initial_drift_result or {"drift_alert": False, "similarity_score": None}

        for attempt in range(1, VOICE_REWRITE_MAX_ATTEMPTS + 1):
            if not self._should_attempt_voice_rewrite(current_result):
                break
            if not self._is_still_running(novel):
                logger.info("[%s] 用户已停止，终止文风修文", novel.novel_id)
                break

            similarity = current_result.get("similarity_score")
            logger.warning(
                "[%s] 章节 %s 文风偏离严重（similarity=%s），开始第 %d/%d 次定向修文",
                novel.novel_id,
                chapter.number,
                similarity,
                attempt,
                VOICE_REWRITE_MAX_ATTEMPTS,
            )
            rewritten = await self._rewrite_chapter_for_voice(
                novel,
                chapter,
                current_content,
                float(similarity),
                attempt,
            )
            if not rewritten or rewritten.strip() == current_content.strip():
                logger.warning("[%s] 定向修文未产生有效变化，停止继续重试", novel.novel_id)
                break

            current_content = rewritten
            # 🔥 核心修复：使用独立短连接写入，避免持有长连接写锁阻塞 API 进程
            self._save_chapter_ephemeral(
                novel.novel_id.value, chapter.number,
                content=current_content,
                word_count=len(current_content.strip()),
            )
            current_result = await self._score_voice_only(
                novel.novel_id.value,
                chapter.number,
                current_content,
            )
            logger.info(
                "[%s] 第 %d 次定向修文后相似度=%s drift_alert=%s",
                novel.novel_id,
                attempt,
                current_result.get("similarity_score"),
                current_result.get("drift_alert"),
            )

        return current_content, current_result

    def _legacy_auditing_tasks_and_voice(
        self,
        novel: Novel,
        chapter_num: int,
        content: str,
        chapter_id: ChapterId,
    ) -> Dict[str, Any]:
        """无统一管线时：VOICE + extract_bundle（单次 LLM 叙事/三元组/伏笔）入队 + 同步文风（可能与队列内 VOICE 重复）。"""
        for task_type in [TaskType.VOICE_ANALYSIS, TaskType.EXTRACT_BUNDLE]:
            self.background_task_service.submit_task(
                task_type=task_type,
                novel_id=novel.novel_id,
                chapter_id=chapter_id,
                payload={"content": content, "chapter_number": chapter_num},
            )
        if self.voice_drift_service and content:
            try:
                return self.voice_drift_service.score_chapter(
                    novel_id=novel.novel_id.value,
                    chapter_number=chapter_num,
                    content=content,
                )
            except Exception as e:
                logger.warning("文风检测失败（跳过）：%s", e)
        return {"drift_alert": False, "similarity_score": None}

    def _sum_completed_chapter_words(self, novel_id: str) -> int:
        """已完结章节字数合计，用于宏观诊断字数间隔锚点。"""
        chapters = self.chapter_repository.list_by_novel(NovelId(novel_id))
        total = 0
        for c in chapters:
            st = getattr(c.status, "value", c.status)
            if st == "completed":
                total += _coerce_word_count_to_int(getattr(c, "word_count", None))
        return total

    def _get_last_macro_word_anchor(self, novel_id: str) -> int:
        from infrastructure.persistence.database.connection import get_database

        db = get_database()
        row = db.fetch_one(
            """
            SELECT total_words_at_run FROM macro_diagnosis_results
            WHERE novel_id=? ORDER BY created_at DESC LIMIT 1
            """,
            (novel_id,),
        )
        if not row:
            return 0
        v = row.get("total_words_at_run")
        return int(v) if v is not None else 0

    def _macro_diagnosis_should_run(self, novel: Novel, completed_count: int) -> tuple:
        """触发：任一卷（Volume）章节范围完结；或累计字数距上次诊断 ≥ 约 6 万字（5~10 万取中）。"""
        from application.audit.services.macro_diagnosis_service import MACRO_DIAGNOSIS_WORD_INTERVAL
        from domain.structure.story_node import NodeType

        novel_id = novel.novel_id.value
        total_words = self._sum_completed_chapter_words(novel_id)

        if self.story_node_repo:
            try:
                nodes = self.story_node_repo.get_by_novel_sync(novel_id)
                for n in nodes:
                    if n.node_type == NodeType.VOLUME and n.chapter_end == completed_count:
                        return True, f"卷「{n.title or n.number}」完结（第{completed_count}章）"
            except Exception as e:
                logger.debug("[%s] 宏观诊断卷检测跳过: %s", novel_id, e)

        last_anchor = self._get_last_macro_word_anchor(novel_id)
        if total_words >= last_anchor + MACRO_DIAGNOSIS_WORD_INTERVAL:
            return True, (
                f"字数间隔（累计约{total_words}字，距上次锚点≥{MACRO_DIAGNOSIS_WORD_INTERVAL // 10000}万字）"
            )
        return False, ""

    async def _auto_trigger_macro_diagnosis(self, novel: Novel, completed_count: int) -> None:
        """自动触发宏观诊断：卷完结或字数间隔；结果仅用于静默 context_patch，不经前端提案。"""
        try:
            should_trigger, trigger_reason = self._macro_diagnosis_should_run(novel, completed_count)
            if not should_trigger:
                return

            total_words = self._sum_completed_chapter_words(novel.novel_id.value)
            logger.info(f"[{novel.novel_id}] 📊 自动触发宏观诊断：{trigger_reason}")

            asyncio.create_task(
                self._run_macro_diagnosis_background(novel.novel_id.value, total_words, trigger_reason)
            )

        except Exception as e:
            logger.warning(f"[{novel.novel_id}] 自动触发宏观诊断失败: {e}")

    async def _run_macro_diagnosis_background(
        self,
        novel_id: str,
        total_words_snapshot: int,
        trigger_reason: str,
    ) -> None:
        """后台执行宏观诊断：扫描结果写入 context_patch，供生成上下文头部静默注入。"""
        try:
            from infrastructure.persistence.database.connection import get_database
            from infrastructure.persistence.database.sqlite_narrative_event_repository import SqliteNarrativeEventRepository
            from application.audit.services.macro_refactor_scanner import MacroRefactorScanner
            from application.audit.services.macro_diagnosis_service import MacroDiagnosisService
            
            logger.info(f"[{novel_id}] 📊 宏观诊断后台任务已启动")
            
            db = get_database()
            narrative_event_repo = SqliteNarrativeEventRepository(db)
            scanner = MacroRefactorScanner(narrative_event_repo)
            diagnosis_service = MacroDiagnosisService(db, scanner)
            
            result = diagnosis_service.run_full_diagnosis(
                novel_id=novel_id,
                trigger_reason=trigger_reason,
                traits=None,
                total_words_at_run=total_words_snapshot,
            )
            
            if result.status == "completed":
                logger.info(
                    f"[{novel_id}] ✅ 宏观诊断完成："
                    f"扫描 {result.trait} 人设，发现 {len(result.breakpoints)} 个冲突断点"
                )
            else:
                logger.warning(f"[{novel_id}] ⚠️ 宏观诊断失败：{result.error_message}")

        except Exception as e:
            logger.warning(f"[{novel_id}] 宏观诊断后台任务失败: {e}", exc_info=True)

    async def _score_tension(self, content: str) -> int:
        """给章节打张力分（1-10），用于判断是否插入缓冲章"""
        if not content or len(content) < 200:
            return 5  # 默认中等张力

        snippet = content[:500]  # 只取前 500 字，节省 token

        try:
            prompt = Prompt(
                system="你是小说节奏分析师，只输出一个 1-10 的整数，不要解释。",
                user=f"""根据以下章节开头，打分当前剧情的张力值（1=日常/轻松，10=生死对决/高潮）：

{snippet}

张力分（只输出数字）："""
            )
            config = GenerationConfig(max_tokens=5, temperature=0.1)
            result = await self.llm_service.generate(prompt, config)
            raw = result.content.strip() if hasattr(result, "content") else str(result).strip()
            score = int(''.join(filter(str.isdigit, raw[:3])))
            return max(1, min(10, score))
        except Exception:
            return 5  # 解析失败，返回默认值


    async def _maybe_generate_summaries(self, novel: Novel, completed_count: int) -> None:
        """摘要生成钩子（双轨融合 - 轨道一）
        
        触发时机：
        1. 检查点摘要：每 20 章
        2. 幕摘要：幕完成时
        3. 卷摘要：卷完成时
        4. 部摘要：部完成时
        """
        if not self.volume_summary_service:
            return
        
        try:
            novel_id = novel.novel_id.value
            
            # 1. 检查点摘要（每 20 章）
            if await self.volume_summary_service.should_generate_checkpoint(novel_id, completed_count):
                logger.info(f"[{novel_id}] 📝 生成检查点摘要（第 {completed_count} 章）")
                result = await self.volume_summary_service.generate_checkpoint_summary(novel_id, completed_count)
                if result.success:
                    logger.info(f"[{novel_id}] ✅ 检查点摘要生成成功")
                else:
                    logger.warning(f"[{novel_id}] 检查点摘要生成失败: {result.error}")
            
            # 2. 幕摘要（幕完成时）
            all_nodes = await self.story_node_repo.get_by_novel(novel_id)
            act_nodes = sorted(
                [n for n in all_nodes if n.node_type.value == "act"],
                key=lambda x: x.number
            )
            
            if act_nodes:
                # 找到最近完成的幕
                for act in reversed(act_nodes):
                    if act.chapter_end and act.chapter_end <= completed_count:
                        # 检查是否已生成摘要
                        has_summary = act.metadata.get("summary") if act.metadata else None
                        if not has_summary:
                            logger.info(f"[{novel_id}] 📝 生成幕摘要: {act.title}")
                            result = await self.volume_summary_service.generate_act_summary(novel_id, act.id)
                            if result.success:
                                logger.info(f"[{novel_id}] ✅ 幕摘要生成成功: {act.title}")
                            break
            
            # 3. 卷摘要（检测卷是否完成）
            volume_nodes = sorted(
                [n for n in all_nodes if n.node_type.value == "volume"],
                key=lambda x: x.number
            )
            
            for vol in volume_nodes:
                if vol.chapter_end and vol.chapter_end <= completed_count:
                    has_summary = vol.metadata.get("summary") if vol.metadata else None
                    if not has_summary:
                        logger.info(f"[{novel_id}] 📝 生成卷摘要: {vol.title}")
                        result = await self.volume_summary_service.generate_volume_summary(novel_id, vol.number)
                        if result.success:
                            logger.info(f"[{novel_id}] ✅ 卷摘要生成成功: {vol.title}")
                        break
            
            # 4. 部摘要（检测部是否完成）
            part_nodes = sorted(
                [n for n in all_nodes if n.node_type.value == "part"],
                key=lambda x: x.number
            )
            
            for part in part_nodes:
                # 部完成的判断：最后一个卷已完成
                child_volumes = [v for v in volume_nodes if v.parent_id == part.id]
                if child_volumes:
                    last_vol = max(child_volumes, key=lambda x: x.number)
                    if last_vol.chapter_end and last_vol.chapter_end <= completed_count:
                        has_summary = part.metadata.get("summary") if part.metadata else None
                        if not has_summary:
                            logger.info(f"[{novel_id}] 📝 生成部摘要: {part.title}")
                            result = await self.volume_summary_service.generate_part_summary(novel_id, part.number)
                            if result.success:
                                logger.info(f"[{novel_id}] ✅ 部摘要生成成功: {part.title}")
                            break
        
        except Exception as e:
            logger.warning(f"[{novel.novel_id}] 摘要生成失败: {e}")

