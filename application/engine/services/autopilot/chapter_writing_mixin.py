"""自动驾驶 — 写作阶段（从 AutopilotDaemon 下沉的 Mixin）。"""
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
from application.engine.services.autopilot_daemon import (
    _coerce_word_count_to_int,
    VOICE_REWRITE_MAX_ATTEMPTS,
    VOICE_REWRITE_THRESHOLD,
    VOICE_WARNING_THRESHOLD_FALLBACK,
)

logger = logging.getLogger(__name__)


class ChapterWritingMixin:
    """写作阶段状态机与流式节拍生成（由 AutopilotDaemon 继承）。"""

    async def _handle_writing(self, novel: Novel):
        """处理写作（节拍级幂等落库 + 章节完整性保证）

        核心改进：
        1. 节拍内容累积，减少 DB 写入频率
        2. 章节完成前检查字数，不足则续写
        3. 中断时保存已完成节拍索引，下次从断点继续
        4. 最终完成条件：字数达标 或 所有节拍完成
        """
        if not self._is_still_running(novel):
            return

        # 0. 叙事结构被清空（无任何卷）：DB 阶段往往仍为 writing，否则会先显示「写作」
        #    再白等一轮幕级规划才发现无卷。此处立即回到宏观规划并刷新共享内存。
        novel_id_v = novel.novel_id.value
        try:
            all_nodes_early = await self.story_node_repo.get_by_novel(novel_id_v)
            volume_nodes_early = [
                n for n in all_nodes_early if getattr(n.node_type, "value", n.node_type) == "volume"
            ]
            if not volume_nodes_early:
                logger.warning(
                    "[%s] 无卷节点（结构可能被清空），写作阶段立即回到宏观规划",
                    novel_id_v,
                )
                novel.current_stage = NovelStage.MACRO_PLANNING
                novel.current_act = 0
                novel.current_chapter_in_act = 0
                novel.current_beat_index = 0
                self._update_shared_state(
                    novel_id_v,
                    current_stage="macro_planning",
                    writing_substep="macro_planning",
                    writing_substep_label="宏观规划",
                )
                self._flush_novel(novel)
                return
        except Exception as e:
            logger.debug("[%s] 写作前结构探测失败（忽略）: %s", novel_id_v, e)

        # 1. 目标控制：达到目标章节数则自动停止
        target_chapters = novel.target_chapters or 50
        max_chapters = novel.max_auto_chapters or 9999
        current_chapters = novel.current_auto_chapters or 0

        if current_chapters >= target_chapters:
            logger.info(f"[{novel.novel_id}] 已达到目标章节数 {target_chapters} 章，全托管完成")
            novel.autopilot_status = AutopilotStatus.STOPPED
            novel.current_stage = NovelStage.COMPLETED
            return

        if current_chapters >= max_chapters:
            logger.info(f"[{novel.novel_id}] 已达保护上限 {max_chapters} 章，自动暂停")
            novel.autopilot_status = AutopilotStatus.STOPPED
            novel.current_stage = NovelStage.PAUSED_FOR_REVIEW
            return

        # 2. 余韵章判断（高潮后插入余韵章——不再是"日常过渡"，而是"高潮余波"）
        # ★ Phase 1: 统一使用 0-100 刻度。阈值 80 对应旧 8/10
        # 兼容旧数据：如果值 <= 10 视为旧刻度，自动 ×10
        raw_tension = novel.last_chapter_tension or 0
        tension_100 = raw_tension * 10 if raw_tension <= 10 else raw_tension
        needs_buffer = tension_100 >= 80
        if needs_buffer:
            logger.info(f"[{novel.novel_id}] 上章张力≥80（raw={raw_tension}），触发余韵章")

        # 3. 找下一个未写章节
        next_chapter_node = await self._find_next_unwritten_chapter_async(novel)
        if not next_chapter_node:
            # 🔥 修复：找不到下一章时，检查当前幕是否全部写完
            if await self._current_act_fully_written(novel):
                # 当前幕已完成，进入下一幕规划
                novel.current_act += 1
                novel.current_chapter_in_act = 0
                novel.current_stage = NovelStage.ACT_PLANNING
                logger.info(f"[{novel.novel_id}] 当前幕已完成，进入第 {novel.current_act + 1} 幕规划")
            else:
                # 🔥 修复：当前幕还有章节但找不到未写章节，说明章节节点可能未创建
                # 进入幕级规划创建章节节点，而不是跳到审计
                novel.current_stage = NovelStage.ACT_PLANNING
                logger.info(f"[{novel.novel_id}] 找不到下一章节点，进入幕级规划创建章节")
            return

        chapter_num = next_chapter_node.number
        self._sync_novel_current_act_from_chapter_story_node(novel, next_chapter_node)
        self._cache_stats_to_shared_memory(novel)
        outline = next_chapter_node.outline or next_chapter_node.description or next_chapter_node.title

        # 合并分章叙事节拍
        if self.knowledge_service:
            try:
                knowledge = self.knowledge_service.get_knowledge(novel.novel_id.value)
                chapter_entry = next(
                    (ch for ch in knowledge.chapters if str(ch.chapter_id) == str(chapter_num)),
                    None
                )
                if chapter_entry and getattr(chapter_entry, "beat_sections", None):
                    beats_text = "\n".join(str(b) for b in chapter_entry.beat_sections if b)
                    if beats_text.strip():
                        outline = f"【分章叙事节拍】\n{beats_text}\n\n【章节大纲】\n{outline}"
                        logger.info(f"[{novel.novel_id}] 已合并第{chapter_num}章分章叙事节拍（{len(chapter_entry.beat_sections)}条）")
            except Exception as _e:
                logger.warning(f"[{novel.novel_id}] 读取分章叙事失败，使用原始大纲：{_e}")

        if needs_buffer:
            # ★ Phase 1: 缓冲章 → 余韵模式
            # 不再"突然日常化"，而是让角色消化冲击、新线索浮现、势力格局变动
            outline = (
                f"【余韵章：高潮余波】{outline}。"
                f"本章节奏适度放缓但不中断叙事势能——"
                f"角色消化刚刚发生的重大冲击（震惊/损失/获得），"
                f"周围势力对主角态度发生明显变化，"
                f"新的暗线/线索/威胁在余波中悄然浮现。"
                f"确保读者在喘息中依然保持期待，而不是「聊天喝茶」式断裂。"
            )

        target_word_count = int(getattr(novel, "target_words_per_chapter", None) or 2500)
        logger.info(f"[{novel.novel_id}] 📖 开始写第 {chapter_num} 章：{outline[:60]}...")
        logger.info(f"[{novel.novel_id}]    进度: {current_chapters}/{target_chapters} 章（目标 {target_word_count} 字/章）")

        # ★ 子步骤状态：找到下一章
        self._update_shared_state(
            novel.novel_id.value,
            writing_substep="chapter_found",
            writing_substep_label="章节定位",
            current_chapter_number=chapter_num,
            planned_micro_beats=[],
            outline_plan_mode="",
            total_beats=0,
        )

        if not self._is_still_running(novel):
            logger.info(f"[{novel.novel_id}] 用户已停止，跳过本章（上下文组装前）")
            return

        # 4. 获取规划阶段的 BeatSheet（如果有）
        beat_sheet = await self._get_beat_sheet_for_chapter(novel.novel_id.value, chapter_num)
        if beat_sheet:
            logger.info(f"[{novel.novel_id}] 📋 使用规划阶段的 BeatSheet：{len(beat_sheet.scenes)} 个场景")

        # ★ 子步骤状态：开始组装上下文
        self._update_shared_state(
            novel.novel_id.value,
            writing_substep="context_assembly",
            writing_substep_label="组装上下文",
            current_chapter_number=chapter_num,
        )

        # 5. 组装上下文
        bundle = None
        context = ""
        if self.chapter_workflow:
            try:
                bundle = self.chapter_workflow.prepare_chapter_generation(
                    novel.novel_id.value, chapter_num, outline, scene_director=None
                )
                context = bundle["context"]
                logger.info(
                    f"[{novel.novel_id}]    上下文（workflow）: {len(context)} 字符, "
                    f"约 {bundle['context_tokens']} tokens"
                )
            except Exception as e:
                logger.warning(f"prepare_chapter_generation 失败，尝试降级：{e}")
                try:
                    bundle = self.chapter_workflow.build_fallback_chapter_bundle(
                        novel.novel_id.value, chapter_num, outline, scene_director=None, max_tokens=20000,
                    )
                    context = bundle["context"]
                except Exception as e2:
                    logger.warning(f"降级失败：{e2}")
                    bundle = None
        if bundle is None and self.context_builder:
            try:
                context = self.context_builder.build_context(
                    novel_id=novel.novel_id.value, chapter_number=chapter_num, outline=outline, max_tokens=20000,
                )
            except Exception as e:
                logger.warning(f"ContextBuilder.build_context 失败：{e}")

        if not self._is_still_running(novel):
            logger.info(f"[{novel.novel_id}] 用户已停止（上下文组装后）")
            return

        voice_anchors = ""
        if bundle is not None:
            voice_anchors = bundle.get("voice_anchors") or ""
        elif self.context_builder:
            try:
                voice_anchors = self.context_builder.build_voice_anchor_system_section(novel.novel_id.value)
            except Exception:
                voice_anchors = ""

        # 6. 节拍放大：先走章前执行计划（与 DAG planning_outline_partition / CPMS 同源），再投影为 Beat
        beats: List[Any] = []
        planned_mb: List[Dict[str, Any]] = []
        plan_mode = ""
        if self.context_builder:
            beat_sheet_json = self._beat_sheet_to_plan_json(beat_sheet)
            chapter_plan = None
            try:
                from application.engine.dag.plan.outline_beat_planner import (
                    build_chapter_execution_plan_async,
                )

                logger.info(
                    "[%s] 📑 章前规划开始（outline_planning / CPMS outline-beat-partition）第 %s 章",
                    novel.novel_id.value,
                    chapter_num,
                )
                self._update_shared_state(
                    novel.novel_id.value,
                    writing_substep="outline_planning",
                    writing_substep_label="章前规划 · 划分节拍",
                    current_chapter_number=chapter_num,
                    context_tokens=bundle.get("context_tokens", 0) if bundle else 0,
                    planned_micro_beats=[],
                    outline_plan_mode="",
                    total_beats=0,
                )

                async def _emit_outline_planning_delta(_piece: str) -> None:
                    if not _piece:
                        return
                    self._update_shared_state(
                        novel.novel_id.value,
                        writing_substep="outline_planning",
                        writing_substep_label="章前规划 · 流式划分节拍…",
                    )

                chapter_plan = await build_chapter_execution_plan_async(
                    outline,
                    target_chapter_words=target_word_count,
                    novel_id=novel.novel_id.value,
                    chapter_number=chapter_num,
                    beat_sheet_json=beat_sheet_json,
                    use_llm=True,
                    emit_llm_delta=_emit_outline_planning_delta,
                    llm_service=self.llm_service,
                )
            except Exception as e:
                logger.warning(
                    "[%s] 章前执行计划（拆节拍）失败，降级为直接用 BeatSheet / 章纲启发式：%s",
                    novel.novel_id.value,
                    e,
                )

            use_plan = chapter_plan is not None and bool(chapter_plan.atoms)
            beats = self.context_builder.magnify_outline_to_beats(
                chapter_num,
                outline,
                target_chapter_words=target_word_count,
                chapter_execution_plan=chapter_plan if use_plan else None,
                beat_sheet=None if use_plan else beat_sheet,
            )

            plan_mode = ""
            if chapter_plan is not None and isinstance(getattr(chapter_plan, "provenance", None), dict):
                plan_mode = str(chapter_plan.provenance.get("mode") or "")
            planned_mb = self._beats_to_planned_micro_beats(beats)
            logger.info(
                "[%s] ✓ 章前规划完成 mode=%s → %d 个指挥器节拍（第 %s 章）",
                novel.novel_id.value,
                plan_mode or "unknown",
                len(beats),
                chapter_num,
            )

        # ★ 子步骤状态：节拍拆分完成
        self._update_shared_state(
            novel.novel_id.value,
            writing_substep="beat_magnification",
            writing_substep_label=f"节拍拆分（{len(beats)}个）",
            total_beats=len(beats),
            planned_micro_beats=planned_mb,
            outline_plan_mode=plan_mode,
            context_tokens=bundle.get('context_tokens', 0) if bundle else 0,
        )

        if not self._is_still_running(novel):
            logger.info(f"[{novel.novel_id}] 用户已停止（节拍拆分后）")
            return

        # 6. 节拍级生成 + 断点续写 + 完整性保证
        start_beat = novel.current_beat_index or 0
        entry_start_beat = start_beat  # 记录本轮入口节拍索引，用于死锁检测
        beats_completed = getattr(novel, 'beats_completed', False)
        chapter_content = await self._get_existing_chapter_content(novel, chapter_num) or ""
        use_wf = self.chapter_workflow is not None and bundle is not None

        # 断点续写：使用已有的章节内容作为上下文
        existing_content = chapter_content.strip()

        # === 关键检查：章节是否已完成 ===
        # 🔥 修复：审计完成后回到 WRITING，这一章已经审计过了（completed+已审计），
        # 不应再进入 AUDITING 重复审计。应跳过这一章，下一轮找新的未写章节。
        existing_chapter = self.chapter_repository.get_by_novel_and_number(
            NovelId(novel.novel_id.value), chapter_num
        )
        already_audited = (
            getattr(novel, 'last_audit_chapter_number', None) == chapter_num
        )
        if existing_chapter and existing_chapter.status == ChapterStatus.COMPLETED:
            if already_audited:
                # 审计完的 completed 章节，直接跳过（下一轮 _find_next_unwritten_chapter_async 会跳过 completed）
                logger.info(
                    f"[{novel.novel_id}] 章节 {chapter_num} 已写完且已审计，等待下一轮找新章节"
                )
                return
            else:
                # 写完但未审计 → 正常进入审计
                logger.info(
                    f"[{novel.novel_id}] 章节 {chapter_num} 已是 completed 状态但未审计，进入审计"
                )
                novel.current_stage = NovelStage.AUDITING
                self._flush_novel(novel)
                return

        # 检查已有内容是否达标（>= 70%）
        # 🔥 修复：如果这一章已经审计过（last_audit_chapter_number 匹配），
        # 说明是从审计回来后持久化队列延迟导致章节还是 draft，
        # 不应再次标记完成+审计，应确保 DB 状态正确后等下一轮找新章节
        if existing_content and len(existing_content) >= target_word_count * 0.7:
            if already_audited:
                logger.warning(
                    f"[{novel.novel_id}] 章节 {chapter_num} 已审计过但 DB 仍为 draft "
                    f"(持久化队列延迟)，强制补写 completed 后等下一轮"
                )
                # 🔥 关键修复：强制直接写 DB 确保章节状态为 completed
                # 这样下一轮 _find_next_unwritten_chapter_async 就不会再找到这一章
                if existing_chapter:
                    # 🔥 核心修复：使用独立短连接写入 completed 状态
                    self._save_chapter_ephemeral(
                        novel.novel_id.value, chapter_num,
                        status="completed",
                    )
                else:
                    await self._upsert_chapter_content(
                        novel, next_chapter_node, existing_content, status="completed"
                    )
                # return 让下一轮主循环重新进入 _handle_writing
                # 此时 DB 中章节已是 completed，_find_next_unwritten_chapter_async 会跳过它
                return
            # ★ 禁止「字数够 70% 但节拍未跑完」提前结章——否则会断在章纲中段就去写下一章
            nb = len(beats)
            cidx = novel.current_beat_index or 0
            # 仅用索引判断；beats_completed 曾可能被错误置位，不能作为提前结章依据
            beats_all_done = nb == 0 or cidx >= nb
            if nb > 0 and not beats_all_done:
                logger.info(
                    f"[{novel.novel_id}] 章节 {chapter_num} 已有 {len(existing_content)} 字 "
                    f"(≥70%)，但节拍未完（current_beat_index={cidx}/{nb}），"
                    f"不提前结章，继续节拍循环"
                )
            else:
                logger.info(
                    f"[{novel.novel_id}] 章节 {chapter_num} 已有 {len(existing_content)} 字 "
                    f"(达标 {int(len(existing_content) / target_word_count * 100)}%)，直接标记完成"
                )
                await self._upsert_chapter_content(
                    novel, next_chapter_node, existing_content, status="completed"
                )
                novel.current_auto_chapters = (novel.current_auto_chapters or 0) + 1
                novel.current_chapter_in_act += 1
                novel.current_beat_index = 0
                novel.beats_completed = False
                novel.current_stage = NovelStage.AUDITING
                self._flush_novel(novel)
                return

        # 若上一轮已标「节拍全跑完」但未达到放行条件：禁止清回节拍 0 叠写
        if beats_completed:
            logger.info(
                f"[{novel.novel_id}] 节拍已全量跑过但未收章，保持断点索引，不回到第 1 拍重复生成"
            )
            novel.beats_completed = False
            if (novel.current_beat_index or 0) > len(beats):
                novel.current_beat_index = len(beats)
            start_beat = novel.current_beat_index or len(beats)

        # 关键检查：节拍索引超出范围——仅有正文时不要 reset 到 0（避免整章叠写）
        if start_beat >= len(beats) and len(beats) > 0:
            if existing_content.strip():
                logger.warning(
                    f"[{novel.novel_id}] 节拍索引 {start_beat} >= {len(beats)} 且已有正文，"
                    f"视为节拍已遍历完，进入收章复核（不重置为 0）"
                )
                novel.current_beat_index = len(beats)
                start_beat = len(beats)
            else:
                logger.warning(
                    f"[{novel.novel_id}] 节拍索引 {start_beat} 超出范围 {len(beats)} 且无正文，"
                    f"重置为 0"
                )
                start_beat = 0
                novel.current_beat_index = 0
                novel.beats_completed = False

        # 日志：start_beat 为 0-based；当 start_beat == len(beats) 时表示节拍已耗尽、仅收章复核，
        # 不得再打印「从第 len+1 拍继续」，否则会出现「从第 2/1 拍继续」类矛盾日志。
        if existing_content and len(beats) > 0:
            if 0 < start_beat < len(beats):
                logger.info(
                    f"[{novel.novel_id}] 断点续写：已有 {len(existing_content)} 字，"
                    f"从第 {start_beat + 1}/{len(beats)} 个节拍继续"
                )
            elif start_beat >= len(beats):
                logger.info(
                    f"[{novel.novel_id}] 断点续写：已有 {len(existing_content)} 字，"
                    f"节拍已全部处理（{len(beats)}/{len(beats)}），进入收章复核（本轮不再撰写新节拍）"
                )

        # 批量写入计数器
        write_counter = 0
        BATCH_WRITE_INTERVAL = 3  # 每 3 个节拍写入一次 DB

        # 累积的章节内容
        accumulated_content = existing_content

        # 章节指挥（三阶段收束：铺陈→收束→着陆）
        from application.engine.services.word_count_tracker import ChapterConductor
        conductor = ChapterConductor(
            total_budget=target_word_count,
            total_beats=len(beats) if beats else 0,
            converge_threshold=novel.generation_prefs.conductor_converge_threshold,
            land_threshold=novel.generation_prefs.conductor_land_threshold,
        )
        # 如果有已存在内容，先同步
        if existing_content:
            conductor.used = len(existing_content)

        # ★ Phase 0: 初始化节拍中间件链（低侵入式增强）
        from application.engine.services.beat_middleware import init_beat_middlewares, BeatMiddlewareContext
        beat_middlewares = init_beat_middlewares(conductor=conductor)
        mw_ctx = BeatMiddlewareContext(
            novel_id=novel.novel_id.value,
            chapter_number=chapter_num,
            total_beats=len(beats),
            accumulated_content=existing_content,
        )

        if beats:
            for i, beat in enumerate(beats):
                if i < start_beat:
                    continue  # 跳过已生成的节拍

                # 获取指挥信号（铺陈/收束/着陆）——须在共享状态写入前取得，供遥测字段使用
                signal = conductor.get_signal(i)
                if not novel.generation_prefs.beat_hard_cap_enabled:
                    signal = replace(signal, hard_cap=0)

                # 🔥 节拍开始前，立即更新共享状态（前端实时看到当前节拍）
                beat_focus = getattr(beat, 'focus', '') or ''
                beat_target_words = getattr(beat, 'target_words', 0) or 0
                self._update_shared_state(
                    novel.novel_id.value,
                    current_beat_index=i,
                    writing_substep="llm_calling",
                    writing_substep_label=f"节拍 {i+1}/{len(beats)} 撰写",
                    total_beats=len(beats),
                    beat_focus=beat_focus,
                    beat_target_words=beat_target_words,
                    accumulated_words=len(accumulated_content),
                    chapter_target_words=target_word_count,
                    context_tokens=bundle.get('context_tokens', 0) if bundle else 0,
                    beat_hard_cap=int(signal.hard_cap or 0),
                    beat_phase=signal.phase.value,
                    beat_max_words_hint=int(signal.max_words_hint or 0),
                    beat_remaining_budget=int(signal.remaining_budget),
                    last_smart_truncate=None,
                )

                if not self._is_still_running(novel):
                    logger.info(f"[{novel.novel_id}] 用户已停止，中断本章（节拍 {i + 1}/{len(beats)} 前）")
                    # 保存已完成的内容和节拍索引
                    if accumulated_content.strip():
                        # 流式被中断时，最后一个节拍可能在句子中间被截断。
                        # 截断到最近的句子边界，避免残篇以半句结尾落盘。
                        safe_content = accumulated_content.strip()
                        if not re.search(r'[。！？…）】》""\'』」]$', safe_content):
                            last_ender = max(
                                safe_content.rfind('。'),
                                safe_content.rfind('！'),
                                safe_content.rfind('？'),
                                safe_content.rfind('…'),
                            )
                            if last_ender > len(safe_content) * 0.4:
                                safe_content = safe_content[:last_ender + 1]
                                logger.info(
                                    f"[{novel.novel_id}] 🔪 中断截断：{len(accumulated_content.strip())} "
                                    f"→ {len(safe_content)} 字（截至句尾）"
                                )
                        await self._upsert_chapter_content(
                            novel, next_chapter_node, safe_content, status="draft"
                        )
                        novel.current_beat_index = i  # 记录当前节拍索引，下次从断点继续
                        self._flush_novel(novel)
                        logger.info(
                            f"[{novel.novel_id}] 已保存 {len(safe_content)} 字，"
                            f"下次从节拍 {i + 1} 继续"
                        )
                    return

                adjusted_target = conductor.allocate_beat(beat.target_words, focus=beat.focus)  # ★ Phase 2: 传入 focus 用于免疫判断

                beat_prompt = self.context_builder.build_beat_prompt(beat, i, len(beats))

                # 🔗 V2：注入上一节拍的衔接诊断提示（如果有）
                if hasattr(novel, '_beat_continuity_hint') and novel._beat_continuity_hint:
                    beat_prompt = f"{novel._beat_continuity_hint}\n\n{beat_prompt}"
                    logger.debug(f"[{novel.novel_id}] 注入节拍衔接诊断到 beat {i+1}")

                # ★ Phase 0: 中间件 pre_beat 钩子（连贯性/过渡/能量免疫）
                mw_ctx.beat_index = i
                mw_ctx.beat = beat
                mw_ctx.original_adjusted_target = adjusted_target
                mw_ctx.phase = signal.phase.value
                mw_ctx.accumulated_content = accumulated_content
                for mw in beat_middlewares:
                    try:
                        beat_prompt, adjusted_target = mw.pre_beat(beat_prompt, adjusted_target, mw_ctx)
                    except Exception as e:
                        logger.debug(f"中间件 pre_beat 异常（不影响主流程）: {e}")

                # 注入指挥信号——核心：引导 LLM 自然收束
                # 1. 阶段指令（铺陈/收束/着陆）
                if signal.beat_instruction:
                    beat_prompt = f"{signal.beat_instruction}\n\n{beat_prompt}"

                # 2. 最后节拍的章节收尾提示
                if signal.chapter_ending_hint:
                    beat_prompt = f"{beat_prompt}\n\n{signal.chapter_ending_hint}"

                # 3. 兼容旧接口的紧急约束
                urgency_hint = conductor.get_urgency_hint()
                if urgency_hint and not signal.beat_instruction:
                    beat_prompt = f"{urgency_hint}\n\n{beat_prompt}"

                if use_wf:
                    prompt = self.chapter_workflow.build_chapter_prompt(
                        bundle["context"], outline,
                        storyline_context=bundle["storyline_context"],
                        plot_tension=bundle["plot_tension"],
                        style_summary=bundle["style_summary"],
                        beat_prompt=beat_prompt,
                        beat_index=i, total_beats=len(beats),
                        beat_target_words=int(adjusted_target),  # 使用调整后的目标
                        voice_anchors=voice_anchors,
                        chapter_draft_so_far=accumulated_content,
                    )
                    max_tokens = int(adjusted_target * 1.3)  # 使用调整后的目标
                    cfg = GenerationConfig(max_tokens=max_tokens, temperature=0.85)
                    beat_content = await self._stream_llm_with_stop_watch(prompt, cfg, novel=novel)
                else:
                    beat_content = await self._stream_one_beat(
                        outline, context, beat_prompt, beat,
                        novel=novel, voice_anchors=voice_anchors,
                        chapter_draft_so_far=accumulated_content,
                    )

                if beat_content.strip():
                    # 截断安全网：超出硬上限时，按书目偏好选择智能截断或字符硬截断
                    if signal.hard_cap > 0 and len(beat_content.strip()) > signal.hard_cap:
                        from application.engine.services.word_count_tracker import (
                            hard_truncate_at_chars,
                            smart_truncate,
                        )

                        stripped = beat_content.strip()
                        original_len = len(stripped)
                        use_smart = novel.generation_prefs.smart_truncate_enabled
                        if use_smart:
                            beat_content = smart_truncate(
                                stripped, signal.hard_cap, focus=str(beat_focus or "")
                            )
                            trunc_mode = "smart"
                            label = "智能截断"
                        else:
                            beat_content = hard_truncate_at_chars(stripped, signal.hard_cap)
                            trunc_mode = "hard"
                            label = "硬截断"
                        logger.warning(
                            f"[{novel.novel_id}] ⚡ {label}：节拍 {i + 1} "
                            f"{original_len} → {len(beat_content)} 字 "
                            f"(硬上限 {signal.hard_cap} 字)"
                        )
                        self._update_shared_state(
                            novel.novel_id.value,
                            last_smart_truncate={
                                "beat_index_1based": i + 1,
                                "total_beats": len(beats),
                                "from_chars": original_len,
                                "to_chars": len(beat_content),
                                "hard_cap": int(signal.hard_cap),
                                "phase": signal.phase.value,
                                "truncate_mode": trunc_mode,
                            },
                        )

                    # ★ 子步骤状态：软着陆
                    self._update_shared_state(
                        novel.novel_id.value,
                        writing_substep="soft_landing",
                        writing_substep_label=f"节拍 {i+1}/{len(beats)} 收尾修整",
                    )

                    # 软着陆：截断检测与自然续写
                    beat_content = await self._soft_landing(
                        beat_content, beat, outline, accumulated_content, novel,
                        signal=signal,
                        emotion_trend=mw_ctx.emotion_trend,  # ★ Phase 2: 传入情绪方向
                    )

                    # 报告实际字数给指挥
                    actual_words = len(beat_content.strip())
                    deviation = conductor.report_actual(actual_words)
                    phase_emoji = {"unfurl": "📖", "converge": "⚡", "land": "🎯"}.get(signal.phase.value, "")
                    if deviation > 50:
                        logger.info(
                            f"[{novel.novel_id}] {phase_emoji} 节拍 {i + 1}/{len(beats)}: "
                            f"实际 {actual_words} 字，超额 {deviation} 字"
                        )

                    # 累积内容
                    if accumulated_content:
                        accumulated_content += "\n\n" + beat_content.strip()
                    else:
                        accumulated_content = beat_content.strip()
                    write_counter += 1

                    # ★ Phase 0: 中间件 post_beat 钩子（上下文提取/情绪推断）
                    mw_ctx.prev_beat_content = beat_content.strip()
                    for mw in beat_middlewares:
                        try:
                            mw_ctx = mw.post_beat(beat_content.strip(), mw_ctx)
                        except Exception as e:
                            logger.debug(f"中间件 post_beat 异常（不影响主流程）: {e}")

                    # 🔗 V2：节拍间衔接质量检查（零 LLM 调用，纯启发式）
                    # 检测常见的节拍间割裂信号：对话断裂、跳跃词、情绪断裂
                    if i > 0 and beat_content.strip():
                        try:
                            from application.engine.services.chapter_bridge_service import ChapterBridgeService
                            prior_parts = accumulated_content.rsplit("\n\n", 1)
                            prior_beat_text = prior_parts[0] if len(prior_parts) > 1 else ""
                            if prior_beat_text:
                                bridge_svc = ChapterBridgeService()
                                beat_score, beat_diag = await bridge_svc.check_beat_continuity(
                                    novel.novel_id.value, chapter_num, i,
                                    prior_beat_text, beat_content.strip(),
                                )
                                if beat_score < 0.6:
                                    logger.warning(
                                        f"[{novel.novel_id}] 🔗 节拍衔接度低 "
                                        f"beat={i+1}/{len(beats)} score={beat_score:.2f} "
                                        f"diag={beat_diag}"
                                    )
                                    if i < len(beats) - 1:
                                        continuity_fix_hint = (
                                            f"\n\n⚠️【节拍衔接诊断】上一节拍衔接度={beat_score:.2f}，"
                                            f"问题：{beat_diag}。本节拍开头必须特别加强衔接！"
                                        )
                                        if not hasattr(novel, '_beat_continuity_hint'):
                                            novel._beat_continuity_hint = ""
                                        novel._beat_continuity_hint = continuity_fix_hint
                                else:
                                    if hasattr(novel, '_beat_continuity_hint'):
                                        novel._beat_continuity_hint = ""
                        except Exception as e:
                            logger.debug(f"节拍衔接检查失败（不影响主流程）: {e}")

                    # AOF：追加写入 .draft 文件（无锁 append，崩溃恢复用）
                    try:
                        from application.engine.services.draft_aof import append_chunk
                        append_chunk(novel.novel_id.value, chapter_num, "\n\n" + beat_content.strip() if accumulated_content != beat_content.strip() else beat_content.strip())
                    except Exception:
                        pass  # AOF 失败不影响主流程

                    # 批量写入（每 BATCH_WRITE_INTERVAL 个节拍或最后一个节拍时写入）
                    if write_counter >= BATCH_WRITE_INTERVAL or i == len(beats) - 1:
                        # ★ 子步骤状态：批量持久化
                        self._update_shared_state(
                            novel.novel_id.value,
                            writing_substep="persisting",
                            writing_substep_label="节拍内容落盘",
                        )
                        await self._upsert_chapter_content(
                            novel, next_chapter_node, accumulated_content, status="draft"
                        )
                        write_counter = 0
                        logger.debug(f"[{novel.novel_id}] 批量写入，当前 {len(accumulated_content)} 字")

                # 更新内存中的节拍索引用于流式推送
                novel.current_beat_index = i + 1

                # 🔥 同步更新共享内存的节拍索引（不写 DB，纳秒级）
                # 这样前端 /status 可以实时看到进度，不会因为 DB 锁而阻塞
                self._update_shared_state(
                    novel.novel_id.value,
                    current_beat_index=i + 1,
                    accumulated_words=len(accumulated_content),
                )

                # 如果是最后一个节拍，标记完成
                if i == len(beats) - 1:
                    novel.beats_completed = True
                    logger.info(f"[{novel.novel_id}] 📝 所有节拍已完成，标记 beats_completed = True")

                # 更新流式元数据
                if hasattr(self, '_update_stream_metadata'):
                    self._update_stream_metadata(novel.novel_id.value, i + 1, len(accumulated_content))

                logger.info(f"[{novel.novel_id}]    ✅ 节拍 {i+1}/{len(beats)} 完成: {len(beat_content)} 字")

            # 循环结束后，使用累积的内容
            chapter_content = accumulated_content
        else:
            # 降级：无节拍，一次生成
            if not self._is_still_running(novel):
                logger.info(f"[{novel.novel_id}] 用户已停止，跳过单段生成")
                return
            if use_wf:
                prompt = self.chapter_workflow.build_chapter_prompt(
                    bundle["context"], outline,
                    storyline_context=bundle["storyline_context"],
                    plot_tension=bundle["plot_tension"],
                    style_summary=bundle["style_summary"],
                    voice_anchors=voice_anchors,
                )
                cfg = GenerationConfig(max_tokens=3000, temperature=0.85)
                beat_content = await self._stream_llm_with_stop_watch(prompt, cfg, novel=novel)
            else:
                beat_content = await self._stream_one_beat(
                    outline, context, None, None, novel=novel, voice_anchors=voice_anchors
                )
            if not self._is_still_running(novel):
                logger.info(f"[{novel.novel_id}] 用户已停止，单段生成已中断")
                novel.current_beat_index = 0
                self._flush_novel(novel)
                return
            chapter_content = beat_content
            await self._upsert_chapter_content(novel, next_chapter_node, chapter_content, status="draft")

        if not self._is_still_running(novel):
            logger.info(f"[{novel.novel_id}] 用户已停止，本章不标记完成")
            self._flush_novel(novel)
            return

        if use_wf and chapter_content.strip():
            try:
                await self.chapter_workflow.post_process_generated_chapter(
                    novel.novel_id.value, chapter_num, outline, chapter_content, scene_director=None
                )
                logger.info(f"[{novel.novel_id}]    ✅ post_process_generated_chapter 完成")
            except Exception as e:
                logger.warning(f"post_process_generated_chapter 失败（仍落库）：{e}")

        # 7. 章节完成检查（弹性边界策略 —— 收紧：减少「章纲未写完就放行」）
        actual_word_count = len(chapter_content.strip())

        # 检测最后节拍是否是悬念收尾
        last_beat_is_suspense = beats and beats[-1].focus == "suspense" if beats else False

        # ★ Phase 3: 检测高能节拍是否存在（用于字数豁免）
        has_high_energy_beat = any(
            b.focus in ("action", "power_reveal", "identity_reveal", "hook", "cultivation")
            for b in (beats or [])
        )

        # ★ Phase 3: 计算节拍完成度（以 conductor 实际产出为准）
        beats_completed_count = sum(1 for b in (conductor.beats or []) if b.actual > 0)
        total_beats_count = len(beats) if beats else 0
        beats_completion_ratio = beats_completed_count / max(total_beats_count, 1)

        # 检测内容是否完整（以句号等结束符结尾）
        import re
        ending_pattern = r'[。！？…）】》"\'』」]$'
        content_complete = bool(re.search(ending_pattern, chapter_content.strip()))

        # 主阈值：72% 以下视为「明显未写满」；88% 视为「字数达标」
        min_word_threshold = int(target_word_count * 0.72)
        good_word_threshold = int(target_word_count * 0.88)
        # 全节拍已跑完且句末完整时的绝对下限（避免极短篇误卡死，但仍高于旧 60%）
        exception_floor = int(target_word_count * 0.62)

        # 死锁检测：本轮入口时节拍索引已 >= 节拍总数，for 循环一个节拍都没跑，
        # 意味着系统无法产出任何新内容，若不强制放行将永远循环
        all_beats_exhausted_no_progress = (
            total_beats_count > 0
            and entry_start_beat >= total_beats_count
        )

        # 字数低于主阈值：默认不结章，续写；仅「全节拍有产出 + 句末完整 + ≥exception_floor」例外放行
        if actual_word_count < min_word_threshold:
            if (
                total_beats_count > 0
                and beats_completion_ratio >= 1.0
                and content_complete
                and actual_word_count >= exception_floor
            ):
                logger.info(
                    f"[{novel.novel_id}] 📝 收紧策略例外放行：全节拍已有产出且句末完整 "
                    f"(字数 {actual_word_count}，约 {int(actual_word_count / target_word_count * 100)}%)"
                )
                should_complete = True
                completion_reason = (
                    f"节拍完成+内容完整 (字数 {int(actual_word_count / target_word_count * 100)}%)"
                )
            elif all_beats_exhausted_no_progress and actual_word_count > 0:
                # 节拍已全部耗尽，本轮无法产出新内容。
                # 优先策略：清除现有 draft 内容，重置节拍索引，下一轮从第 0 拍重新生成。
                # 重试超过 2 次后退化为强制放行，避免无限循环。
                rewrite_key = (novel.novel_id.value, chapter_num)
                rewrite_count = self._beat_exhausted_rewrite_count.get(rewrite_key, 0)
                MAX_REWRITE = 2
                if rewrite_count < MAX_REWRITE:
                    self._beat_exhausted_rewrite_count[rewrite_key] = rewrite_count + 1
                    logger.warning(
                        f"[{novel.novel_id}] ⚠️ 第 {chapter_num} 章节拍已遍历完但字数不足 "
                        f"({actual_word_count}/{target_word_count})，"
                        f"清除 draft 内容并从第 0 拍重写（第 {rewrite_count + 1}/{MAX_REWRITE} 次）"
                    )
                    # 清除章节内容，让下一轮从零开始生成
                    self._save_chapter_ephemeral(
                        novel.novel_id.value, chapter_num,
                        content="", status="draft", word_count=0
                    )
                    novel.current_beat_index = 0
                    novel.beats_completed = False
                    self._flush_novel(novel)
                    return
                else:
                    # 已重写 MAX_REWRITE 次仍不足，强制放行避免无限循环
                    self._beat_exhausted_rewrite_count.pop(rewrite_key, None)
                    logger.warning(
                        f"[{novel.novel_id}] ⚠️ 第 {chapter_num} 章已重写 {MAX_REWRITE} 次仍字数不足 "
                        f"({actual_word_count}/{target_word_count})，强制放行以打破死循环"
                    )
                    should_complete = True
                    completion_reason = (
                        f"重写{MAX_REWRITE}次后强制放行 ({int(actual_word_count / target_word_count * 100)}%)"
                    )
            else:
                logger.warning(
                    f"[{novel.novel_id}] ⚠️ 第 {chapter_num} 章字数不足：{actual_word_count} 字 "
                    f"(目标 {target_word_count} 字，低于 {int(min_word_threshold / target_word_count * 100)}%)"
                )
                # 保持 draft 状态，下一轮继续生成
                self._flush_novel(novel)
                logger.info(f"[{novel.novel_id}] 章节保持 draft 状态，下一轮尝试续写")
                return
        else:
            should_complete = False
            completion_reason = ""

        if not should_complete:
            # 字数达标：无节拍或节拍全部有产出才可放行（避免只写了前几拍就停）
            if actual_word_count >= good_word_threshold:
                if total_beats_count > 0 and beats_completion_ratio < 1.0:
                    should_complete = False
                    logger.warning(
                        f"[{novel.novel_id}] ⚠️ 第 {chapter_num} 章字数已高 "
                        f"({int(actual_word_count / target_word_count * 100)}%)，"
                        f"但节拍未全部产出 ({beats_completed_count}/{total_beats_count})，不结章"
                    )
                else:
                    should_complete = True
                    completion_reason = f"字数达标 ({int(actual_word_count / target_word_count * 100)}%)"
            elif total_beats_count == 0 and content_complete and actual_word_count >= min_word_threshold:
                # 降级：无节拍拆分时仅看字数与句末完整
                should_complete = True
                completion_reason = f"单段生成+内容完整 ({int(actual_word_count / target_word_count * 100)}%)"
            elif (
                has_high_energy_beat
                and content_complete
                and actual_word_count >= min_word_threshold
                and beats_completion_ratio >= 1.0
            ):
                # 高能章：仍须写满全部节拍，且不低于 72%
                should_complete = True
                completion_reason = f"高能节拍+全拍完成+内容完整 ({int(actual_word_count / target_word_count * 100)}%)"
                logger.info(
                    f"[{novel.novel_id}] 📝 高能豁免放行：爽点章全拍完成，{actual_word_count} 字"
                )
            elif (
                last_beat_is_suspense
                and content_complete
                and actual_word_count >= min_word_threshold
                and beats_completion_ratio >= 1.0
            ):
                should_complete = True
                completion_reason = f"悬念收尾+全拍完成 ({int(actual_word_count / target_word_count * 100)}%)"
                logger.info(
                    f"[{novel.novel_id}] 📝 悬念章全拍完成放行，{actual_word_count} 字"
                )
            elif (
                beats_completion_ratio >= 1.0
                and content_complete
                and actual_word_count >= min_word_threshold
            ):
                should_complete = True
                completion_reason = f"节拍完成+内容完整 ({int(actual_word_count / target_word_count * 100)}%)"
                logger.info(
                    f"[{novel.novel_id}] 📝 弹性放行：所有节拍已有产出，{actual_word_count} 字"
                )

        if not should_complete:
            # 不满足放行条件，保持 draft 状态
            logger.warning(
                f"[{novel.novel_id}] ⚠️ 第 {chapter_num} 章未达到放行条件，保持 draft 状态"
            )
            self._flush_novel(novel)
            return

        # 8. 更新计数器，重置节拍状态
        novel.current_auto_chapters = (novel.current_auto_chapters or 0) + 1
        novel.current_chapter_in_act += 1
        novel.current_beat_index = 0
        novel.beats_completed = False  # 重置节拍完成标志
        nid = novel.novel_id.value
        if beats:
            self._pending_chapter_micro_beats[(nid, chapter_num)] = [
                {
                    "description": b.description,
                    "target_words": b.target_words,
                    "focus": b.focus,
                    "location_id": getattr(b, "location_id", "") or "",
                }
                for b in beats
            ]
        else:
            self._pending_chapter_micro_beats.pop((nid, chapter_num), None)
        novel.current_stage = NovelStage.AUDITING
        # 章节正常完成，清理对应的重写计数
        self._beat_exhausted_rewrite_count.pop((novel.novel_id.value, chapter_num), None)

        # 🔗 衔接引擎：章节完成后自检衔接度（非第 1 章）
        # 如果衔接度 < 0.6，自动修整首段（最多 2 轮）
        if chapter_num > 1:
            # ★ 子步骤状态：衔接自检
            self._update_shared_state(
                novel.novel_id.value,
                writing_substep="continuity_check",
                writing_substep_label="衔接度自检",
            )
            chapter_content = await self._continuity_self_check(
                novel.novel_id.value, chapter_num, chapter_content
            )

        # ── 信息密度检测：事实密度低时补写一拍推进情节 ──
        density = self._estimate_info_density(chapter_content)
        if density < self.INFO_DENSITY_MIN_FACTS_PER_500 and len(chapter_content) > 500:
            logger.info(
                "[%s] 📉 信息密度低（%.2f facts/500字 < %.2f），触发补写 ch=%d",
                novel.novel_id.value, density, self.INFO_DENSITY_MIN_FACTS_PER_500, chapter_num,
            )
            self._update_shared_state(
                novel.novel_id.value,
                writing_substep="density_supplement",
                writing_substep_label="信息密度补写",
            )
            chapter_content = await self._density_supplement_beat(
                novel.novel_id.value, chapter_num, outline, chapter_content,
                target_word_count, novel,
            )

        # 🔥 先更新阶段到共享内存（不写章节聚合，避免占位 0 覆盖真实数据）
        self._update_shared_state(
            novel.novel_id.value,
            current_stage="auditing",
            current_auto_chapters=novel.current_auto_chapters,
            current_act=novel.current_act,
            current_chapter_in_act=novel.current_chapter_in_act,
            target_chapters=novel.target_chapters,
            target_words_per_chapter=novel.target_words_per_chapter,
            autopilot_status=novel.autopilot_status.value,
        )

        # 标记章节完成（DB 写入，可能阻塞）
        # ★ 子步骤状态：章节落盘
        self._update_shared_state(
            novel.novel_id.value,
            writing_substep="chapter_persist",
            writing_substep_label="章节落盘",
        )
        await self._upsert_chapter_content(novel, next_chapter_node, chapter_content, status="completed")

        # 🔥 落库后用短连接读真实聚合，刷新 /status 缓存（与接口 SQL 一致）
        st = self._read_chapter_stats_ephemeral(novel.novel_id.value)
        if st:
            cc, mc, tw = st
            self._update_shared_state(
                novel.novel_id.value,
                _cached_completed_chapters=cc,
                _cached_manuscript_chapters=mc,
                _cached_total_words=tw,
                _cached_current_chapter_number=chapter_num,
            )

        # AOF：章节完成后删除 .draft 文件（数据已安全落盘到 DB）
        try:
            from application.engine.services.draft_aof import delete_draft
            delete_draft(novel.novel_id.value, chapter_num)
        except Exception:
            pass

        self._flush_novel(novel)

        logger.info(
            f"[{novel.novel_id}] 🎉 第 {chapter_num} 章完成：{actual_word_count} 字 "
            f"(目标 {target_word_count} 字，共 {novel.current_auto_chapters}/{novel.target_chapters} 章)"
        )
    @staticmethod
    def _beats_to_planned_micro_beats(beats: List[Any]) -> List[Dict[str, Any]]:
        """供共享内存 /status 与前端侧栏展示的指挥器节拍快照。"""
        out: List[Dict[str, Any]] = []
        for b in beats or []:
            out.append(
                {
                    "description": getattr(b, "description", "") or "",
                    "target_words": int(getattr(b, "target_words", 0) or 0),
                    "focus": getattr(b, "focus", "") or "pacing",
                    "location_id": getattr(b, "location_id", "") or "",
                }
            )
        return out

    @staticmethod
    def _beat_sheet_to_plan_json(beat_sheet: Optional[Any]) -> Optional[Dict[str, Any]]:
        """将仓储 BeatSheet 转为 ``build_chapter_execution_plan_async`` 的 beat_sheet_json。"""
        if not beat_sheet:
            return None
        scenes_raw = getattr(beat_sheet, "scenes", None)
        if not scenes_raw:
            return None

        scenes: List[Dict[str, Any]] = []
        for s in scenes_raw:
            scenes.append(
                {
                    "title": getattr(s, "title", "") or "",
                    "goal": getattr(s, "goal", "") or "",
                    "estimated_words": getattr(s, "estimated_words", None) or 600,
                    "pov_character": getattr(s, "pov_character", "") or "",
                    "location": getattr(s, "location", None),
                    "tone": getattr(s, "tone", None),
                    "transition_from_prev": getattr(s, "transition_from_prev", None),
                }
            )
        return {"scenes": scenes}

    async def _get_beat_sheet_for_chapter(self, novel_id: str, chapter_number: int) -> Optional[Any]:
        """获取章节的 BeatSheet（规划阶段的预估字数）

        Args:
            novel_id: 小说 ID
            chapter_number: 章节号

        Returns:
            BeatSheet 对象或 None
        """
        try:
            # 获取章节 ID
            chapter = self.chapter_repository.get_by_novel_and_number(
                NovelId(novel_id), chapter_number
            )
            if not chapter:
                return None

            # 尝试从仓储获取 BeatSheet
            from infrastructure.persistence.database.sqlite_beat_sheet_repository import SqliteBeatSheetRepository
            from infrastructure.persistence.database.connection import get_database

            beat_sheet_repo = SqliteBeatSheetRepository(get_database())
            # 🔥 get_by_chapter_id 是 async 方法，必须 await
            beat_sheet = await beat_sheet_repo.get_by_chapter_id(chapter.id)

            if beat_sheet and beat_sheet.scenes:
                return beat_sheet

        except Exception as e:
            logger.debug(f"获取 BeatSheet 失败: {e}")

        return None
    async def _stream_llm_with_stop_watch(
        self, prompt: Prompt, config: GenerationConfig, novel=None,
        total_timeout: float = 600.0, idle_timeout: float = 120.0,
    ) -> str:
        """与 workflow 共用同一套 Prompt + LLM；novel 传入时并行轮询 DB 是否已停止。

        优化点：
        1. 快速响应停止信号（0.3s 轮询间隔）
        2. 批量推送 chunks，减少跨进程通信开销
        3. 使用共享状态缓存，减少 DB 访问
        4. 超时保护：总时间上限 + 空闲超时（防止 LLM 挂起）

        Args:
            prompt: LLM 提示词
            config: 生成配置
            novel: 小说对象
            total_timeout: 总时间上限（秒），默认 10 分钟
            idle_timeout: 空闲超时（秒），默认 2 分钟无数据则终止
        """
        content = ""
        stop_detected = asyncio.Event()
        watch_task = None
        idle_watch_task = None
        nid = getattr(novel.novel_id, "value", novel.novel_id) if novel else None

        # 批量推送缓冲
        chunk_buffer: List[str] = []
        last_push_time = time.time()
        last_chunk_time = time.time()  # 追踪最后一次收到数据的时间
        # 🔥 高频小批量推送：实现真正的流式打字机效果
        # 每隔 0.15 秒推送一次，让前端有足够时间渲染，但又不会积攒太多
        CHUNK_PUSH_INTERVAL = 0.15
        start_time = time.time()

        async def _watch_stop_signal() -> None:
            """停止信号监听（三通道，快速响应）。

            优先级：
            1. threading.Event.is_set() → 亚微秒级，零 I/O
            2. mp.Queue 主动消费 → 毫秒级，设置 threading.Event
            3. DB 降级 → 每 10 秒检查一次（🔥 之前每 50ms 查 DB = 每秒 20 次 SQLite 连接，
               虽然在守护进程独立进程中不直接阻塞 API，但会加剧 SQLite 锁竞争，
               间接导致 API 进程的 DB 查询更频繁地超时）
            """
            db_check_counter = 0
            DB_CHECK_INTERVAL = 200  # 每 200 次循环（约 10 秒）查一次 DB

            while not stop_detected.is_set():
                await asyncio.sleep(0.05)  # 50ms 检查间隔

                # 通道 1：本地 threading.Event（亚微秒级）
                try:
                    from application.engine.services.novel_stop_signal import is_novel_stopped
                    if is_novel_stopped(novel_id_ref.value):
                        logger.info(f"[{nid}] IPC 停止信号已触发，结束流式")
                        stop_detected.set()
                        return
                except Exception:
                    pass

                # 通道 2：主动消费 mp.Queue（确保停止消息被及时处理）
                try:
                    from application.engine.services.streaming_bus import streaming_bus
                    streaming_bus.consume_control_signals(novel_id_ref.value)
                except Exception:
                    pass

                # 🔥 消费后下一轮循环的通道 1 会立即检查，无需重复

                # 通道 3：DB 降级（🔥 降频：每 ~10 秒检查一次，不再每 50ms）
                db_check_counter += 1
                if db_check_counter >= DB_CHECK_INTERVAL:
                    db_check_counter = 0
                    if not self._novel_is_running_in_db(novel_id_ref):
                        logger.info(f"[{nid}] DB 降级检测：已停止，结束流式")
                        stop_detected.set()
                        return

        async def _watch_idle_timeout() -> None:
            """空闲超时检测：长时间无数据则终止"""
            while not stop_detected.is_set():
                await asyncio.sleep(5.0)  # 每 5 秒检查一次
                elapsed_since_chunk = time.time() - last_chunk_time
                if elapsed_since_chunk >= idle_timeout:
                    logger.warning(
                        f"[{nid}] ⚠️ 流式生成空闲超时（{idle_timeout}s 无数据），强制终止"
                    )
                    stop_detected.set()
                    return

                # 检查总时间
                total_elapsed = time.time() - start_time
                if total_elapsed >= total_timeout:
                    logger.warning(
                        f"[{nid}] ⚠️ 流式生成总时间超限（{total_timeout}s），强制终止"
                    )
                    stop_detected.set()
                    return

        if novel is not None:
            novel_id_ref = novel.novel_id
            watch_task = asyncio.create_task(_watch_stop_signal())

        # 启动空闲超时检测
        idle_watch_task = asyncio.create_task(_watch_idle_timeout())

        try:
            async for chunk in self.llm_service.stream_generate(prompt, config):
                if stop_detected.is_set():
                    break
                content += chunk
                last_chunk_time = time.time()  # 更新最后收到数据的时间

                # 🔧 优化：高频小批量推送，实现流式打字机效果
                if novel is not None and chunk:
                    chunk_buffer.append(chunk)
                    current_time = time.time()
                    # 定期推送（每 0.15 秒），让前端有时间渲染
                    if current_time - last_push_time >= CHUNK_PUSH_INTERVAL:
                        await self._push_streaming_chunk(novel.novel_id.value, "".join(chunk_buffer))
                        chunk_buffer.clear()
                        last_push_time = current_time

                if stop_detected.is_set():
                    break
        except asyncio.CancelledError:
            logger.info(f"[{nid}] 流式生成被取消")
            raise
        except Exception as e:
            logger.error(f"[{nid}] 流式生成异常: {e}")
            raise
        finally:
            # 🔧 确保推送剩余的 chunks
            if novel is not None and chunk_buffer:
                await self._push_streaming_chunk(novel.novel_id.value, "".join(chunk_buffer))

            stop_detected.set()
            if watch_task is not None:
                watch_task.cancel()
                try:
                    await watch_task
                except asyncio.CancelledError:
                    pass
            if idle_watch_task is not None:
                idle_watch_task.cancel()
                try:
                    await idle_watch_task
                except asyncio.CancelledError:
                    pass

        if novel is not None:
            self._merge_autopilot_status_from_db(novel)

        return strip_reasoning_artifacts(content)

    async def _push_streaming_chunk(self, novel_id: str, chunk: str):
        """推送增量文字到全局流式队列，供 SSE 接口消费
        
        🔥 同时更新心跳——流式生成可能持续 30-120 秒，
        期间前端需要知道守护进程仍在工作。
        """
        from application.engine.services.streaming_bus import streaming_bus
        streaming_bus.publish(novel_id, chunk)
        # 🔥 流式生成期间更新心跳，避免前端误判"后端无响应"
        self._write_daemon_heartbeat()

    def _update_stream_metadata(self, novel_id: str, beat_index: int, word_count: int):
        """更新流式元数据（供外部调用）"""
        from application.engine.services.streaming_bus import streaming_bus
        streaming_bus.update_beat(novel_id, beat_index, word_count)

    async def _soft_landing(
        self,
        content: str,
        beat: "Beat",
        outline: str,
        chapter_draft_so_far: str,
        novel=None,
        signal=None,
        emotion_trend: str = "stable",  # ★ Phase 2: rising/peak/falling/stable
    ) -> str:
        """V9: 软着陆——专业小说家的截断修复

        不是粗暴地"补句号"，而是像一个真正的作家那样处理：
        1. 先检测截断位置——是在对话中间？叙述中间？还是场景中间？
        2. 根据截断类型选择不同的续写策略
        3. 续写时参考大纲和前后文，确保结尾与章节弧线衔接
        4. 控制续写长度，避免续写过度
        ★ Phase 2: 情绪方向感知——根据前一节拍的情绪趋势决定收尾方式：
          - rising/peak: 用省略号或动作残影收尾（保留势能）
          - falling/stable: 用句号或完整结论收尾（闭合叙事）

        Args:
            content: 已生成的内容
            beat: 当前节拍对象
            outline: 章节大纲
            chapter_draft_so_far: 本章已生成的正文
            novel: 小说对象
            signal: ConductorSignal（指挥信号）
            emotion_trend: 情绪方向（★ Phase 2）

        Returns:
            完整的内容（可能包含续写部分）
        """
        import re

        if not content or not content.strip():
            return content

        # 🔥 停止信号检查：用户已停止时不发起续写 LLM 调用，直接返回已有内容
        if novel is not None and not self._is_still_running(novel):
            logger.info(f"[{novel.novel_id}] 软着陆跳过：用户已停止自动驾驶")
            return content

        stripped = content.rstrip()

        # 检测是否以句子结束符结尾
        ending_pattern = r'[。！？…）】》"\'』」]$'
        if re.search(ending_pattern, stripped):
            return content  # 结尾完整，无需续写

        # ── 诊断截断类型 ──
        truncation_type = self._diagnose_truncation(stripped)

        # ★ Phase 2: 情绪方向决定续写策略
        is_rising = emotion_trend in ("rising", "peak")

        # ── 确定续写预算 ──
        is_final_beat = signal.is_final_beat if signal else False
        if is_final_beat:
            # 最后节拍：允许稍长续写，确保章节有完整收尾
            max_continuation = 200
            continuation_role = "你是小说收尾助手。为被截断的章节最后一段提供自然、有画面感的收束。"
        elif truncation_type == "dialogue":
            # 对话中间截断：续写要简短，补完对话即可
            max_continuation = 100
            continuation_role = "你是小说续写助手。为被截断的对话提供简短自然的收尾，补完当前对话回合即可。"
        else:
            # 叙述/场景中间截断：中等续写
            max_continuation = 150
            continuation_role = "你是小说续写助手。为被截断的段落提供简短自然的收尾，让段落有完整的结尾。"

        logger.warning(
            f"[软着陆] 检测到截断（类型={truncation_type}，"
            f"最后节拍={is_final_beat}），发起续写（预算{max_continuation}字）"
        )

        # ── 构建续写 Prompt ──
        # 关键：给 LLM 足够的上下文，让它知道"该怎么收"
        context_snippet = stripped[-600:]  # 截断位置前文
        
        # 增加章节上下文，帮助续写保持连贯
        chapter_context_hint = ""
        if chapter_draft_so_far and len(chapter_draft_so_far) > 600:
            # 提供本章开头部分，帮助维持整体连贯性
            beginning_snippet = chapter_draft_so_far[:300]
            chapter_context_hint = f"\n本章开头参考：\n{beginning_snippet}...\n"
        
        outline_hint = ""
        if outline:
            # 取大纲最后部分作为方向指引
            outline_hint = f"\n章节大纲参考：\n{outline[-200:]}\n"

        final_beat_hint = ""
        if is_final_beat:
            final_beat_hint = (
                "\n这是本章最后一段。续写时：\n"
                "- 给出完整的段落结尾\n"
                "- 可以用一句有悬念感的话作为章节钩子\n"
                "- 不要强行总结全章\n"
                "- 确保与本章其他节拍形成完整的叙事弧线\n"
            )

        # 连贯性增强指南
        coherence_guide = ""
        if chapter_draft_so_far:
            coherence_guide = (
                "\n---连贯性要求---\n"
                "1. 续写内容必须与本章已生成的其他节拍保持情节连贯\n"
                "2. 保持相同的场景设定和人物状态\n"
                "3. 如果前文有未完成的情节线索，优先处理这些线索\n"
            )

        # ★ Phase 2: 情绪方向决定收尾风格
        emotion_guide = ""
        if is_rising:
            emotion_guide = (
                "\n---情绪方向指示---\n"
                "当前叙事情绪正在上升或达到高潮。续写时：\n"
                "- 用动作残影、未完的话语、或省略号收尾，保留叙事势能\n"
                "- 不要用句号「杀死」正在上升的张力——用破折号或省略号更好\n"
                "- 如果是战斗/对峙场景，留下一个未落下的动作\n"
            )
        else:
            emotion_guide = (
                "\n---情绪方向指示---\n"
                "当前叙事情绪在下降或平稳。续写时：\n"
                "- 给出完整的结论性收尾，用句号闭合\n"
                "- 可以用一个画面感强的小细节作为结束\n"
            )

        continuation_prompt = Prompt(
            system=continuation_role,
            user=f"""以下段落被截断了，请续写一个简短的结尾（{max_continuation}字以内）让它完整结束：

---截断的内容---
{context_snippet}
{chapter_context_hint}{outline_hint}{final_beat_hint}{coherence_guide}{emotion_guide}
---续写要求---
1. 承接上文语气和节奏，给出自然的收尾
2. 不要重复已有内容
3. 必须以完整句子结束
4. 字数控制在 {max_continuation} 字以内
5. 保持与上文一致的人物语气和叙事视角
6. 确保与本章整体情节发展相符

请直接续写，不要解释："""
        )

        try:
            config = GenerationConfig(max_tokens=int(max_continuation * 0.8), temperature=0.6)
            continuation = await self._stream_llm_with_stop_watch(
                continuation_prompt, config, novel=novel
            )

            if continuation and continuation.strip():
                # 拼接续写内容
                result = stripped + continuation.strip()
                # ★ Phase 2: 二次安全检查——根据情绪方向决定补全符
                if not re.search(ending_pattern, result.rstrip()):
                    if is_rising:
                        result = result.rstrip() + "……"  # 保留势能
                    else:
                        result = result.rstrip() + "。"  # 闭合叙事
                logger.info(f"[软着陆] 成功续写 {len(continuation.strip())} 字（截断类型={truncation_type}，情绪={emotion_trend}）")
                return result

        except Exception as e:
            logger.warning(f"[软着陆] 续写失败: {e}")

        # 续写失败——智能补结尾（不是粗暴加句号，而是截到上一个完整句子）
        result = self._fallback_close_sentence(stripped)
        return result

    def _diagnose_truncation(self, text: str) -> str:
        """诊断截断类型

        Returns:
            "dialogue" - 对话中间截断（有未闭合的引号）
            "narration" - 叙述中间截断（正常段落中间）
            "scene" - 场景中间截断（环境描写中间）
        """
        import re
        # 检查是否有未闭合的中文引号
        open_quotes = text.count('「') + text.count('"') + text.count('"')
        close_quotes = text.count('」') + text.count('"') + text.count('"')
        if open_quotes > close_quotes:
            return "dialogue"

        # 检查是否在环境描写中间（最后若干字没有对话标点）
        last_50 = text[-50:] if len(text) > 50 else text
        if not re.search(r'[「」""''：]', last_50) and re.search(r'[的着了过]', last_50[-5:]):
            return "scene"

        return "narration"

    def _fallback_close_sentence(self, text: str) -> str:
        """降级收尾：找到最后一个完整句子边界

        如果找不到好的边界，至少保证不留下半句话。
        """
        import re
        ending_pattern = r'[。！？…）】》"\'』」]'

        # 从后往前找最后一个句子结束符
        for i in range(len(text) - 1, max(len(text) - 200, -1), -1):
            if re.match(ending_pattern, text[i]):
                return text[:i + 1]

        # 实在找不到，补句号
        return text.rstrip() + "。"

    async def _stream_one_beat(
        self,
        outline,
        context,
        beat_prompt,
        beat,
        novel=None,
        voice_anchors: str = "",
        chapter_draft_so_far: str = "",
    ) -> str:
        """无 AutoNovelGenerationWorkflow 时的降级：爽文短 Prompt + 流式。"""
        va = (voice_anchors or "").strip()
        voice_block = ""
        if va:
            voice_block = (
                "【角色声线与肢体语言（Bible 锚点，必须遵守）】\n"
                f"{va}\n\n"
            )
        system = f"""你是一位资深网文作家，擅长写爽文。
{voice_block}写作要求：
1. 严格按节拍字数和聚焦点写作
2. 必须有对话和人物互动，保持人物性格一致
3. 增加感官细节：视觉、听觉、触觉、情绪
4. 节奏控制：不要一章推进太多剧情
5. 不要写章节标题"""

        user_parts = []
        if context:
            user_parts.append(context)
        user_parts.append(f"\n【本章大纲】\n{outline}")
        prior = format_prior_draft_for_prompt(chapter_draft_so_far)
        if prior:
            user_parts.append(
                "\n【本章上文（近期全文精确衔接 + 远期回溯避免重复；禁止复述或重复已写对白与情节）】\n"
                f"{prior}"
            )
            # V2：节拍间衔接锚点注入
            try:
                from application.workflows.beat_continuation import (
                    extract_beat_tail_anchor,
                    build_beat_transition_directive,
                )
                anchor = extract_beat_tail_anchor(prior)
                if anchor.tail_state or anchor.last_moment:
                    next_desc = (beat_prompt or "").strip()[:80] if beat_prompt else ""
                    directive = build_beat_transition_directive(
                        anchor, getattr(beat, 'index', 0) or 0, 1, next_desc,
                    )
                    user_parts.append(f"\n{directive}")
            except Exception:
                pass  # 降级：无锚点则不加

        if beat_prompt:
            user_parts.append(f"\n{beat_prompt}")
        user_parts.append("\n\n开始撰写：")

        # 字数控制策略（与主流程一致）
        max_tokens = int(beat.target_words * 1.3) if beat else 3000

        prompt = Prompt(system=system, user="\n".join(user_parts))
        config = GenerationConfig(max_tokens=max_tokens, temperature=0.85)
        return await self._stream_llm_with_stop_watch(prompt, config, novel=novel)

    async def _upsert_chapter_content(self, novel, chapter_node, content: str, status: str):
        """最小事务：只更新章节内容，不涉及其他表

        🔥 CQRS 优化：draft 状态通过持久化队列写入，避免多进程锁竞争。
        但 completed 状态是关键状态转换，必须直接写 DB 确保立即可见，
        否则后续逻辑（如 _find_next_unwritten_chapter_async）会因持久化队列
        延迟而读到旧 draft 状态，导致重复审计同一章节。

        安全规则：
        1. 空内容不能将状态更新为 completed（防止空章节被标记为完成）
        2. 空内容不会覆盖已有内容（防止意外清空）
        """
        from domain.novel.entities.chapter import Chapter, ChapterStatus
        from domain.novel.value_objects.novel_id import NovelId
        from application.engine.services.persistence_queue import PersistenceCommandType

        stripped = (content or "").strip()
        try:
            if getattr(novel, "generation_prefs", None) is not None and getattr(
                novel.generation_prefs, "inline_prose_aggregation_enabled", False
            ):
                content_str = aggregate_inline_prose_fragments(stripped)
            else:
                content_str = stripped
        except Exception:
            content_str = stripped
        novel_id = novel.novel_id.value
        chapter_number = chapter_node.number

        # 🔥 关键修复：completed 状态必须直接写 DB
        # 之前全部走持久化队列，如果队列消费延迟，_find_next_unwritten_chapter_async
        # 会读到旧 draft 状态，导致审计完成→写文→跳过→再审计的死循环
        is_critical_status = status == "completed"

        if not is_critical_status:
            # draft 状态：优先使用持久化队列（无锁竞争）
            payload = {
                "novel_id": novel_id,
                "chapter_number": chapter_number,
                "content": content_str,
                "status": status if content_str else "draft",
            }

            if self._push_persistence_command(PersistenceCommandType.UPSERT_CHAPTER.value, payload):
                logger.debug(f"[{novel_id}] 章节内容已推送到持久化队列: ch={chapter_number}")
                return

        # 🔥 completed 状态或持久化队列不可用：用独立短连接写 DB
        # 替代原来的 chapter_repository.save()（长连接持有写锁阻塞 API 进程）
        existing = self.chapter_repository.get_by_novel_and_number(
            NovelId(novel_id), chapter_number
        )
        if existing:
            existing_content = (existing.content or "").strip()

            # 安全检查：空内容不能标记为 completed
            if not content_str and status == "completed":
                logger.warning(
                    f"[{novel_id}] 拒绝将章节 {chapter_number} 标记为 completed：内容为空"
                )
                return

            # 防御：避免意外用空串覆盖已有正文
            if not content_str:
                if status == "draft" and existing_content:
                    logger.debug(
                        f"[{novel_id}] 章节 {chapter_number} 内容为空，仅更新状态为 draft（保留已有内容）"
                    )
                    self._save_chapter_ephemeral(novel_id, chapter_number, status="draft")
                return

            # 正常更新：使用独立短连接
            import uuid
            wc = len(content_str)
            ok = self._save_chapter_ephemeral(
                novel_id, chapter_number,
                content=content_str,
                status=status,
                word_count=wc,
            )
            if ok:
                logger.debug(f"[{novel_id}] 章节已通过短连接落盘: ch={chapter_number} status={status}")
            else:
                logger.warning(f"[{novel_id}] 短连接写入失败，已降级到持久化队列: ch={chapter_number}")
        else:
            # 新建章节：需要 INSERT，用短连接
            if not content_str and status == "completed":
                logger.warning(
                    f"[{novel_id}] 拒绝创建空的 completed 章节 {chapter_number}"
                )
                return

            import uuid
            ch_id = chapter_node.id or str(uuid.uuid4())
            ch_title = chapter_node.title or ""
            ch_outline = chapter_node.outline or ""
            ch_status = status if content_str else "draft"
            wc = len(content_str)

            sql = """INSERT INTO chapters (id, novel_id, number, title, content, outline, status, word_count,
                                             tension_score, plot_tension, emotional_tension, pacing_tension,
                                             created_at, updated_at)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 0, 0, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"""
            # 🔥 CQRS：推队列，由 API 进程消费者串行执行（零锁竞争）
            self._queue_sql(
                sql, [ch_id, novel_id, chapter_number, ch_title, content_str, ch_outline, ch_status, wc]
            )

    def _find_parent_volume_for_new_act(
        self,
        volume_nodes: list,
        act_nodes: list,
        current_auto_chapters: int,
        target_chapters: int,
        rec_acts_per_volume: int,
        novel_id: str,
    ):
        """智能选择新幕的父卷。

        核心改进（替代原来的线性均分算法）：
        1. 统计每个已有卷下已挂载了多少幕
        2. 优先选择「当前写入卷」（幕数尚未达到 rec_acts_per_volume 的卷）
        3. 只有当前卷的幕数已经满了，才跳到下一卷
        4. 这样确保每卷都能写够足够多的幕，而不是写3幕就跑路
        """
        if not volume_nodes:
            logger.warning(f"[{novel_id}] 无可用卷节点，无法确定父卷")
            return None

        # 统计每个卷下的幕数量
        volume_act_counts: Dict[int, int] = {}
        for v in volume_nodes:
            volume_act_counts[v.number] = sum(
                1 for a in act_nodes if a.parent_id == v.id
            )

        # 策略：从第一个卷开始找，返回第一个「幕数 < rec_acts_per_volume」的卷
        # 如果所有卷都满了，返回最后一个卷（允许超发）
        for v in volume_nodes:
            current_count = volume_act_counts.get(v.number, 0)
            if current_count < rec_acts_per_volume:
                logger.info(
                    f"[{novel_id}] 父卷选择：第{v}卷已有{current_count}幕"
                    f"（上限{rec_acts_per_volume}），继续在本卷创建新幕"
                )
                return v

        # 所有卷都已达到建议幕数，挂在最后一个卷上（允许超发）
        last_volume = volume_nodes[-1]
        logger.info(
            f"[{novel_id}] 父卷选择：所有卷已达{rec_acts_per_volume}幕上限"
            f"，新幕挂到最后一个卷（第{last_volume.number}卷）"
        )
        return last_volume

    async def _find_next_unwritten_chapter_async(self, novel):
        """找到下一个未写的章节节点

        🔥 修复：增加已审计章节的跳过逻辑。
        当持久化队列延迟导致章节在 DB 中仍为 draft 时，
        通过 last_audit_chapter_number 判断该章节已经审计完成，
        避免重复生成同一章节。
        """
        novel_id = novel.novel_id.value
        all_nodes = await self.story_node_repo.get_by_novel(novel_id)
        chapter_nodes = sorted(
            [n for n in all_nodes if n.node_type.value == "chapter"],
            key=lambda n: n.number
        )

        last_audited_num = getattr(novel, 'last_audit_chapter_number', None)

        for node in chapter_nodes:
            # 🔥 跳过已审计的章节（即使 DB 中仍为 draft，也视为已完成）
            if last_audited_num is not None and node.number <= last_audited_num:
                # 确保已审计但 DB 仍为 draft 的章节被强制标记为 completed
                chapter = self.chapter_repository.get_by_novel_and_number(
                    NovelId(novel_id), node.number
                )
                if chapter and chapter.status.value != "completed":
                    logger.warning(
                        f"[{novel_id}] 章节 {node.number} 已审计但 DB 仍为 {chapter.status.value}，"
                        f"强制修正为 completed"
                    )
                    chapter.status = ChapterStatus.COMPLETED
                    # 🔥 核心修复：使用独立短连接写入 completed 状态
                    self._save_chapter_ephemeral(novel_id, node.number, status="completed")
                continue

            chapter = self.chapter_repository.get_by_novel_and_number(
                NovelId(novel_id), node.number
            )
            if not chapter or chapter.status.value != "completed":
                return node
        return None

    async def _current_act_fully_written(self, novel) -> bool:
        """检查当前幕是否已全部写完"""
        novel_id = novel.novel_id.value
        all_nodes = await self.story_node_repo.get_by_novel(novel_id)
        act_nodes = sorted(
            [n for n in all_nodes if n.node_type.value == "act"],
            key=lambda n: n.number
        )

        current_act_node = next(
            (n for n in act_nodes if n.number == novel.current_act + 1),
            None
        )
        if not current_act_node:
            return True

        act_children = self.story_node_repo.get_children_sync(current_act_node.id)
        chapter_nodes = [n for n in act_children if n.node_type.value == "chapter"]

        for node in chapter_nodes:
            chapter = self.chapter_repository.get_by_novel_and_number(
                NovelId(novel_id), node.number
            )
            if not chapter or chapter.status.value != "completed":
                return False
        return True

    async def _get_existing_chapter_content(self, novel, chapter_num) -> Optional[str]:
        """获取已存在的章节内容（用于断点续写）"""
        chapter = self.chapter_repository.get_by_novel_and_number(
            NovelId(novel.novel_id.value), chapter_num
        )
        return chapter.content if chapter else None
