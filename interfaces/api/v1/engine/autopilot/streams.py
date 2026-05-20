"""Autopilot streams 路由。"""
from fastapi import APIRouter
from interfaces.api.v1.engine.autopilot.shared import *  # noqa: F403,F401

router = APIRouter()
@router.get("/{novel_id}/stream")
@router.get("/{novel_id}/log-stream", include_in_schema=False)
async def autopilot_log_stream(
    novel_id: str,
    after_seq: int = Query(0, ge=0, description="仅推送 seq 大于该值的守护进程日志行；重连时传入上次最后一条 seq"),
):
    """
    SSE 实时日志流（用于监控大盘）

    - log_line: API 进程内存环 + LOG_FILE 增量 tail（独立守护进程日志，按书目过滤）
    - beat_start / beat_complete / stage_change / progress 等：状态机摘要
    """
    novel_repo = get_novel_repository()
    chapter_repo = get_chapter_repository()

    async def event_generator():
        install_autopilot_log_ring_handler()

        # SSE 连接超时控制
        start_time = asyncio.get_running_loop().time()

        # 发送初始连接事件（前端可不写入时间线；metadata 用于工具栏「当前阶段」标签）
        loop = asyncio.get_running_loop()
        init_meta = await loop.run_in_executor(_SSE_THREAD_POOL, _log_stream_boot_meta_sync, novel_repo, novel_id)
        init_event = {
            "type": "connected",
            "message": "日志流已连接（含守护进程实时日志；阶段变更约 4s 去抖）",
            "timestamp": datetime.now().isoformat(),
            "metadata": init_meta,
        }
        yield f"data: {json.dumps(init_event, ensure_ascii=False)}\n\n"

        last_seq_cursor = after_seq
        replay_lines, last_seq_cursor = await loop.run_in_executor(
            _SSE_THREAD_POOL, _log_stream_replay_sync, novel_id, after_seq, last_seq_cursor
        )
        for line in replay_lines:
            yield line

        log_file_path = os.getenv("LOG_FILE", "logs/plotpilot.log")
        file_cursor = await loop.run_in_executor(
            _SSE_THREAD_POOL, _log_stream_file_cursor_init_sync, log_file_path, after_seq
        )

        last_beat = None
        heartbeat_counter = 0
        last_error_broadcast = -1
        complete_sent = False
        # 阶段变更去抖：同一阶段需连续 2 次轮询（约 4s）一致才推送，避免幕级规划↔待审阅 来回刷屏
        first_stage_poll = True
        last_emitted_stage: Optional[str] = None
        stage_pending: Optional[str] = None
        stage_pending_ticks = 0

        while True:
            try:
                # 连接超时检测
                if (loop.time() - start_time) > _SSE_MAX_LIFETIME_SECONDS:
                    logger.info("SSE log stream reached max lifetime, closing: novel=%s", novel_id)
                    break

                # 客户端断开检测
                if await _is_client_disconnected():
                    logger.debug("SSE log stream client disconnected: novel=%s", novel_id)
                    break

                # 🔥 加超时保护：DB 被锁时 2 秒超时，避免线程池阻塞
                try:
                    tick_result = await asyncio.wait_for(
                        loop.run_in_executor(
                            _SSE_THREAD_POOL,
                            _log_stream_io_tick_sync,
                            novel_repo,
                            chapter_repo,
                            novel_id,
                            log_file_path,
                            file_cursor,
                            last_seq_cursor,
                        ),
                        timeout=2.0,
                    )
                    novel, chapters_stats, file_lines, file_cursor, ring_batch, audit_events = tick_result
                except asyncio.TimeoutError:
                    logger.debug("SSE log stream tick 超时 novel=%s，跳过本轮 DB 查询", novel_id)
                    # 超时时只读日志文件和内存环（不碰 DB）
                    file_lines, file_cursor = read_incremental_log_file_lines(log_file_path, novel_id, file_cursor)
                    ring_batch = list(iter_new_for_novel(novel_id, last_seq_cursor, limit=200))
                    # 🔥 超时时也获取审计事件
                    from application.engine.services.streaming_bus import streaming_bus
                    stream_data = streaming_bus.get_chunks_and_events_batch(novel_id, max_chunks=200)
                    audit_events = stream_data.get("audit_events", [])
                    # 从共享内存读取降级状态
                    shared = _get_shared_state_for_novel_cached(novel_id)
                    if shared and shared.get("_updated_at"):
                        # 构造一个最小 novel 代理对象用于阶段检测
                        current_stage = shared.get("current_stage", "")
                        current_beat = shared.get("current_beat_index", 0) or 0
                        current_chapter_number = shared.get("_cached_current_chapter_number")
                    else:
                        current_stage = ""
                        current_beat = 0
                        current_chapter_number = None
                    # 降级处理：只推送日志，不推送进度
                    for item in file_lines:
                        ev = {
                            "type": "log_line",
                            "message": item["message"],
                            "timestamp": item["timestamp"],
                            "metadata": {
                                "seq": item["seq"],
                                "level": item["level"],
                                "logger": item["logger"],
                                "source": "file",
                            },
                        }
                        yield f"data: {json.dumps(ev, ensure_ascii=False)}\n\n"
                        last_seq_cursor = max(last_seq_cursor, item["seq"])
                    for e in ring_batch:
                        ev = {
                            "type": "log_line",
                            "message": shorten_log_message(e.message),
                            "timestamp": e.timestamp_iso,
                            "metadata": {
                                "seq": e.seq,
                                "level": e.level,
                                "logger": e.logger_name,
                            },
                        }
                        yield f"data: {json.dumps(ev, ensure_ascii=False)}\n\n"
                        last_seq_cursor = max(last_seq_cursor, e.seq)
                    # 🔥 推送审计事件
                    for audit_event in audit_events:
                        event = {
                            "type": "audit_event",
                            "message": _audit_event_message(audit_event["event_type"], audit_event["data"]),
                            "timestamp": datetime.now().isoformat(),
                            "metadata": {
                                "event_type": audit_event["event_type"],
                                "data": audit_event["data"],
                            },
                        }
                        yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
                    await asyncio.sleep(2)
                    continue

                if not novel:
                    # 🔥 novel=None 时先查共享内存，避免 DB 临时不可用时误断 SSE
                    shared_chk = _get_shared_state_for_novel_cached(novel_id)
                    if shared_chk and shared_chk.get("autopilot_status") in ("running", "paused_for_review"):
                        logger.debug("SSE log stream novel=None but shared shows running, keep alive: novel=%s", novel_id)
                        await asyncio.sleep(3.0)
                        continue
                    logger.info("SSE log stream novel not found, closing: novel=%s", novel_id)
                    break

                # 🔥 chapters_stats 是轻量聚合结果，不再是全量章节列表
                current_chapter_number = None
                if chapters_stats and chapters_stats.get('current_chapter_number'):
                    current_chapter_number = chapters_stats['current_chapter_number']
                elif chapters_stats is None:
                    # 非写作阶段：从共享内存读取当前章节号
                    shared = _get_shared_state_for_novel_cached(novel_id)
                    if shared:
                        current_chapter_number = shared.get("_cached_current_chapter_number")
                chapter_label = f"第 {current_chapter_number} 章 · " if current_chapter_number else ""

                for item in file_lines:
                    ev = {
                        "type": "log_line",
                        "message": item["message"],
                        "timestamp": item["timestamp"],
                        "metadata": {
                            "seq": item["seq"],
                            "level": item["level"],
                            "logger": item["logger"],
                            "source": "file",
                        },
                    }
                    yield f"data: {json.dumps(ev, ensure_ascii=False)}\n\n"
                    last_seq_cursor = max(last_seq_cursor, item["seq"])

                for e in ring_batch:
                    ev = {
                        "type": "log_line",
                        "message": shorten_log_message(e.message),
                        "timestamp": e.timestamp_iso,
                        "metadata": {
                            "seq": e.seq,
                            "level": e.level,
                            "logger": e.logger_name,
                        },
                    }
                    yield f"data: {json.dumps(ev, ensure_ascii=False)}\n\n"
                    last_seq_cursor = max(last_seq_cursor, e.seq)

                # 🔥 推送审计事件
                for audit_event in audit_events:
                    event = {
                        "type": "audit_event",
                        "message": _audit_event_message(audit_event["event_type"], audit_event["data"]),
                        "timestamp": datetime.now().isoformat(),
                        "metadata": {
                            "event_type": audit_event["event_type"],
                            "data": audit_event["data"],
                        },
                    }
                    yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"

                current_stage = novel.current_stage.value
                current_beat = getattr(novel, "current_beat_index", 0) or 0
                # current_beat 为守护进程 0-based「下一节拍索引」；面向用户统一用 1-based 展示

                # 检测阶段变更（去抖后推送）
                if first_stage_poll:
                    last_emitted_stage = current_stage
                    first_stage_poll = False
                elif current_stage == last_emitted_stage:
                    stage_pending = None
                    stage_pending_ticks = 0
                else:
                    if stage_pending != current_stage:
                        stage_pending = current_stage
                        stage_pending_ticks = 1
                    else:
                        stage_pending_ticks += 1
                    if stage_pending_ticks >= 2 and current_stage != last_emitted_stage:
                        from_zh = _stage_name_zh(last_emitted_stage or current_stage)
                        to_zh = _stage_name_zh(current_stage)
                        event = {
                            "type": "stage_change",
                            "message": f"阶段变更：{from_zh} → {to_zh}",
                            "timestamp": datetime.now().isoformat(),
                            "metadata": {
                                "from_stage": last_emitted_stage,
                                "to_stage": current_stage,
                                "from_label": from_zh,
                                "to_label": to_zh,
                            },
                        }
                        yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
                        last_emitted_stage = current_stage
                        stage_pending = None
                        stage_pending_ticks = 0

                # 检测 beat 变更（表示上一个 beat 完成）
                act_display = (novel.current_act or 0) + 1
                if last_beat is not None and current_beat > last_beat:
                    done_1based = int(last_beat) + 1
                    next_1based = int(current_beat) + 1
                    event = {
                        "type": "beat_complete",
                        "message": f"{chapter_label}第 {act_display} 幕 · 节拍 {done_1based} 已生成完毕",
                        "timestamp": datetime.now().isoformat(),
                        "metadata": {
                            "beat_index": last_beat,
                            "beat_index_1based": done_1based,
                            "act": novel.current_act,
                            "act_display": act_display,
                            "chapter_number": current_chapter_number,
                        },
                    }
                    yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"

                    # 新 beat 开始
                    event = {
                        "type": "beat_start",
                        "message": f"{chapter_label}第 {act_display} 幕 · 正在生成节拍 {next_1based}",
                        "timestamp": datetime.now().isoformat(),
                        "metadata": {
                            "beat_index": current_beat,
                            "beat_index_1based": next_1based,
                            "act": novel.current_act,
                            "act_display": act_display,
                            "chapter_number": current_chapter_number,
                        },
                    }
                    yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"

                # 检测错误（仅在计数变化时推送，避免每 2 秒刷屏）
                error_count = getattr(novel, "consecutive_error_count", 0) or 0
                if error_count > 0 and error_count != last_error_broadcast:
                    last_error_broadcast = error_count
                    if error_count >= 3:
                        err_msg = (
                            f"连续失败已达 {error_count} 次，本书可能被标为异常并停止；"
                            "请在驾驶舱「解除挂起并清零计数」后重试，并确认守护进程与 LLM 可用。"
                        )
                    else:
                        err_msg = (
                            f"记录到连续失败 {error_count} 次（满 3 次将挂起）。"
                            "若持续出现，请检查模型/API 与守护进程日志。"
                        )
                    event = {
                        "type": "beat_error",
                        "message": err_msg,
                        "timestamp": datetime.now().isoformat(),
                        "metadata": {"error_count": error_count},
                    }
                    yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
                if error_count == 0:
                    last_error_broadcast = -1

                last_beat = current_beat

                # 托管进入终态：单连接只发一次「自动驾驶已停止」事件；不断开 SSE，继续 tail 日志与心跳，
                # 避免前端误以为「未连接」且无法再看后续守护进程日志。
                terminal_states = {"stopped", "error", "completed"}
                if novel.autopilot_status.value in terminal_states:
                    if not complete_sent:
                        complete_sent = True
                        st = novel.autopilot_status.value
                        event = {
                            "type": "autopilot_complete",
                            "message": f"自动驾驶{_autopilot_status_zh(st)}",
                            "timestamp": datetime.now().isoformat(),
                            "metadata": {
                                "status": st,
                                "status_label": _autopilot_status_zh(st),
                                "tail": True,
                            },
                        }
                        yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"

                # 运行中：定期推送进度快照（仅用于前端进度条，不写时间线刷屏）
                if novel.autopilot_status.value == AutopilotStatus.RUNNING.value:
                    # 🔥 优先使用轻量聚合结果，不再遍历章节列表
                    tgt = novel.target_chapters or 1
                    if chapters_stats:
                        n_done = chapters_stats.get('completed_count', 0)
                        tw = int(chapters_stats.get('total_words', 0)) if chapters_stats.get('total_words') else 0
                        current_chapter_number = chapters_stats.get('current_chapter_number')
                    else:
                        # 审计/规划阶段：从共享内存读取统计
                        shared = _get_shared_state_for_novel_cached(novel_id)
                        n_done = shared.get("_cached_completed_chapters", 0) if shared else 0
                        tw = int(shared.get("_cached_total_words", 0)) if shared else 0
                        current_chapter_number = shared.get("_cached_current_chapter_number") if shared else None
                    pct = round(n_done / tgt * 100, 1) if tgt else 0.0
                    stage_zh = _stage_name_zh(current_stage)
                    act_display = (novel.current_act or 0) + 1
                    beat_1based = int(current_beat) + 1
                    # ★ 从共享内存读取细化子步骤字段
                    _shared_sub = _get_shared_state_for_novel_cached(novel_id) or {}
                    writing_substep = _shared_sub.get("writing_substep", "")
                    writing_substep_label = _shared_sub.get("writing_substep_label", "")
                    total_beats = _shared_sub.get("total_beats", 0)
                    beat_focus = _shared_sub.get("beat_focus", "")
                    beat_target_words = _shared_sub.get("beat_target_words", 0)
                    accumulated_words = _shared_sub.get("accumulated_words", 0)
                    chapter_target_words = _shared_sub.get("chapter_target_words", 0)
                    context_tokens = _shared_sub.get("context_tokens", 0)

                    # 构建细化的进度消息
                    substep_hint = f" · {writing_substep_label}" if writing_substep_label else ""
                    beat_progress = f"节拍 {beat_1based}/{total_beats}" if total_beats else f"节拍 {beat_1based}"
                    word_progress = ""
                    if accumulated_words and chapter_target_words:
                        word_pct = min(100, int(accumulated_words / chapter_target_words * 100))
                        word_progress = f" · {accumulated_words}/{chapter_target_words}字({word_pct}%)"

                    progress_event = {
                        "type": "progress",
                        "message": (
                            f"全书 {n_done}/{tgt} 章 · 约 {tw} 字 · "
                            f"第 {act_display} 幕 · {beat_progress} · {stage_zh}{substep_hint}"
                        ),
                        "timestamp": datetime.now().isoformat(),
                        "metadata": {
                            "completed_chapters": n_done,
                            "target_chapters": tgt,
                            "progress_pct": pct,
                            "total_words": tw,
                            "current_act": novel.current_act,
                            "act_display": act_display,
                            "current_beat_index": current_beat,
                            "current_beat_index_1based": beat_1based,
                            "stage": current_stage,
                            "stage_label": stage_zh,
                            "chapter_number": current_chapter_number,
                            "autopilot_status": novel.autopilot_status.value,
                            "autopilot_status_label": _autopilot_status_zh(
                                novel.autopilot_status.value
                            ),
                            # ★ V9 细化字段
                            "writing_substep": writing_substep,
                            "writing_substep_label": writing_substep_label,
                            "total_beats": int(total_beats or 0),
                            "beat_focus": beat_focus,
                            "beat_target_words": int(beat_target_words or 0),
                            "accumulated_words": int(accumulated_words or 0),
                            "chapter_target_words": int(chapter_target_words or 0),
                            "context_tokens": int(context_tokens or 0),
                        },
                    }
                    yield f"data: {json.dumps(progress_event, ensure_ascii=False)}\n\n"

                # 每 10 次循环（20秒）发送一次心跳
                heartbeat_counter += 1
                if heartbeat_counter >= 10:
                    heartbeat_event = {
                        "type": "heartbeat",
                        "message": "keepalive",
                        "timestamp": datetime.now().isoformat()
                    }
                    yield f"data: {json.dumps(heartbeat_event, ensure_ascii=False)}\n\n"
                    heartbeat_counter = 0

                await asyncio.sleep(2)  # 每2秒检查一次

            except Exception as e:
                logger.error(f"SSE log stream error: {e}")
                break

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
    )


@router.get("/{novel_id}/chapter-stream")
async def autopilot_chapter_stream(novel_id: str):
    """SSE 实时推送正在写作的章节内容（优化版 v2）

    推送事件类型：
    - outline_planning: 章前规划（CPMS 拆节拍）进行中
    - beats_planned: 章前规划完成，指挥器节拍已就绪
    - chapter_chunk: 增量文字片段
    - chapter_start: 开始撰写正文（首个节拍流式输出前）
    - autopilot_stopped: 自动驾驶停止

    优化点：
    1. 批量获取 chunks 减少 SSE 事件数量
    2. 审阅状态时快速断开，避免占用资源
    3. 不再调用 clear()，避免数据丢失
    4. 更快的轮询间隔，提高响应速度
    """
    novel_repo = get_novel_repository()
    chapter_repo = get_chapter_repository()

    async def event_generator():
        loop = asyncio.get_running_loop()
        start_time = loop.time()
        # 发送初始连接事件
        init_event = {
            "type": "connected",
            "message": "章节内容流已连接",
            "timestamp": datetime.now().isoformat()
        }
        yield f"data: {json.dumps(init_event, ensure_ascii=False)}\n\n"

        last_chapter_number = None
        last_outline_planning_key: Optional[str] = None
        last_beats_planned_key: Optional[str] = None
        heartbeat_counter = 0
        empty_poll_count = 0
        MAX_EMPTY_POLLS = 24  # 连续空轮询约 12 秒后检查状态
        _PROSE_SUBSTEPS = frozenset(
            {
                "llm_calling",
                "soft_landing",
                "persisting",
                "continuity_check",
                "density_supplement",
                "chapter_persist",
            }
        )

        try:
            while True:
                # 连接超时检测
                if (loop.time() - start_time) > _SSE_MAX_LIFETIME_SECONDS:
                    logger.info("SSE chapter stream reached max lifetime, closing: novel=%s", novel_id)
                    break

                # 客户端断开检测
                if await _is_client_disconnected():
                    logger.debug("SSE chapter stream client disconnected: novel=%s", novel_id)
                    break
                # 🔥 加超时保护：DB 被锁时 2 秒超时，避免线程池被阻塞线程耗尽
                try:
                    novel, chapters, chunks = await asyncio.wait_for(
                        loop.run_in_executor(
                            _SSE_THREAD_POOL, _chapter_stream_tick_sync, novel_repo, chapter_repo, novel_id, 50
                        ),
                        timeout=2.0,
                    )
                except asyncio.TimeoutError:
                    # DB 被锁时只读 chunks（不碰 DB），前端不会卡死
                    logger.debug("SSE chapter stream tick 超时 novel=%s，跳过 DB", novel_id)
                    chunks = _chapter_stream_chunks_sync(novel_id, 50)
                    novel = None
                    # 从共享内存判断是否仍在运行
                    shared = _get_shared_state_for_novel_cached(novel_id)
                    if shared and shared.get("autopilot_status") in ("stopped", "error", "completed"):
                        event = {
                            "type": "autopilot_stopped",
                            "message": f"自动驾驶已停止: {shared['autopilot_status']}",
                            "timestamp": datetime.now().isoformat(),
                        }
                        yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
                        break
                    # 仍然推送 chunks，让前端看到正文流
                    if chunks:
                        combined = "".join(chunks)
                        if combined:
                            beat_idx = (shared.get("current_beat_index", 0) or 0) if shared else 0
                            event = {
                                "type": "chapter_chunk",
                                "message": "",
                                "timestamp": datetime.now().isoformat(),
                                "metadata": {
                                    "chunk": combined,
                                    "beat_index": beat_idx,
                                },
                            }
                            yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
                    await asyncio.sleep(poll_interval if 'poll_interval' in dir() else 0.8)
                    continue
                if not novel:
                    # 🔥 novel=None 时先查共享内存确认小说是否真的不存在，
                    # 避免 DB 被锁/慢查询时误断 SSE 导致前端疯狂重连
                    shared_chk = _get_shared_state_for_novel_cached(novel_id)
                    if shared_chk and shared_chk.get("autopilot_status") in ("running", "paused_for_review"):
                        # 共享内存显示仍在运行，DB 临时不可用，保持 SSE
                        logger.debug("SSE chapter stream novel=None but shared shows running, keep alive: novel=%s", novel_id)
                        await asyncio.sleep(poll_interval if 'poll_interval' in dir() else 3.0)
                        continue
                    # 共享内存也无数据，小说可能真的不存在，断开
                    logger.info("SSE chapter stream novel not found, closing: novel=%s", novel_id)
                    break

                terminal_states = {"stopped", "error", "completed"}
                if novel.autopilot_status.value in terminal_states:
                    event = {
                        "type": "autopilot_stopped",
                        "message": f"自动驾驶已停止: {novel.autopilot_status.value}",
                        "timestamp": datetime.now().isoformat(),
                    }
                    yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
                    break

                # 审阅状态时断开 SSE，避免卡界面
                if _stage_needs_human_review(novel.current_stage.value):
                    event = {
                        "type": "paused_for_review",
                        "message": "等待审阅确认",
                        "timestamp": datetime.now().isoformat(),
                    }
                    yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
                    break

                shared_live = _get_shared_state_for_novel_cached(novel_id) or {}
                ch_live = shared_live.get("current_chapter_number")
                sub_live = str(shared_live.get("writing_substep") or "")

                if ch_live is not None:
                    ch_n = int(ch_live)
                    if sub_live == "outline_planning":
                        op_key = f"op:{ch_n}"
                        if op_key != last_outline_planning_key:
                            event = {
                                "type": "outline_planning",
                                "message": shared_live.get(
                                    "writing_substep_label", "章前规划 · 划分节拍"
                                ),
                                "timestamp": datetime.now().isoformat(),
                                "metadata": {"chapter_number": ch_n},
                            }
                            yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
                            logger.debug("[SSE] outline_planning: 第 %s 章", ch_n)
                            last_outline_planning_key = op_key

                    planned = shared_live.get("planned_micro_beats") or []
                    tb = int(shared_live.get("total_beats") or 0)
                    if planned and tb > 0:
                        bp_key = f"bp:{ch_n}:{tb}"
                        if bp_key != last_beats_planned_key:
                            event = {
                                "type": "beats_planned",
                                "message": f"章前规划完成，{tb} 个节拍",
                                "timestamp": datetime.now().isoformat(),
                                "metadata": {
                                    "chapter_number": ch_n,
                                    "beats": planned,
                                    "outline_plan_mode": shared_live.get("outline_plan_mode", ""),
                                    "total_beats": tb,
                                },
                            }
                            yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
                            logger.debug("[SSE] beats_planned: 第 %s 章 ×%s", ch_n, tb)
                            last_beats_planned_key = bp_key

                # 正文撰写开始：进入 llm_calling 或已有流式 chunk（不再在 draft 创建时误报「开写」）
                prose_started = bool(chunks) or sub_live in _PROSE_SUBSTEPS
                if novel.current_stage.value == "writing" and prose_started:
                    chapter_number = int(ch_live) if ch_live is not None else None
                    if chapter_number is None and chapters:
                        _st = lambda c: c.status.value if hasattr(c.status, "value") else c.status
                        drafts = sorted(
                            [c for c in chapters if _st(c) == "draft"],
                            key=lambda c: c.number,
                        )
                        if drafts:
                            chapter_number = drafts[0].number
                    if chapter_number is not None and (
                        last_chapter_number is None or chapter_number != last_chapter_number
                    ):
                        event = {
                            "type": "chapter_start",
                            "message": f"开始撰写第 {chapter_number} 章正文",
                            "timestamp": datetime.now().isoformat(),
                            "metadata": {"chapter_number": chapter_number},
                        }
                        yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
                        logger.debug("[SSE] chapter_start: 第 %s 章（正文）", chapter_number)
                    if chapter_number is not None:
                        last_chapter_number = chapter_number

                if chunks:
                    empty_poll_count = 0
                    # 合并小 chunks 为单个事件，减少 SSE 事件数量
                    combined = "".join(chunks)
                    if combined:
                        # 🔥 优先从共享状态读取 beat_index（实时更新），而非 DB
                        shared = _get_shared_state_for_novel_cached(novel_id)
                        beat_idx = (shared.get("current_beat_index", 0) or 0) if shared else 0
                        event = {
                            "type": "chapter_chunk",
                            "message": "",
                            "timestamp": datetime.now().isoformat(),
                            "metadata": {
                                "chunk": combined,
                                "beat_index": beat_idx,
                            },
                        }
                        yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
                else:
                    empty_poll_count += 1
                    # 连续空轮询过多时检查状态
                    if empty_poll_count >= MAX_EMPTY_POLLS:
                        empty_poll_count = 0
                        # 🔥 优先从共享内存检查状态（零 DB IO），避免 DB 被锁时阻塞线程池
                        shared_chk = _get_shared_state_for_novel_cached(novel_id)
                        if shared_chk and shared_chk.get("autopilot_status") in terminal_states:
                            break
                        # 共享内存没有数据时才查 DB（加超时保护）
                        if not shared_chk or not shared_chk.get("_updated_at"):
                            try:
                                novel_chk = await asyncio.wait_for(
                                    loop.run_in_executor(
                                        _SSE_THREAD_POOL, novel_repo.get_by_id, NovelId(novel_id)
                                    ),
                                    timeout=1.0,
                                )
                                if not novel_chk or novel_chk.autopilot_status.value in terminal_states:
                                    break
                            except asyncio.TimeoutError:
                                pass  # DB 被锁，跳过，下轮再查

                # 心跳（每 10 次循环约 5 秒）
                heartbeat_counter += 1
                if heartbeat_counter >= 10:
                    heartbeat_event = {
                        "type": "heartbeat",
                        "message": "keepalive",
                        "timestamp": datetime.now().isoformat()
                    }
                    yield f"data: {json.dumps(heartbeat_event, ensure_ascii=False)}\n\n"
                    heartbeat_counter = 0

                # 轮询间隔：写作阶段 800ms，审计/规划阶段 3 秒（审计期间无 chunks 推送，
                # 无需高频轮询；减少 DB 查询可显著降低线程池压力和锁竞争）
                current_stage_val = novel.current_stage.value if novel else "writing"
                poll_interval = 3.0 if current_stage_val in ("auditing", "macro_planning", "act_planning") else 0.8
                await asyncio.sleep(poll_interval)

        except Exception as e:
            logger.error(f"Chapter stream error: {e}")

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
    )


@router.get("/{novel_id}/events")
async def autopilot_events(novel_id: str):
    """SSE 实时状态推送（每 3 秒，带 2 秒 DB 查询超时保护）。"""
    novel_repo = get_novel_repository()
    chapter_repo = get_chapter_repository()

    async def event_generator():
        loop = asyncio.get_running_loop()
        start_time = loop.time()
        while True:
            try:
                # 连接超时检测
                if (loop.time() - start_time) > _SSE_MAX_LIFETIME_SECONDS:
                    logger.info("SSE events stream reached max lifetime, closing: novel=%s", novel_id)
                    break
                if await _is_client_disconnected():
                    break

                # 🔥 防御性编程：SSE tick 也加 2 秒超时，防止 DB 锁阻塞线程池
                try:
                    payload, should_break = await asyncio.wait_for(
                        loop.run_in_executor(
                            _SSE_THREAD_POOL, _autopilot_events_tick_sync, novel_repo, chapter_repo, novel_id
                        ),
                        timeout=2.0,
                    )
                except asyncio.TimeoutError:
                    logger.warning("⏱️ SSE events tick 超时 novel=%s，发送降级心跳", novel_id)
                    payload = {
                        "type": "heartbeat",
                        "current_stage": "syncing",
                        "audit_progress": None,
                        "_degraded": True,
                        "_message": "数据同步中...",
                    }
                    should_break = False

                if payload is None:
                    break
                yield f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"
                if should_break:
                    break
                await asyncio.sleep(3)
            except Exception as e:
                logger.error(f"SSE error: {e}")
                break

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
    )


@router.get("/{novel_id}/stream-debug")
async def stream_debug(novel_id: str):
    """调试端点：检查流式队列状态"""
    from application.engine.services.streaming_bus import get_stream_queue, streaming_bus
    import multiprocessing as mp

    queue = get_stream_queue()
    current_process = mp.current_process()

    # 尝试读取一条消息（非阻塞）
    sample_msg = None
    queue_size = 0
    if queue is not None:
        try:
            # 尝试获取队列大小
            queue_size = streaming_bus.get_queue_size()
            # 读取一条消息作为样本
            sample_msg = queue.get_nowait()
            # 把消息放回去
            queue.put_nowait(sample_msg)
        except Exception as e:
            sample_msg = f"Error: {e}"

    return {
        "novel_id": novel_id,
        "current_process": current_process.name,
        "is_daemon": current_process.daemon,
        "queue_available": queue is not None,
        "queue_size": queue_size,
        "sample_message": sample_msg,
    }


