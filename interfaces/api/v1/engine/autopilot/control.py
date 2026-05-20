"""Autopilot control 路由。"""
from fastapi import APIRouter
from interfaces.api.v1.engine.autopilot.shared import *  # noqa: F403,F401

router = APIRouter()
@router.post("/{novel_id}/start")
async def start_autopilot(novel_id: str, body: StartRequest = StartRequest()):
    """启动自动驾驶（共享内存先行；目标章数字数原子落库后再发 IPC，避免与 PUT 竞态）。

    架构：
    1. 解析当前阶段并合并本次请求的 target_chapters / target_words_per_chapter（可选）。
    2. 立即写入共享内存（含目标字数，供 /status 与前端进度条）。
    3. await 线程池中的 DB 持久化（RUNNING + 目标字段），再发布 IPC —— 守护进程下一轮读 DB 即可拿到正确每章字数。
    """
    loop = asyncio.get_running_loop()

    # ── 第一步：从共享内存快速校验小说是否存在（优先）──
    next_stage = None
    current_act = 0
    current_chapter_in_act = 0
    resolved_tc = 1
    resolved_twpc = 2500
    current_stage_str = "macro_planning"

    shared = _get_shared_state_for_novel(novel_id)
    if shared and shared.get("_updated_at"):
        # 共享内存有数据：零 DB IO 路径
        current_stage_str = shared.get("current_stage", "macro_planning")
        current_act = shared.get("current_act", 0) or 0
        current_chapter_in_act = shared.get("current_chapter_in_act", 0) or 0
        resolved_tc = int(shared.get("target_chapters", 1) or 1)
        resolved_twpc = int(shared.get("target_words_per_chapter") or 2500)

        # 计算下一阶段
        fresh_stages = {"planning", "macro_planning"}
        if current_stage_str in fresh_stages:
            next_stage = NovelStage.MACRO_PLANNING.value
        elif current_stage_str == "paused_for_review":
            # 幕下已有章节节点则直接写作，否则幕级规划
            if _has_chapter_nodes_under_current_act(novel_id, current_act):
                next_stage = NovelStage.WRITING.value
            else:
                next_stage = NovelStage.ACT_PLANNING.value
        else:
            next_stage = current_stage_str
    else:
        # ── 降级路径：共享内存无数据，必须读 DB（在线程池中执行）──
        def _start_read_sync():
            repo = get_novel_repository()
            n = repo.get_by_id(NovelId(novel_id))
            if not n:
                return None
            return {
                "current_stage": n.current_stage.value if hasattr(n.current_stage, 'value') else str(n.current_stage),
                "current_act": n.current_act or 0,
                "current_chapter_in_act": n.current_chapter_in_act or 0,
                "target_chapters": n.target_chapters or 1,
                "target_words_per_chapter": getattr(n, "target_words_per_chapter", None) or 2500,
            }

        try:
            novel_data = await asyncio.wait_for(
                loop.run_in_executor(_SSE_THREAD_POOL, _start_read_sync),
                timeout=5.0,
            )
        except asyncio.TimeoutError:
            raise HTTPException(503, "数据库繁忙，请稍后重试")

        if novel_data is None:
            raise HTTPException(404, "小说不存在")

        current_stage_str = novel_data["current_stage"]
        current_act = novel_data["current_act"]
        current_chapter_in_act = novel_data["current_chapter_in_act"]
        resolved_tc = int(novel_data["target_chapters"])
        resolved_twpc = int(novel_data.get("target_words_per_chapter") or 2500)

        fresh_stages = {"planning", "macro_planning"}
        if current_stage_str in fresh_stages:
            next_stage = NovelStage.MACRO_PLANNING.value
        elif current_stage_str == "paused_for_review":
            if _has_chapter_nodes_under_current_act(novel_id, current_act):
                next_stage = NovelStage.WRITING.value
            else:
                next_stage = NovelStage.ACT_PLANNING.value
        else:
            next_stage = current_stage_str

    if body.target_chapters is not None:
        resolved_tc = _clamp_autopilot_target_chapters(body.target_chapters)
    if body.target_words_per_chapter is not None:
        resolved_twpc = _clamp_autopilot_words_per_chapter(body.target_words_per_chapter)

    # ── 第二步：立即写入共享内存（前端立即可见）──
    try:
        from interfaces.main import update_shared_novel_state
        update_shared_novel_state(novel_id,
            autopilot_status="running",
            current_stage=next_stage,
            current_act=current_act,
            current_chapter_in_act=current_chapter_in_act,
            current_beat_index=0,
            consecutive_error_count=0,
            target_chapters=resolved_tc,
            target_words_per_chapter=resolved_twpc,
        )
        logger.debug("autopilot start: 已刷新共享内存状态 novel=%s", novel_id)
    except Exception as e:
        logger.debug("刷新共享内存失败（可忽略）: %s", e)

    # ── 第三步：持久化到 DB（await：确保守护进程 wake 时已能读到正确目标字数）──
    def _start_persist_sync():
        """线程池中执行：DB 读取 + 写入"""
        try:
            _persist_autopilot_running_sync(
                novel_id,
                max_auto_chapters=body.max_auto_chapters,
                target_chapters=resolved_tc,
                target_words_per_chapter=resolved_twpc,
            )
            logger.info(
                "autopilot start: novel_id=%s persisted RUNNING (DB) tc=%s twpc=%s",
                novel_id,
                resolved_tc,
                resolved_twpc,
            )
        except Exception as e:
            logger.warning("autopilot start DB 持久化失败（共享内存已生效）: %s", e)

    try:
        await asyncio.wait_for(loop.run_in_executor(_SSE_THREAD_POOL, _start_persist_sync), timeout=30.0)
    except asyncio.TimeoutError:
        logger.warning("autopilot start DB 持久化超时 novel=%s（IPC 仍将发送）", novel_id)

    # ── 第四步：发布 IPC 启动信号 ──
    try:
        from application.engine.services.novel_stop_signal import publish_start_signal
        publish_start_signal(novel_id)
    except Exception as e:
        logger.debug("发布启动信号失败（可忽略，守护进程将通过 DB 降级路径感知）: %s", e)

    return {
        "success": True,
        "message": f"自动驾驶已启动，目标 {resolved_tc} 章 × {resolved_twpc} 字/章（保护上限 {body.max_auto_chapters} 章）",
        "autopilot_status": "running",
        "current_stage": next_stage,
        "target_chapters": resolved_tc,
        "target_words_per_chapter": resolved_twpc,
    }


@router.post("/{novel_id}/stop")
async def stop_autopilot(novel_id: str):
    """停止自动驾驶（IPC 零延迟版）

    双通道停止机制：
    1. mp.Event.set() → 守护进程亚毫秒级感知（主通道，零 DB 开销）
    2. DB UPDATE → 降级兜底（守护进程重启后仍能读到 STOPPED）

    SQLite 操作在线程池中执行，不阻塞 uvicorn 事件循环。

    幂等性：如果已经是 stopped 状态，直接返回成功，避免重复发布停止信号和 DB 写入。
    """
    # 🔥 幂等保护：检查共享内存状态，已是 stopped 则直接返回
    # 防止前端因响应延迟重复调 /stop 导致日志刷屏和 DB 竞争
    try:
        from interfaces.main import get_shared_novel_state
        shared = get_shared_novel_state(novel_id)
        if shared and shared.get("autopilot_status") == "stopped":
            logger.debug("autopilot stop: novel_id=%s 已是 stopped，跳过重复停止", novel_id)
            return {"success": True, "message": "自动驾驶已停止（幂等跳过）"}
    except Exception:
        pass  # 共享内存不可用时走正常流程

    # 通道 1：IPC 停止信号（亚毫秒级，零 DB 开销）
    try:
        from application.engine.services.novel_stop_signal import publish_stop_signal
        publish_stop_signal(novel_id)
        logger.info("autopilot stop: novel_id=%s IPC 停止信号已发布", novel_id)
    except Exception as e:
        logger.debug("发布 IPC 停止信号失败（将依赖 DB 降级路径）: %s", e)

    # 🔥 关键修复：立即更新共享内存状态，让 SSE 流能检测到状态变化
    # 否则 SSE 流从共享内存读取时仍看到 running，不会推送 autopilot_complete 事件
    try:
        from interfaces.main import update_shared_novel_state
        update_shared_novel_state(novel_id,
            autopilot_status="stopped",
        )
        logger.debug("autopilot stop: 已更新共享内存状态 novel=%s", novel_id)
    except Exception as e:
        logger.debug("更新共享内存失败（可忽略）: %s", e)

    # 通道 2：DB 持久化（降级兜底，守护进程重启后仍能读到 STOPPED）
    def _stop_sync():
        from application.paths import get_db_path
        from infrastructure.persistence.database.connection import get_database

        db = get_database(get_db_path())
        db.execute(
            """UPDATE novels SET autopilot_status = 'stopped', updated_at = CURRENT_TIMESTAMP
               WHERE id = ?""",
            (novel_id,),
        )
        db.commit()
        logger.info("autopilot stop: novel_id=%s committed STOPPED (DB 兜底)", novel_id)

    try:
        await asyncio.get_running_loop().run_in_executor(_SSE_THREAD_POOL, _stop_sync)
        return {"success": True, "message": "自动驾驶已停止"}
    except Exception as e:
        logger.warning("autopilot stop DB 写入失败, falling back: %s", e)
        # 🔥 修复：fallback 路径同样可能 database is locked，
        # IPC 信号（通道 1）已保证守护进程亚毫秒级停止，
        # DB 持久化只是兜底，失败时不应阻塞 API 返回
        try:
            repo = get_novel_repository()
            novel = repo.get_by_id(NovelId(novel_id))
            if novel:
                novel.autopilot_status = AutopilotStatus.STOPPED
                repo.save(novel)
                logger.info("autopilot stop: novel_id=%s committed STOPPED (fallback)", novel_id)
        except Exception as fallback_err:
            # fallback 也失败（大概率也是 database is locked），仅记日志
            # IPC 通道已确保停止信号送达，DB 兜底可延迟生效
            logger.warning(
                "autopilot stop fallback 也失败（IPC 通道已保证停止）: %s", fallback_err
            )
        return {"success": True, "message": "自动驾驶已停止（停止信号已通过 IPC 送达）"}


@router.post("/{novel_id}/resume")
async def resume_from_review(novel_id: str):
    """从人工审阅点恢复（PAUSED_FOR_REVIEW → RUNNING）（非阻塞版）

    架构优化：与 start_autopilot 一致
    1. 先从共享内存校验 + 计算下一阶段
    2. 立即写入共享内存（前端立即可见）
    3. 异步持久化到 DB（不阻塞事件循环）
    4. 发布 IPC 启动信号
    """
    loop = asyncio.get_running_loop()

    # ── 第一步：从共享内存校验当前状态 ──
    current_act = 0
    current_stage_str = ""

    shared = _get_shared_state_for_novel(novel_id)
    if shared and shared.get("_updated_at"):
        current_stage_str = shared.get("current_stage", "")
        current_act = shared.get("current_act", 0) or 0

        if not _stage_needs_human_review(current_stage_str):
            raise HTTPException(400, f"当前不在审阅等待状态（当前：{current_stage_str}）")
    else:
        # 降级路径：共享内存无数据，读 DB（在线程池中）
        def _resume_read_sync():
            repo = get_novel_repository()
            n = repo.get_by_id(NovelId(novel_id))
            if not n:
                return None
            return {
                "current_stage": n.current_stage.value if hasattr(n.current_stage, 'value') else str(n.current_stage),
                "current_act": n.current_act or 0,
            }

        try:
            novel_data = await asyncio.wait_for(
                loop.run_in_executor(_SSE_THREAD_POOL, _resume_read_sync),
                timeout=5.0,
            )
        except asyncio.TimeoutError:
            raise HTTPException(503, "数据库繁忙，请稍后重试")

        if novel_data is None:
            raise HTTPException(404, "小说不存在")

        current_stage_str = novel_data["current_stage"]
        current_act = novel_data["current_act"]

        if not _stage_needs_human_review(current_stage_str):
            raise HTTPException(400, f"当前不在审阅等待状态（当前：{current_stage_str}）")

    # 计算下一阶段
    if _has_chapter_nodes_under_current_act(novel_id, current_act):
        next_stage = NovelStage.WRITING.value
        msg = "已恢复：当前幕已有章节规划，进入正文撰写"
    else:
        next_stage = NovelStage.ACT_PLANNING.value
        msg = "已恢复：继续幕级规划"

    # ── 第二步：立即写入共享内存（前端立即可见）──
    try:
        from interfaces.main import update_shared_novel_state
        update_shared_novel_state(novel_id,
            autopilot_status="running",
            current_stage=next_stage,
            current_act=current_act,
        )
    except Exception as e:
        logger.debug("刷新共享内存失败（可忽略）: %s", e)

    # ── 第三步：异步持久化到 DB ──
    def _resume_persist_sync():
        try:
            repo = get_novel_repository()
            novel = repo.get_by_id(NovelId(novel_id))
            if not novel:
                return
            _persist_autopilot_running_sync(
                novel_id,
                max_auto_chapters=getattr(novel, "max_auto_chapters", 9999) or 9999,
                target_chapters=novel.target_chapters or 1,
                target_words_per_chapter=getattr(novel, "target_words_per_chapter", None) or 2500,
            )
            logger.info("autopilot resume: novel_id=%s persisted (DB)", novel_id)
        except Exception as e:
            logger.warning("autopilot resume DB 持久化失败（共享内存已生效）: %s", e)

    try:
        await asyncio.wait_for(
            loop.run_in_executor(_SSE_THREAD_POOL, _resume_persist_sync),
            timeout=30.0,
        )
    except asyncio.TimeoutError:
        logger.warning("autopilot resume DB 持久化超时 novel=%s", novel_id)

    # ── 第四步：发布 IPC 启动信号 ──
    try:
        from application.engine.services.novel_stop_signal import publish_start_signal
        publish_start_signal(novel_id)
    except Exception as e:
        logger.debug("发布启动信号失败（可忽略）: %s", e)

    logger.info("autopilot resume novel=%s -> %s", novel_id, next_stage)
    return {"success": True, "message": msg, "current_stage": next_stage}


@router.get("/{novel_id}/status")
async def get_autopilot_status(novel_id: str):
    """获取完整运行状态。

    🔥 核心架构优化：纯内存读取，纳秒级响应，永不阻塞事件循环。

    所有数据都从共享内存读取，完全不走 DB。
    这是"内存优先读取"架构的核心端点。
    """
    from application.engine.services.query_service import get_query_service

    query = get_query_service()
    status = query.get_novel_status_dict(novel_id)

    if status is None:
        # 小说不在共享内存中，可能是不存在或未加载
        # 返回 404 而不是尝试读 DB（避免阻塞）
        raise HTTPException(404, "小说不存在或未加载")

    return status


@router.get("/{novel_id}/circuit-breaker")
async def get_circuit_breaker(novel_id: str):
    """
    熔断面板数据：基于小说落库的连续失败计数与自动驾驶状态。

    🔥 优化：从共享内存读取，不阻塞事件循环。
    """
    from application.engine.services.query_service import get_query_service

    query = get_query_service()
    state = query.get_novel_status(novel_id)

    if state is None:
        raise HTTPException(404, "小说不存在")

    error_count = state.consecutive_error_count
    ap = state.autopilot_status

    if ap == "error":
        breaker_status = "open"
    elif ap == "running" and 0 < error_count < PER_NOVEL_FAILURE_THRESHOLD:
        breaker_status = "half_open"
    else:
        breaker_status = "closed"

    return {
        "status": breaker_status,
        "error_count": error_count,
        "max_errors": PER_NOVEL_FAILURE_THRESHOLD,
        "last_error": None,
        "error_history": [],
    }


@router.post("/{novel_id}/circuit-breaker/reset")
async def reset_circuit_breaker(novel_id: str):
    """清零连续失败计数；若因错误挂起则切回停止，需用户重新启动自动驾驶。

    🔥 优化：通过 StatePublisher 更新，避免直接 DB 操作。
    """
    from application.engine.services.state_publisher import get_state_publisher
    from application.engine.services.query_service import get_query_service

    query = get_query_service()
    state = query.get_novel_status(novel_id)

    if state is None:
        raise HTTPException(404, "小说不存在")

    publisher = get_state_publisher()

    # 更新状态
    new_status = "stopped" if state.autopilot_status == "error" else state.autopilot_status
    publisher.update_novel_state(
        novel_id,
        consecutive_error_count=0,
        autopilot_status=new_status,
    )

    return {"success": True, "message": "熔断计数已清零"}


