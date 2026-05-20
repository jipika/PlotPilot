"""进程与守护进程生命周期 — 从 interfaces.main 抽取。"""
import logging
import multiprocessing
import os
import signal
import threading
import time
from typing import Any, Dict

logger = logging.getLogger(__name__)

# 由 main 在启动时注入
log_level: int = logging.INFO
log_file: str = "logs/plotpilot.log"


def configure_daemon_logging(level: int, file: str) -> None:
    global log_level, log_file
    log_level = level
    log_file = file


# 守护进程进程管理（使用独立进程避免阻塞主事件循环）
_daemon_process = None
_daemon_stop_event = None

# ── 跨进程共享状态字典（核心架构：状态走内存，数据走磁盘）──
# 在启动守护进程前初始化，供 API 进程零 DB IO 读取实时状态
_mp_manager: multiprocessing.Manager | None = None
_shared_state: dict | None = None


def _get_shared_state() -> dict:
    """获取跨进程共享状态字典（惰性初始化）。

    架构原则：
    - 守护进程写入：stage、audit_progress、last_chapter_tension 等高频状态字段
    - API 进程读取：/status 和 SSE 直接读内存，零 DB IO，纳秒级响应
    - DB 只负责：核心业务数据固化（低频、可延迟）
    """
    global _mp_manager, _shared_state
    if _shared_state is not None:
        return _shared_state
    _mp_manager = multiprocessing.Manager()
    _shared_state = _mp_manager.dict()
    logger.info("✅ 跨进程共享状态字典已初始化 (multiprocessing.Manager)")
    return _shared_state


def update_shared_novel_state(novel_id: str, **fields) -> None:
    """守护进程调用：更新指定小说的实时状态到共享内存。

    Args:
        novel_id: 小说 ID
        **fields: 状态字段，如 stage="auditing", audit_progress="voice_check"
    """
    state = _get_shared_state()
    key = f"novel:{novel_id}"
    # Manager.dict 中的值需要是可序列化的，用普通 dict
    current = dict(state.get(key, {}))
    # 🔥 确保 novel_id 始终在数据中
    fields["novel_id"] = novel_id
    current.update(fields)
    current["_updated_at"] = time.time()
    state[key] = current


def get_shared_novel_state(novel_id: str) -> Dict[str, Any]:
    """API 进程调用：从共享内存读取小说实时状态（零 DB IO）。

    Returns:
        状态字典，如果不存在返回空 dict
    """
    state = _get_shared_state()
    key = f"novel:{novel_id}"
    return dict(state.get(key, {}))


def _is_expected_daemon_shutdown_exception(exc: BaseException) -> bool:
    """热重载/停止时的中断视为正常退出，避免子进程打印长栈。"""
    import asyncio

    current = exc
    visited = set()
    while current is not None and id(current) not in visited:
        visited.add(id(current))
        if isinstance(current, (KeyboardInterrupt, asyncio.CancelledError)):
            return True
        current = current.__cause__ or current.__context__
    return False


def _stop_all_running_novels():
    """重启时将所有运行中的小说设置为停止状态

    经由 `get_database().execute`：在持久化消费者已启动时走队列入库；在
    `startup_sqlite_writes_bypass_queue` 内则直连 SQLite（启动早期，无 writer 争抢）。

    保留 WAL 残留清理与 disk I/O 重试；重试时重置全局 DB 单例以换新连接。
    """
    import sqlite3
    import time
    from pathlib import Path

    from application.paths import get_db_path
    from infrastructure.persistence.database import connection as db_connection
    from infrastructure.persistence.database.connection import get_database

    db_path = get_db_path()
    db_path_str = str(Path(db_path))
    db_path_obj = Path(db_path) if isinstance(db_path, str) else db_path

    if not db_path_obj.exists():
        logger.warning(f"⚠️  数据库文件不存在: {db_path}")
        return

    max_retries = 3
    for attempt in range(1, max_retries + 1):
        try:
            db = get_database(db_path_str)

            chk = db.fetch_one(
                "SELECT 1 AS ok FROM sqlite_master WHERE type='table' AND name='novels' LIMIT 1"
            )
            if chk is None:
                logger.info("ℹ️  新库尚无 novels 表，跳过运行中小说复位")
                return

            cnt_row = db.fetch_one(
                "SELECT COUNT(*) AS c FROM novels WHERE autopilot_status = 'running'"
            )
            running_count = int(cnt_row["c"]) if cnt_row and cnt_row.get("c") is not None else 0

            if running_count > 0:
                db.execute(
                    """UPDATE novels SET autopilot_status = 'stopped', updated_at = CURRENT_TIMESTAMP
                       WHERE autopilot_status = 'running'"""
                )
                db.commit()
                logger.info(
                    "🔒 已将 %s 本运行中的小说设置为停止状态（服务重启）",
                    running_count,
                )
            else:
                logger.info("✅ 没有运行中的小说需要停止")

            try:
                db.get_connection().execute("PRAGMA wal_checkpoint(TRUNCATE)")
            except Exception:
                pass
            return

        except sqlite3.OperationalError as e:
            if "disk I/O error" in str(e) and attempt < max_retries:
                logger.warning(
                    "⚠️  停止运行中小说遇到 disk I/O error（第 %s/%s 次），"
                    "尝试清理 WAL 残留并重置连接后重试...",
                    attempt,
                    max_retries,
                )
                for suffix in ("-wal", "-shm"):
                    wal_file = db_path_obj.parent / (db_path_obj.name + suffix)
                    if wal_file.exists():
                        try:
                            wal_file.unlink()
                            logger.info("🧹 已清理残留 WAL 文件: %s", wal_file)
                        except OSError as unlink_err:
                            logger.warning("清理 WAL 文件失败: %s — %s", wal_file, unlink_err)
                try:
                    if db_connection._db_instance is not None:
                        db_connection._db_instance.close_all(skip_checkpoint=True)
                except Exception:
                    pass
                db_connection._db_instance = None
                time.sleep(1.0 * attempt)
            else:
                logger.error(
                    "❌ 停止运行中小说失败: db=%s err=%s",
                    db_path_obj,
                    e,
                    exc_info=True,
                )
                return
        except Exception as e:
            logger.error(
                "❌ 停止运行中小说失败: db=%s err=%s",
                db_path_obj,
                e,
                exc_info=True,
            )
            return


def _recover_drafts_on_startup():
    """AOF 崩溃恢复：扫描残留的 .draft 文件，恢复到 DB"""
    try:
        from application.engine.services.draft_aof import recover_all_drafts
        recovered = recover_all_drafts()
        if recovered > 0:
            logger.info(f"🔧 AOF 崩溃恢复：已恢复 {recovered} 个章节的草稿数据")
        else:
            logger.info("✅ AOF 检查：无残留草稿需要恢复")
    except Exception as e:
        logger.warning(f"⚠️ AOF 崩溃恢复失败（可忽略）: {e}")


def _run_daemon_in_process(
    stop_event: threading.Event,
    log_level: int,
    log_file: str,
    stream_queue=None,
    shared_state=None,
    persistence_queue=None,
):
    """在独立进程中运行守护进程（完全隔离，不阻塞主进程）

    Args:
        stop_event: 停止信号
        log_level: 日志级别
        log_file: 日志文件路径
        stream_queue: StreamingBus 的队列对象（从主进程传入）
        shared_state: multiprocessing.Manager().dict() 共享状态字典
        persistence_queue: 持久化队列（CQRS 单一写入者模式）
    """
    # 重新配置日志（子进程需要独立配置）
    from interfaces.api.middleware.logging_config import setup_logging
    setup_logging(level=log_level, log_file=log_file)

    # 注入流式队列（必须在导入任何使用 streaming_bus 的模块前设置）
    if stream_queue is not None:
        from application.engine.services.streaming_bus import inject_stream_queue
        inject_stream_queue(stream_queue)
        logger.info("✅ 守护进程：流式队列已注入")

    # 注入共享状态字典（供守护进程写入实时状态）
    if shared_state is not None:
        try:
            # 将共享状态注入到全局，供 daemon 使用
            import sys
            sys.modules["__shared_state"] = shared_state

            # 🔥 初始化共享状态仓库（守护进程端）
            from application.engine.services.shared_state_repository import (
                inject_shared_dict,
                get_shared_state_repository,
            )
            inject_shared_dict(shared_state)
            logger.info("✅ 守护进程：共享状态字典已注入")

            # 🔥 初始化状态发布器（守护进程的唯一写入入口）
            from application.engine.services.state_publisher import get_state_publisher
            get_state_publisher()  # 会自动获取共享状态仓库和持久化队列
            logger.info("✅ 守护进程：状态发布器已初始化")

        except Exception as e:
            logger.warning("共享状态注入失败（可忽略）: %s", e)

    # 🔥 注入持久化队列（守护进程通过此队列发送 DB 写命令）
    if persistence_queue is not None:
        try:
            from application.engine.services.persistence_queue import inject_persistence_queue
            inject_persistence_queue(persistence_queue)
            logger.info("✅ 守护进程：持久化队列已注入")
        except Exception as e:
            logger.warning("持久化队列注入失败（可忽略）: %s", e)

    # 初始化小说停止信号模块（Queue 驱动，无需额外注入）
    try:
        from application.engine.services.novel_stop_signal import inject_novel_stop_events
        inject_novel_stop_events()
    except Exception as e:
        logger.debug("小说停止信号模块初始化失败（可忽略）: %s", e)

    try:
        from scripts.start_daemon import build_daemon
        daemon = build_daemon()
        logger.info("🚀 守护进程已启动（独立进程），开始轮询...")

        # 创建持久化事件循环（避免每个小说都 asyncio.run() 创建/销毁循环）
        import asyncio
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        logger.info("✅ 守护进程：持久化事件循环已创建")

        while not stop_event.is_set():
            try:
                # 消费 mp.Queue 中的停止信号消息（设置本地 threading.Event）
                try:
                    from application.engine.services.streaming_bus import streaming_bus
                    streaming_bus.consume_stop_signals()
                except Exception:
                    pass

                # 执行守护进程的一个轮询周期
                active_novels = daemon._get_active_novels()

                if active_novels:
                    for novel in active_novels:
                        if stop_event.is_set():
                            break
                        # 使用持久化事件循环处理每个小说
                        loop.run_until_complete(daemon._process_novel(novel))

                # 轮询间隔（使用 wait 而非 sleep，以便快速响应停止信号）
                stop_event.wait(timeout=daemon.poll_interval)

            except BaseException as e:
                if stop_event.is_set() or _is_expected_daemon_shutdown_exception(e):
                    logger.info("ℹ️ 守护进程在停止/热重载期间中断，正常退出")
                    break
                logger.error(f"❌ 守护进程异常: {e}", exc_info=True)
                stop_event.wait(timeout=10)  # 异常后等待10秒

    except BaseException as e:
        if stop_event.is_set() or _is_expected_daemon_shutdown_exception(e):
            logger.info("ℹ️ 守护进程收到停止信号，正常退出")
        else:
            logger.error(f"❌ 守护进程初始化失败: {e}", exc_info=True)
    finally:
        logger.info("🛑 守护进程已停止")


def _init_dag_node_registry():
    """初始化 DAG 节点注册表 — 加载所有 V1 节点实现"""
    try:
        # 导入所有节点模块，触发 @NodeRegistry.register 装饰器
        from application.engine.dag.nodes import (  # noqa: F401
            context_nodes,
            execution_nodes,
            validation_nodes,
            gateway_nodes,
            world_nodes,
            review_nodes,
            anti_ai_nodes,
            planning_nodes,
            gen_supplement_nodes,
            ext_supplement_nodes,
        )
        from application.engine.dag.registry import NodeRegistry
        logger.info(f"✅ DAG 节点注册表已初始化: {sorted(NodeRegistry.all_types())}")
    except Exception as e:
        logger.warning(f"⚠️ DAG 节点注册表初始化失败（DAG 引擎将不可用）: {e}")


def _start_autopilot_daemon_thread():
    """启动自动驾驶守护进程（独立进程，不阻塞主事件循环）"""
    global _daemon_process, _daemon_stop_event

    if _daemon_process is not None and _daemon_process.is_alive():
        logger.warning("⚠️  守护进程已在运行，跳过重复启动")
        return

    # 检查环境变量是否禁用自动启动守护进程
    if os.getenv("DISABLE_AUTO_DAEMON", "").lower() in ("1", "true", "yes"):
        logger.info("🔒 守护进程自动启动已禁用（DISABLE_AUTO_DAEMON=1）")
        return

    # 重要：在启动守护进程前初始化 StreamingBus 的队列
    # 使用 mp.Queue（可 pickle 序列化传递给子进程）
    from application.engine.services.streaming_bus import init_streaming_bus
    stream_queue = init_streaming_bus()

    # 初始化跨进程共享状态字典（必须在启动子进程前完成）
    shared_state = _get_shared_state()

    # 🔥 初始化共享状态仓库（内存优先读取的核心组件）
    from application.engine.services.shared_state_repository import (
        init_shared_state_repository,
    )
    shared_state_repo = init_shared_state_repository(shared_state)
    logger.info("✅ 共享状态仓库已初始化")

    # 🔥 启动时从 DB 加载所有数据到共享内存
    from application.engine.services.state_bootstrap import bootstrap_state
    bootstrap_stats = bootstrap_state()
    logger.info(f"✅ 状态已从 DB 加载到共享内存: {bootstrap_stats}")

    # 🔥 初始化查询服务（API 层的唯一查询入口）
    from application.engine.services.query_service import init_query_service
    init_query_service(shared_state_repo)
    logger.info("✅ 查询服务已初始化")

    # 🔥 初始化持久化队列（CQRS 单一写入者模式）
    from application.engine.services.persistence_queue import (
        initialize_persistence_queue, get_persistence_queue,
        register_persistence_handlers
    )
    persistence_queue = initialize_persistence_queue()

    # 注册持久化处理器（在主进程执行 DB 写入）
    register_persistence_handlers()

    pq = get_persistence_queue()
    if not pq.is_consumer_running():
        pq.start_consumer()
        logger.info("✅ 持久化消费者线程已启动（单一写入者模式）")
    else:
        logger.debug("持久化消费者已在启动早期就绪（守护进程阶段不重复启动）")

    _daemon_stop_event = multiprocessing.Event()

    # 使用独立进程运行守护进程，完全隔离于主进程的事件循环
    _daemon_process = multiprocessing.Process(
        target=_run_daemon_in_process,
        args=(_daemon_stop_event, log_level, log_file, stream_queue, shared_state, persistence_queue),
        name="AutopilotDaemon",
        daemon=True,
    )
    _daemon_process.start()
    logger.info("✅ 守护进程已创建并启动（独立进程模式，流式队列 + 共享状态 + 持久化队列已传递）")


def _cleanup_orphan_python_processes():
    """Windows: 清理可能残留的 plotpilot/uvicorn 相关 Python 进程。

    仅当命令行包含 plotpilot、autopilot、uvicorn、interfaces.main 之一时才终结进程，避免误杀。
    优先 PowerShell + CIM（新系统已移除 wmic）；不可用时回退 wmic。
    """
    import subprocess

    current_pid = os.getpid()
    logger.info("🔍 检查残留进程（当前 PID=%s）...", current_pid)

    ps_script = r"""$ErrorActionPreference = 'SilentlyContinue'
Get-CimInstance Win32_Process | ForEach-Object {
  $nl = ([string]$_.Name).ToLowerInvariant()
  if ($nl -notin @('python.exe','python3.exe','pythonw.exe','plotpilot-backend.exe')) { return }
  $cl = if ($null -eq $_.CommandLine) { '' } else { [string]$_.CommandLine }
  $cl = $cl -replace "`t", ' '
  [Console]::Out.WriteLine($_.ProcessId.ToString() + [char]9 + $cl)
}
"""

    def _list_via_powershell() -> list[tuple[int, str]]:
        r = subprocess.run(
            [
                "powershell",
                "-NoProfile",
                "-ExecutionPolicy",
                "Bypass",
                "-Command",
                ps_script,
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=15,
        )
        if r.returncode != 0:
            return []
        rows: list[tuple[int, str]] = []
        for line in r.stdout.splitlines():
            line = line.strip()
            if not line or "\t" not in line:
                continue
            pid_str, _, cmd = line.partition("\t")
            pid_str = pid_str.strip()
            if not pid_str.isdigit():
                continue
            rows.append((int(pid_str), cmd.strip()))
        return rows

    def _list_via_wmic() -> list[tuple[int, str]]:
        result = subprocess.run(
            [
                "wmic",
                "process",
                "where",
                "name='python.exe' or name='python3.exe' or name='plotpilot-backend.exe'",
                "get",
                "processid,commandline",
            ],
            capture_output=True,
            text=True,
            timeout=8,
        )
        if result.returncode != 0:
            return []
        rows: list[tuple[int, str]] = []
        for line in result.stdout.strip().split("\n"):
            line = line.strip()
            if not line or "CommandLine" in line:
                continue
            if any(keyword in line.lower() for keyword in ("plotpilot", "autopilot", "uvicorn", "interfaces.main")):
                parts = line.split()
                for part in reversed(parts):
                    if part.isdigit():
                        rows.append((int(part), line))
                        break
        return rows

    keywords = ("plotpilot", "autopilot", "uvicorn", "interfaces.main")
    killed_count = 0

    try:
        candidates: list[tuple[int, str]] = []
        try:
            candidates = _list_via_powershell()
        except OSError as e:
            logger.debug("PowerShell 枚举进程不可用: %s", e)
        except subprocess.TimeoutExpired:
            logger.warning("⚠️ PowerShell 枚举进程超时，尝试 wmic")
        if not candidates:
            try:
                candidates = _list_via_wmic()
            except OSError as e:
                logger.debug("wmic 枚举进程不可用: %s", e)
            except subprocess.TimeoutExpired:
                logger.warning("⚠️ wmic 枚举进程超时")

        for pid, cmdline in candidates:
            low = cmdline.lower()
            if not any(k in low for k in keywords):
                continue
            if pid == current_pid:
                continue
            try:
                logger.info("🧹 清理残留进程 PID=%s: %s...", pid, cmdline[:80])
                subprocess.run(
                    ["taskkill", "/F", "/PID", str(pid)],
                    capture_output=True,
                    timeout=5,
                )
                killed_count += 1
            except Exception as e:
                logger.warning("清理进程 %s 失败: %s", pid, e)

        if killed_count > 0:
            logger.info("✅ 已清理 %s 个残留进程", killed_count)
        else:
            logger.info("✅ 未发现残留进程")

    except subprocess.TimeoutExpired:
        logger.warning("⚠️ 进程清理超时")
    except FileNotFoundError as e:
        logger.warning("⚠️ 进程清理失败（未找到 PowerShell/wmic）: %s", e)
    except Exception as e:
        logger.warning("⚠️ 进程清理失败: %s", e)


def _stop_autopilot_daemon_thread():
    """停止守护进程

    关键修复：确保 multiprocessing.Process 子进程在 os._exit 之前被彻底终止。
    Windows 上 os._exit(0) 不会触发 Python 的正常清理流程，
    daemon=True 的子进程可能变成孤儿进程，导致应用无法关闭。
    """
    global _daemon_process, _daemon_stop_event

    daemon_pid = _daemon_process.pid if _daemon_process else None

    # 🔥 停止持久化消费者线程（先停止消费，再停止守护进程）
    try:
        from application.engine.services.persistence_queue import get_persistence_queue
        get_persistence_queue().stop_consumer()
    except Exception as e:
        logger.debug(f"停止持久化消费者失败（可忽略）: {e}")

    # 通过 StreamingBus 发布全局停止信号（确保守护进程内正在运行的小说也能立即感知）
    try:
        from application.engine.services.streaming_bus import streaming_bus
        streaming_bus.publish_stop_signal("__all__")  # 特殊 ID，通知守护进程所有小说
    except Exception as e:
        logger.debug("发布全局停止信号失败（可忽略）: %s", e)

    if _daemon_stop_event:
        logger.info("🛑 正在停止守护进程...")
        _daemon_stop_event.set()

    if _daemon_process and _daemon_process.is_alive():
        _daemon_process.join(timeout=2)  # 给守护进程 2 秒完成当前轮询
        if _daemon_process.is_alive():
            logger.warning("⚠️  守护进程未在超时时间内停止，强制终止")
            _daemon_process.terminate()
            _daemon_process.join(timeout=1)
            # 如果还是活着，强制kill
            if _daemon_process.is_alive():
                logger.warning("⚠️  守护进程仍未停止，使用 SIGKILL")
                try:
                    os.kill(_daemon_process.pid, signal.SIGKILL)
                    _daemon_process.join(timeout=1)
                except Exception as e:
                    logger.error(f"强制终止守护进程失败: {e}")
        else:
            logger.info("✅ 守护进程已成功停止")

    # Windows: 使用 taskkill 强制杀死已知 PID 的子进程（双保险）
    # multiprocessing 在 Windows 上使用 spawn 方式，子进程可能不在同一进程树
    if os.name == "nt" and daemon_pid:
        try:
            import subprocess
            subprocess.run(
                ['taskkill', '/F', '/T', '/PID', str(daemon_pid)],
                capture_output=True, timeout=3
            )
            logger.info(f"🛑 Windows: 已通过 taskkill 终止守护进程 PID={daemon_pid}")
        except Exception as e:
            logger.debug(f"taskkill 终止守护进程失败（可能已退出）: {e}")

    _daemon_process = None
    _daemon_stop_event = None

    # Windows: 额外清理可能残留的 Python 子进程
    if os.name == "nt":
        _cleanup_orphan_python_processes()


def restart_autopilot_daemon():
    """重启守护进程以拾取新的 LLM / 嵌入配置（跨进程 env 不可共享，必须重启）。"""
    _stop_autopilot_daemon_thread()
    _start_autopilot_daemon_thread()
    logger.info("🔄 守护进程已因配置变更重启")
