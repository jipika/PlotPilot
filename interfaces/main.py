"""FastAPI 主应用

提供 RESTful API 接口。
"""
# 必须在任何 HuggingFace/Transformers 导入前设置离线模式
import os
os.environ['HF_HUB_OFFLINE'] = '1'
os.environ['TRANSFORMERS_OFFLINE'] = '1'
os.environ['HF_DATASETS_OFFLINE'] = '1'
if os.getenv('DISABLE_SSL_VERIFY', 'false').lower() == 'true':
    os.environ['CURL_CA_BUNDLE'] = ''
    os.environ['REQUESTS_CA_BUNDLE'] = ''

from pathlib import Path
import sys
import time
import logging
from datetime import datetime
from typing import Any, Dict, Optional

# 必须在其他应用模块导入前执行：将仓库根目录 `.env` 写入 os.environ
_PLOTPILOT_ROOT = Path(__file__).resolve().parents[1]
if str(_PLOTPILOT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PLOTPILOT_ROOT))
try:
    from load_env import load_env

    load_env()
except Exception:
    # 无 .env 或非标准启动方式时忽略
    pass

# 配置日志（必须在导入其他模块前）
from interfaces.api.middleware.logging_config import setup_logging

log_level = getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO)
log_file = os.getenv("LOG_FILE", "logs/plotpilot.log")
setup_logging(level=log_level, log_file=log_file)

logger = logging.getLogger(__name__)

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse, JSONResponse
from starlette.requests import Request
import threading
import multiprocessing
import signal

# Core module
from interfaces.api.v1.core import novels, chapters, manuscript_entity_routes, scene_generation_routes, settings as llm_settings, export
from interfaces.api.v1.meta import taxonomy_routes

# World module
from interfaces.api.v1.world import bible, cast, knowledge, knowledge_graph_routes, worldbuilding_routes

# Blueprint module
from interfaces.api.v1.blueprint import continuous_planning_routes, beat_sheet_routes, story_structure
from interfaces.api.v1.blueprint.confluence_routes import router as confluence_router

# Engine module routes
from interfaces.api.v1.engine import (
    generation,
    context_intelligence,
    autopilot_routes,
    chronicles,
    snapshot_routes,
    workbench_context_routes,
    character_scheduler_routes,  # 角色调度API（正式功能）
    checkpoint_routes,  # Checkpoint + QualityGuardrail + StoryPhase
    narrative_engine_routes,  # 小说家向叙事引擎只读聚合
    worldline_routes,  # 世界线管理（故事 Git 模型）
)
from interfaces.api.v1.prop import prop_routes

# Audit module
from interfaces.api.v1.audit import chapter_review_routes, macro_refactor, chapter_element_routes

# Analyst module
from interfaces.api.v1.analyst import voice, narrative_state, foreshadow_ledger

# System module (internal tooling)
from interfaces.api.v1 import system as system_routes

# Reader Simulation module
from interfaces.api.v1 import reader as reader_module

# Workbench module
from interfaces.api.v1.workbench import sandbox, writer_block, monitor, llm_control
from interfaces.api.stats.routers.stats import create_stats_router
from interfaces.api.stats.services.stats_service import StatsService
from interfaces.api.stats.repositories.sqlite_stats_repository_adapter import SqliteStatsRepositoryAdapter
from infrastructure.persistence.database.connection import get_database

# 产品发布版本（与前端 / 安装包一致）
APP_RELEASE_VERSION = "1.0.2"
# 构建标识（与安装包/发布说明一致，便于对账）
BACKEND_BUILD_ID = "build-20260209-1200-c4d2"
STARTUP_TIME = time.time()

logger.info("=" * 80)
logger.info(
    "🚀 BACKEND STARTING - Release %s (build %s)",
    APP_RELEASE_VERSION,
    BACKEND_BUILD_ID,
)
logger.info(f"   Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
logger.info(f"   Log Level: {logging.getLevelName(log_level)}")
logger.info(f"   Log File: {log_file}")
logger.info(f"   Python: {sys.version.split()[0]}")
logger.info(f"   Working Dir: {Path.cwd()}")
logger.info("=" * 80)

# 创建 FastAPI 应用
app = FastAPI(
    title="PlotPilot API",
    version="1.0.2",
    description="PlotPilot（墨枢）AI 小说创作平台 API",
    redirect_slashes=True,  # 自动将 /api/v1/novels 重定向到 /api/v1/novels/
)

# 守护进程生命周期（须在 startup 钩子之前导入）
from interfaces.runtime.daemon_lifecycle import (
    configure_daemon_logging,
    get_shared_novel_state,
    restart_autopilot_daemon,
    update_shared_novel_state,
    _cleanup_orphan_python_processes,
    _get_shared_state,
    _init_dag_node_registry,
    _recover_drafts_on_startup,
    _start_autopilot_daemon_thread,
    _stop_all_running_novels,
    _stop_autopilot_daemon_thread,
)
from interfaces.runtime import daemon_lifecycle as _dl

configure_daemon_logging(log_level, log_file)

# ── 前端静态文件托管 ──
_FRONTEND_DIR = Path(__file__).resolve().parents[1] / "frontend" / "dist"
if _FRONTEND_DIR.exists():
    app.mount("/assets", StaticFiles(directory=str(_FRONTEND_DIR / "assets")), name="frontend-assets")
    # favicon 等根级静态资源
    _favicon = _FRONTEND_DIR / "favicon.svg"
    if _favicon.exists():
        app.get("/favicon.svg", include_in_schema=False, response_class=FileResponse)(
            lambda: FileResponse(str(_favicon), media_type="image/svg+xml")
        )
    # SPA fallback: 所有非 API 路径都返回 index.html
    _INDEX_HTML = _FRONTEND_DIR / "index.html"

# 修复反向代理场景下 trailing slash 重定向使用后端本地地址的 bug
# 当 FastAPI 的 trailing slash 重定向指向 127.0.0.1 时，
# 从 X-Forwarded-Host / Host / Referer 获取真实地址并改写 Location header
@app.middleware("http")
async def fix_redirect_host(request, call_next):
    response = await call_next(request)
    if response.status_code in (301, 307, 308):
        location = response.headers.get("location", "")
        if location and ("127.0.0.1" in location or "localhost" in location):
            from urllib.parse import urlparse, urlunparse
            parsed = urlparse(location)
            original_host = request.headers.get("x-forwarded-host") or request.headers.get("host", "")
            if not original_host or "127.0.0.1" in original_host or "localhost" in original_host:
                referer = request.headers.get("referer", "")
                if referer:
                    from urllib.parse import urlparse as _urlparse
                    ref_host = _urlparse(referer).netloc
                    if ref_host and "127.0.0.1" not in ref_host and "localhost" not in ref_host:
                        original_host = ref_host
            if original_host and "127.0.0.1" not in original_host and "localhost" not in original_host:
                scheme = request.headers.get("x-forwarded-proto", request.url.scheme)
                new_location = urlunparse((scheme, original_host, parsed.path, parsed.params, parsed.query, parsed.fragment))
                response.headers["location"] = new_location
    return response


@app.on_event("startup")
async def startup_event():
    """应用启动事件"""
    logger.info("📦 Loading modules and routes...")
    logger.info("✅ FastAPI application started successfully")
    logger.info(f"📊 Registered {len(app.routes)} routes")

    # Windows: 启动前清理上次可能残留的进程
    if os.name == "nt":
        logger.info("🧹 Windows 启动前检查残留进程...")
        _cleanup_orphan_python_processes()

    # 先于持久化消费者复位「运行中」标志（单线程 + 短时直连 SQLite），避免与 writer 争抢连接时出现 ~busy_timeout 级卡顿
    from infrastructure.persistence.database.write_dispatch import startup_sqlite_writes_bypass_queue

    with startup_sqlite_writes_bypass_queue():
        _stop_all_running_novels()

    _bootstrap_persistence_consumer_early()

    # AOF 崩溃恢复：扫描残留的 .draft 文件，恢复到 DB
    _recover_drafts_on_startup()

    # 启动自动驾驶守护进程（后台线程）
    _start_autopilot_daemon_thread()

    # 初始化 DAG 节点注册表（加载所有 V1 节点实现）
    _init_dag_node_registry()

def _bootstrap_persistence_consumer_early() -> None:
    """启动 mp.Queue + 处理器 + 消费者线程（与 daemon 共用同一队列单例）。

    须在 AOF 恢复等依赖「非 writer 线程 mutate → 持久化队列」的逻辑之前调用。

    「运行中小说→stopped」已在 `startup_sqlite_writes_bypass_queue` 中与消费者拉起解耦并完成。
    """
    try:
        from application.engine.services.persistence_queue import (
            get_persistence_queue,
            initialize_persistence_queue,
            register_persistence_handlers,
        )

        initialize_persistence_queue()
        register_persistence_handlers()
        get_persistence_queue().start_consumer()
        logger.info("✅ 持久化消费者已先于启动钩子就绪（单写者内核）")
    except Exception as e:
        logger.warning("持久化队列提前初始化失败（部分启动写将依赖直连兜底）: %s", e)


def _checkpoint_sqlite_wal_safe() -> None:
    """桌面端优雅退出时尽量将 WAL 落盘，降低异常断电时的损坏概率。"""
    try:
        import sqlite3

        from application.paths import get_db_path

        dbp = get_db_path()
        conn = sqlite3.connect(dbp, timeout=2.0)  # 减少超时
        try:
            conn.execute("PRAGMA journal_mode=WAL")       # 确保与主连接一致
            conn.execute("PRAGMA busy_timeout=2000")      # 最多等 2 秒拿锁
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        finally:
            conn.close()
    except Exception as e:
        logger.warning("WAL checkpoint 失败（可忽略）: %s", e)


def _run_backend_shutdown_hooks() -> None:
    """与 shutdown 生命周期钩子共用：守护进程停止 + DB 连接关闭 + WAL + 日志。"""
    _start_force_exit_watchdog()  # 启动看门狗，防止关闭流程卡死
    _stop_autopilot_daemon_thread()
    # 关闭所有数据库连接（跳过 WAL checkpoint，避免锁等待卡死）
    try:
        from infrastructure.persistence.database.connection import get_database
        db = get_database()
        db.close_all(skip_checkpoint=True)
    except Exception as e:
        logger.warning("关闭数据库连接失败: %s", e)
    _checkpoint_sqlite_wal_safe()

    # 关闭 LLM Provider HTTP 连接池
    try:
        import asyncio
        from interfaces.api.dependencies import get_llm_service
        svc = get_llm_service()
        if hasattr(svc, 'aclose'):
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    loop.create_task(svc.aclose())
                else:
                    loop.run_until_complete(svc.aclose())
            except RuntimeError:
                pass
    except Exception:
        pass

    uptime = time.time() - STARTUP_TIME
    logger.info("=" * 80)
    logger.info("\U0001f6d1 BACKEND SHUTTING DOWN")
    logger.info("   Total uptime: %.2f seconds (%.2f hours)", uptime, uptime / 3600)
    logger.info("=" * 80)


@app.on_event("shutdown")
async def shutdown_event():
    """应用关闭事件（uvicorn 优雅退出时触发；Windows 桌面专用路径见 /internal/shutdown）。"""
    _run_backend_shutdown_hooks()


def _assert_internal_shutdown_localhost(request: Request) -> None:
    if not request.client:
        raise HTTPException(status_code=403, detail="forbidden")
    host = request.client.host or ""
    if host not in ("127.0.0.1", "::1", "::ffff:127.0.0.1"):
        raise HTTPException(status_code=403, detail="forbidden")


def _internal_shutdown_after_response() -> None:
    """HTTP 响应已发出后再触发进程级退出，避免截断响应体。"""
    time.sleep(0.15)  # 让 HTTP 响应先发出去
    if os.name == "nt":
        # Windows: 必须在 os._exit 之前确保守护子进程被终止
        # os._exit(0) 不会触发 Python 的正常清理流程，
        # multiprocessing.Process 的 daemon 子进程可能变成孤儿
        _stop_autopilot_daemon_thread()
        # 关闭数据库连接（跳过 checkpoint，避免锁等待卡死）
        try:
            from infrastructure.persistence.database.connection import get_database
            db = get_database()
            db.close_all(skip_checkpoint=True)
        except Exception as e:
            logger.warning("关闭数据库连接失败: %s", e)
        _checkpoint_sqlite_wal_safe()

        uptime = time.time() - STARTUP_TIME
        logger.info("=" * 80)
        logger.info("\U0001f6d1 BACKEND SHUTTING DOWN (Windows forced exit)")
        logger.info("   Total uptime: %.2f seconds (%.2f hours)", uptime, uptime / 3600)
        logger.info("=" * 80)
        logging.shutdown()
        os._exit(0)
    os.kill(os.getpid(), signal.SIGINT)


@app.post("/internal/shutdown", include_in_schema=False)
async def internal_shutdown(request: Request):
    """仅本机：供 Tauri 在关闭窗口前触发优雅停机（Unix 走 SIGINT→uvicorn；Windows 走钩子+_exit）。"""
    _assert_internal_shutdown_localhost(request)
    threading.Thread(target=_internal_shutdown_after_response, daemon=True).start()
    return {"ok": True, "message": "shutting down"}

# 配置 CORS
# 前后端同端口部署：前端是同源请求，默认允许所有源。
# 开发环境可通过 CORS_ORIGINS 环境变量限制。
_cors_origins_env = os.getenv("CORS_ORIGINS", "")
if _cors_origins_env:
    _allowed_origins = [o.strip() for o in _cors_origins_env.split(",") if o.strip()]
else:
    _allowed_origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_allowed_origins,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 注册统一错误处理器（捕获未处理异常并记录日志）
from interfaces.api.middleware.error_handler import add_error_handlers
add_error_handlers(app)

# HTTP 访问日志由 uvicorn.access 输出（与 uvicorn 默认格式一致：IP + 请求行 + 状态码）

# ════════════════════════════════════════════════════════════════════════════
# 路由注册
#
# 约定：
#   1. 所有 API 路由统一使用 /api/v1 前缀，在 include_router 处一次性注入
#   2. 各 router 模块内部只声明语义路径（如 /novels、/bible），不得硬编码 /api/v1
#   3. 前端 apiClient.baseURL = '/api/v1'，调用时使用相对路径（如 /novels/{id}）
# ════════════════════════════════════════════════════════════════════════════

_V1 = "/api/v1"

# ── Core：小说 / 章节 / 导出 / 设置 ──
app.include_router(novels.router,                   prefix=_V1)
app.include_router(taxonomy_routes.router,          prefix=_V1)
app.include_router(chapters.router,                 prefix="/api/v1/novels")  # chapters 路由无自身 prefix，挂在 /novels 下
app.include_router(manuscript_entity_routes.router, prefix="/api/v1/novels")
app.include_router(export.router,                   prefix=_V1)
app.include_router(llm_settings.router,             prefix=_V1)
app.include_router(llm_settings.embedding_router,   prefix=_V1)
app.include_router(scene_generation_routes.router,  prefix=_V1)  # /scenes

# ── World：世界观 / 人物谱 / 知识库 / 知识图谱 ──
app.include_router(bible.router,                    prefix=_V1)  # /bible
app.include_router(cast.router,                     prefix=_V1)  # 无 prefix，路由自身含 /novels/{id}/cast
app.include_router(knowledge.router,                prefix=_V1)  # /novels/{id}/knowledge
app.include_router(knowledge_graph_routes.router,   prefix=_V1)  # /knowledge-graph
app.include_router(worldbuilding_routes.router,     prefix=_V1)  # /novels/{id}/worldbuilding

# ── Blueprint：规划 / 节拍表 / 故事结构 ──
app.include_router(continuous_planning_routes.router, prefix=_V1)  # /planning
app.include_router(beat_sheet_routes.router,          prefix=_V1)  # /beat-sheets
app.include_router(story_structure.router,             prefix=_V1)  # 无 prefix，路由自身含 /novels/{id}/structure
app.include_router(confluence_router,                  prefix=_V1)  # /novels/{id}/confluence-points

# ── Engine：生成 / 上下文 / 编年史 / 快照 / 自动驾驶 / 工作台 / 角色调度 / 检查点 ──
app.include_router(generation.router,                     prefix=_V1)
app.include_router(context_intelligence.router,           prefix=_V1)
app.include_router(chronicles.router,                     prefix=_V1)
app.include_router(snapshot_routes.router,                prefix=_V1)
app.include_router(autopilot_routes.router,               prefix=_V1)  # /autopilot
app.include_router(workbench_context_routes.router,       prefix=_V1)
app.include_router(character_scheduler_routes.router,     prefix=_V1)  # /character-scheduler
app.include_router(checkpoint_routes.router,              prefix=_V1)  # Checkpoint + QualityGuardrail + StoryPhase + CharacterPsyche
app.include_router(narrative_engine_routes.router,          prefix=_V1)
app.include_router(narrative_engine_routes.surface_router, prefix=_V1)  # 叙事引擎 read model（故事演进 / 角色声线）
app.include_router(worldline_routes.router,                prefix=_V1)  # 世界线管理（故事 Git 模型）
app.include_router(prop_routes.router,                     prefix=_V1)  # 道具全周期管理

# ── Engine：溯源 / DAG 工作流 ──
from interfaces.api.v1.engine.trace_routes import router as trace_router
app.include_router(trace_router,                          prefix=_V1)

from interfaces.api.v1.engine.dag.dag_routes import router as dag_router
app.include_router(dag_router,                            prefix=_V1)  # /dag

# ── Audit：审稿 / 宏观重构 / 章节元素 ──
app.include_router(chapter_review_routes.router,          prefix=_V1)  # /chapter-reviews
app.include_router(macro_refactor.router,                 prefix=_V1)
app.include_router(chapter_element_routes.router,         prefix=_V1)  # /chapters (元素关联)

# ── Analyst：文风 / 叙事状态 / 伏笔 ──
app.include_router(voice.router,                          prefix=_V1)
app.include_router(narrative_state.router,                prefix=_V1)
app.include_router(foreshadow_ledger.router,              prefix=_V1)

# ── System：内部工具（不暴露到 OpenAPI 文档）──
app.include_router(system_routes.router,                  prefix=_V1)

# ── Reader Simulation：读者模拟 ──
app.include_router(reader_module.router,                  prefix=_V1)  # /reader

# ── Workbench：写作工具 ──
app.include_router(writer_block.router,                   prefix=_V1)
app.include_router(sandbox.router,                        prefix=_V1)
app.include_router(monitor.router,                        prefix=_V1)
app.include_router(llm_control.router,                    prefix=_V1)  # /llm-control

# ── Anti-AI：防御系统 ──
from interfaces.api.v1 import anti_ai as anti_ai_routes
app.include_router(anti_ai_routes.router,                 prefix=_V1)  # /anti-ai

# ── Stats：统计（独立前缀 /api/stats，不走 /api/v1）──
stats_repository = SqliteStatsRepositoryAdapter(get_database())
stats_service = StatsService(stats_repository)
stats_router = create_stats_router(stats_service)
app.include_router(stats_router, prefix="/api/stats", tags=["statistics"])


@app.get("/")
async def root():
    """根路径 — 返回前端页面（SPA）或 API 欢迎消息"""
    if _FRONTEND_DIR.exists() and _INDEX_HTML.exists():
        return FileResponse(str(_INDEX_HTML), media_type="text/html")
    return {"message": "PlotPilot API", "release": APP_RELEASE_VERSION}


@app.get("/health")
async def health_check():
    """健康检查

    Returns:
        健康状态
    """
    uptime = time.time() - STARTUP_TIME
    _dp = _dl._daemon_process
    daemon_alive = _dp is not None and _dp.is_alive()
    return {
        "status": "healthy",
        "version": APP_RELEASE_VERSION,
        "build_id": BACKEND_BUILD_ID,
        "uptime_seconds": round(uptime, 2),
        "daemon_process": {
            "running": daemon_alive,
            "pid": _dp.pid if _dp else None
        }
    }


# ── SPA fallback：前端路由兜底（必须在 API 路由之后注册）──
if _FRONTEND_DIR.exists() and _INDEX_HTML.exists():
    @app.get("/{full_path:path}", include_in_schema=False)
    @app.post("/{full_path:path}", include_in_schema=False)
    @app.put("/{full_path:path}", include_in_schema=False)
    @app.patch("/{full_path:path}", include_in_schema=False)
    @app.delete("/{full_path:path}", include_in_schema=False)
    async def spa_fallback(full_path: str, req: Request):
        """SPA fallback — 所有未匹配的路径返回 index.html"""
        # 排除 API 路径、统计路由和静态资源
        if (full_path.startswith("api/") or full_path.startswith("stats/")
                or full_path.startswith("assets/") or full_path.startswith("_")):
            # 对无尾部斜杠的 API 路径做 307 重定向到带斜杠版本
            if not full_path.endswith('/'):
                redirect_url = req.url.path + '/'
                if req.url.query:
                    redirect_url += '?' + req.url.query
                return RedirectResponse(url=redirect_url, status_code=307)
            return JSONResponse({"error": "Not Found"}, status_code=404)
        return FileResponse(str(_INDEX_HTML), media_type="text/html")


# ── Windows CTRL+C 防卡死：看门狗线程 + atexit 双保险 ──
_shutdown_deadline: float | None = None


def _force_exit_watchdog() -> None:
    """看门狗线程：监控关闭流程，超时后强制 os._exit(0)。

    问题背景：
    - uvicorn 收到 SIGINT 后走优雅关闭（shutdown event → close_all → WAL checkpoint）
    - 但 Windows 下守护进程可能持有 DB 写锁，close_all 的 checkpoint 会无限等待
    - Intel Fortran runtime 也会拦截 CTRL+C（forrtl: error 200）
    - multiprocessing 子进程可能在 uvicorn join 时卡住

    解决方案：
    - 在 shutdown event 触发时启动看门狗，给优雅关闭 8 秒时间
    - 超时后直接 os._exit(0)，确保进程能退出
    """
    global _shutdown_deadline
    if _shutdown_deadline is None:
        return
    # 等待优雅关闭完成或超时
    while time.time() < _shutdown_deadline:
        time.sleep(0.5)
    # 超时，强制退出
    logger.warning("看门狗：优雅关闭超时（8s），强制退出")
    logging.shutdown()
    os._exit(0)


def _start_force_exit_watchdog() -> None:
    """在关闭流程开始时启动看门狗线程。"""
    global _shutdown_deadline
    _shutdown_deadline = time.time() + 8.0
    t = threading.Thread(target=_force_exit_watchdog, daemon=True)
    t.start()


# atexit 钩子：确保无论哪种退出路径都能清理
import atexit as _atexit


def _atexit_shutdown_guard() -> None:
    """atexit 钩子：在 Python 正常退出时确保进程能终止。

    如果 uvicorn 的 shutdown event 已经处理了清理，这里什么也不做。
    如果是 os._exit 之外的退出路径（如 sys.exit），看门狗确保不会卡住。
    """
    _start_force_exit_watchdog()


_atexit.register(_atexit_shutdown_guard)


if os.name == "nt":
    # Windows: 注册 SIGBREAK 处理器（CTRL+BREAK 比 CTRL+C 更不容易被拦截）
    try:
        def _sigbreak_handler(signum, frame):
            logger.info("收到 SIGBREAK 信号，强制退出")
            _stop_autopilot_daemon_thread()
            logging.shutdown()
            os._exit(0)

        signal.signal(signal.SIGBREAK, _sigbreak_handler)
    except (OSError, ValueError, AttributeError):
        pass  # SIGBREAK 仅 Windows，非主线程也无法注册


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
