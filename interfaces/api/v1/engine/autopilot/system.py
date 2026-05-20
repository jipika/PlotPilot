"""Autopilot system 路由。"""
from fastapi import APIRouter
from interfaces.api.v1.engine.autopilot.shared import *  # noqa: F403,F401

router = APIRouter()
@router.get("/system/resources")
async def get_system_resources():
    """获取系统资源状态（线程池、缓存、队列等）"""
    return _rm.health_check()


@router.get("/system/cache/stats")
async def get_cache_stats():
    """获取缓存统计信息"""
    return _SHARED_STATE_CACHE.get_stats()


@router.post("/system/cache/cleanup")
async def cleanup_cache():
    """手动清理过期缓存"""
    cleaned = _SHARED_STATE_CACHE.cleanup_expired()
    return {"cleaned": cleaned}


@router.post("/system/resources/cleanup-idle")
async def cleanup_idle_resources(idle_seconds: float = 300):
    """清理空闲资源"""
    cleaned = _rm.cleanup_idle(idle_seconds)
    return {"cleaned": cleaned}


@router.get("/debug/thread-pool")
async def debug_thread_pool():
    """调试：线程池状态"""
    import threading
    executor = _SSE_THREAD_POOL._resource if hasattr(_SSE_THREAD_POOL, '_resource') else _SSE_THREAD_POOL
    return {
        "thread_pool_type": type(executor).__name__,
        "max_workers": getattr(executor, '_max_workers', 'unknown'),
        "threads_count": len([t for t in threading.enumerate() if 'sse-io' in t.name]),
        "all_threads": [{"name": t.name, "alive": t.is_alive()} for t in threading.enumerate()],
    }


@router.get("/debug/shared-state")
async def debug_shared_state(novel_id: str = None):
    """调试：共享内存状态"""
    from interfaces.main import _get_shared_state
    import multiprocessing as mp

    try:
        state = _get_shared_state()
        keys = list(state.keys()) if state else []
        result = {
            "state_available": state is not None,
            "keys_count": len(keys),
            "keys": keys[:20],  # 只显示前20个
            "process_name": mp.current_process().name,
        }

        # 如果指定了 novel_id，显示详细信息
        if novel_id:
            key = f"novel:{novel_id}"
            novel_state = dict(state.get(key, {}))
            result["novel_state"] = novel_state
            result["novel_updated_at"] = novel_state.get("_updated_at")
            result["novel_age_seconds"] = time.time() - novel_state.get("_updated_at", 0) if novel_state.get("_updated_at") else None

        # 守护进程心跳
        daemon_heartbeat = state.get("_daemon_heartbeat")
        result["daemon_heartbeat"] = daemon_heartbeat
        result["daemon_heartbeat_age"] = time.time() - daemon_heartbeat if daemon_heartbeat else None
        result["daemon_alive"] = (time.time() - daemon_heartbeat) < 60.0 if daemon_heartbeat else False

        return result
    except Exception as e:
        return {"error": str(e)}


@router.get("/debug/db-lock")
async def debug_db_lock():
    """调试：检查 DB 锁状态"""
    import sqlite3
    from application.paths import get_db_path
    from pathlib import Path

    db_path = get_db_path()
    db_path_obj = Path(db_path) if isinstance(db_path, str) else db_path

    result = {
        "db_path": str(db_path_obj),
        "db_exists": db_path_obj.exists(),
        "wal_exists": db_path_obj.with_suffix('.db-wal').exists(),
        "shm_exists": db_path_obj.with_suffix('.db-shm').exists(),
    }

    # 尝试获取锁（带超时）
    if db_path_obj.exists():
        try:
            conn = sqlite3.connect(str(db_path_obj), timeout=0.5)
            conn.execute("BEGIN IMMEDIATE")
            conn.commit()
            conn.close()
            result["lock_test"] = "success"
        except sqlite3.OperationalError as e:
            result["lock_test"] = f"locked: {e}"
        except Exception as e:
            result["lock_test"] = f"error: {e}"

    # 检查是否有 -journal 文件（回滚日志）
    journal_path = db_path_obj.with_suffix('.db-journal')
    result["journal_exists"] = journal_path.exists()

    return result


@router.get("/debug/all")
async def debug_all(novel_id: str = None):
    """调试：综合诊断"""
    import threading
    import sqlite3
    from interfaces.main import _get_shared_state
    from application.paths import get_db_path
    from pathlib import Path

    # 线程池状态
    executor = _SSE_THREAD_POOL._resource if hasattr(_SSE_THREAD_POOL, '_resource') else _SSE_THREAD_POOL
    thread_info = {
        "max_workers": getattr(executor, '_max_workers', 'unknown'),
        "sse_threads": len([t for t in threading.enumerate() if 'sse-io' in t.name]),
        "total_threads": threading.active_count(),
    }

    # 共享内存状态
    try:
        state = _get_shared_state()
        shared_info = {
            "available": state is not None,
            "keys": list(state.keys())[:10] if state else [],
        }
        daemon_heartbeat = state.get("_daemon_heartbeat") if state else None
        shared_info["daemon_alive"] = (time.time() - daemon_heartbeat) < 60.0 if daemon_heartbeat else False
        shared_info["daemon_heartbeat_age"] = time.time() - daemon_heartbeat if daemon_heartbeat else None
    except Exception as e:
        shared_info = {"error": str(e)}

    # DB 状态
    db_path_obj = Path(get_db_path())
    db_info = {
        "exists": db_path_obj.exists(),
        "wal_exists": db_path_obj.with_suffix('.db-wal').exists(),
    }
    if db_path_obj.exists():
        try:
            conn = sqlite3.connect(str(db_path_obj), timeout=0.5)
            conn.execute("SELECT 1 FROM novels LIMIT 1")
            conn.close()
            db_info["accessible"] = True
        except Exception as e:
            db_info["accessible"] = False
            db_info["error"] = str(e)

    # 指定小说状态
    novel_info = None
    if novel_id:
        try:
            shared = _get_shared_state_for_novel_cached(novel_id)
            if shared:
                novel_info = {
                    "in_shared_memory": True,
                    "updated_at": shared.get("_updated_at"),
                    "age_seconds": time.time() - shared.get("_updated_at", 0) if shared.get("_updated_at") else None,
                    "cached_chapters": shared.get("_cached_completed_chapters"),
                    "stage": shared.get("current_stage"),
                    "status": shared.get("autopilot_status"),
                    "beat_index": shared.get("current_beat_index"),
                }
            else:
                novel_info = {"in_shared_memory": False}
        except Exception as e:
            novel_info = {"error": str(e)}

    return {
        "timestamp": time.time(),
        "thread_pool": thread_info,
        "shared_memory": shared_info,
        "database": db_info,
        "novel": novel_info,
        "cache_stats": _SHARED_STATE_CACHE.get_stats(),
    }

