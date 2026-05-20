"""Alembic 升级入口 — 替代后续手写编号 SQL（P2-10）。"""
from __future__ import annotations

import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

_ALEMBIC_INI = Path(__file__).resolve().parents[3] / "alembic.ini"


def run_alembic_upgrade(db_path: str | None = None) -> None:
    """对指定 SQLite 执行 ``alembic upgrade head``。"""
    if db_path:
        os.environ["PLOTPILOT_DATABASE_URL"] = f"sqlite:///{db_path}"

    try:
        from alembic import command
        from alembic.config import Config
    except ImportError:
        logger.debug("Alembic 未安装，跳过在线迁移")
        return

    if not _ALEMBIC_INI.is_file():
        logger.warning("未找到 alembic.ini: %s", _ALEMBIC_INI)
        return

    cfg = Config(str(_ALEMBIC_INI))
    command.upgrade(cfg, "head")
    logger.info("Alembic upgrade head 完成")
