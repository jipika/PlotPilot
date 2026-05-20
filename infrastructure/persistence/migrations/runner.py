"""迁移运行器 — 遗留 SQL 目录 + Alembic head（见 connection.py）。"""
from __future__ import annotations

import logging
from pathlib import Path

from infrastructure.persistence.migrations.alembic_runner import run_alembic_upgrade

logger = logging.getLogger(__name__)


def run_pending_migrations(db_path: str | None = None) -> None:
    """应用 Alembic 链上尚未执行的版本（新迁移请 ``alembic revision``）。"""
    try:
        run_alembic_upgrade(db_path)
    except Exception as exc:
        logger.warning("Alembic 迁移未执行: %s", exc)
