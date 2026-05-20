"""SQLite 访问统一入口 — 禁止业务代码直接 new Connection。"""
from infrastructure.persistence.database.connection import (
    DatabaseConnection,
    get_connection_pool,
    get_database,
)

__all__ = ["DatabaseConnection", "get_database", "get_connection_pool"]
