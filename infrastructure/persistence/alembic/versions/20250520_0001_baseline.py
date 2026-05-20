"""基线：历史 schema 由 schema.sql + migrations/*.sql 已应用，此版本仅登记 Alembic 链头。

Revision ID: 20250520_0001
Revises:
Create Date: 2025-05-20
"""
from __future__ import annotations

revision = "20250520_0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
