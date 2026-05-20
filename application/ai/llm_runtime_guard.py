"""LLM 运行时门禁 — 未配置可用模型时拒绝 AI 写操作。"""
from __future__ import annotations

from typing import Any, Optional

from fastapi import HTTPException


def require_llm_configured(control_service: Optional[Any] = None) -> None:
    """激活档案缺少 API Key 或模型名时抛出 503（前端据此弹窗引导配置）。"""
    if control_service is None:
        from interfaces.api.dependencies import get_llm_control_service

        control_service = get_llm_control_service()
    runtime = control_service.get_runtime_summary()
    if runtime.using_mock:
        raise HTTPException(
            status_code=503,
            detail={
                "code": "LLM_NOT_CONFIGURED",
                "message": runtime.reason
                or "未配置可用的 API Key 或模型名，请先在设置中完成模型引擎配置",
            },
        )
