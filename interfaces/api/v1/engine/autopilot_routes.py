"""自动驾驶控制 API（v2：含审阅确认 + SSE 生成流）

实现已拆分至 interfaces.api.v1.engine.autopilot 包。
"""
from interfaces.api.v1.engine.autopilot import router

# 兼容：部分测试直接 import 本模块内的 helper
from interfaces.api.v1.engine.autopilot.shared import (  # noqa: F401
    resolve_autopilot_current_chapter_number,
)
