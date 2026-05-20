"""一次性脚本：拆分 autopilot_routes.py 为子模块。"""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
src_path = ROOT / "interfaces/api/v1/engine/autopilot_routes.py"
lines = src_path.read_text(encoding="utf-8").splitlines(keepends=True)

shared = "".join(lines[:1138])
control = "".join(lines[1138:1555])
streams = "".join(lines[1555:2329])
system = "".join(lines[2329:])

base = ROOT / "interfaces/api/v1/engine/autopilot"
base.mkdir(exist_ok=True)

(base / "shared.py").write_text(
    '"""Autopilot 路由共享状态与构建逻辑。"""\n' + shared.lstrip(),
    encoding="utf-8",
)

imp = "from interfaces.api.v1.engine.autopilot.shared import *  # noqa: F403,F401\n\n"

for name, chunk in [("control", control), ("streams", streams), ("system", system)]:
    body = chunk.replace(
        'router = APIRouter(prefix="/autopilot", tags=["autopilot"])\n\n', ""
    )
    text = (
        f'"""Autopilot {name} 路由。"""\n'
        "from fastapi import APIRouter\n"
        f"{imp}"
        "router = APIRouter()\n"
        + body
    )
    (base / f"{name}.py").write_text(text, encoding="utf-8")

agg = '''"""Autopilot API 聚合路由。"""
from fastapi import APIRouter

from interfaces.api.v1.engine.autopilot.control import router as control_router
from interfaces.api.v1.engine.autopilot.streams import router as streams_router
from interfaces.api.v1.engine.autopilot.system import router as system_router

router = APIRouter(prefix="/autopilot", tags=["autopilot"])
router.include_router(control_router)
router.include_router(streams_router)
router.include_router(system_router)
'''
(base / "__init__.py").write_text(agg, encoding="utf-8")

new_main = '''"""自动驾驶控制 API（v2：含审阅确认 + SSE 生成流）

实现已拆分至 interfaces.api.v1.engine.autopilot 包。
"""
from interfaces.api.v1.engine.autopilot import router

# 兼容：部分测试直接 import 本模块内的 helper
from interfaces.api.v1.engine.autopilot.shared import (  # noqa: F401
    resolve_autopilot_current_chapter_number,
)
'''
src_path.write_text(new_main, encoding="utf-8")
print("split ok", base)
