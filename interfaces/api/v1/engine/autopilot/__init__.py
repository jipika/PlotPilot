"""Autopilot API 聚合路由。"""
from fastapi import APIRouter

from interfaces.api.v1.engine.autopilot.control import router as control_router
from interfaces.api.v1.engine.autopilot.streams import router as streams_router
from interfaces.api.v1.engine.autopilot.system import router as system_router

router = APIRouter(prefix="/autopilot", tags=["autopilot"])
router.include_router(control_router)
router.include_router(streams_router)
router.include_router(system_router)
