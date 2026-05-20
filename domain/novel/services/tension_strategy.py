"""剧情弧张力策略 — PlotArc 硬编码规则外提（P2-8）。"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict, List


class TensionStrategy(ABC):
    @abstractmethod
    def phase_tension_curve(self, phase_index: int, total_phases: int) -> Dict[str, float]:
        ...


class DefaultTensionStrategy(TensionStrategy):
    """默认四阶段张力曲线。"""

    def phase_tension_curve(self, phase_index: int, total_phases: int) -> Dict[str, float]:
        base = 40.0 + (phase_index / max(total_phases - 1, 1)) * 50.0
        return {
            "tension_base": base,
            "tension_peak": min(100.0, base + 20.0),
            "daily_ratio": max(0.0, 0.3 - phase_index * 0.08),
        }


def get_default_tension_strategy() -> TensionStrategy:
    return DefaultTensionStrategy()
