"""兼容层：engine.application → engine.runtime

旧代码 `from engine.application...` 仍然可用。
新代码请使用 `from engine.runtime...`。

canonical 实现在 engine.runtime；本包仅 re-export。
"""

from engine.runtime.quality_guardrails.language_style_guardrail import LanguageStyleGuardrail
from engine.runtime.quality_guardrails.character_consistency_guardrail import (
    CharacterConsistencyGuardrail,
)
from engine.runtime.quality_guardrails.plot_density_guardrail import PlotDensityGuardrail
from engine.runtime.quality_guardrails.naming_guardrail import NamingGuardrail
from engine.runtime.quality_guardrails.viewpoint_guardrail import ViewpointGuardrail
from engine.runtime.quality_guardrails.rhythm_guardrail import RhythmGuardrail
from engine.runtime.quality_guardrails.quality_guardrail import (
    QualityGuardrail,
    QualityViolationError,
)
from engine.runtime.plot_state_machine.state_machine import PlotStateMachine
from engine.runtime.checkpoint_manager.manager import CheckpointManager
