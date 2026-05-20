"""兼容层 — LLM 契约见 engine.core.ports.ai_contracts。"""
from engine.core.ports.ai_contracts import (
    GenerationConfig,
    GenerationResult,
    LLMService,
)

__all__ = ["GenerationConfig", "GenerationResult", "LLMService"]
