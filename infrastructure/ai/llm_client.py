"""LLM 客户端包装器"""
from typing import AsyncIterator, Optional

from domain.ai.services.llm_service import GenerationConfig
from domain.ai.value_objects.prompt import Prompt
from infrastructure.ai.provider_factory import DynamicLLMService

_DEFAULT_SYSTEM = "你是一个专业的小说创作助手。"


class LLMClient:
    """LLM 客户端包装器，自动选择当前激活的提供者。"""

    def __init__(self, provider=None):
        self.provider = provider or DynamicLLMService()

    def _build_config(self, **kwargs) -> GenerationConfig:
        settings = getattr(self.provider, "settings", None)
        return GenerationConfig(
            model=kwargs.get("model", getattr(settings, "default_model", None)),
            max_tokens=kwargs.get("max_tokens", getattr(settings, "default_max_tokens", 4096)),
            temperature=kwargs.get("temperature", getattr(settings, "default_temperature", 1.0)),
        )

    def _prompt_from_string(self, user: str, system: Optional[str] = None) -> Prompt:
        return Prompt(system=system or _DEFAULT_SYSTEM, user=user)

    async def generate(
        self,
        prompt: str,
        *,
        system: Optional[str] = None,
        **kwargs,
    ) -> str:
        """生成文本。传入完整 Prompt 对象时请使用 provider.generate 直接调用。"""
        prompt_obj = self._prompt_from_string(prompt, system=system)
        config = self._build_config(**kwargs)
        result = await self.provider.generate(prompt_obj, config)
        return result.content

    async def stream_generate(
        self,
        prompt,
        config=None,
        *,
        system: Optional[str] = None,
        **kwargs,
    ) -> AsyncIterator[str]:
        """流式生成，代理到底层 provider"""
        if isinstance(prompt, str):
            prompt_obj = self._prompt_from_string(prompt, system=system)
        else:
            prompt_obj = prompt

        if config is None:
            config = self._build_config(**kwargs)

        async for chunk in self.provider.stream_generate(prompt_obj, config):
            yield chunk
