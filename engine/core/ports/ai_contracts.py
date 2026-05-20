"""应用层 AI 契约 — 原 domain.ai 收敛至此（引擎端口层）。

叙事生成、嵌入、向量检索等技术能力端口；与 engine.core.ports.LLMPort（内核精简口）并存。
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, AsyncIterator, Dict, List, Optional


# ─── 值对象 ───


@dataclass(frozen=True)
class Prompt:
    """提示词值对象"""
    system: str
    user: str

    def __post_init__(self) -> None:
        if not self.user or not self.user.strip():
            raise ValueError("User message cannot be empty")
        if not self.system or not self.system.strip():
            raise ValueError("System message cannot be empty")

    def to_messages(self) -> List[Dict[str, Any]]:
        messages: List[Dict[str, Any]] = []
        if self.system:
            messages.append({"role": "system", "content": self.system})
        messages.append({"role": "user", "content": self.user})
        return messages


@dataclass(frozen=True)
class TokenUsage:
    """Token 使用量值对象"""
    input_tokens: int
    output_tokens: int

    def __post_init__(self) -> None:
        if self.input_tokens < 0 or self.output_tokens < 0:
            raise ValueError("Token counts cannot be negative")

    @property
    def total_tokens(self) -> int:
        return self.input_tokens + self.output_tokens

    def __add__(self, other: TokenUsage) -> TokenUsage:
        return TokenUsage(
            input_tokens=self.input_tokens + other.input_tokens,
            output_tokens=self.output_tokens + other.output_tokens,
        )


class GenerationConfig:
    """生成配置"""

    def __init__(
        self,
        model: str = "",
        max_tokens: int = 4096,
        temperature: float = 1.0,
        response_format: Optional[Dict] = None,
    ):
        self.model = model
        self.max_tokens = max_tokens
        self.temperature = temperature
        self.response_format = response_format
        self.__post_init__()

    def __post_init__(self) -> None:
        if not (0.0 <= self.temperature <= 2.0):
            raise ValueError("Temperature must be between 0.0 and 2.0")
        if self.max_tokens <= 0:
            raise ValueError("max_tokens must be greater than 0")


class GenerationResult:
    """生成结果"""

    def __init__(self, content: str, token_usage: TokenUsage):
        self.content = content
        self.token_usage = token_usage
        self.__post_init__()

    def __post_init__(self) -> None:
        if not self.content or not self.content.strip():
            raise ValueError("Content cannot be empty")


# ─── 服务端口 ───


class LLMService(ABC):
    """LLM 服务端口（应用层）"""

    @abstractmethod
    async def generate(self, prompt: Prompt, config: GenerationConfig) -> GenerationResult:
        ...

    @abstractmethod
    async def stream_generate(
        self, prompt: Prompt, config: GenerationConfig
    ) -> AsyncIterator[str]:
        ...


class EmbeddingService(ABC):
    """嵌入服务端口"""

    @abstractmethod
    async def embed(self, text: str) -> List[float]:
        ...

    @abstractmethod
    async def embed_batch(self, texts: List[str]) -> List[List[float]]:
        ...


class VectorStore(ABC):
    """向量存储端口"""

    @abstractmethod
    async def insert(
        self, collection: str, id: str, vector: List[float], payload: dict
    ) -> None:
        ...

    @abstractmethod
    async def search(
        self, collection: str, query_vector: List[float], limit: int
    ) -> List[dict]:
        ...

    @abstractmethod
    async def delete(self, collection: str, id: str) -> None:
        ...

    @abstractmethod
    async def create_collection(self, collection: str, dimension: int) -> None:
        ...

    @abstractmethod
    async def delete_collection(self, collection: str) -> None:
        ...

    @abstractmethod
    async def list_collections(self) -> List[str]:
        ...


class ChapterSummarizer(ABC):
    """章节摘要端口"""

    @abstractmethod
    async def summarize(self, content: str, max_length: int = 300) -> str:
        ...
