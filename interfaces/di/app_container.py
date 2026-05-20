"""应用 DI 容器（轻量）— 启动时显式组装，替代散落的全局单例。"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional


@dataclass
class AppContainer:
    """显式依赖注册表。"""

    _singletons: Dict[str, Any] = field(default_factory=dict)
    _factories: Dict[str, Callable[[], Any]] = field(default_factory=dict)

    def register_factory(self, name: str, factory: Callable[[], Any]) -> None:
        self._factories[name] = factory

    def register_singleton(self, name: str, instance: Any) -> None:
        self._singletons[name] = instance

    def resolve(self, name: str) -> Any:
        if name in self._singletons:
            return self._singletons[name]
        if name in self._factories:
            inst = self._factories[name]()
            self._singletons[name] = inst
            return inst
        raise KeyError(f"未注册依赖: {name}")


_container: Optional[AppContainer] = None


def get_container() -> AppContainer:
    global _container
    if _container is None:
        _container = AppContainer()
        _bootstrap(_container)
    return _container


def _bootstrap(c: AppContainer) -> None:
    from infrastructure.persistence.database.db_gateway import get_database

    c.register_factory("database", get_database)
