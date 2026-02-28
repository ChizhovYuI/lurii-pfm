"""Provider registry internals (avoids circular import with __init__)."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pfm.ai.base import ProviderName

if TYPE_CHECKING:
    from pfm.ai.base import LLMProvider

# Registry: provider name -> provider class
PROVIDER_REGISTRY: dict[ProviderName, type[LLMProvider]] = {}


def register_provider(cls: type[LLMProvider]) -> type[LLMProvider]:
    """Decorator to register a provider class by its ``name`` attribute."""
    name = getattr(cls, "name", None)
    if name is None or not name:
        msg = f"Provider class {cls.__name__} must define a 'name' class attribute."
        raise ValueError(msg)
    PROVIDER_REGISTRY[ProviderName(name)] = cls
    return cls


def get_provider_names() -> list[str]:
    """Return sorted list of registered provider names."""
    return sorted(str(n) for n in PROVIDER_REGISTRY)
