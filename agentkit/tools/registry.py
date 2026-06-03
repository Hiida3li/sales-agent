"""Tool registry.

A small lookup that lets the agent loop enumerate declarations/allowed names
and lets the generic tool service resolve a tool by name. The module exposes a
``default_registry`` populated with the built-in tools; new tools become
available everywhere simply by registering them here.
"""

from __future__ import annotations

from typing import Any, Dict, List

from agentkit.tools.base import Tool


class ToolRegistry:
    def __init__(self) -> None:
        self._tools: Dict[str, Tool] = {}

    def register(self, tool: Tool) -> "ToolRegistry":
        self._tools[tool.name] = tool
        return self

    def get(self, name: str) -> Tool:
        if name not in self._tools:
            raise KeyError(f"Unknown tool: {name!r}. Registered: {self.names()}")
        return self._tools[name]

    def names(self) -> List[str]:
        return list(self._tools)

    def declarations(self) -> List[Dict[str, Any]]:
        return [tool.declaration() for tool in self._tools.values()]


def build_default_registry() -> ToolRegistry:
    """Construct the registry with the built-in tools.

    Imported lazily so the registry module has no import cycle with the tools.
    """
    from agentkit.tools.respond_to_user import RespondToUserTool
    from agentkit.tools.search_faqs import SearchFaqsTool
    from agentkit.tools.search_products import SearchProductsTool

    registry = ToolRegistry()
    registry.register(SearchProductsTool())
    registry.register(SearchFaqsTool())
    registry.register(RespondToUserTool())
    return registry


default_registry = build_default_registry()
