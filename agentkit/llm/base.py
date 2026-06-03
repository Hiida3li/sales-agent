"""The LLMProvider interface.

The agent loop depends on this abstraction, not on any concrete SDK, so a
different model vendor can be dropped in by implementing :meth:`generate`.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from agentkit.llm.result import LLMResult


class LLMProvider(ABC):
    @abstractmethod
    def generate(
        self,
        prompt: str,
        declarations: List[Dict[str, Any]],
        allowed_function_names: Optional[List[str]] = None,
    ) -> LLMResult:
        """Generate a response that must take the form of function calls.

        Args:
            prompt: the fully rendered prompt text.
            declarations: provider-agnostic tool declarations, each
                ``{"name", "description", "parameters"}``.
            allowed_function_names: optional whitelist restricting which tools
                the model may call this turn.
        """
        raise NotImplementedError
