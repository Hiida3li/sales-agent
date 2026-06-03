"""Provider-agnostic result of a single LLM generation."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from agentkit.domain.function_call import FunctionCall


@dataclass
class LLMResult:
    """What a provider returns: free text, the function calls it requested,
    and the first text part treated as reasoning.
    """

    text_parts: List[str] = field(default_factory=list)
    function_calls: List[FunctionCall] = field(default_factory=list)
    reasoning: str = ""
