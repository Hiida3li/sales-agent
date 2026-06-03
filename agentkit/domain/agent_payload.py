"""The AgentPayload — everything under ``payload.agent`` on the wire.

Besides the durable :class:`Context` and the prompt template, this carries the
transient routing fields the services hand back and forth:

- ``current_function_execution`` the call a tool service should run now
- ``remaining_function_calls``    the rest of the batch, awaiting their turn
- ``function_call``               a tool's result envelope on the way back
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from agentkit.domain.context import Context
from agentkit.domain.function_call import FunctionCall


@dataclass
class AgentPayload:
    context: Context = field(default_factory=Context)
    prompt: str = ""
    current_function_execution: Optional[FunctionCall] = None
    remaining_function_calls: List[FunctionCall] = field(default_factory=list)
    # Tool-response envelope ({id, name, args, response}); kept as a raw dict
    # because it is an opaque pass-through carrying arbitrary tool output.
    function_call: Optional[Dict[str, Any]] = None

    def set_batch(self, calls: List[FunctionCall]) -> None:
        """Route the first call now and queue the remainder."""
        self.current_function_execution = calls[0]
        self.remaining_function_calls = calls[1:]

    def clear_routing_state(self) -> None:
        """Reset transient fields before the next agent decision."""
        self.current_function_execution = None
        self.remaining_function_calls = []
        self.function_call = None

    def to_dict(self) -> Dict[str, Any]:
        data: Dict[str, Any] = {
            "context": self.context.to_dict(),
            "prompt": self.prompt,
        }
        if self.current_function_execution is not None:
            data["current_function_execution"] = self.current_function_execution.to_dict()
        if self.remaining_function_calls:
            data["remaining_function_calls"] = [
                c.to_dict() for c in self.remaining_function_calls
            ]
        if self.function_call is not None:
            data["function_call"] = self.function_call
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "AgentPayload":
        current = data.get("current_function_execution")
        return cls(
            context=Context.from_dict(data.get("context", {}) or {}),
            prompt=data.get("prompt", ""),
            current_function_execution=(
                FunctionCall.from_dict(current) if current else None
            ),
            remaining_function_calls=[
                FunctionCall.from_dict(c)
                for c in data.get("remaining_function_calls", []) or []
            ],
            function_call=data.get("function_call"),
        )
