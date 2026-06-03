"""The Interaction model — one turn of LLM reasoning plus its tool executions."""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List

from agentkit.domain.enums import ExecutionStatus
from agentkit.domain.function_call import FunctionExecution


@dataclass
class Interaction:
    """A single agent decision step and the functions it scheduled."""

    user_query: str = ""
    llm_reasoning: str = "No reasoning provided"
    text_responses: List[str] = field(default_factory=list)
    function_executions: List[FunctionExecution] = field(default_factory=list)
    all_executions_completed: bool = False
    interaction_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: float = field(default_factory=time.time)

    def all_complete(self) -> bool:
        """True when every scheduled execution has completed."""
        return all(
            e.execution_status == ExecutionStatus.COMPLETED
            for e in self.function_executions
        )

    def find_execution(self, execution_id: str) -> FunctionExecution | None:
        for execution in self.function_executions:
            if execution.execution_id == execution_id:
                return execution
        return None

    def to_dict(self) -> Dict[str, Any]:
        data: Dict[str, Any] = {
            "interaction_id": self.interaction_id,
            "timestamp": self.timestamp,
            "user_query": self.user_query,
            "llm_reasoning": self.llm_reasoning,
            "text_responses": self.text_responses,
            "function_executions": [e.to_dict() for e in self.function_executions],
        }
        # The original code writes this key only once the batch completes;
        # mirror that so unfinished interactions stay byte-identical.
        if self.all_executions_completed:
            data["all_executions_completed"] = True
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Interaction":
        return cls(
            user_query=data.get("user_query", ""),
            llm_reasoning=data.get("llm_reasoning", "No reasoning provided"),
            text_responses=data.get("text_responses", []) or [],
            function_executions=[
                FunctionExecution.from_dict(e)
                for e in data.get("function_executions", []) or []
            ],
            all_executions_completed=data.get("all_executions_completed", False),
            interaction_id=data.get("interaction_id", str(uuid.uuid4())),
            timestamp=data.get("timestamp", time.time()),
        )
