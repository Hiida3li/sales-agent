"""Function-call models.

``FunctionCall`` is what the LLM emits (a decision to invoke a tool).
``FunctionExecution`` is the record of that call as it moves through the
queued -> pending -> completed lifecycle inside an interaction.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from agentkit.domain.enums import ExecutionStatus


@dataclass
class FunctionCall:
    """A tool invocation requested by the model.

    Serializes as ``{"id", "name", "args"}`` to match the shape consumed by
    the tool services in ``current_function_execution``.
    """

    name: str
    args: Dict[str, Any] = field(default_factory=dict)
    id: str = field(default_factory=lambda: str(uuid.uuid4()))

    def to_dict(self) -> Dict[str, Any]:
        return {"id": self.id, "name": self.name, "args": self.args}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "FunctionCall":
        return cls(
            name=data.get("name", ""),
            args=data.get("args", {}) or {},
            id=data.get("id", str(uuid.uuid4())),
        )


@dataclass
class FunctionExecution:
    """The execution record for one function call within an interaction."""

    execution_id: str
    function_name: str
    parameters: Dict[str, Any] = field(default_factory=dict)
    execution_status: ExecutionStatus = ExecutionStatus.QUEUED
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    execution_result: Any = None

    @classmethod
    def from_call(
        cls, call: FunctionCall, status: ExecutionStatus
    ) -> "FunctionExecution":
        """Build a fresh execution record from a model function call."""
        return cls(
            execution_id=call.id,
            function_name=call.name,
            parameters=call.args,
            execution_status=status,
        )

    def mark_completed(self, result: Any) -> None:
        self.execution_status = ExecutionStatus.COMPLETED
        self.completed_at = time.time()
        self.execution_result = result

    def to_dict(self) -> Dict[str, Any]:
        data: Dict[str, Any] = {
            "execution_id": self.execution_id,
            "function_name": self.function_name,
            "parameters": self.parameters,
            "execution_status": ExecutionStatus(self.execution_status).value,
        }
        if self.started_at is not None:
            data["started_at"] = self.started_at
        if self.completed_at is not None:
            data["completed_at"] = self.completed_at
        if self.execution_result is not None:
            data["execution_result"] = self.execution_result
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "FunctionExecution":
        return cls(
            execution_id=data.get("execution_id", ""),
            function_name=data.get("function_name", ""),
            parameters=data.get("parameters", {}) or {},
            execution_status=ExecutionStatus(
                data.get("execution_status", ExecutionStatus.QUEUED.value)
            ),
            started_at=data.get("started_at"),
            completed_at=data.get("completed_at"),
            execution_result=data.get("execution_result"),
        )
