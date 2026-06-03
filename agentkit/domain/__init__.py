"""Domain models — pure data with no I/O.

Every model serializes to and from the exact JSON shape used on the Kafka
topics, so the wire contract is owned in one place and the rest of the code
works with typed objects instead of nested ``dict.get`` chains.
"""

from agentkit.domain.enums import ExecutionStatus
from agentkit.domain.function_call import FunctionCall, FunctionExecution
from agentkit.domain.interaction import Interaction
from agentkit.domain.context import Context
from agentkit.domain.agent_payload import AgentPayload
from agentkit.domain.message import Header, Message

__all__ = [
    "ExecutionStatus",
    "FunctionCall",
    "FunctionExecution",
    "Interaction",
    "Context",
    "AgentPayload",
    "Header",
    "Message",
]
