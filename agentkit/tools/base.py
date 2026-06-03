"""The Tool interface.

A tool advertises a provider-agnostic declaration (name, description, JSON
schema) and knows how to execute itself given the model-supplied arguments.
The generic tool service and the LLM provider both depend only on this
interface, so adding a capability never requires touching the core.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict

# Default topic that tool results are published to, where the agent service
# consumes them. ``respond_to_user`` overrides this to reach the user directly.
DEFAULT_RESPONSE_TOPIC = "agent-function-responses"


class Tool(ABC):
    #: The tool name. Doubles as the Kafka topic the tool consumes from.
    name: str

    #: Topic this tool publishes its result to.
    response_topic: str = DEFAULT_RESPONSE_TOPIC

    @abstractmethod
    def declaration(self) -> Dict[str, Any]:
        """Return ``{"name", "description", "parameters"}`` (JSON schema)."""
        raise NotImplementedError

    @abstractmethod
    def execute(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Run the tool and return a result dict to attach to the response."""
        raise NotImplementedError
