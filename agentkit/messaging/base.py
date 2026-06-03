"""The MessageBus interface.

Services depend on this rather than on Quix Streams or Kafka directly. A
handler receives a decoded message dict and may produce follow-up messages via
:meth:`produce`. :meth:`run` blocks, processing the subscribed topics.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Callable, Dict

Handler = Callable[[Dict[str, Any]], None]


class MessageBus(ABC):
    @abstractmethod
    def consume(self, topic: str, handler: Handler) -> None:
        """Register ``handler`` to be called for each message on ``topic``."""
        raise NotImplementedError

    @abstractmethod
    def produce(self, topic: str, key: str, value: Dict[str, Any]) -> None:
        """Publish ``value`` to ``topic`` under ``key``."""
        raise NotImplementedError

    @abstractmethod
    def run(self) -> None:
        """Start processing; blocks until the process is stopped."""
        raise NotImplementedError
