"""The Message envelope — the top-level object on every Kafka topic.

Shape: ``{"header": {"id", "timestamp"}, "payload": {"agent": {...}}}``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict

from agentkit.domain.agent_payload import AgentPayload


@dataclass
class Header:
    id: str = ""
    timestamp: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {"id": self.id, "timestamp": self.timestamp}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Header":
        return cls(id=data.get("id", ""), timestamp=data.get("timestamp", ""))


@dataclass
class Message:
    header: Header = field(default_factory=Header)
    agent: AgentPayload = field(default_factory=AgentPayload)
    # Any extra top-level keys under ``payload`` are preserved verbatim so we
    # never silently drop fields a producer added (e.g. ``error``/``timestamp``).
    extra_payload: Dict[str, Any] = field(default_factory=dict)

    @property
    def key(self) -> str:
        """Kafka partition key — the header id, as used by every service."""
        return self.header.id

    def to_dict(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = dict(self.extra_payload)
        payload["agent"] = self.agent.to_dict()
        return {"header": self.header.to_dict(), "payload": payload}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Message":
        payload = data.get("payload", {}) or {}
        extra = {k: v for k, v in payload.items() if k != "agent"}
        return cls(
            header=Header.from_dict(data.get("header", {}) or {}),
            agent=AgentPayload.from_dict(payload.get("agent", {}) or {}),
            extra_payload=extra,
        )
