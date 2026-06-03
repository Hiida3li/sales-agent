"""The conversation Context — the agent's working memory for a session.

Holds the current query, a flat human-readable history, the structured
interaction records, and the list of tools the agent is allowed to use.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List

from agentkit.domain.interaction import Interaction


@dataclass
class Context:
    query: str = ""
    history: List[str] = field(default_factory=list)
    interactions: List[Interaction] = field(default_factory=list)
    allowed_tools: List[str] | None = None

    def append_history(self, line: str) -> None:
        self.history.append(line)

    def add_interaction(self, interaction: Interaction) -> None:
        self.interactions.append(interaction)

    def interaction_count(self) -> int:
        return len(self.interactions)

    def latest_interaction(self) -> Interaction | None:
        return self.interactions[-1] if self.interactions else None

    def find_execution(self, execution_id: str):
        """Locate an execution and its owning interaction by execution id."""
        for interaction in self.interactions:
            execution = interaction.find_execution(execution_id)
            if execution is not None:
                return interaction, execution
        return None, None

    def to_dict(self) -> Dict[str, Any]:
        data: Dict[str, Any] = {
            "query": self.query,
            "history": self.history,
            "interactions": [i.to_dict() for i in self.interactions],
        }
        # Preserve the key only when present, matching the original payloads.
        if self.allowed_tools is not None:
            data["allowed_tools"] = self.allowed_tools
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Context":
        return cls(
            query=data.get("query", ""),
            history=data.get("history", []) or [],
            interactions=[
                Interaction.from_dict(i) for i in data.get("interactions", []) or []
            ],
            allowed_tools=data.get("allowed_tools"),
        )
