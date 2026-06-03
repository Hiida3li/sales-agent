"""Termination safety for the agent loop.

Prevents the recursive agent <-> tools cycle from running forever: once the
interaction count crosses a ceiling, the guard forces a ``respond_to_user``
call summarizing what was done.
"""

from __future__ import annotations

import logging

from agentkit.domain.context import Context
from agentkit.domain.function_call import FunctionCall

logger = logging.getLogger(__name__)


class LoopGuard:
    def __init__(self, max_interactions: int = 10):
        self.max_interactions = max_interactions

    def should_force_respond(self, context: Context) -> bool:
        return context.interaction_count() >= self.max_interactions

    def build_forced_response(self, context: Context) -> FunctionCall:
        logger.warning(
            f"Forcing respond_to_user after {context.interaction_count()} "
            "interactions to prevent infinite loop"
        )

        counts: dict[str, int] = {}
        for interaction in context.interactions:
            for execution in interaction.function_executions:
                name = execution.function_name
                if name != "respond_to_user":
                    counts[name] = counts.get(name, 0) + 1

        summary = "I have completed the following actions:\n"
        for name, count in counts.items():
            summary += f"- Called {name} {count} time(s)\n"
        summary += (
            "\nBased on the results from these functions, here is my response "
            "to your request."
        )

        return FunctionCall(name="respond_to_user", args={"content": summary})
