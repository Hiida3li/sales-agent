"""The respond_to_user tool — the only path that reaches the user.

Unlike the search tools, its result is published to ``user-responses`` rather
than back into the agent loop, so it overrides ``response_topic``.
"""

from __future__ import annotations

import time
from typing import Any, Dict

from agentkit.tools.base import Tool


class RespondToUserTool(Tool):
    name = "respond_to_user"
    response_topic = "user-responses"

    def declaration(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "description": (
                "Use this tool to send a response directly to the user. This "
                "tool also handles the formatting and routing of the message, "
                "and allows the AI to incorporate all available information to "
                "send an informative response that includes thoughts, function "
                "calls and instructions."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "content": {
                        "type": "string",
                        "description": "The response to be sent to the user",
                    }
                },
                "required": ["content"],
            },
        }

    def execute(self, args: Dict[str, Any]) -> Dict[str, Any]:
        return {"content": args.get("content", ""), "timestamp": time.time()}
