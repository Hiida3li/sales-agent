"""Renders the prompt from the conversation context and a template.

The template uses ``$query``, ``$history`` and ``$interactions`` placeholders
(see ``init_payload.json``). ``safe_substitute`` leaves unknown placeholders
untouched rather than raising.
"""

from __future__ import annotations

import json
from string import Template

from agentkit.domain.context import Context


class PromptBuilder:
    def build(self, context: Context, template: str) -> str:
        return Template(template).safe_substitute(
            query=context.query,
            history="\n".join(context.history),
            interactions=json.dumps(
                [i.to_dict() for i in context.interactions], indent=2
            ),
        )
