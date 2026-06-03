"""The search_faqs tool."""

from __future__ import annotations

import time
from typing import Any, Dict

from agentkit.tools.base import Tool
from agentkit.tools.knowledge_base import FaqKnowledgeBase


class SearchFaqsTool(Tool):
    name = "search_faqs"

    def __init__(self, knowledge_base: FaqKnowledgeBase | None = None):
        self.knowledge_base = knowledge_base or FaqKnowledgeBase()

    def declaration(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "description": (
                "Searches a knowledge base of Frequently Asked Questions (FAQs) "
                "based on a user's query. Returns a list of relevant FAQs "
                "including their questions, answers, and categories."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "text": {
                        "type": "string",
                        "description": (
                            "The user's question or search query to find relevant "
                            "FAQs. For example: 'How long does shipping take?' or "
                            "'return policy'."
                        ),
                    }
                },
                "required": ["text"],
            },
        }

    def execute(self, args: Dict[str, Any]) -> Dict[str, Any]:
        text = args.get("text", "")
        faqs = self.knowledge_base.search(text)

        summary = f"Total FAQs found: {len(faqs)}\n"
        if faqs:
            for faq in faqs:
                summary += (
                    "\nFAQ Found:\n"
                    f"- Question: {faq.get('question', 'N/A')}\n"
                    f"- Answer: {faq.get('answer', 'N/A')}\n"
                )
        else:
            summary += "No FAQs matching the search criteria.\n"

        return {
            "faqs": faqs,
            "total_found": len(faqs),
            "search_text": text,
            "timestamp": time.time(),
            "formatted_summary": summary,
        }
