"""FAQ knowledge base — mock data plus the search logic.

Separated from the tool plumbing so the source can be swapped for a real
knowledge base without touching the Kafka/agent wiring.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

_MOCK_FAQS: List[Dict[str, Any]] = [
    {
        "id": "shipping-times",
        "question": "How long does shipping take?",
        "answer": "Standard shipping takes 3-5 business days. Express shipping takes 1-2 business days. Overnight shipping is available for next-day delivery.",
        "category": "Shipping",
        "tags": ["shipping", "delivery", "days", "time"],
    },
    {
        "id": "shipping-cost",
        "question": "How much does shipping cost?",
        "answer": "Standard shipping is free for orders over $50. Express shipping costs $9.99. Overnight shipping costs $19.99.",
        "category": "Shipping",
        "tags": ["shipping", "cost", "price", "free"],
    },
    {
        "id": "return-policy",
        "question": "What is your return policy?",
        "answer": "We offer a 30-day return policy for all items in original condition. Returns are free with our prepaid return label.",
        "category": "Returns",
        "tags": ["return", "policy", "30", "days", "refund"],
    },
    {
        "id": "warranty",
        "question": "Do products come with warranty?",
        "answer": "All electronics come with manufacturer warranty. Apple products have 1-year limited warranty. Extended warranty options are available.",
        "category": "Warranty",
        "tags": ["warranty", "guarantee", "apple", "electronics"],
    },
    {
        "id": "payment-methods",
        "question": "What payment methods do you accept?",
        "answer": "We accept all major credit cards (Visa, MasterCard, American Express), PayPal, Apple Pay, and Google Pay.",
        "category": "Payment",
        "tags": ["payment", "credit", "card", "paypal", "apple", "pay"],
    },
    {
        "id": "store-hours",
        "question": "What are your store hours?",
        "answer": "Our online store is available 24/7. Customer service is available Monday-Friday 9AM-6PM EST.",
        "category": "Store Info",
        "tags": ["hours", "customer", "service", "support"],
    },
]


class FaqKnowledgeBase:
    def __init__(self, faqs: List[Dict[str, Any]] | None = None):
        self._faqs = faqs if faqs is not None else _MOCK_FAQS

    def search(self, text: str) -> List[Dict[str, Any]]:
        logger.info(f"Searching FAQs with text: '{text}'")
        terms = text.lower().split()
        results = [faq for faq in self._faqs if self._matches(faq, terms)]
        logger.info(f"Found {len(results)} FAQs")
        return results

    @staticmethod
    def _matches(faq: Dict[str, Any], terms: List[str]) -> bool:
        haystack = (
            f"{faq['question']} {faq['answer']} {' '.join(faq['tags'])}"
        ).lower()
        return any(term in haystack for term in terms)
