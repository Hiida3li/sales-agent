"""Product catalog — mock data plus the search logic.

Separated from the tool plumbing so the data source can be swapped for a real
database or search engine without touching the Kafka/agent wiring.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

_MOCK_PRODUCTS: List[Dict[str, Any]] = [
    {
        "id": "iphone-15-pro-red",
        "name": "iPhone 15 Pro",
        "color": "Red",
        "price": 999.99,
        "category": "Electronics/Smartphones",
        "brand": "Apple",
        "in_stock": True,
        "description": "Latest iPhone 15 Pro in stunning red color",
    },
    {
        "id": "iphone-15-pro-blue",
        "name": "iPhone 15 Pro",
        "color": "Blue",
        "price": 999.99,
        "category": "Electronics/Smartphones",
        "brand": "Apple",
        "in_stock": True,
        "description": "Latest iPhone 15 Pro in deep blue color",
    },
    {
        "id": "samsung-galaxy-s24",
        "name": "Samsung Galaxy S24",
        "color": "Black",
        "price": 899.99,
        "category": "Electronics/Smartphones",
        "brand": "Samsung",
        "in_stock": False,
        "description": "Samsung Galaxy S24 in midnight black",
    },
]


class ProductCatalog:
    def __init__(self, products: List[Dict[str, Any]] | None = None):
        self._products = products if products is not None else _MOCK_PRODUCTS

    def search(self, query: str, filters: Dict[str, Any] | None = None) -> List[Dict[str, Any]]:
        logger.info(f"Searching products with query: '{query}' and filters: {filters}")

        # No query -> return everything (useful for counting).
        if not query or not query.strip():
            results = list(self._products)
        else:
            terms = query.lower().split()
            results = [p for p in self._products if self._matches(p, terms)]

        if filters and results:
            results = [p for p in results if self._passes_filters(p, filters)]

        logger.info(f"Found {len(results)} products")
        return results

    @staticmethod
    def _matches(product: Dict[str, Any], terms: List[str]) -> bool:
        haystack = (
            f"{product['name']} {product['color']} "
            f"{product['brand']} {product['description']}"
        ).lower()
        return any(term in haystack for term in terms)

    @staticmethod
    def _passes_filters(product: Dict[str, Any], filters: Dict[str, Any]) -> bool:
        # Color filter, as advertised in the tool declaration.
        color = filters.get("color")
        if color and color.lower() not in product["color"].lower():
            return False

        # Optional price-range filter (tolerated even though the declaration
        # keeps the schema minimal).
        price_range = filters.get("price_range")
        if price_range:
            operation = price_range.get("operation", "eq")
            price = product["price"]
            if operation == "lt" and price >= price_range.get("max", 0):
                return False
            if operation == "gt" and price <= price_range.get("min", 0):
                return False
            if operation == "between":
                low = price_range.get("min", 0)
                high = price_range.get("max", float("inf"))
                if not (low <= price <= high):
                    return False

        return True
