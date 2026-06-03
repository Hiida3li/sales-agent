"""The search_products tool."""

from __future__ import annotations

import time
from typing import Any, Dict

from agentkit.tools.base import Tool
from agentkit.tools.catalog import ProductCatalog


class SearchProductsTool(Tool):
    name = "search_products"

    def __init__(self, catalog: ProductCatalog | None = None):
        self.catalog = catalog or ProductCatalog()

    def declaration(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "description": (
                "Searches the product catalog for items based on a text query "
                "and optional filters."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "A search query for products.",
                    },
                    "filters": {
                        "type": "object",
                        "description": "Optional filters like color.",
                        "properties": {"color": {"type": "string"}},
                    },
                },
                "required": ["query"],
            },
        }

    def execute(self, args: Dict[str, Any]) -> Dict[str, Any]:
        # Read the parameter the declaration actually advertises. (The legacy
        # service read "text", which never matched and returned all products.)
        query = args.get("query", "")
        filters = args.get("filters", {})
        if not isinstance(filters, dict):
            filters = {}

        products = self.catalog.search(query, filters)

        summary = f"Total products found: {len(products)}\n"
        if products:
            for product in products:
                summary += (
                    "\nProduct Found:\n"
                    f"- Name: {product.get('name', 'N/A')}\n"
                    f"- Color: {product.get('color', 'N/A')}\n"
                    f"- Price: ${product.get('price', 'N/A')}\n"
                    f"- In Stock: {product.get('in_stock', False)}\n"
                    f"- Description: {product.get('description', 'N/A')}\n"
                )
        else:
            summary += "No products matching the search criteria.\n"

        return {
            "products": products,
            "total_found": len(products),
            "search_text": query,
            "filters_applied": filters,
            "timestamp": time.time(),
            "formatted_summary": summary,
        }
