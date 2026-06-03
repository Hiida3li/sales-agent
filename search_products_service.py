import os
import logging
import json
import time
from typing import Dict, Any, List
from quixstreams import Application

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def search_products(text: str, filters: Dict[str, Any] = None) -> List[Dict[str, Any]]:
    """
    Mock search function for products
    In a real implementation, this would query a database or search engine
    """
    logger.info(f"Searching products with text: '{text}' and filters: {filters}")
    
    # Mock product data
    mock_products = [
        {
            "id": "iphone-15-pro-red",
            "name": "iPhone 15 Pro",
            "color": "Red",
            "price": 999.99,
            "category": "Electronics/Smartphones",
            "brand": "Apple",
            "in_stock": True,
            "description": "Latest iPhone 15 Pro in stunning red color"
        },
        {
            "id": "iphone-15-pro-blue",
            "name": "iPhone 15 Pro",
            "color": "Blue",
            "price": 999.99,
            "category": "Electronics/Smartphones", 
            "brand": "Apple",
            "in_stock": True,
            "description": "Latest iPhone 15 Pro in deep blue color"
        },
        {
            "id": "samsung-galaxy-s24",
            "name": "Samsung Galaxy S24",
            "color": "Black",
            "price": 899.99,
            "category": "Electronics/Smartphones",
            "brand": "Samsung",
            "in_stock": False,
            "description": "Samsung Galaxy S24 in midnight black"
        }
    ]
    
    # Simple text matching
    results = []
    
    # If no search text provided, return all products (for counting)
    if not text or text.strip() == "":
        results = mock_products.copy()
    else:
        search_terms = text.lower().split()
        
        for product in mock_products:
            # Check if any search terms match product fields
            product_text = f"{product['name']} {product['color']} {product['brand']} {product['description']}".lower()
            
            if any(term in product_text for term in search_terms):
                results.append(product)
    
    # Apply filters if provided
    if filters and results:
        filtered_results = []
        for product in results:
            skip_product = False
            
            # Check color filter
            if filters.get("attributes", {}).get("color"):
                filter_color = filters["attributes"]["color"].lower()
                if filter_color not in product["color"].lower():
                    skip_product = True
            
            # Check price range filter
            if not skip_product and filters.get("price_range"):
                price_filter = filters["price_range"]
                operation = price_filter.get("operation", "eq")
                
                if operation == "lt" and product["price"] >= price_filter.get("max", 0):
                    skip_product = True
                elif operation == "gt" and product["price"] <= price_filter.get("min", 0):
                    skip_product = True
                elif operation == "between":
                    min_price = price_filter.get("min", 0)
                    max_price = price_filter.get("max", float('inf'))
                    if not (min_price <= product["price"] <= max_price):
                        skip_product = True
            
            if not skip_product:
                filtered_results.append(product)
        
        results = filtered_results
    
    logger.info(f"Found {len(results)} products")
    return results

def process_search_request(msg: Dict[str, Any]) -> Dict[str, Any]:
    """Process a search_products function call with structured hierarchy"""
    function_call = {}  # Initialize to avoid UnboundLocalError
    
    try:
        # Add debugging to see what we're receiving
        logger.debug(f"Received message type: {type(msg)}")
        logger.debug(f"Message content: {json.dumps(msg, indent=2)}")
        
        # Validate msg is a dictionary
        if not isinstance(msg, dict):
            logger.error(f"Expected dict but got {type(msg)}: {msg}")
            return None
        
        # Extract payload with validation
        payload = msg.get("payload")
        if not isinstance(payload, dict):
            logger.error(f"payload is not a dict: {type(payload)}")
            return None
            
        # Extract agent with validation
        agent = payload.get("agent")
        if not isinstance(agent, dict):
            logger.error(f"agent is not a dict: {type(agent)}")
            return None
        
        # Extract function call details from current_function_execution
        function_call = agent.get("current_function_execution", {})
        
        # Validate function_call is a dictionary
        if not isinstance(function_call, dict):
            logger.error(f"current_function_execution is not a dict: {type(function_call)}")
            return None
        
        if function_call.get("name") != "search_products":
            logger.error(f"Unexpected function call: {function_call.get('name')}")
            return None
        
        # Extract search parameters with validation
        args = function_call.get("args", {})
        if not isinstance(args, dict):
            logger.error(f"args is not a dict: {type(args)}")
            args = {}
        
        text = args.get("text", "")
        filters = args.get("filters", {})
        
        # Ensure filters is a dict
        if not isinstance(filters, dict):
            logger.warning(f"filters is not a dict: {type(filters)}, using empty dict")
            filters = {}
        
        # Perform the search
        products = search_products(text, filters)
        
        # Create response with function call result
        response_msg = msg.copy()
        
        # Create formatted summary for LLM consumption
        formatted_summary = f"Total products found: {len(products)}\n"
        
        if products:
            for product in products:
                formatted_summary += f"\nProduct Found:\n"
                formatted_summary += f"- Name: {product.get('name', 'N/A')}\n"
                formatted_summary += f"- Color: {product.get('color', 'N/A')}\n"
                formatted_summary += f"- Price: ${product.get('price', 'N/A')}\n"
                formatted_summary += f"- In Stock: {product.get('in_stock', False)}\n"
                formatted_summary += f"- Description: {product.get('description', 'N/A')}\n"
        else:
            formatted_summary += "No products matching the search criteria.\n"
        
        # Add response to the current function call
        response_msg["payload"]["agent"]["function_call"] = function_call.copy()
        response_msg["payload"]["agent"]["function_call"]["response"] = {
            "products": products,
            "total_found": len(products),
            "search_text": text,
            "filters_applied": filters,
            "timestamp": time.time(),
            "formatted_summary": formatted_summary
        }
        
        logger.info(f"Processed search request for function call {function_call.get('id')}, returning {len(products)} products")
        return response_msg
        
    except Exception as e:
        logger.error(f"Error processing search request: {e}", exc_info=True)
        
        # Return error response
        error_msg = msg.copy() if isinstance(msg, dict) else {"payload": {"agent": {}}}
        
        # Safely add the error response
        if "payload" not in error_msg:
            error_msg["payload"] = {}
        if "agent" not in error_msg["payload"]:
            error_msg["payload"]["agent"] = {}
            
        error_msg["payload"]["agent"]["function_call"] = function_call.copy() if function_call else {}
        error_msg["payload"]["agent"]["function_call"]["response"] = {
            "error": str(e),
            "products": [],
            "total_found": 0,
            "timestamp": time.time(),
            "formatted_summary": f"Error occurred during product search: {str(e)}\n"
        }
        return error_msg
def main():
    # Initialize Quix Streams application
    app = Application(
        broker_address=os.getenv("KAFKA_BROKER", "localhost:9092"),
        consumer_group="search-products-group",
        auto_offset_reset="latest"
    )
    
    # Create input topic for search_products function calls
    input_topic = app.topic("search_products", value_deserializer="json")
    
    # Create producer for sending responses back to LLM service
    producer = app.get_producer()
    
    # Create stream
    sdf = app.dataframe(input_topic)
    
    # Process search requests and send responses
    def process_and_respond(msg):
        response = process_search_request(msg)
        if response:
            # Send response back to agent-function-responses topic
            producer.produce(
                topic="agent-function-responses",
                key=response.get("header", {}).get("id", ""),
                value=json.dumps(response)
            )
            logger.info(f"Sent response back to agent-function-responses topic")
    
    # Apply processing
    sdf = sdf.update(process_and_respond)
    
    # Run the application
    logger.info("Starting search_products service...")
    app.run(sdf)

if __name__ == "__main__":
    main()