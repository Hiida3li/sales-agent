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

def search_faqs(text: str) -> List[Dict[str, Any]]:
    """
    Mock search function for FAQs
    In a real implementation, this would query a knowledge base or FAQ database
    """
    logger.info(f"Searching FAQs with text: '{text}'")
    
    # Mock FAQ data
    mock_faqs = [
        {
            "id": "shipping-times",
            "question": "How long does shipping take?",
            "answer": "Standard shipping takes 3-5 business days. Express shipping takes 1-2 business days. Overnight shipping is available for next-day delivery.",
            "category": "Shipping",
            "tags": ["shipping", "delivery", "days", "time"]
        },
        {
            "id": "shipping-cost",
            "question": "How much does shipping cost?",
            "answer": "Standard shipping is free for orders over $50. Express shipping costs $9.99. Overnight shipping costs $19.99.",
            "category": "Shipping",
            "tags": ["shipping", "cost", "price", "free"]
        },
        {
            "id": "return-policy",
            "question": "What is your return policy?",
            "answer": "We offer a 30-day return policy for all items in original condition. Returns are free with our prepaid return label.",
            "category": "Returns",
            "tags": ["return", "policy", "30", "days", "refund"]
        },
        {
            "id": "warranty",
            "question": "Do products come with warranty?",
            "answer": "All electronics come with manufacturer warranty. Apple products have 1-year limited warranty. Extended warranty options are available.",
            "category": "Warranty",
            "tags": ["warranty", "guarantee", "apple", "electronics"]
        },
        {
            "id": "payment-methods",
            "question": "What payment methods do you accept?",
            "answer": "We accept all major credit cards (Visa, MasterCard, American Express), PayPal, Apple Pay, and Google Pay.",
            "category": "Payment",
            "tags": ["payment", "credit", "card", "paypal", "apple", "pay"]
        },
        {
            "id": "store-hours",
            "question": "What are your store hours?",
            "answer": "Our online store is available 24/7. Customer service is available Monday-Friday 9AM-6PM EST.",
            "category": "Store Info",
            "tags": ["hours", "customer", "service", "support"]
        }
    ]
    
    # Simple text matching
    results = []
    search_terms = text.lower().split()
    
    for faq in mock_faqs:
        # Check if any search terms match FAQ fields
        faq_text = f"{faq['question']} {faq['answer']} {' '.join(faq['tags'])}".lower()
        
        if any(term in faq_text for term in search_terms):
            results.append(faq)
    
    logger.info(f"Found {len(results)} FAQs")
    return results

def process_search_request(msg: Dict[str, Any]) -> Dict[str, Any]:
    """Process a search_faqs function call with structured hierarchy"""
    try:
        # Extract function call details from current_function_execution
        agent = msg.get("payload", {}).get("agent", {})
        function_call = agent.get("current_function_execution", {})
        
        if function_call.get("name") != "search_faqs":
            logger.error(f"Unexpected function call: {function_call.get('name')}")
            return None
        
        # Extract search parameters
        args = function_call.get("args", {})
        text = args.get("text", "")
        
        # Perform the search
        faqs = search_faqs(text)
        
        # Create response with function call result
        response_msg = msg.copy()
        
        # Create formatted summary for LLM consumption
        formatted_summary = f"Total FAQs found: {len(faqs)}\n"
        
        if faqs:
            for faq in faqs:
                formatted_summary += f"\nFAQ Found:\n"
                formatted_summary += f"- Question: {faq.get('question', 'N/A')}\n"
                formatted_summary += f"- Answer: {faq.get('answer', 'N/A')}\n"
        else:
            formatted_summary += "No FAQs matching the search criteria.\n"
        
        # Add response to the current function call
        response_msg["payload"]["agent"]["function_call"] = function_call.copy()
        response_msg["payload"]["agent"]["function_call"]["response"] = {
            "faqs": faqs,
            "total_found": len(faqs),
            "search_text": text,
            "timestamp": time.time(),
            "formatted_summary": formatted_summary
        }
        
        logger.info(f"Processed FAQ search request for function call {function_call.get('id')}, returning {len(faqs)} FAQs")
        return response_msg
        
    except Exception as e:
        logger.error(f"Error processing FAQ search request: {e}")
        
        # Return error response
        error_msg = msg.copy()
        error_msg["payload"]["agent"]["function_call"] = function_call.copy()
        error_msg["payload"]["agent"]["function_call"]["response"] = {
            "error": str(e),
            "faqs": [],
            "total_found": 0,
            "timestamp": time.time(),
            "formatted_summary": f"Error occurred during FAQ search: {str(e)}\n"
        }
        return error_msg

def main():
    # Initialize Quix Streams application
    app = Application(
        broker_address=os.getenv("KAFKA_BROKER", "localhost:9092"),
        consumer_group="search-faqs-group",
        auto_offset_reset="latest"
    )
    
    # Create input topic for search_faqs function calls
    input_topic = app.topic("search_faqs", value_deserializer="json")
    
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
            logger.info(f"Sent FAQ response back to agent-function-responses topic")
    
    # Apply processing
    sdf = sdf.update(process_and_respond)
    
    # Run the application
    logger.info("Starting search_faqs service...")
    app.run(sdf)

if __name__ == "__main__":
    main()