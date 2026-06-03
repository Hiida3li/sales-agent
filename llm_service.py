"""
Gemini LLM Service with Function-Only Architecture

Architecture Rules:
1. NO direct responses - agent MUST use function calls for everything
2. Agent receives request and decides which functions to call
3. Functions execute sequentially one by one
4. After all functions complete, payload returns to agent-requests
5. Agent evaluates results and decides next action:
   - Call more functions ONLY if critical info is missing
   - Call respond_to_user when enough info is gathered
6. Process repeats until agent calls respond_to_user
7. respond_to_user is the ONLY way to send responses to users

Generic Decision Rules:
- If sufficient information gathered, call respond_to_user
- If no results after reasonable attempts, call respond_to_user explaining this
- Never call the same function with same parameters more than twice
- Always prefer responding with available info over endless function calls
"""

import os
import logging
import json
import uuid
from typing import Dict, Any, List, Optional
from dotenv import load_dotenv
from google import genai
from google.genai import types
import time
from quixstreams import Application
from string import Template

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

search_products_function = types.FunctionDeclaration(
    name="search_products",
    description="Searches the product catalog for items based on a text query and optional filters.",
    parameters_json_schema={
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "A search query for products."
            },
            "filters": {
                "type": "object",
                "description": "Optional filters like color.",
                "properties": {
                    "color": {
                        "type": "string"
                    }
                }
            }
        },
        "required": ["query"]
    }
)

search_faqs_function = types.FunctionDeclaration(
    name="search_faqs",
    description="Searches a knowledge base of Frequently Asked Questions (FAQs) based on a user's query. Returns a list of relevant FAQs including their questions, answers, and categories.",
    parameters_json_schema={
        "type": "object",
        "properties": {
            "text": {
                "type": "string",
                "description": "The user's question or search query to find relevant FAQs. For example: 'How long does shipping take?' or 'return policy'."
            }
        },
        "required": ["text"]
    }
)

respond_to_user_function = types.FunctionDeclaration(
    name="respond_to_user",
    description="Use this tool to send a response directly to the user. This tool also handles the formatting and routing of the message, and allows the AI to incorporate all available information to send an informative response that includes thoughts, function calls and instructions.",
    parameters_json_schema={
        "type": "object",
        "properties": {
            "content": {
                "type": "string",
                "description": "The response to be sent to the user"
            }
        },
        "required": ["content"]
    }
)

TOOLS = [
    types.Tool(function_declarations=[
        search_products_function,
        search_faqs_function,
        respond_to_user_function
    ])
]

class GeminiProvider:
    """Minimized Gemini provider for clear API interaction"""
    
    def __init__(self, model_name: str = "gemini-2.5-flash"):
        api_key = os.getenv("GOOGLE_API_KEY")
        if not api_key:
            raise ValueError("GOOGLE_API_KEY required")
        
        self.client = genai.Client(api_key=api_key)
        self.model_name = model_name
        
    def generate(self, prompt: str,allowed_function_names=None) -> Optional[types.GenerateContentResponse]:
        """Generate content with automatic retry"""
        for attempt in range(3):
            try:
                return self.client.models.generate_content(
                    model=self.model_name,
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        tools=TOOLS,
                        tool_config=types.ToolConfig(
                            function_calling_config=types.FunctionCallingConfig(
                                mode='ANY',  # Always require function calls
                                allowed_function_names=allowed_function_names  # Allow all defined functions
                            )
                        ),
                        temperature=0,
                        max_output_tokens=10000
                    )
                )
            except Exception as e:
                logger.error(f"Attempt {attempt + 1} failed: {e}")
                if attempt < 2:
                    time.sleep(1)
        return None


def extract_all_parts(response: types.GenerateContentResponse) -> Dict[str, Any]:
    """Extract ALL parts from response - text and function calls"""
    result = {
        "text_parts": [],
        "function_calls": [],
        "reasoning": ""
    }
    
    if not response or not response.candidates:
        logger.warning("No candidates in response")
        return result
    
    # Process ALL parts from the response
    for candidate in response.candidates:
        for part in candidate.content.parts:
            # Handle text parts
            if hasattr(part, 'text') and part.text:
                result["text_parts"].append(part.text.strip())
                if not result["reasoning"]:
                    result["reasoning"] = part.text.strip()
                logger.debug(f"Found text part: {part.text.strip()[:100]}...")
            
            # Handle function call parts
            elif hasattr(part, 'function_call') and part.function_call:
                fc = part.function_call
                # Extract args properly
                args = {}
                if hasattr(fc.args, 'items'):
                    # Legacy format
                    for key, value in fc.args.items():
                        if hasattr(value, 'string_value'):
                            args[key] = value.string_value
                        elif hasattr(value, 'struct_value'):
                            # Handle nested structures
                            nested = {}
                            for nk, nv in value.struct_value.fields.items():
                                nested[nk] = nv.string_value if hasattr(nv, 'string_value') else str(nv)
                            args[key] = nested
                        else:
                            args[key] = str(value)
                elif isinstance(fc.args, dict):
                    args = fc.args
                else:
                    # Try to convert to dict
                    try:
                        args = dict(fc.args) if fc.args else {}
                    except:
                        args = {}
                
                fc_data = {
                    "name": fc.name,
                    "args": args,
                    "id": str(uuid.uuid4())
                }
                result["function_calls"].append(fc_data)
                logger.info(f"Found function call: {fc.name} with args: {args}")
    
    logger.info(f"Extracted {len(result['text_parts'])} text parts and {len(result['function_calls'])} function calls")
    return result


def process_llm_request(context: Dict[str, Any], prompt_template: str, llm_provider: GeminiProvider) -> Dict[str, Any]:
    """Process request and extract all parts cleanly"""
    # Log context state
    interaction_count = len(context.get("interactions", []))
    history_count = len(context.get("history", []))
    logger.info(f"Processing request with {interaction_count} previous interactions and {history_count} history items")
    
    # Build prompt
    template = Template(prompt_template)
    prompt = template.safe_substitute(
        query=context.get("query", ""),
        history="\n".join(context.get("history", [])),
        interactions=json.dumps(context.get("interactions", []), indent=2)
    )
    
    # Get LLM response
    response = llm_provider.generate(prompt,context['allowed_tools'])
    parts = extract_all_parts(response)
    
    # Build interaction record
    interaction = {
        "interaction_id": str(uuid.uuid4()),
        "timestamp": time.time(),
        "user_query": context.get("query", ""),
        "llm_reasoning": parts["reasoning"] or "No reasoning provided",
        "text_responses": parts["text_parts"],
        "function_executions": []
    }
    
    # Add function executions
    for i, fc in enumerate(parts["function_calls"]):
        interaction["function_executions"].append({
            "execution_id": fc["id"],
            "function_name": fc["name"],
            "parameters": fc["args"],
            "execution_status": "pending" if i == 0 else "queued"
        })
    
    # Update context
    if "interactions" not in context:
        context["interactions"] = []
    context["interactions"].append(interaction)
    
    # Update history
    if "history" not in context:
        context["history"] = []
    
    # Add text responses to history (for reasoning tracking)
    for text in parts["text_parts"]:
        if text:
            context["history"].append(f"Assistant Reasoning: {text}")
    
    # Add function calls to history
    for fc in parts["function_calls"]:
        args_str = ", ".join([f"{k}={v}" for k, v in fc["args"].items()])
        context["history"].append(f"Tool Call: {fc['name']}({args_str})")
    
    logger.info(f"LLM decided on {len(parts['function_calls'])} function calls")
    
    return {
        "context": context,
        "function_calls": parts["function_calls"]
    }


def process_function_results(context: Dict[str, Any], function_id: str, result: Any) -> Dict[str, Any]:
    """Update context with function results"""
    # Find and update the execution
    for interaction in context.get("interactions", []):
        for execution in interaction.get("function_executions", []):
            if execution.get("execution_id") == function_id:
                execution["execution_status"] = "completed"
                execution["completed_at"] = time.time()
                execution["execution_result"] = result
                
                # Add to history with formatted result
                # if isinstance(result, dict) and "formatted_summary" in result:
                #     context["history"].append(f"Tool Response: {result['formatted_summary']}")
                # else:
                context["history"].append(f"Tool Response: {json.dumps(result)}")
                
                # Check if all executions in this interaction are complete
                all_complete = all(
                    e.get("execution_status") == "completed" 
                    for e in interaction.get("function_executions", [])
                )
                
                if all_complete:
                    interaction["all_executions_completed"] = True
                    logger.info(f"All {len(interaction['function_executions'])} functions completed for interaction {interaction['interaction_id']}")
                
                return {
                    "context": context,
                    "all_complete": all_complete,
                    "interaction_id": interaction["interaction_id"]
                }
    
    logger.warning(f"Could not find execution with ID {function_id}")
    return {"context": context, "all_complete": False, "interaction_id": None}


class LLMService:
    """Main service class for cleaner organization"""
    
    def __init__(self):
        self.app = Application(
            broker_address=os.getenv("KAFKA_BROKER", "127.0.0.1:9092"),
            consumer_group="llm-processor-group",
            auto_offset_reset="latest"
        )
        self.llm_provider = GeminiProvider(model_name="gemini-2.5-flash")
        self.producer = self.app.get_producer()
        
    def route_message(self, msg: Dict[str, Any], topic: str):
        """Route message to specified topic"""
        partition_key = msg.get("header", {}).get("id", "")
        self.producer.produce(
            topic=topic,
            key=partition_key,
            value=json.dumps(msg)
        )
        logger.info(f"Routed to {topic}")
        
    def handle_agent_request(self, msg: Dict[str, Any]):
        """Handle agent requests - always routes to functions"""
        agent = msg.get("payload", {}).get("agent", {})
        context = agent.get("context", {})
        prompt_template = agent.get("prompt", "")
        
        # Safety check: Force respond_to_user after too many interactions
        interaction_count = len(context.get("interactions", []))
        if interaction_count >= 10 :
            logger.warning(f"Forcing respond_to_user after {interaction_count} interactions to prevent infinite loop")
            
            # Generic summary of what has been done
            function_summary = "I have completed the following actions:\n"
            
            # Count function executions
            function_counts = {}
            for interaction in context.get("interactions", []):
                for execution in interaction.get("function_executions", []):
                    func_name = execution.get("function_name", "unknown")
                    if func_name != "respond_to_user":
                        function_counts[func_name] = function_counts.get(func_name, 0) + 1
            
            # Build summary
            for func_name, count in function_counts.items():
                function_summary += f"- Called {func_name} {count} time(s)\n"
            
            function_summary += "\nBased on the results from these functions, here is my response to your request."
            
            forced_call = {
                "name": "respond_to_user",
                "args": {"content": function_summary},
                "id": str(uuid.uuid4())
            }
            agent["current_function_execution"] = forced_call
            agent["remaining_function_calls"] = []
            self.route_message(msg, "respond_to_user")
            return
        
        # Process with LLM
        result = process_llm_request(context, prompt_template, self.llm_provider)
        
        # Update message
        agent["context"] = result["context"]
        
        # Always expect function calls - no direct responses allowed
        if result["function_calls"]:
            first_call = result["function_calls"][0]
            agent["current_function_execution"] = first_call
            agent["remaining_function_calls"] = result["function_calls"][1:]
            
            # Log the routing decision
            logger.info(f"Routing to function: {first_call['name']}")
            
            # Route to the function topic (including respond_to_user)
            self.route_message(msg, first_call["name"])
        else:
            # This should not happen if prompt is configured correctly
            logger.error("No function calls returned by LLM - this violates the design")
            # Force a respond_to_user call as fallback
            fallback_call = {
                "name": "respond_to_user",
                "args": {"content": "I apologize, but I couldn't process your request properly."},
                "id": str(uuid.uuid4())
            }
            agent["current_function_execution"] = fallback_call
            agent["remaining_function_calls"] = []
            self.route_message(msg, "respond_to_user")
            
    def handle_function_response(self, msg: Dict[str, Any]):
        """Handle function execution responses"""
        agent = msg.get("payload", {}).get("agent", {})
        function_call = agent.get("function_call", {})
        
        if "response" not in function_call:
            logger.warning("Function response missing - routing to respond_to_user as fallback")
            fallback_call = {
                "name": "respond_to_user",
                "args": {"content": "An error occurred processing your request."},
                "id": str(uuid.uuid4())
            }
            agent["current_function_execution"] = fallback_call
            agent["remaining_function_calls"] = []
            self.route_message(msg, "respond_to_user")
            return
            
        # Update context with result
        context = agent.get("context", {})
        result = process_function_results(
            context, 
            function_call.get("id"),
            function_call["response"]
        )
        
        agent["context"] = result["context"]
        
        # Check if more functions to execute in current batch
        remaining = agent.get("remaining_function_calls", [])
        if remaining and not result["all_complete"]:
            # Execute next function in the batch
            next_call = remaining[0]
            agent["current_function_execution"] = next_call
            agent["remaining_function_calls"] = remaining[1:]
            logger.info(f"Executing next function in batch: {next_call['name']}")
            self.route_message(msg, next_call["name"])
        elif result["all_complete"]:
            # All functions complete - send back to agent for next decision
            logger.info("All functions complete, routing back to agent-requests for next decision")
            
            # Clean up agent state for fresh processing
            agent.pop("current_function_execution", None)
            agent.pop("remaining_function_calls", None)
            agent.pop("function_call", None)
            
            # Route back to agent-requests for recursive processing
            self.route_message(msg, "agent-requests")
        else:
            logger.error("Unexpected state - routing back to agent")
            self.route_message(msg, "agent-requests")
            
    def run(self):
        """Start the service"""
        request_topic = self.app.topic("agent-requests", value_deserializer="json")
        response_topic = self.app.topic("agent-function-responses", value_deserializer="json")
        
        request_stream = self.app.dataframe(request_topic)
        response_stream = self.app.dataframe(response_topic)
        
        request_stream.update(self.handle_agent_request)
        response_stream.update(self.handle_function_response)
        
        logger.info("=" * 60)
        logger.info("LLM Service started - Function-Only Architecture")
        logger.info("Rules:")
        logger.info("  - NO direct responses allowed")
        logger.info("  - All interactions MUST use function calls")
        logger.info("  - Only respond_to_user can send final responses")
        logger.info("  - Recursive flow until respond_to_user is called")
        logger.info("=" * 60)
        
        self.app.run()


if __name__ == "__main__":
    service = LLMService()
    service.run()