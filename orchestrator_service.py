import os
import logging
import json
import time
from typing import Dict, Any
from quixstreams import Application

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def check_and_route_next_function(msg: Dict[str, Any], producer) -> bool:
    """Check if there are pending functions to execute and route them"""
    try:
        agent = msg.get("payload", {}).get("agent", {})
        context = agent.get("context", {})
        history = context.get("history", [])

        if not history:
            return False

        latest_interaction = history[-1]
        executions = latest_interaction.get("function_executions", [])

        # Find the next queued function
        for execution in executions:
            if execution.get("execution_status") == "queued":
                logger.info(f"Found queued function: {execution['function_name']}")

                # Update status to pending
                execution["execution_status"] = "pending"
                execution["started_at"] = time.time()

                # Create message for the next function
                next_msg = {
                    "header": msg.get("header", {}),
                    "payload": {
                        "agent": {
                            "context": context,
                            "prompt": agent.get("prompt", ""),
                            "current_function_execution": {
                                "id": execution["execution_id"],
                                "name": execution["function_name"],
                                "args": execution["parameters"],
                                "status": "pending",
                                "response": None
                            }
                        }
                    }
                }

                # Route to the function service
                producer.produce(
                    topic=execution["function_name"],
                    key=msg.get("header", {}).get("id", ""),
                    value=json.dumps(next_msg)
                )
                logger.info(f"Routed next function {execution['function_name']} to topic")
                return True

        return False

    except Exception as e:
        logger.error(f"Error in orchestrator: {e}")
        return False

def main():
    # Initialize Quix Streams application
    app = Application(
        broker_address=os.getenv("KAFKA_BROKER", "localhost:9092"),
        consumer_group="orchestrator-group",
        auto_offset_reset="latest"
    )

    # Create input topic for agent responses
    input_topic = app.topic("agent-responses", value_deserializer="json")

    # Create producer for routing next functions
    producer = app.get_producer()

    # Create stream
    sdf = app.dataframe(input_topic)

    # Process responses and check for next functions
    def process_and_orchestrate(msg):
        # Check if we need to route the next function
        has_next = check_and_route_next_function(msg, producer)

        if has_next:
            logger.info("Next function routed, workflow continues")
        else:
            logger.info("No more functions to execute, workflow complete")

    # Apply processing
    sdf = sdf.update(process_and_orchestrate)

    # Run the application
    logger.info("Starting orchestrator service...")
    app.run(sdf)

if __name__ == "__main__":
    main()