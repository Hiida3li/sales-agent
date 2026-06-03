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


def process_response_request(msg: Dict[str, Any]) -> Dict[str, Any]:
    """Process a response request and pass through the content"""
    try:
        agent = msg.get("payload", {}).get("agent", {})

        if "current_function_execution" in agent:
            function_call = agent.get("current_function_execution", {})

            args = function_call.get("args", {})
            content = args.get("content", "")

            response_msg = msg.copy()
            response_msg["payload"]["agent"]["function_call"] = function_call.copy()
            response_msg["payload"]["agent"]["function_call"]["response"] = {
                "content": content,
                "timestamp": time.time()
            }

            logger.info(f"Processed function call response: {function_call.get('id')}")
            return response_msg

        else:
            content = agent.get("response", "") or agent.get("content", "")

            response_msg = msg.copy()
            response_msg["payload"]["agent"]["response"] = content
            response_msg["payload"]["timestamp"] = time.time()

            logger.info("Processed direct LLM response")
            return response_msg

    except Exception as e:
        logger.error(f"Error processing response request: {e}")

        error_msg = msg.copy()
        error_msg["payload"]["agent"]["response"] = "I'm experiencing technical difficulties. Please try again."
        error_msg["payload"]["error"] = str(e)
        error_msg["payload"]["timestamp"] = time.time()
        return error_msg


def main():
    app = Application(
        broker_address=os.getenv("KAFKA_BROKER", "localhost:9092"),
        consumer_group="respond_to_user-group",
        auto_offset_reset="latest"
    )

    input_topic = app.topic("respond_to_user", value_deserializer="json")

    producer = app.get_producer()

    sdf = app.dataframe(input_topic)

    def process_and_respond(msg):
        response = process_response_request(msg)
        if response:
            producer.produce(
                topic="user-responses",
                key=response.get("header", {}).get("id", ""),
                value=json.dumps(response)
            )
            logger.info("Sent response to user-responses topic")

    sdf = sdf.update(process_and_respond)

    logger.info("Starting LLM response service...")
    app.run(sdf)


if __name__ == "__main__":
    main()