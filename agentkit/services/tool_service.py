"""Generic tool service.

Runs a single :class:`Tool` as a Kafka consumer. It replaces the three nearly
identical legacy scripts: the tool to run is resolved from the registry by the
``TOOL_NAME`` env var (or argv), so adding a capability needs no new service.

Flow: consume the tool's topic -> read ``current_function_execution`` ->
execute -> attach the result under ``function_call.response`` -> produce to the
tool's ``response_topic``.
"""

from __future__ import annotations

import logging
import sys
import time

from agentkit.config import settings
from agentkit.domain.message import Message
from agentkit.messaging.base import MessageBus
from agentkit.messaging.quix_bus import QuixMessageBus
from agentkit.tools.base import Tool
from agentkit.tools.registry import default_registry

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class ToolService:
    def __init__(self, tool: Tool, bus: MessageBus):
        self.tool = tool
        self.bus = bus

    def handle(self, raw: dict) -> None:
        message = Message.from_dict(raw)
        current = message.agent.current_function_execution

        if current is None or current.name != self.tool.name:
            got = current.name if current else None
            logger.error(f"Unexpected function call: {got} (expected {self.tool.name})")
            return

        try:
            response = self.tool.execute(current.args)
        except Exception as e:  # noqa: BLE001 - surface tool errors to the agent
            logger.error(f"Error executing {self.tool.name}: {e}", exc_info=True)
            response = {
                "error": str(e),
                "timestamp": time.time(),
                "formatted_summary": f"Error occurred during {self.tool.name}: {e}\n",
            }

        envelope = current.to_dict()
        envelope["response"] = response
        message.agent.function_call = envelope

        self.bus.produce(self.tool.response_topic, message.key, message.to_dict())
        logger.info(f"Sent {self.tool.name} response to '{self.tool.response_topic}'")

    def run(self) -> None:
        self.bus.consume(self.tool.name, self.handle)
        logger.info(f"Starting {self.tool.name} service...")
        self.bus.run()


def main() -> None:
    tool_name = settings.tool_name or (sys.argv[1] if len(sys.argv) > 1 else "")
    if not tool_name:
        raise SystemExit(
            "No tool specified. Set TOOL_NAME or pass the tool name as an argument. "
            f"Available: {default_registry.names()}"
        )

    tool = default_registry.get(tool_name)
    bus = QuixMessageBus(
        broker_address=settings.kafka_broker,
        consumer_group=f"{tool_name}-group",
    )
    ToolService(tool, bus).run()


if __name__ == "__main__":
    main()
