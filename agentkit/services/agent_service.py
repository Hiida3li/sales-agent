"""The agent service.

Consumes ``agent-requests`` and ``agent-function-responses``, runs each through
the :class:`AgentLoop`, and produces the resulting message to the topic the
loop selects. This module is pure wiring; all behavior lives in the layers.
"""

from __future__ import annotations

import logging

from agentkit.agent.agent_loop import AgentLoop, RoutingDecision
from agentkit.config import settings
from agentkit.domain.message import Message
from agentkit.llm.gemini import GeminiProvider
from agentkit.messaging.base import MessageBus
from agentkit.messaging.quix_bus import QuixMessageBus

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

REQUESTS_TOPIC = "agent-requests"
FUNCTION_RESPONSES_TOPIC = "agent-function-responses"


class AgentService:
    def __init__(self, bus: MessageBus, loop: AgentLoop):
        self.bus = bus
        self.loop = loop

    def _dispatch(self, decision: RoutingDecision) -> None:
        self.bus.produce(
            decision.topic, decision.message.key, decision.message.to_dict()
        )

    def handle_request(self, raw: dict) -> None:
        decision = self.loop.on_request(Message.from_dict(raw))
        self._dispatch(decision)

    def handle_function_response(self, raw: dict) -> None:
        decision = self.loop.on_function_response(Message.from_dict(raw))
        self._dispatch(decision)

    def run(self) -> None:
        self.bus.consume(REQUESTS_TOPIC, self.handle_request)
        self.bus.consume(FUNCTION_RESPONSES_TOPIC, self.handle_function_response)
        logger.info("=" * 60)
        logger.info("Agent service started - Function-Only Architecture")
        logger.info("  - All interactions MUST use function calls")
        logger.info("  - Only respond_to_user can send final responses")
        logger.info("  - Recursive flow until respond_to_user is called")
        logger.info("=" * 60)
        self.bus.run()


def main() -> None:
    bus = QuixMessageBus(
        broker_address=settings.kafka_broker,
        consumer_group="llm-processor-group",
    )
    provider = GeminiProvider(
        api_key=settings.google_api_key,
        model_name=settings.gemini_model,
        temperature=settings.temperature,
        max_output_tokens=settings.max_output_tokens,
    )
    loop = AgentLoop(provider=provider)
    AgentService(bus, loop).run()


if __name__ == "__main__":
    main()
