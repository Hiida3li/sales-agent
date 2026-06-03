"""Orchestrator service.

A standalone router that consumes ``agent-responses`` and advances any queued
function in the latest interaction: it flips the first queued execution to
pending and routes a call to that function's topic. The primary flow routes
batches inside the agent service itself; this component remains for setups that
drive routing through the interaction's execution states.
"""

from __future__ import annotations

import logging
import time

from agentkit.config import settings
from agentkit.domain.enums import ExecutionStatus
from agentkit.domain.function_call import FunctionCall
from agentkit.domain.message import Message
from agentkit.messaging.base import MessageBus
from agentkit.messaging.quix_bus import QuixMessageBus

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

RESPONSES_TOPIC = "agent-responses"


class OrchestratorService:
    def __init__(self, bus: MessageBus):
        self.bus = bus

    def handle(self, raw: dict) -> None:
        message = Message.from_dict(raw)
        interaction = message.agent.context.latest_interaction()
        if interaction is None:
            logger.info("No interactions to orchestrate")
            return

        for execution in interaction.function_executions:
            if execution.execution_status == ExecutionStatus.QUEUED:
                logger.info(f"Found queued function: {execution.function_name}")
                execution.execution_status = ExecutionStatus.PENDING
                execution.started_at = time.time()

                message.agent.current_function_execution = FunctionCall(
                    name=execution.function_name,
                    args=execution.parameters,
                    id=execution.execution_id,
                )
                self.bus.produce(
                    execution.function_name, message.key, message.to_dict()
                )
                logger.info(f"Routed next function {execution.function_name}")
                return

        logger.info("No more functions to execute, workflow complete")

    def run(self) -> None:
        self.bus.consume(RESPONSES_TOPIC, self.handle)
        logger.info("Starting orchestrator service...")
        self.bus.run()


def main() -> None:
    bus = QuixMessageBus(
        broker_address=settings.kafka_broker,
        consumer_group="orchestrator-group",
    )
    OrchestratorService(bus).run()


if __name__ == "__main__":
    main()
