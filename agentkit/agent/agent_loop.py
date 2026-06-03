"""The agent decision loop.

Pure routing logic with no I/O: given a decoded :class:`Message`, it decides
which topic the message should go to next and returns the (mutated) message.
The service layer is responsible for the actual produce. This separation keeps
the loop fully unit-testable and independent of Kafka.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from agentkit.agent.conversation import ConversationManager
from agentkit.agent.loop_guard import LoopGuard
from agentkit.agent.prompt_builder import PromptBuilder
from agentkit.domain.function_call import FunctionCall
from agentkit.domain.message import Message
from agentkit.llm.base import LLMProvider
from agentkit.tools.registry import ToolRegistry, default_registry

logger = logging.getLogger(__name__)

REQUESTS_TOPIC = "agent-requests"
RESPOND_TOPIC = "respond_to_user"


@dataclass
class RoutingDecision:
    """Where a message should be produced next, and the message to send."""

    topic: str
    message: Message


class AgentLoop:
    def __init__(
        self,
        provider: LLMProvider,
        registry: ToolRegistry = default_registry,
        prompt_builder: PromptBuilder | None = None,
        conversation: ConversationManager | None = None,
        loop_guard: LoopGuard | None = None,
    ):
        self.provider = provider
        self.registry = registry
        self.prompt_builder = prompt_builder or PromptBuilder()
        self.conversation = conversation or ConversationManager()
        self.loop_guard = loop_guard or LoopGuard()

    def on_request(self, message: Message) -> RoutingDecision:
        """Handle an inbound agent request. Always routes to a function."""
        agent = message.agent
        context = agent.context

        # Safety: force a final response after too many interactions.
        if self.loop_guard.should_force_respond(context):
            forced = self.loop_guard.build_forced_response(context)
            agent.current_function_execution = forced
            agent.remaining_function_calls = []
            return RoutingDecision(RESPOND_TOPIC, message)

        prompt = self.prompt_builder.build(context, agent.prompt)
        result = self.provider.generate(
            prompt, self.registry.declarations(), context.allowed_tools
        )
        calls = self.conversation.record_llm_step(context, result)

        if calls:
            agent.set_batch(calls)
            logger.info(f"Routing to function: {calls[0].name}")
            return RoutingDecision(calls[0].name, message)

        # No calls returned violates the function-only design; fall back.
        logger.error("No function calls returned by LLM - this violates the design")
        return self._fallback(
            message, "I apologize, but I couldn't process your request properly."
        )

    def on_function_response(self, message: Message) -> RoutingDecision:
        """Handle a tool result: advance the batch or return to the agent."""
        agent = message.agent
        function_call = agent.function_call

        if not function_call or "response" not in function_call:
            logger.warning(
                "Function response missing - routing to respond_to_user as fallback"
            )
            return self._fallback(message, "An error occurred processing your request.")

        status = self.conversation.record_function_result(
            agent.context, function_call.get("id"), function_call["response"]
        )

        remaining = agent.remaining_function_calls
        if remaining and not status.all_complete:
            next_call = remaining[0]
            agent.current_function_execution = next_call
            agent.remaining_function_calls = remaining[1:]
            logger.info(f"Executing next function in batch: {next_call.name}")
            return RoutingDecision(next_call.name, message)

        if status.all_complete:
            logger.info("All functions complete, routing back to agent-requests")
            agent.clear_routing_state()
            return RoutingDecision(REQUESTS_TOPIC, message)

        logger.error("Unexpected state - routing back to agent")
        return RoutingDecision(REQUESTS_TOPIC, message)

    @staticmethod
    def _fallback(message: Message, content: str) -> RoutingDecision:
        message.agent.current_function_execution = FunctionCall(
            name="respond_to_user", args={"content": content}
        )
        message.agent.remaining_function_calls = []
        return RoutingDecision(RESPOND_TOPIC, message)
