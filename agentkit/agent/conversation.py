"""Mutates the conversation Context as the agent loop progresses.

This owns the bookkeeping that the legacy ``process_llm_request`` and
``process_function_results`` performed: recording interactions, scheduling
executions, and folding tool results and history lines back into the context.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, List, Optional

from agentkit.domain.context import Context
from agentkit.domain.enums import ExecutionStatus
from agentkit.domain.function_call import FunctionCall, FunctionExecution
from agentkit.domain.interaction import Interaction
from agentkit.llm.result import LLMResult

logger = logging.getLogger(__name__)


@dataclass
class ResultStatus:
    all_complete: bool
    interaction_id: Optional[str]


class ConversationManager:
    def record_llm_step(
        self, context: Context, result: LLMResult
    ) -> List[FunctionCall]:
        """Record one LLM decision: store the interaction, schedule executions
        (first ``pending``, the rest ``queued``), and append history lines.
        Returns the requested function calls in order.
        """
        interaction = Interaction(
            user_query=context.query,
            llm_reasoning=result.reasoning or "No reasoning provided",
            text_responses=result.text_parts,
        )

        for i, call in enumerate(result.function_calls):
            status = ExecutionStatus.PENDING if i == 0 else ExecutionStatus.QUEUED
            interaction.function_executions.append(
                FunctionExecution.from_call(call, status)
            )

        context.add_interaction(interaction)

        # History: reasoning lines first, then the tool calls (legacy order).
        for text in result.text_parts:
            if text:
                context.append_history(f"Assistant Reasoning: {text}")
        for call in result.function_calls:
            args_str = ", ".join(f"{k}={v}" for k, v in call.args.items())
            context.append_history(f"Tool Call: {call.name}({args_str})")

        logger.info(f"LLM decided on {len(result.function_calls)} function calls")
        return result.function_calls

    def record_function_result(
        self, context: Context, execution_id: str, result: Any
    ) -> ResultStatus:
        """Mark an execution completed, append its result to history, and report
        whether its interaction's whole batch is now complete.
        """
        interaction, execution = context.find_execution(execution_id)
        if execution is None or interaction is None:
            logger.warning(f"Could not find execution with ID {execution_id}")
            return ResultStatus(all_complete=False, interaction_id=None)

        execution.mark_completed(result)
        context.append_history(f"Tool Response: {json.dumps(result)}")

        all_complete = interaction.all_complete()
        if all_complete:
            interaction.all_executions_completed = True
            logger.info(
                f"All {len(interaction.function_executions)} functions completed "
                f"for interaction {interaction.interaction_id}"
            )

        return ResultStatus(
            all_complete=all_complete, interaction_id=interaction.interaction_id
        )
