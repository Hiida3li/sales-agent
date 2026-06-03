"""Gemini implementation of :class:`LLMProvider`.

This is the only module that knows about the ``google-genai`` SDK. It converts
provider-agnostic tool declarations into genai types, forces function calling
(``mode='ANY'``), retries on failure, and parses the response parts into an
:class:`LLMResult`.
"""

from __future__ import annotations

import logging
import time
import uuid
from typing import Any, Dict, List, Optional

from google import genai
from google.genai import types

from agentkit.domain.function_call import FunctionCall
from agentkit.llm.base import LLMProvider
from agentkit.llm.result import LLMResult

logger = logging.getLogger(__name__)


class GeminiProvider(LLMProvider):
    def __init__(
        self,
        api_key: str,
        model_name: str = "gemini-2.5-flash",
        temperature: float = 0,
        max_output_tokens: int = 10000,
        max_retries: int = 3,
    ):
        if not api_key:
            raise ValueError("GOOGLE_API_KEY required")
        self.client = genai.Client(api_key=api_key)
        self.model_name = model_name
        self.temperature = temperature
        self.max_output_tokens = max_output_tokens
        self.max_retries = max_retries

    @staticmethod
    def _build_tools(declarations: List[Dict[str, Any]]) -> List[types.Tool]:
        """Turn agnostic declaration dicts into a single genai Tool."""
        function_declarations = [
            types.FunctionDeclaration(
                name=d["name"],
                description=d.get("description", ""),
                parameters_json_schema=d.get("parameters", {}),
            )
            for d in declarations
        ]
        return [types.Tool(function_declarations=function_declarations)]

    def generate(
        self,
        prompt: str,
        declarations: List[Dict[str, Any]],
        allowed_function_names: Optional[List[str]] = None,
    ) -> LLMResult:
        tools = self._build_tools(declarations)
        config = types.GenerateContentConfig(
            tools=tools,
            tool_config=types.ToolConfig(
                function_calling_config=types.FunctionCallingConfig(
                    mode="ANY",  # always require function calls
                    allowed_function_names=allowed_function_names,
                )
            ),
            temperature=self.temperature,
            max_output_tokens=self.max_output_tokens,
        )

        for attempt in range(self.max_retries):
            try:
                response = self.client.models.generate_content(
                    model=self.model_name,
                    contents=prompt,
                    config=config,
                )
                return self._parse(response)
            except Exception as e:  # noqa: BLE001 - retry on any SDK error
                logger.error(f"Attempt {attempt + 1} failed: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(1)

        logger.warning("All generation attempts failed; returning empty result")
        return LLMResult()

    @staticmethod
    def _parse(response: Optional[types.GenerateContentResponse]) -> LLMResult:
        """Extract every text and function-call part from the response."""
        result = LLMResult()

        if not response or not response.candidates:
            logger.warning("No candidates in response")
            return result

        for candidate in response.candidates:
            for part in candidate.content.parts:
                if getattr(part, "text", None):
                    text = part.text.strip()
                    result.text_parts.append(text)
                    if not result.reasoning:
                        result.reasoning = text
                    logger.debug(f"Found text part: {text[:100]}...")
                elif getattr(part, "function_call", None):
                    fc = part.function_call
                    result.function_calls.append(
                        FunctionCall(
                            name=fc.name,
                            args=GeminiProvider._extract_args(fc.args),
                            id=str(uuid.uuid4()),
                        )
                    )
                    logger.info(f"Found function call: {fc.name}")

        logger.info(
            f"Extracted {len(result.text_parts)} text parts and "
            f"{len(result.function_calls)} function calls"
        )
        return result

    @staticmethod
    def _extract_args(raw_args: Any) -> Dict[str, Any]:
        """Normalize genai function-call args into a plain dict."""
        if raw_args is None:
            return {}
        if isinstance(raw_args, dict):
            return raw_args

        args: Dict[str, Any] = {}
        if hasattr(raw_args, "items"):
            for key, value in raw_args.items():
                if hasattr(value, "string_value"):
                    args[key] = value.string_value
                elif hasattr(value, "struct_value"):
                    nested = {}
                    for nk, nv in value.struct_value.fields.items():
                        nested[nk] = (
                            nv.string_value if hasattr(nv, "string_value") else str(nv)
                        )
                    args[key] = nested
                else:
                    args[key] = str(value)
            return args

        try:
            return dict(raw_args)
        except (TypeError, ValueError):
            return {}
