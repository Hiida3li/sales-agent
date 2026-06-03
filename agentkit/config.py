"""Centralized configuration loaded from the environment.

Keeps env-var names and defaults in one place so services never read
``os.getenv`` ad hoc. Values mirror the defaults used by the legacy scripts.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


@dataclass
class Settings:
    kafka_broker: str = os.getenv("KAFKA_BROKER", "localhost:9092")
    google_api_key: str = os.getenv("GOOGLE_API_KEY", "")
    gemini_model: str = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
    max_interactions: int = int(os.getenv("MAX_INTERACTIONS", "10"))
    temperature: float = float(os.getenv("LLM_TEMPERATURE", "0"))
    max_output_tokens: int = int(os.getenv("LLM_MAX_OUTPUT_TOKENS", "10000"))
    tool_name: str = os.getenv("TOOL_NAME", "")
    web_host: str = os.getenv("WEB_HOST", "0.0.0.0")
    web_port: int = int(os.getenv("WEB_PORT", "8800"))


settings = Settings()
