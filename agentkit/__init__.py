"""agentkit — an event-driven, function-calling agent framework on Kafka.

The package is organized into layers that depend only on abstractions:

- ``domain``    pure data models (no I/O) that mirror the Kafka message contract
- ``llm``       the language-model abstraction and its Gemini implementation
- ``messaging`` the message-bus abstraction and its Quix Streams implementation
- ``tools``     the Tool interface, a registry, and the concrete tools
- ``agent``     the agent decision loop, split into focused collaborators
- ``services``  thin runnable entrypoints that wire the layers together
"""

__version__ = "1.0.0"
