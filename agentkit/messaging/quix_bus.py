"""Quix Streams implementation of :class:`MessageBus`.

Wraps a single ``quixstreams.Application``. Each :meth:`consume` registration
creates a JSON-deserialized input topic and attaches the handler; :meth:`run`
starts the underlying application, which drives every registered topic.
"""

from __future__ import annotations

import json
import logging

from quixstreams import Application

from agentkit.messaging.base import Handler, MessageBus

logger = logging.getLogger(__name__)


class QuixMessageBus(MessageBus):
    def __init__(self, broker_address: str, consumer_group: str, auto_offset_reset: str = "latest"):
        self.app = Application(
            broker_address=broker_address,
            consumer_group=consumer_group,
            auto_offset_reset=auto_offset_reset,
        )
        self._producer = self.app.get_producer()

    def consume(self, topic: str, handler: Handler) -> None:
        input_topic = self.app.topic(topic, value_deserializer="json")
        sdf = self.app.dataframe(input_topic)
        sdf.update(handler)
        logger.info(f"Subscribed to topic '{topic}'")

    def produce(self, topic: str, key: str, value: dict) -> None:
        self._producer.produce(topic=topic, key=key, value=json.dumps(value))
        logger.info(f"Produced message to '{topic}'")

    def run(self) -> None:
        self.app.run()
