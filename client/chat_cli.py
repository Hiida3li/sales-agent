#!/usr/bin/env python3
"""CLI chat client for the agent.

A thin host-side client (kafka-python, no Quix/Gemini deps). It publishes user
queries to ``agent-requests`` and listens on ``respond_to_user`` for the
agent's final message. Responsibilities are split into focused classes:

- ``KafkaTransport``    produce/consume plumbing
- ``PayloadStore``      persist each request/response payload under ``sessions/``
- ``ResponseExtractor`` pull the final answer / workflow status from a payload
- ``ChatClient``        the REPL that wires them together

Run from the repository root so ``init_payload.json`` and ``sessions/`` resolve:
    python client/chat_cli.py
"""

from __future__ import annotations

import json
import os
import queue
import threading
import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

BOOTSTRAP_SERVERS = ["localhost:9092"]
REQUESTS_TOPIC = "agent-requests"
RESPONSE_TOPIC = "respond_to_user"
SESSIONS_DIR = "sessions"
INIT_PAYLOAD_FILE = "init_payload.json"
MAX_WAIT_SECONDS = 30


class KafkaTransport:
    """Produce to a topic and stream messages from another in the background."""

    def __init__(self, bootstrap_servers: Optional[List[str]] = None):
        self.bootstrap_servers = bootstrap_servers or BOOTSTRAP_SERVERS
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        print("Kafka producer initialized")
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def send(self, topic: str, payload: Dict[str, Any]) -> None:
        future = self.producer.send(topic, payload)
        future.get(timeout=10)

    def start_consumer(self, topic: str, on_message: Callable[[Dict[str, Any]], None]) -> None:
        def consume() -> None:
            try:
                consumer = KafkaConsumer(
                    topic,
                    bootstrap_servers=self.bootstrap_servers,
                    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                    auto_offset_reset="latest",
                    consumer_timeout_ms=5000,
                    group_id=f"chat-cli-{int(time.time())}",
                )
                print("Kafka consumer started")
                while self._running:
                    try:
                        for message in consumer:
                            if not self._running:
                                break
                            on_message(message.value)
                    except Exception as e:  # noqa: BLE001
                        if self._running:
                            print(f"Consumer error: {e}")
                        break
                consumer.close()
            except Exception as e:  # noqa: BLE001
                print(f"Failed to start consumer: {e}")

        self._running = True
        self._thread = threading.Thread(target=consume, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=2)
        self.producer.close()


class PayloadStore:
    """Persist payloads to timestamped JSON files for inspection/replay."""

    def __init__(self, directory: str = SESSIONS_DIR):
        self.directory = directory

    def save(self, step_name: str, payload: Dict[str, Any]) -> str:
        os.makedirs(self.directory, exist_ok=True)
        timestamp = int(time.time() * 1000)
        filename = os.path.join(self.directory, f"payload_{step_name}_{timestamp}.json")
        with open(filename, "w") as f:
            json.dump(payload, f, indent=2)
        return filename

    def list_files(self) -> List[str]:
        if not os.path.exists(self.directory):
            return []
        files = [f for f in os.listdir(self.directory) if f.startswith("payload_")]
        files.sort(reverse=True)
        return files


class ResponseExtractor:
    """Read the agent's final answer and workflow status out of a payload."""

    @staticmethod
    def final_response(payload: Dict[str, Any]) -> str:
        agent = payload.get("payload", {}).get("agent", {})

        current = agent.get("current_function_execution", {})
        if current.get("name") == "respond_to_user":
            content = current.get("args", {}).get("content", "")
            if content:
                return content

        final_call = agent.get("final_function_call", {})
        if final_call.get("name") == "respond_to_user":
            return final_call.get("args", {}).get("content", "")

        interactions = agent.get("context", {}).get("interactions", [])
        for interaction in reversed(interactions):
            for execution in interaction.get("function_executions", []):
                if (
                    execution.get("function_name") == "respond_to_user"
                    and execution.get("execution_status") == "completed"
                ):
                    return execution.get("parameters", {}).get("content", "")

        return agent.get("response", "")

    @staticmethod
    def workflow_lines(payload: Dict[str, Any]) -> List[str]:
        context = payload.get("payload", {}).get("agent", {}).get("context", {})
        interactions = context.get("interactions", [])
        if not interactions:
            return []

        lines: List[str] = []
        for execution in interactions[-1].get("function_executions", []):
            status = execution.get("execution_status", "unknown")
            name = execution.get("function_name", "unknown")
            lines.append(f"   [{status}] {name}")
            if status == "completed" and name != "respond_to_user":
                result = execution.get("execution_result", {})
                if isinstance(result, dict) and "formatted_summary" in result:
                    first = result["formatted_summary"].split("\n")[0]
                    lines.append(f"      -> {first}")
        return lines


class ChatClient:
    """Interactive REPL that drives the agent through Kafka."""

    def __init__(self):
        self.payload = self._load_initial_payload()
        self.transport = KafkaTransport()
        self.store = PayloadStore()
        self.extractor = ResponseExtractor()
        self.inbox: "queue.Queue[Dict[str, Any]]" = queue.Queue()
        self.final_received = False

    @staticmethod
    def _load_initial_payload() -> Dict[str, Any]:
        try:
            with open(INIT_PAYLOAD_FILE) as f:
                payload = json.load(f)
            print(f"Loaded initial payload from {INIT_PAYLOAD_FILE}")
            return payload
        except FileNotFoundError:
            print(f"{INIT_PAYLOAD_FILE} not found. Using default payload.")
            return {
                "header": {"id": "chat-session", "timestamp": ""},
                "payload": {
                    "agent": {
                        "context": {"query": "", "history": [], "interactions": []},
                        "prompt": "Answer the query: $query",
                    }
                },
            }

    def send_message(self, query: str) -> bool:
        self.final_received = False
        self.payload["header"]["id"] = f"chat-{int(time.time())}"
        self.payload["header"]["timestamp"] = datetime.now().isoformat()
        self.payload["payload"]["agent"]["context"]["query"] = query
        try:
            self.transport.send(REQUESTS_TOPIC, self.payload)
            filename = self.store.save("request", self.payload)
            print(f"Message sent. Saved to: {filename}")
            return True
        except KafkaError as e:
            print(f"Error sending message: {e}")
            return False

    def drain_responses(self) -> int:
        count = 0
        try:
            while not self.inbox.empty():
                payload = self.inbox.get_nowait()
                count += 1
                self.payload = payload
                self.store.save("response", payload)

                answer = self.extractor.final_response(payload)
                if answer:
                    self.final_received = True
                    print(f"\nAgent: {answer}\n")
                else:
                    lines = self.extractor.workflow_lines(payload)
                    if lines:
                        print("\nProcessing functions:")
                        print("\n".join(lines))
        except queue.Empty:
            pass
        return count

    # -- commands -----------------------------------------------------------
    def cmd_status(self) -> None:
        context = self.payload.get("payload", {}).get("agent", {}).get("context", {})
        print("\nStatus:")
        print(f"   Query: {context.get('query', 'None')}")
        print(f"   History items: {len(context.get('history', []))}")
        print(f"   Interactions: {len(context.get('interactions', []))}")
        print(f"   Last updated: {self.payload.get('header', {}).get('timestamp', 'Never')}\n")

    def cmd_history(self) -> None:
        history = self.payload.get("payload", {}).get("agent", {}).get("context", {}).get("history", [])
        if not history:
            print("No conversation history yet.")
            return
        print(f"\nConversation history ({len(history)} items):")
        for item in history:
            print(f"  - {str(item)[:120]}")
        print()

    def cmd_files(self) -> None:
        files = self.store.list_files()
        if not files:
            print("No payload files found.")
            return
        print(f"\nSaved payload files ({len(files)}):")
        for f in files[:10]:
            print(f"   {f}")
        if len(files) > 10:
            print(f"   ... and {len(files) - 10} more")
        print()

    def cmd_clear(self) -> None:
        context = self.payload["payload"]["agent"]["context"]
        context["history"] = []
        context["interactions"] = []
        print("Conversation history cleared.")

    @staticmethod
    def cmd_help() -> None:
        print(
            "\nCommands:\n"
            "  <message>   Send a message to the agent\n"
            "  /status     Show current payload status\n"
            "  /history    Show conversation history\n"
            "  /files      List saved payload files\n"
            "  /clear      Clear conversation history\n"
            "  /check      Check for new responses\n"
            "  /quit       Exit\n"
        )

    def _handle_command(self, command: str) -> bool:
        """Return False to quit, True to continue."""
        if command == "/help":
            self.cmd_help()
        elif command == "/status":
            self.cmd_status()
        elif command == "/history":
            self.cmd_history()
        elif command == "/files":
            self.cmd_files()
        elif command == "/clear":
            self.cmd_clear()
        elif command == "/check":
            if self.drain_responses() == 0:
                print("No new responses found.")
        elif command == "/quit":
            print("Goodbye!")
            return False
        else:
            print(f"Unknown command: {command}. Type /help.")
        return True

    def _await_response(self) -> None:
        print("Waiting for agent response...")
        start = time.time()
        while not self.final_received and (time.time() - start) < MAX_WAIT_SECONDS:
            time.sleep(0.5)
            self.drain_responses()
        if not self.final_received:
            print(f"\nResponse timeout after {MAX_WAIT_SECONDS}s. Use /check to retry.")

    def run(self) -> None:
        print("Starting agent chat CLI. Type /help for commands.")
        self.transport.start_consumer(RESPONSE_TOPIC, self.inbox.put)
        try:
            while True:
                self.drain_responses()
                user_input = input("\nYou: ").strip()
                if not user_input:
                    continue
                if user_input.startswith("/"):
                    if not self._handle_command(user_input.lower()):
                        break
                    continue
                if self.send_message(user_input):
                    self._await_response()
        except KeyboardInterrupt:
            print("\nChat interrupted. Goodbye!")
        finally:
            self.transport.stop()


def main() -> None:
    ChatClient().run()


if __name__ == "__main__":
    main()
