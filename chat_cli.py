#!/usr/bin/env python3
"""
CLI Chat Interface for LLM Agent
Reads init_payload.json, takes user input, sends to Kafka, and displays responses
"""

import json
import time
import uuid
import threading
import queue
import os
from datetime import datetime
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError

class ChatCLI:
    def __init__(self):
        self.load_initial_payload()
        self.setup_kafka()
        self.message_queue = queue.Queue()
        self.consumer_thread = None
        self.consumer_running = False
        self.final_response_received = False

    def load_initial_payload(self):
        """Load initial payload from init_payload.json"""
        try:
            with open('init_payload.json', 'r') as f:
                self.current_payload = json.load(f)
            print("✅ Loaded initial payload from init_payload.json")
        except FileNotFoundError:
            print("❌ init_payload.json not found. Creating default payload...")
            self.current_payload = {
                "header": {"id": "chat-session", "timestamp": ""},
                "payload": {
                    "agent": {
                        "context": {"query": "", "history": [], "interactions": []},
                        "prompt": "Answer the query: $query"
                    }
                }
            }

    def setup_kafka(self):
        """Setup Kafka producer and consumer"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=['localhost:9092'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("🔌 Kafka producer initialized")
        except Exception as e:
            print(f"❌ Failed to initialize Kafka producer: {e}")
            exit(1)

    def start_consumer(self):
        """Start Kafka consumer in background thread"""
        def consume_messages():
            try:
                consumer = KafkaConsumer(
                    'respond_to_user',
                    bootstrap_servers=['localhost:9092'],
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    auto_offset_reset='latest',
                    consumer_timeout_ms=5000,
                    group_id=f"chat-cli-{int(time.time())}"
                )
                print("👂 Kafka consumer started")

                while self.consumer_running:
                    try:
                        for message in consumer:
                            if not self.consumer_running:
                                break
                            self.message_queue.put(message.value)
                    except Exception as e:
                        if self.consumer_running:
                            print(f"Consumer error: {e}")
                        break
                consumer.close()
            except Exception as e:
                print(f"❌ Failed to start consumer: {e}")

        self.consumer_running = True
        self.consumer_thread = threading.Thread(target=consume_messages, daemon=True)
        self.consumer_thread.start()

    def stop_consumer(self):
        """Stop Kafka consumer"""
        self.consumer_running = False
        if self.consumer_thread:
            self.consumer_thread.join(timeout=2)

    def send_message(self, query):
        """Send message to Kafka and save payload"""
        # Reset final response flag for new query
        self.final_response_received = False
        
        # Update payload
        self.current_payload["header"]["id"] = f"chat-{int(time.time())}"
        self.current_payload["header"]["timestamp"] = datetime.now().isoformat()
        self.current_payload["payload"]["agent"]["context"]["query"] = query

        try:
            # Send to Kafka
            future = self.producer.send('agent-requests', self.current_payload)
            record_metadata = future.get(timeout=10)

            # Save payload step
            os.makedirs('sessions', exist_ok=True)
            filename = self.save_payload_step("request", self.current_payload)
            print(f"📤 Message sent! Saved to: {filename}")
            return True

        except KafkaError as e:
            print(f"❌ Error sending message: {e}")
            return False

    def save_payload_step(self, step_name, payload):
        """Save payload to JSON file"""
        timestamp = int(time.time() * 1000)  # milliseconds for uniqueness
        filename = f"sessions/payload_{step_name}_{timestamp}.json"
        with open(filename, 'w') as f:
            json.dump(payload, f, indent=2)
        return filename

    def extract_final_response(self, payload):
        """Extract final response from respond_to_user function"""
        agent = payload.get("payload", {}).get("agent", {})
        
        # Check current_function_execution first (this seems to be where pending responses are)
        current_execution = agent.get("current_function_execution", {})
        if current_execution.get("name") == "respond_to_user":
            content = current_execution.get("args", {}).get("content", "")
            if content:
                return content
        
        # Check if this is a direct response from respond_to_user
        if agent.get("final_function_call", {}).get("name") == "respond_to_user":
            return agent["final_function_call"]["args"].get("content", "")
        
        # Check interactions for respond_to_user execution
        context = agent.get("context", {})
        interactions = context.get("interactions", [])
        
        for interaction in reversed(interactions):  # Check most recent first
            executions = interaction.get("function_executions", [])
            for execution in executions:
                if (execution.get("function_name") == "respond_to_user" and 
                    execution.get("execution_status") == "completed"):
                    # Get the content from parameters
                    return execution.get("parameters", {}).get("content", "")
        
        # Fallback to the main response field if it exists
        return agent.get("response", "")
    def check_responses(self):
        """Check for and process incoming responses"""
        responses_received = []

        try:
            while not self.message_queue.empty():
                response_payload = self.message_queue.get_nowait()
                responses_received.append(response_payload)

                # Update current payload
                self.current_payload = response_payload

                # Save response payload
                filename = self.save_payload_step("response", response_payload)
                
                # Extract final response
                final_response = self.extract_final_response(response_payload)
                
                if final_response:
                    self.final_response_received = True
                    print(f"\n🤖 Agent: {final_response}\n")
                else:
                    # Display workflow info for intermediate steps
                    self.display_workflow_info(response_payload)

        except queue.Empty:
            pass

        return len(responses_received)

    def display_workflow_info(self, payload):
        """Display workflow information for intermediate steps"""
        context = payload.get("payload", {}).get("agent", {}).get("context", {})
        interactions = context.get("interactions", [])
        
        if interactions:
            latest = interactions[-1]
            executions = latest.get("function_executions", [])
            
            if executions:
                print(f"\n🔄 Processing Functions:")
                for exec in executions:
                    status = exec.get("execution_status", "unknown")
                    name = exec.get("function_name", "unknown")
                    
                    # Show function call with status icon
                    status_icon = "✅" if status == "completed" else "⏳" if status == "pending" else "📋"
                    print(f"   {status_icon} {name}: {status}")
                    
                    # If it's a completed function (not respond_to_user), show brief result
                    if status == "completed" and name != "respond_to_user":
                        result = exec.get("execution_result", {})
                        if isinstance(result, dict) and "formatted_summary" in result:
                            # Show first line of formatted summary
                            summary_lines = result["formatted_summary"].split('\n')
                            if summary_lines:
                                print(f"      → {summary_lines[0]}")

    def display_help(self):
        """Display help information"""
        print("""
🤖 LLM Agent Chat CLI Commands:
  
  <message>     - Send a message to the agent
  /help         - Show this help
  /status       - Show current payload status
  /history      - Show conversation history
  /files        - List saved payload files
  /clear        - Clear conversation history
  /check        - Check for new responses manually
  /quit         - Exit the chat
        """)

    def display_status(self):
        """Display current payload status"""
        context = self.current_payload.get("payload", {}).get("agent", {}).get("context", {})
        print(f"\n📊 Current Status:")
        print(f"   Current Query: {context.get('query', 'None')}")
        print(f"   History Count: {len(context.get('history', []))}")
        print(f"   Interactions Count: {len(context.get('interactions', []))}")
        print(f"   Last Updated: {self.current_payload.get('header', {}).get('timestamp', 'Never')}")
        
        # Show recent function calls
        interactions = context.get("interactions", [])
        if interactions:
            latest = interactions[-1]
            executions = latest.get("function_executions", [])
            if executions:
                print(f"\n   Recent Functions:")
                for exec in executions[-3:]:  # Show last 3
                    print(f"     - {exec.get('function_name')}: {exec.get('execution_status')}")
        print()

    def display_history(self):
        """Display conversation history"""
        context = self.current_payload.get("payload", {}).get("agent", {}).get("context", {})
        history = context.get("history", [])

        if not history:
            print("📭 No conversation history yet.")
            return

        print(f"\n📜 Conversation History ({len(history)} items):")
        
        # Group by interaction type
        for i, item in enumerate(history, 1):
            if isinstance(item, str):
                if item.startswith("Tool Call:"):
                    print(f"  🔧 {item}")
                elif item.startswith("Tool Response:"):
                    print(f"  📋 {item[:100]}{'...' if len(item) > 100 else ''}")
                elif item.startswith("Assistant"):
                    print(f"  💭 {item}")
                else:
                    print(f"  👤 User: {item}")
            else:
                print(f"  📝 {str(item)[:100]}{'...' if len(str(item)) > 100 else ''}")
        print()

    def list_payload_files(self):
        """List saved payload files"""
        if not os.path.exists('sessions'):
            print("📭 No sessions directory found.")
            return
            
        files = [f for f in os.listdir('sessions') if f.startswith('payload_')]
        files.sort(reverse=True)

        if not files:
            print("📭 No payload files found.")
            return

        print(f"\n📁 Saved Payload Files ({len(files)} files):")
        for file in files[:10]:  # Show last 10 files
            filepath = os.path.join('sessions', file)
            size = os.path.getsize(filepath)
            print(f"   {file} ({size} bytes)")

        if len(files) > 10:
            print(f"   ... and {len(files) - 10} more files")
        print()

    def clear_history(self):
        """Clear conversation history"""
        self.current_payload["payload"]["agent"]["context"]["history"] = []
        self.current_payload["payload"]["agent"]["context"]["interactions"] = []
        print("🗑️ Conversation history cleared.")

    def run(self):
        """Main chat loop"""
        print("🚀 Starting LLM Agent Chat CLI...")
        print("Type '/help' for commands or start chatting!")

        # Start consumer
        self.start_consumer()

        try:
            while True:
                # Check for any pending responses
                self.check_responses()

                # Get user input
                user_input = input("\n👤 You: ").strip()

                if not user_input:
                    continue

                if user_input.startswith('/'):
                    command = user_input.lower()

                    if command == '/help':
                        self.display_help()
                    elif command == '/status':
                        self.display_status()
                    elif command == '/history':
                        self.display_history()
                    elif command == '/files':
                        self.list_payload_files()
                    elif command == '/clear':
                        self.clear_history()
                    elif command == '/check':
                        response_count = self.check_responses()
                        if response_count == 0:
                            print("📭 No new responses found.")
                    elif command == '/quit':
                        print("👋 Goodbye!")
                        break
                    else:
                        print(f"❓ Unknown command: {user_input}")
                        print("Type '/help' for available commands.")
                    continue

                # Send message
                success = self.send_message(user_input)
                if success:
                    print("⏳ Waiting for agent response...")

                    # Wait for final response with periodic checking
                    start_time = time.time()
                    max_wait_time = 30  # Maximum 30 seconds
                    
                    while not self.final_response_received and (time.time() - start_time) < max_wait_time:
                        time.sleep(0.5)
                        self.check_responses()
                        
                        # Show progress indicator
                        if not self.final_response_received:
                            elapsed = int(time.time() - start_time)
                            if elapsed % 2 == 0:  # Update every 2 seconds
                                print(f"⏳ Processing... ({elapsed}s)", end='\r', flush=True)
                    
                    if not self.final_response_received:
                        print(f"\n⏱️ Response timeout after {max_wait_time} seconds. Use /check to check for responses.")

        except KeyboardInterrupt:
            print("\n\n💨 Chat interrupted. Goodbye!")
        finally:
            self.stop_consumer()
            self.producer.close()

if __name__ == "__main__":
    chat = ChatCLI()
    chat.run()