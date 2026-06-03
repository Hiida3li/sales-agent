# Event-Driven Conversational Agent

A distributed, function-calling AI agent for e-commerce customer service, built on Apache Kafka and Google Gemini. The agent answers product and policy questions by reasoning over a conversation, deciding which tools to call, executing those tools as independent stream-processing services, and returning a single grounded reply to the user.

---

## What this is

This project implements a **function-only agent architecture**. The language model is never allowed to answer the user directly. Instead, every action it takes is a function call routed through a message bus. Tools run as separate services, their results are folded back into the conversation context, and the model is invoked again to decide the next step. The loop continues until the model explicitly calls `respond_to_user`, which is the only path that delivers a message to the customer.

This design separates **reasoning** (the model) from **capabilities** (the tools) and from **transport** (Kafka). Each tool is an independent, horizontally scalable consumer. The model service holds no business logic, and the tools hold no conversational state.

The included tools simulate a retail assistant:

- `search_products` — looks up items in a product catalog with optional filters (color, price range).
- `search_faqs` — searches a knowledge base of frequently asked questions (shipping, returns, warranty, payment, store hours).
- `respond_to_user` — formats and delivers the final answer.

The product and FAQ data are mocked in code, so the system runs end to end with no external database. The tool services are deliberately thin wrappers, intended to be replaced with real catalog and knowledge-base backends.

---

## Who it is for

This is a reference implementation and a starting point for teams that need a **scalable, decoupled agent backend** rather than a single monolithic chatbot script.

It is useful for:

- **E-commerce and retail** teams building automated customer service that can search inventory and answer policy questions.
- **Platform and infrastructure engineers** who want an agent that scales tool execution independently of model inference, with each tool deployable, monitored, and rate-limited on its own.
- **AI and applied-ML engineers** studying function-calling orchestration, tool routing, and stateful multi-turn agent loops on a streaming backbone.
- **Any business** with structured data (catalogs, FAQs, order systems, CRMs) that wants a conversational front end where each data source is exposed as a separately maintained tool service.

The architecture generalizes beyond retail. Replacing the mock tools with real integrations turns it into a general framework for tool-using agents on Kafka.

---

## How it works

The system is a set of small Python services that communicate only through Kafka topics. No service calls another directly.

```
                ┌─────────────┐
   user query   │  chat_cli   │  final answer
  ───────────►  │  (client)   │  ◄───────────
                └──────┬──────┘
                       │ agent-requests
                       ▼
                ┌─────────────┐   routes function call
                │ llm_service │ ──────────────┐
                │  (Gemini)   │               │  search_products
                └──────┬──────┘               │  search_faqs
                  ▲    │                       │  respond_to_user
   agent-function-     │                       ▼
   responses     │     │             ┌───────────────────────┐
                 │     │             │  tool services        │
                 └─────┴─────────────┤  - search_products    │
                                     │  - search_faqs        │
                                     │  - respond_to_user    │
                                     └───────────────────────┘
```

The control loop:

1. The client (`client/chat_cli.py`) publishes a request payload to the `agent-requests` topic. The payload carries the conversation context, the system prompt, and the list of tools the agent is allowed to use.
2. The agent service consumes the request, builds the prompt from the context, and calls Gemini with function calling forced on (`mode='ANY'`). The model must return one or more function calls.
3. The service queues those calls and routes the first one to a topic named after the function (for example, `search_products`).
4. The matching tool service consumes the call, does its work, attaches the result, and publishes to `agent-function-responses`.
5. The agent service consumes the response, merges the result into the conversation context, and either routes the next queued tool or, when the batch is complete, sends the payload back to `agent-requests` for the next reasoning step.
6. The loop repeats until the model calls `respond_to_user`, whose service delivers the final content. The client consumes it and prints the answer.

Safety and termination are built in: after a hard ceiling of interactions the agent forces a `respond_to_user` to prevent infinite loops, and the prompt instructs the model never to repeat a tool call with identical arguments.

Conversation state — history, interactions, and per-function execution status (`queued`, `pending`, `completed`) — travels inside the message payload itself, so any service instance can process any message without shared session storage.

### Services

| Service | File | Consumes | Produces |
|---|---|---|---|
| Client CLI | `chat_cli.py` | `respond_to_user` | `agent-requests` |
| Model / router | `llm_service.py` | `agent-requests`, `agent-function-responses` | tool topics, `agent-requests` |
| Product search | `search_products_service.py` | `search_products` | `agent-function-responses` |
| FAQ search | `search_faqs_service.py` | `search_faqs` | `agent-function-responses` |
| Final response | `respond_to_user_service.py` | `respond_to_user` | `user-responses` |
| Orchestrator | `orchestrator_service.py` | `agent-responses` | tool topics |

---

## Technologies used

- **Python 3.11** — all services.
- **Apache Kafka** — the message bus that decouples the model from its tools. Run locally via the bundled `docker-compose.yml` (Confluent Kafka 7.5.0 with ZooKeeper).
- **Quix Streams** — the stream-processing framework used by the backend services to consume, transform, and produce Kafka messages.
- **Google Gemini (`gemini-2.5-flash`)** via the `google-genai` SDK — the reasoning model, driven entirely through function calling.
- **kafka-python** — the lightweight producer/consumer used by the interactive CLI client.
- **Docker and Docker Compose** — containerization and local orchestration of Kafka, the UI, Redis, and all backend services.
- **Kafka UI** (provectuslabs) — a web console for inspecting topics and messages, exposed on port `8081`.
- **Redis** — provisioned in the compose stack for future stateful extensions.

---

## How to run and test it

### Prerequisites

- Docker and Docker Compose
- A Google Gemini API key
- Python 3.11+ (only needed for the local CLI client)

### 1. Configure environment

Create a `.env` file in the project root:

```
GOOGLE_API_KEY=your_gemini_api_key_here
KAFKA_BROKER=localhost:9092
```

### 2. Start the stack

Build and launch Kafka and all backend services:

```bash
docker compose up -d --build
```

Or use the helper script, which tears down volumes first for a clean start:

```bash
./run.sh
```

This brings up ZooKeeper, Kafka, the backend services (`llm-service`, `search-products-service`, `search-faqs-service`, `respond-to-user-service`, `orchestrator-service`), Redis, and Kafka UI. Topics are auto-created on first use.

Confirm the services are healthy:

```bash
docker compose ps
docker compose logs -f llm-service
```

### 3. Inspect the message flow (optional)

Open the Kafka UI at `http://localhost:8081` to watch messages move across topics in real time. This is the clearest way to observe the agent loop: a request on `agent-requests`, a tool call on `search_products`, a result on `agent-function-responses`, and a final message on `respond_to_user`.

### 4. Talk to the agent

The client runs on the host and talks to Kafka over `localhost:9092`. Install its dependencies and start it:

```bash
pip install -r requirements_chat.txt
python chat_cli.py
```

Then chat:

```
You: do you have the iPhone 15 Pro in red?
You: what is your return policy?
```

The CLI streams workflow progress (which tools are queued, pending, and completed) and prints the final agent reply. Every request and response payload is also written to `sessions/` as timestamped JSON, which is useful for debugging the exact state passed between services.

### CLI commands

| Command | Action |
|---|---|
| `/status` | Show current payload state |
| `/history` | Show the conversation history |
| `/files` | List saved payload files |
| `/check` | Manually poll for new responses |
| `/clear` | Reset the conversation |
| `/help` | List commands |
| `/quit` | Exit |

### Testing and exploration

- `init_payload.json` defines the initial agent payload: the system prompt, the allowed tools, and an empty context. Edit it to change the agent's persona, instructions, or available tools.
- The notebooks (`tst_gemini_client.ipynb`, `kafka_test.ipynb`, `tst_mh.ipynb`) contain scratch experiments for the Gemini client and Kafka connectivity, useful for verifying credentials and broker reachability in isolation.
- Saved payloads under `sessions/` let you replay or diff the exact request and response messages from a run.

### Shut down

```bash
docker compose down        # stop services
docker compose down -v     # stop and remove Kafka data volumes
```

---

## Extending the agent

To add a new capability:

1. Declare the tool as a `FunctionDeclaration` in `llm_service.py` and add it to `TOOLS`.
2. Add its name to `allowed_tools` in `init_payload.json`.
3. Create a new service that consumes the tool's topic, performs the work, attaches the result under `function_call.response`, and publishes to `agent-function-responses`. The existing search services are the template.
4. Add the service to `docker-compose.yml`.

Because every component is decoupled through Kafka, a new tool requires no changes to the model service beyond its declaration, and tools can be scaled, deployed, and replaced independently.
