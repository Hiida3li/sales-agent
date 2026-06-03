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
                ┌────────────────────┐
   user query   │  client/chat_cli   │  final answer
  ───────────►  │      (client)      │  ◄───────────
                └─────────┬──────────┘
                          │ agent-requests
                          ▼
                ┌────────────────────┐   routes function call
                │   agent service    │ ──────────────┐
                │      (Gemini)      │               │  search_products
                └─────────┬──────────┘               │  search_faqs
                   ▲      │                           │  respond_to_user
   agent-function- │      │                           ▼
   responses       │      │             ┌───────────────────────┐
                   │      │             │  tool service  (xN)   │
                   └──────┴─────────────┤  - search_products    │
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

| Service | Entry point | Consumes | Produces |
|---|---|---|---|
| Client CLI | `client/chat_cli.py` | `respond_to_user` | `agent-requests` |
| Agent / router | `agentkit.services.agent_service` | `agent-requests`, `agent-function-responses` | tool topics, `agent-requests` |
| Product search | `agentkit.services.tool_service` (`TOOL_NAME=search_products`) | `search_products` | `agent-function-responses` |
| FAQ search | `agentkit.services.tool_service` (`TOOL_NAME=search_faqs`) | `search_faqs` | `agent-function-responses` |
| Final response | `agentkit.services.tool_service` (`TOOL_NAME=respond_to_user`) | `respond_to_user` | `user-responses` |
| Orchestrator | `agentkit.services.orchestrator_service` | `agent-responses` | tool topics |

The three tool services are the **same** generic service (`tool_service`) running a different registered tool, selected by the `TOOL_NAME` environment variable.

### Project structure

```
agentkit/                 # the package (clean-architecture layers)
  config.py               # Settings loaded from the environment
  domain/                 # pure data models that mirror the Kafka JSON contract
  llm/                    # LLMProvider interface + GeminiProvider
  messaging/              # MessageBus interface + QuixMessageBus
  tools/                  # Tool interface, ToolRegistry, and the built-in tools
  agent/                  # the decision loop: PromptBuilder, ConversationManager,
                          #   LoopGuard, AgentLoop
  services/               # thin runnable entry points (agent, tool, orchestrator)
client/
  chat_cli.py             # host-side CLI client (kafka-python only)

pyproject.toml            # package metadata, dependencies, entry points
Dockerfile                # one shared image for all backend services
docker-compose.yml        # base stack (Kafka UI behind the "dev" profile)
docker-compose.prod.yml   # production overrides (restart, limits, logging)
Makefile                  # build / up / down / logs / deploy / chat
DEPLOYMENT.md             # single-host deployment guide
.github/workflows/ci.yml  # lint, compile, build, publish image
```

Each layer depends only on abstractions: the agent loop and services are written against the `LLMProvider`, `MessageBus`, and `Tool` interfaces, never against the Gemini or Quix Streams concretions.

### Design

The package follows a clean, layered architecture so that vendors and transports are swappable and capabilities are additive:

- **Domain** (`domain/`) is pure data. Every model serializes to and from the exact JSON shape on the wire, so the message contract lives in one place instead of scattered `dict.get` chains.
- **Abstractions** define the seams: `LLMProvider` (the model), `MessageBus` (transport), and `Tool` (a capability). The agent loop and services depend only on these interfaces.
- **Implementations** are isolated at the edges: `GeminiProvider` is the only module that imports the Gemini SDK; `QuixMessageBus` is the only one that imports Quix Streams. Swapping either means writing one new class, not touching the core.
- **The agent loop** is split into single-purpose collaborators — `PromptBuilder`, `ConversationManager`, `LoopGuard`, and `AgentLoop` — each independently testable, with the loop itself doing no I/O.

How this maps to SOLID:

- **Single responsibility** — reasoning, transport, state, safety, and routing are separate classes rather than one service doing everything.
- **Open/closed** — a new tool is a new `Tool` subclass registered once; the loop, the provider, and the generic tool service are never modified.
- **Liskov / interface segregation** — every `Tool`, `LLMProvider`, and `MessageBus` is substitutable behind a small, focused interface.
- **Dependency inversion** — high-level policy (the agent loop) depends on abstractions; the concrete SDKs depend on those same abstractions, not the other way around.

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

Copy the example and fill in your key:

```bash
cp .env.example .env   # then set GOOGLE_API_KEY
```

### 2. Start the stack

Build and launch Kafka and all backend services:

```bash
make up          # or: docker compose up -d --build
```

To also start the Kafka UI for debugging, use the dev profile:

```bash
make up-dev      # or: docker compose --profile dev up -d --build
```

This brings up ZooKeeper, Kafka, the backend services (`agent-service`, `search-products-service`, `search-faqs-service`, `respond-to-user-service`, `orchestrator-service`), and Redis. All backend services share one built image and differ only by the command and `TOOL_NAME` they run with. Kafka exposes a healthcheck, so the app services wait for the broker before starting; the first boot takes ~30–60s. Topics are auto-created on first use.

Confirm the services are healthy:

```bash
make ps          # docker compose ps
make logs        # docker compose logs -f
```

For a production single-host deployment (always-restart, bounded logging, memory limits), see **[DEPLOYMENT.md](DEPLOYMENT.md)** or run `make deploy`.

### 3. Inspect the message flow (optional)

With the dev profile running (`make up-dev`), open the Kafka UI at `http://localhost:8081` to watch messages move across topics in real time. This is the clearest way to observe the agent loop: a request on `agent-requests`, a tool call on `search_products`, a result on `agent-function-responses`, and a final message on `respond_to_user`.

### 4. Talk to the agent

The client runs on the host and talks to Kafka over `localhost:9092`. Install its dependencies and start it:

```bash
pip install -e ".[client]"   # or: pip install -r requirements_chat.txt
python client/chat_cli.py    # run from the repository root
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
make down        # stop services        (docker compose down)
make down-v      # stop and wipe data   (docker compose down -v)
```

---

## Extending the agent

To add a new capability:

1. Implement a `Tool` subclass in `agentkit/tools/` with a `declaration()` (name, description, JSON-schema parameters) and an `execute(args)` method.
2. Register it in `agentkit/tools/registry.py` (`build_default_registry`). It is now visible to the agent and runnable by the generic tool service.
3. Add its name to `allowed_tools` in `init_payload.json`.
4. Add a service entry to `docker-compose.yml` reusing the shared image with `command: python -u -m agentkit.services.tool_service` and `TOOL_NAME: your_tool`.

No changes to the agent loop, the Gemini provider, or the tool service are required — that is the open/closed payoff of the `Tool` interface and registry. Tools can be scaled, deployed, and replaced independently because every component is decoupled through Kafka.
