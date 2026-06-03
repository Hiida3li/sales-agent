# Deployment

This guide covers deploying the stack to a single host with Docker Compose.
All backend services run from one shared image and communicate over Kafka.

## Prerequisites

- A host with Docker Engine and the Docker Compose plugin
- A Google Gemini API key
- Inbound access to the ports you intend to expose (see below)

## 1. Configure

Clone the repository onto the host and create the environment file:

```bash
cp .env.example .env
# edit .env and set GOOGLE_API_KEY
```

`KAFKA_BROKER` in `.env` only affects processes you run directly on the host
(such as the CLI client). Inside the containers, Compose sets it to
`kafka:29092` automatically.

## 2. Deploy

Build and start everything with the production overrides (always-restart,
bounded logging, memory limits):

```bash
make deploy
# equivalent to:
# docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build
```

Check status and logs:

```bash
docker compose ps
docker compose logs -f agent-service
```

Services start in order: Kafka must pass its healthcheck before the agent and
tool services boot. The first start therefore takes ~30–60s.

## 3. Verify

Open the web UI in a browser:

```
http://localhost:8800
```

Click **Start chatting** and ask Aura a couple of questions
(`do you have the iPhone 15 Pro in red?`, `what is your return policy?`).

Or use the terminal client from the host (Python 3.11+):

```bash
pip install -e ".[client]"   # or: pip install -r requirements_chat.txt
python client/chat_cli.py
```

Request and response payloads are written to `sessions/` for inspection.

## What runs

| Container | Role |
|---|---|
| `zookeeper`, `kafka` | message bus |
| `agent-service` | model reasoning + routing |
| `search-products-service` | `search_products` tool |
| `search-faqs-service` | `search_faqs` tool |
| `respond-to-user-service` | delivers final answers |
| `orchestrator-service` | queued-function router |
| `web` | browser chat UI (FastAPI, port 8800) |
| `redis` | provisioned for future stateful use |

The Kafka UI is **not** started in production. To bring it up temporarily for
debugging:

```bash
docker compose --profile dev up -d kafka-ui   # http://localhost:8081
```

## Ports

| Port | Service | Notes |
|---|---|---|
| 8800 | Web UI | the browser chat interface |
| 9092 | Kafka (host listener) | needed only if the CLI runs on the host |
| 29092 | Kafka (internal) | container-to-container |
| 6379 | Redis | unused by the app today |
| 8081 | Kafka UI | dev profile only |

For a locked-down host, expose only 8800 (the chat UI) publicly; restrict the
rest with the firewall or bind them to `127.0.0.1`.

## Operations

```bash
make logs            # tail all logs
make ps              # service status
make down            # stop
make down-v          # stop and wipe Kafka data
make restart         # stop then start
```

To update after pulling new code, rebuild and recreate:

```bash
make deploy
```

## Images via CI

`.github/workflows/ci.yml` lints, compiles, and builds the image on every
push, and on the default branch publishes it to GitHub Container Registry
(`ghcr.io/<owner>/<repo>`) using the built-in `GITHUB_TOKEN`. To deploy a
pre-built image instead of building on the host, set `image:` on the services
to the published tag and drop the `build:` line from `agent-service`.
