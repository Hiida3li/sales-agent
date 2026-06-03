# Convenience commands for building, running, and deploying the stack.
# Usage: `make <target>`. Run `make help` to list targets.

COMPOSE      := docker compose
PROD_COMPOSE := docker compose -f docker-compose.yml -f docker-compose.prod.yml

.DEFAULT_GOAL := help
.PHONY: help build up up-dev down down-v restart logs ps deploy chat lint compile install-dev clean

help: ## List available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

build: ## Build the shared service image
	$(COMPOSE) build

up: ## Start the stack in the background (no Kafka UI)
	$(COMPOSE) up -d --build

up-dev: ## Start the stack plus the Kafka UI (http://localhost:8081)
	$(COMPOSE) --profile dev up -d --build

down: ## Stop the stack
	$(COMPOSE) down

down-v: ## Stop the stack and remove Kafka data volumes
	$(COMPOSE) down -v

restart: down up ## Restart the stack

logs: ## Tail logs for all services
	$(COMPOSE) logs -f

ps: ## Show service status
	$(COMPOSE) ps

deploy: ## Build and start with production overrides (restart/limits/logging)
	$(PROD_COMPOSE) up -d --build

chat: ## Run the host-side CLI client (needs the stack up)
	python client/chat_cli.py

install-dev: ## Install the package with client + dev extras locally
	pip install -e ".[client,dev]"

lint: ## Run ruff over the package and client
	ruff check agentkit client

compile: ## Byte-compile all sources (quick syntax gate)
	python -m py_compile $$(find agentkit client -name '*.py')

clean: ## Remove Python caches and build artifacts
	find . -type d -name __pycache__ -prune -exec rm -rf {} + 2>/dev/null || true
	rm -rf build dist *.egg-info .ruff_cache
