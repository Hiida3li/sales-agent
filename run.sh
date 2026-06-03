#!/usr/bin/env bash
# Clean local restart: tear down (incl. volumes), rebuild, and start the stack
# with the Kafka UI enabled (dev profile). For production use `make deploy`.
set -euo pipefail

docker compose down -v
docker compose --profile dev up -d --build
