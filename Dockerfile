FROM python:3.11-slim

# Runtime hygiene: no .pyc files, unbuffered stdout for live container logs.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Build-time system dependencies for any packages needing compilation.
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Install the package and its dependencies from the canonical project metadata.
# pyproject.toml is the single source of truth for runtime dependencies. The
# "web" extra adds FastAPI/uvicorn so the one shared image can also run the web
# chat server. init_payload.json is bundled so the web server has the prompt.
COPY pyproject.toml README.md init_payload.json ./
COPY agentkit ./agentkit
RUN pip install ".[web]"

# Drop root for runtime. Make /app writable by the runtime user: Quix Streams
# creates a local state store at /app/state on startup.
RUN useradd --create-home --uid 1000 appuser \
    && chown -R appuser:appuser /app
USER appuser

# Default service; each backend service overrides `command` in docker-compose.
# All services share this one image (built once) and differ only by the
# command and the TOOL_NAME they run with.
CMD ["python", "-u", "-m", "agentkit.services.agent_service"]
