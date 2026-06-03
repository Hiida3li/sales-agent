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
# pyproject.toml is the single source of truth for runtime dependencies.
COPY pyproject.toml README.md ./
COPY agentkit ./agentkit
RUN pip install .

# Drop root for runtime.
RUN useradd --create-home --uid 1000 appuser
USER appuser

# Default service; each backend service overrides `command` in docker-compose.
# All services share this one image (built once) and differ only by the
# command and the TOOL_NAME they run with.
CMD ["python", "-u", "-m", "agentkit.services.agent_service"]
