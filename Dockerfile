FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the package. All backend services share this one image and differ only
# by the command they run (see docker-compose.yml).
COPY agentkit ./agentkit

# Default service; overridden per service via `command` in docker-compose.
CMD ["python", "-u", "-m", "agentkit.services.agent_service"]
