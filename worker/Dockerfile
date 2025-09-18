# Development-friendly Dockerfile with dependency caching
FROM python:3.12-slim

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

# Set working directory
WORKDIR /app

# Copy dependency files for caching
COPY pyproject.toml ./
COPY uv.lock ./

# Pre-install dependencies (will be cached in Docker layer)
RUN uv sync --frozen --no-cache

# Create non-root user for security
RUN groupadd --gid 1000 worker && \
    useradd --uid 1000 --gid worker --shell /bin/bash --create-home worker

# Change ownership of app directory
RUN chown -R worker:worker /app

# Switch to non-root user
USER worker

# Create directories that will be mounted
RUN mkdir -p /app/worker/debug /app/worker/exports

# Set default command (will be overridden by docker-compose)
CMD ["python", "worker/worker.py"]