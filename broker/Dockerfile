# Multi-stage build for FastAPI broker
FROM python:3.12-slim AS builder

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

# Set working directory
WORKDIR /app

# Copy dependency files from parent directory (build context must be parent)
COPY pyproject.toml ./
COPY uv.lock ./

# Install dependencies
RUN uv sync --frozen --no-cache

# Production stage
FROM python:3.12-slim

# Install uv in production stage
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

# Set working directory
WORKDIR /app

# Copy the virtual environment from builder stage
COPY --from=builder /app/.venv /app/.venv

# Copy project files
COPY pyproject.toml ./
COPY uv.lock ./

# Copy broker application code
COPY broker/main.py ./

# Copy worker tasks for Celery integration
COPY worker/celery_app.py ./worker/celery_app.py
COPY worker/tasks.py ./worker/tasks.py
COPY worker/worker_main.py ./worker/worker_main.py
COPY worker/directus_tools.py ./worker/directus_tools.py
COPY worker/graphql/ ./worker/graphql/

# Ensure the virtual environment is in PATH
ENV PATH="/app/.venv/bin:$PATH"

# Create non-root user for security
RUN groupadd --gid 1000 broker && \
    useradd --uid 1000 --gid broker --shell /bin/bash --create-home broker

# Change ownership of app directory
RUN chown -R broker:broker /app

# Switch to non-root user
USER broker

# Expose port
EXPOSE 8000

# Set default command to run FastAPI with uvicorn
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]