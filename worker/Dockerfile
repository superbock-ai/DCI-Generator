# Multi-stage build for optimized container size
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

# Copy worker application code
COPY worker/ ./worker/

# Ensure the virtual environment is in PATH
ENV PATH="/app/.venv/bin:$PATH"

# Create non-root user for security
RUN groupadd --gid 1000 worker && \
    useradd --uid 1000 --gid worker --shell /bin/bash --create-home worker

# Change ownership of app directory
RUN chown -R worker:worker /app

# Switch to non-root user
USER worker

# Set default command
CMD ["python", "worker/main.py", "--help"]