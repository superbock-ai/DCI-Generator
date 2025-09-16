# Goal

Prepare my Python-based AI document extraction and seeding tool for production as a scalable, containerized Celery worker with API control. The app must run extraction or cleanup jobs triggered via a FastAPI broker, with parameterizable jobs and robust logging.

---

## Requirements

- Use my existing `directus_tools.py` for all Directus operations (REST/GraphQL).
- Logging via current `print()` statements is sufficient.
- The API server should use FastAPI.
- Testing scripts for each feature/sub-feature are required; automated pytest is optional but appreciated.
- Expose all configs and secrets via environment variables in `.env` files.
- Workers should remain alive and idle between jobs.
- All containers should have their own directory in the project structure.

---

## Features

### Feature 1: Document Analyzer containerized

- Refactor the app to run as a Docker container 
- Create a Dockerfile to build the container
- The application including all its files and folders should move into a worker/ folder
    - debug/*
    - exports/*
    - graphql/*
    - directus_tools.py
    - main.py
- The .env and .env.example files should remain in the root directory

### Feature 2: Celery Broker Queue (Redis)

- Provide a Redis container for Celery communication (celery will be implemented in Feature 3 + 4).
- Worker and API broker must read the Redis URL and Celery settings from `.env`.

### Feature 3: Celery Worker (Containerized)

- Refactor the app you containerized in Feature 1 to run as a Celery worker.
- On receiving a Celery job, accept all parameters that are currently accepted as CLI arguments when running `main.py` and then are analyzed using argparse.
- Celery worker must support:
    - Analysis job (params: at least `product_id`, with support for `debug`, `seed`, `chunk_size`, etc.)
    - Cleanup job (deleting in Directus, param: `product_id` at minimum)
- Job results must follow:
    - Analysis: JSON `{ 'num_segments': int, 'num_benefits': int, 'num_limits': int, 'seeding_success': bool }`
    - Cleanup: JSON `{ 'success': true | false }`
- All progress and key steps must be logged with `print()` and viewable in docker logs.

### Feature 4: FastAPI Broker (Containerized)

- Container logic should sit in broker/
- Expose endpoints:
    - `POST /jobs/analysis`: Accepts JSON with `product_id` (and any other analysis parameters your app supports).
    - `POST /jobs/cleanup`: Accepts JSON with `product_id`.
    - `GET /jobs/{job_id}/status`: Returns job completion result or current status.
- Authenticate all API calls using JWT, validated with `DIRECTUS_SECRET` provided via `.env`.
- Only support HTTP (no SSL needed).
- Expose a minimal OpenAPI schema via FastAPI auto-generated docs.

### Feature 5: Docker Compose

- Provide a `docker-compose.yml` bringing up the API broker, Celery worker(s), and Redis together.
- Must allow scaling up worker replicas easily.


---

## Workflow

- Start with a markdown plan detailing how each feature and sub-feature will be divided into testable iterations. Each sub-feature should be noted down as User Stories including Goal, Purpose, and Acceptance Criteria. **Stop and wait for my approval after writing this plan.**
- After each implemented sub-feature, pause for me to test. Do not continue until I approve.
- Once I approve:
    - update `README.md`
    - update Project plan markdown with your current progress
    - perform git commit and push
- Never use mock data or implement complex workarounds. Tell me immediately if you get stuck.
- Every sub-feature must include a test script for easy manual testing and verification.
- Document all API endpoints, job arguments, and expected responses in the readme.

---

## Special Instructions

- If you have any questions about the arguments my app supports or any other detail, always ask before planning or coding that part.
- Begin by analyzing my current parameter-passing logic and mapping it to a FastAPI POST payload and to Celery task arguments.
