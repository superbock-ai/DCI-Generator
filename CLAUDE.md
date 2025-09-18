# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## System Architecture

This is a **containerized Docker-based DCI Generator** with a **FastAPI REST API broker**, **Redis queue**, and **Celery workers**. There is NO standalone main.py CLI - the system operates as a distributed microservices architecture.

### Core Components
- **FastAPI Broker** (`broker/main.py`): REST API for job submission and monitoring
- **Celery Workers** (`worker/`): Document analysis processing engines  
- **Redis**: Message queue and result backend
- **Docker Compose**: Container orchestration

## Development Commands

### System Management
- **Start platform**: `docker compose up -d`
- **Stop platform**: `docker compose down`
- **View all logs**: `docker compose logs -f`
- **Scale workers**: `docker compose up -d --scale worker=4`
- **Rebuild containers**: `docker compose up -d --build`

### Environment Setup
- **Environment file**: Copy `.env.example` to `.env` and configure:
  - `OPENAI_API_KEY`: Your OpenAI API key
  - `DIRECTUS_AUTH_TOKEN`: Directus/GraphQL authentication token
  - `DIRECTUS_SECRET`: JWT secret for API authentication
  - `DIRECTUS_URL`: Directus API endpoint URL

### Development Workflow

#### 1. Code Changes
Code changes in `worker/` and `broker/` directories are **automatically mounted** into containers via volume mounts. No container rebuild needed for Python code changes.

#### 2. Testing API Endpoints
```bash
# Generate JWT token for testing
python3 -c "
import jwt
import datetime
payload = {
    'user_id': 'test-user',
    'exp': datetime.datetime.utcnow() + datetime.timedelta(hours=1)
}
token = jwt.encode(payload, 'your-directus-secret', algorithm='HS256')
print(f'Export this: export JWT_TOKEN=\"{token}\"')
"

# Set token for API calls
export JWT_TOKEN="your-generated-token"
```

#### 3. Submit Analysis Jobs
```bash
# Basic analysis job
curl -X POST "http://localhost:8000/jobs/analysis" \
  -H "Authorization: Bearer $JWT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "product_id": "2258e45a-531e-4412-ab47-3c6bd96eed8a",
    "export": true,
    "debug": true
  }'

# With custom parameters
curl -X POST "http://localhost:8000/jobs/analysis" \
  -H "Authorization: Bearer $JWT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "product_id": "2258e45a-531e-4412-ab47-3c6bd96eed8a",
    "segment_chunks": 5,
    "benefit_chunks": 4,
    "modifier_chunks": 2,
    "debug": true,
    "export": true,
    "seed_directus": false
  }'
```

#### 4. Monitor Job Status
```bash
# Check job status (replace {job_id} with actual ID)
curl -X GET "http://localhost:8000/jobs/{job_id}/status" \
  -H "Authorization: Bearer $JWT_TOKEN"
```

### Directus Integration
```bash
# Analyze and seed to Directus
curl -X POST "http://localhost:8000/jobs/analysis" \
  -H "Authorization: Bearer $JWT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "product_id": "2258e45a-531e-4412-ab47-3c6bd96eed8a",
    "seed_directus": true,
    "debug": true
  }'

# Dry run seeding (preview only)
curl -X POST "http://localhost:8000/jobs/analysis" \
  -H "Authorization: Bearer $JWT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "product_id": "2258e45a-531e-4412-ab47-3c6bd96eed8a",
    "seed_directus": true,
    "dry_run_directus": true
  }'

# Cleanup product data
curl -X POST "http://localhost:8000/jobs/cleanup" \
  -H "Authorization: Bearer $JWT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"product_id": "2258e45a-531e-4412-ab47-3c6bd96eed8a"}'
```

### Development Debugging

#### Container Access
```bash
# Execute commands in worker container
docker compose exec worker python -c "print('Worker container access')"

# Access container shell
docker compose exec worker /bin/bash

# View specific service logs
docker compose logs -f worker
docker compose logs -f broker
docker compose logs -f redis
```

#### Direct Task Submission (Development Only)
For development/testing, you can submit tasks directly to Celery:
```python
# Add worker directory to path and import tasks
import sys
sys.path.insert(0, 'worker')
from tasks import analyze_document_task

# Submit task directly
result = analyze_document_task.delay(
    product_id="2258e45a-531e-4412-ab47-3c6bd96eed8a",
    export=True,
    debug=True,
    modifier_chunks=2
)

print(f"Task ID: {result.id}")
```

## Architecture Overview

This is a **Domain Context Item (DCI) Generator** for comprehensive insurance document analysis that performs three-tier analysis: segments → benefits → modifiers (limits/conditions/exclusions) using AI.

### Core System
- **Microservices Architecture**: FastAPI broker + Celery workers + Redis queue
- **GraphQL Integration**: Fetches live taxonomy hierarchies from Quinsights platform
- **Three-Tier Analysis**: Segments → Benefits → Modifiers (limits/conditions/exclusions)
- **LLM Analysis**: Uses OpenAI models with enhanced structured output (AnalysisResult)
- **Conditional Processing**: Only analyzes benefits for found segments, modifiers for found benefits
- **Parallel Processing**: Each tier analyzes all items simultaneously for maximum speed
- **Context-Aware Prompts**: Each tier builds on previous analysis results
- **Comprehensive Caching**: In-memory caching across all analysis tiers

### Key Files
- `broker/main.py`: FastAPI REST API server for job management
- `worker/worker_main.py`: Core DocumentAnalyzer class with analysis engine
- `worker/tasks.py`: Celery task definitions for analysis and cleanup jobs
- `worker/celery_app.py`: Celery configuration and setup
- `worker/directus_tools.py`: Directus integration module
- `docker-compose.yml`: Container orchestration configuration
- `worker/graphql/GetCompleteTaxonomyHierarchy.graphql`: GraphQL query for taxonomy data
- `.env.example`: Template for environment variables
- `.env`: Environment configuration (gitignored)

### DocumentAnalyzer Functionality
The `worker/worker_main.py` DocumentAnalyzer class implements a comprehensive three-tier document analysis pipeline:

1. **Environment Setup**: Loads configuration from environment variables
2. **GraphQL Fetching**: Retrieves complete taxonomy (segments, benefits, modifiers) from live endpoint
3. **Document Loading**: Fetches document text from Directus product
4. **Tier 1 - Segment Analysis**: Analyzes all segments in parallel using RunnableParallel
5. **Tier 2 - Benefit Analysis**: Conditionally analyzes benefits for found segments in parallel
6. **Tier 3 - Modifier Analysis**: Conditionally analyzes limits/conditions/exclusions for found benefits in parallel
7. **Context Integration**: Each tier uses results from previous tiers in prompts
8. **Structured Output**: Returns comprehensive results using enhanced AnalysisResult schema
9. **Export Options**: Can save complete hierarchical results to JSON and show detailed information
10. **Directus Integration**: Optional seeding of analysis results into Directus CMS with hierarchical relationships

### Configuration (Environment Variables)
Required environment variables:
- `OPENAI_API_KEY`: Your OpenAI API key
- `DIRECTUS_AUTH_TOKEN`: Authentication token for Directus/GraphQL endpoint
- `DIRECTUS_SECRET`: JWT secret for API authentication
- `DIRECTUS_URL`: Directus API endpoint URL
- `OPENAI_MODEL`: Model to use (optional, defaults to gpt-4o-mini)
- `REDIS_URL`: Redis connection URL (default: redis://localhost:6379/0)
- `CELERY_BROKER_URL`: Celery broker URL (default: redis://localhost:6379/0)
- `CELERY_RESULT_BACKEND`: Celery result backend URL (default: redis://localhost:6379/0)

### Data Flow Architecture
1. **Job Submission**: FastAPI broker receives REST API requests
2. **Task Queuing**: Celery tasks are queued in Redis
3. **Worker Processing**: Available Celery workers pick up tasks
4. **Taxonomy Retrieval**: Live GraphQL query fetches complete hierarchy
5. **Document Fetching**: Product text retrieved from Directus
6. **Tier 1 Analysis**: Parallel processing analyzes document for all segments
7. **Tier 2 Analysis**: Parallel processing analyzes benefits with segment context
8. **Tier 3 Analysis**: Parallel processing analyzes modifiers with segment + benefit context
9. **Results Integration**: Combines all tiers into comprehensive hierarchical output
10. **Job Completion**: Results stored in Redis and accessible via API

### Analysis Result Schema (AnalysisResult)
Each analysis item (segment/benefit/modifier) returns:
- `section_reference`: Document section where item is found
- `full_text_part`: Relevant text from the document
- `llm_summary`: AI-generated summary of the coverage
- `item_name`: Name of the analyzed item
- `is_included`: Boolean indicating if item is covered
- `description`: LLM-extracted description of what the item covers
- `unit`: LLM-extracted unit of measurement (e.g., CHF, days, percentage)
- `value`: LLM-extracted specific value/amount found in document

### GraphQL Schema Structure
The system works with a complete insurance taxonomy hierarchy:
- **product_type** → **segment_type** → **benefit_type** → **limit_type/condition_type/exclusion_type**
- All segments and their associated benefits with modifiers
- Each item includes descriptions, aliases, examples, and specific LLM analysis instructions for German insurance documents

### Three-Tier Analysis Flow
```
SEGMENTS (Tier 1): Identify coverage areas in document
    ↓ (only for found segments)
BENEFITS (Tier 2): Identify specific benefits within found segments
    ↓ (only for found benefits)
MODIFIERS (Tier 3): Identify limits, conditions, exclusions for found benefits
```

### API Job Parameters
All parameters for analysis jobs:
- `product_id` (string, required): Product ID from Directus to analyze
- `export` (boolean, default: false): Export results to JSON file
- `detailed` (boolean, default: false): Show detailed results
- `no_cache` (boolean, default: false): Disable caching for this run
- `segment_chunks` (int, default: 8): Number of segments to process per chunk
- `benefit_chunks` (int, default: 8): Number of benefits to process per chunk
- `modifier_chunks` (int, default: 3): Number of modifiers to process per chunk
- `debug` (boolean, default: false): Enable debug mode
- `debug_clean` (boolean, default: false): Delete existing debug files before running
- `debug_from` (string, optional): Force re-run from specific tier
- `seed_directus` (boolean, default: false): Seed analysis results to Directus
- `dry_run_directus` (boolean, default: false): Dry run mode for Directus seeding

### Rate Limiting & Error Handling
- **Chunked Parallel Processing**: Processes items in configurable chunks to avoid overwhelming OpenAI API
- **Smart Retry Logic**: Uses `tenacity` library with intelligent wait time parsing
- **OpenAI Wait Time Parsing**: Extracts exact wait times from OpenAI error messages
- **Exponential Backoff Fallback**: Falls back to exponential backoff if wait time parsing fails
- **Chunk-Level Retries**: Only retries failed chunks, not entire batches
- **6 Retry Attempts**: Industry standard retry count with detailed logging
- **Guaranteed Completion**: Tasks complete successfully even with rate limit hits

### Debug Mode & Auto-Resume
- **Automatic Save**: Saves intermediate results after each successful analysis tier
- **Smart Resume**: Automatically loads existing debug files and resumes from last incomplete tier
- **Flexible Resumption**: Uses existing debug files regardless of chunk size changes between runs
- **Per-Document Files**: `{product_id}_segments.debug.json`, `{product_id}_benefits.debug.json`, `{product_id}_modifiers.debug.json`
- **Progress Visibility**: Clear logging shows what's loaded vs. what's running
- **Failure Recovery**: Resume from failures with different configurations

### Caching
- **Comprehensive in-memory caching**: LangChain automatic caching across all three analysis tiers
- **Performance**: Subsequent runs on same document are nearly instant for all analyzed items
- **Development**: Perfect for testing and prompt iteration across the entire analysis pipeline
- **Intelligent cache keys**: Based on document content + analysis context + model parameters
- **Control**: `no_cache` parameter to disable when needed

### Directus Integration & Data Seeding
- **Seamless Integration**: Built-in Directus seeding functionality using `directus_tools.py` module
- **Hierarchical Data Insertion**: Maintains proper relationships between dcm_product → segments → benefits → conditions/limits/exclusions
- **Existing Product Support**: Works with existing dcm_product entries (no product creation/modification)
- **Taxonomy Mapping**: Automatically fetches and maps taxonomy relationships from GraphQL for proper Directus field population
- **Swiss Number Handling**: Intelligent processing of Swiss number formats and German text
- **Debug Mode Compatibility**: Can seed from existing debug files without re-running analysis
- **Dry Run Support**: Test seeding operations without making actual changes
- **Cleanup Functionality**: Complete removal of seeded data while preserving original products
- **Progress Tracking**: Detailed logging and progress indicators during seeding operations
- **Data Validation**: Robust error handling for Directus API interactions and data validation

### Security
- **Environment variables**: All secrets stored in .env file
- **Gitignored secrets**: .env file excluded from version control
- **Token validation**: Startup checks for required credentials
- **JWT Authentication**: Secure API access control with Directus secret validation
- **API Authentication**: Secure token-based authentication for both GraphQL and Directus APIs

## Sample Product IDs
Test the system with these verified product IDs:
- `2258e45a-531e-4412-ab47-3c6bd96eed8a` - Zurich Reiseversicherung
- `321560a9-5687-4a35-9bc0-ff8aa9f836c7` - Generali Travel Insurance

## Quick Reference

### Essential Commands
```bash
# Start the platform
docker compose up -d

# Check service status  
docker compose ps

# View logs
docker compose logs -f

# Submit job via API
curl -X POST "http://localhost:8000/jobs/analysis" \
  -H "Authorization: Bearer $JWT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"product_id": "2258e45a-531e-4412-ab47-3c6bd96eed8a", "debug": true}'

# Check job status
curl -X GET "http://localhost:8000/jobs/{job_id}/status" \
  -H "Authorization: Bearer $JWT_TOKEN"

# Stop the platform
docker compose down
```

### API Endpoints
- `GET /health` - Service health check
- `GET /docs` - OpenAPI/Swagger documentation  
- `GET /redoc` - ReDoc documentation
- `POST /jobs/analysis` - Submit analysis job (requires JWT auth)
- `POST /jobs/cleanup` - Submit cleanup job (requires JWT auth)
- `GET /jobs/{job_id}/status` - Get job status (requires JWT auth)

### Development Files to Watch
- `worker/worker_main.py` - Main analysis engine
- `worker/tasks.py` - Celery task definitions
- `broker/main.py` - FastAPI REST API server
- `docker-compose.yml` - Container orchestration
- `.env` - Environment configuration