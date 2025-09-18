# DCI Generator - Scalable Insurance Document Analysis Platform

A production-ready, containerized AI platform for analyzing insurance documents using a three-tier hierarchical approach: **Segments** → **Benefits** → **Modifiers** (limits/conditions/exclusions). Built with Celery workers, Redis queue, and Docker orchestration for scalable processing.

## 🔍 Overview

The DCI (Domain Context Item) Generator performs intelligent, context-aware analysis of German insurance documents by:

1. **Identifying coverage segments** (e.g., luggage coverage, home assistance)
2. **Finding specific benefits** within discovered segments (e.g., missed connections, emergency purchases)
3. **Extracting modifiers** (limits, conditions, and exclusions) for found benefits

## 🚀 Features

### Core Capabilities
- **Three-tier conditional analysis** - Only analyzes what exists in the document
- **Parallel processing** - Maximum speed at each analysis tier
- **Context-aware prompts** - Each tier builds on previous analysis results
- **Comprehensive caching** - Near-instant repeated analyses
- **Live GraphQL integration** - Real-time taxonomy data from Quinsights platform
- **Hierarchical tree output** - Natural taxonomy structure with nested relationships

### Production Features
- **Containerized Architecture** - Docker Compose orchestration with Redis + Celery workers
- **Scalable Processing** - Horizontal scaling with multiple worker instances
- **Full document analysis** - Not limited to specific sections
- **German insurance expertise** - Specialized prompts for German AVB documents
- **Flexible export options** - JSON export and detailed console output
- **Directus CMS integration** - Seed analysis results directly into Directus with hierarchical relationships
- **Environment-based configuration** - Secure credential management
- **Chunked parallel processing** - Configurable chunk sizes per analysis tier
- **Smart rate limiting** - Automatic retry with OpenAI wait time parsing
- **Debug mode with auto-resume** - Save progress and resume from failures
- **Token-aware processing** - Handles OpenAI response length limits
- **Robust error handling** - Retry logic and graceful failure recovery
- **Task Queue Management** - Redis-backed job queuing with progress tracking

## ⚙️ Requirements

- **Docker & Docker Compose** - Container orchestration
- **Python 3.12+** - For local development
- **OpenAI API access** - For AI document analysis
- **Quinsights GraphQL endpoint access** - For taxonomy data
- **uv package manager** - For dependency management

## 🛠️ Installation & Setup

### 1. **Clone and Configure**
```bash
cd dci_generator
cp .env.example .env
# Edit .env with your credentials
```

### 2. **Required Environment Variables**
```env
# OpenAI Configuration
OPENAI_API_KEY=your-openai-api-key
OPENAI_MODEL=gpt-4o-mini

# Directus Configuration
DIRECTUS_URL=https://app-uat.quinsights.tech
DIRECTUS_AUTH_TOKEN=your-directus-auth-token

# Redis/Celery Configuration
REDIS_URL=redis://localhost:6379/0
CELERY_BROKER_URL=redis://localhost:6379/0
CELERY_RESULT_BACKEND=redis://localhost:6379/0

# FastAPI Broker Configuration
DIRECTUS_SECRET=your-directus-secret-for-jwt-validation
FASTAPI_PORT=8000
```

### 3. **Start the Platform**
```bash
# Start Redis + Celery Workers + FastAPI Broker
docker compose up -d

# Check service status
docker compose ps

# View all logs
docker compose logs -f

# View specific service logs
docker compose logs -f worker   # Worker logs
docker compose logs -f broker   # API logs
docker compose logs -f redis    # Redis logs

# Access API documentation
open http://localhost:8000/docs  # OpenAPI docs
open http://localhost:8000/redoc # ReDoc documentation
```

## 🎯 Usage

The platform operates as a **containerized API-driven system** with FastAPI REST endpoints for job management and Celery workers for processing.

### Production API Usage (Recommended)

#### 1. Authentication & Job Submission

```bash
# Get JWT token (from your authentication system)
export JWT_TOKEN="your-jwt-token"

# Submit analysis job
curl -X POST "http://localhost:8000/jobs/analysis" \
  -H "Authorization: Bearer $JWT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "product_id": "2258e45a-531e-4412-ab47-3c6bd96eed8a",
    "export": true,
    "debug": true,
    "segment_chunks": 5,
    "benefit_chunks": 4,
    "modifier_chunks": 2,
    "seed_directus": false
  }'

# Response: {"job_id": "uuid", "status": "submitted", "message": "Analysis job submitted..."}
```

#### 2. Job Status Monitoring

```bash
# Check job status
curl -X GET "http://localhost:8000/jobs/{job_id}/status" \
  -H "Authorization: Bearer $JWT_TOKEN"

# Response: {"job_id": "uuid", "status": "processing|completed|failed", "result": {...}}
```

#### 3. Cleanup Jobs

```bash
# Submit cleanup job
curl -X POST "http://localhost:8000/jobs/cleanup" \
  -H "Authorization: Bearer $JWT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"product_id": "2258e45a-531e-4412-ab47-3c6bd96eed8a"}'
```

### Development Task Submission (Direct Celery)

```python
# Add worker directory to path
import sys
sys.path.insert(0, 'worker')
from tasks import analyze_document_task, cleanup_product_task

# Submit analysis task
result = analyze_document_task.delay(
    product_id="2258e45a-531e-4412-ab47-3c6bd96eed8a",
    export=True,
    debug=True,
    segment_chunks=5,
    benefit_chunks=4,
    modifier_chunks=2,
    seed_directus=False
)

print(f"Task ID: {result.id}")
print(f"Status: {result.status}")

# Monitor progress
while result.status == 'PENDING':
    time.sleep(5)
    print(f"Current status: {result.status}")

# Get results
if result.status == 'SUCCESS':
    results = result.result
    print(f"Analysis completed: {results}")
```

### Analysis Task Parameters

```python
analyze_document_task.delay(
    product_id="uuid-string",          # Required: Product ID from Directus
    export=False,                      # Export results to JSON
    detailed=False,                    # Show detailed console output
    no_cache=False,                    # Disable LLM caching
    segment_chunks=8,                  # Segments per parallel chunk
    benefit_chunks=8,                  # Benefits per parallel chunk
    modifier_chunks=3,                 # Modifiers per parallel chunk
    debug=False,                       # Enable debug mode & file saving
    debug_clean=False,                 # Clean debug files before run
    debug_from=None,                   # Force re-run from tier ('segments'|'benefits'|'modifiers')
    seed_directus=False,               # Seed results to Directus
    dry_run_directus=False            # Dry run seeding (show what would be inserted)
)
```

### Cleanup Task

```python
# Clean up previously seeded data
cleanup_result = cleanup_product_task.delay("product-uuid")
```

### Legacy CLI (Development Only)

For development and testing, the original CLI is still available inside containers:

```bash
# Direct CLI usage via container (development only)
docker compose exec dci_generator-worker-1 python worker_main.py 2258e45a-531e-4412-ab47-3c6bd96eed8a --export --debug
```

### API Endpoints

The FastAPI broker provides the following REST endpoints:

#### Health & Documentation
- `GET /health` - Service health check
- `GET /docs` - OpenAPI/Swagger documentation
- `GET /redoc` - ReDoc documentation
- `GET /` - API information

#### Job Management (Authentication Required)
- `POST /jobs/analysis` - Submit analysis job
- `POST /jobs/cleanup` - Submit cleanup job
- `GET /jobs/{job_id}/status` - Get job status and results

#### Authentication
All job endpoints require JWT authentication via `Authorization: Bearer {token}` header. The JWT secret is validated using `DIRECTUS_SECRET` from environment variables.

#### Request/Response Models

**Analysis Job Request:**
```json
{
  "product_id": "uuid-string",
  "export": false,
  "detailed": false,
  "no_cache": false,
  "segment_chunks": 8,
  "benefit_chunks": 8,
  "modifier_chunks": 3,
  "debug": false,
  "debug_clean": false,
  "debug_from": null,
  "seed_directus": false,
  "dry_run_directus": false
}
```

**Job Response:**
```json
{
  "job_id": "uuid",
  "status": "submitted|processing|completed|failed",
  "message": "Description",
  "result": {...}  // Present when completed
}
```

## 🔌 API Access & Monitoring

### Starting the Platform
```bash
# Start all services (Redis + Workers + FastAPI Broker)
docker compose up -d

# Verify all services are running
docker compose ps

# Should show: redis (healthy), broker (healthy), 2+ workers (running)
```

### Accessing the API

#### 1. API Documentation
```bash
# Interactive OpenAPI/Swagger UI
open http://localhost:8000/docs

# Clean ReDoc documentation
open http://localhost:8000/redoc

# Simple health check
curl http://localhost:8000/health
```

#### 2. Authentication Setup
Create a JWT token for API access (replace `your-directus-secret` with your actual secret):

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

# Set the token for API calls
export JWT_TOKEN="your-generated-token"
```

#### 3. Submit Jobs via API
```bash
# Submit analysis job
curl -X POST "http://localhost:8000/jobs/analysis" \
  -H "Authorization: Bearer $JWT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "product_id": "2258e45a-531e-4412-ab47-3c6bd96eed8a",
    "debug": true,
    "export": true,
    "segment_chunks": 4,
    "seed_directus": false
  }'

# Monitor job status (replace job-id with returned job_id)
curl -X GET "http://localhost:8000/jobs/{job-id}/status" \
  -H "Authorization: Bearer $JWT_TOKEN"

# Submit cleanup job
curl -X POST "http://localhost:8000/jobs/cleanup" \
  -H "Authorization: Bearer $JWT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"product_id": "2258e45a-531e-4412-ab47-3c6bd96eed8a"}'
```

### Log Monitoring (Parallel)

#### 1. Real-time Monitoring Setup
```bash
# Terminal 1: All service logs combined
docker compose logs -f

# Terminal 2: Worker logs only (all workers)
docker compose logs -f worker

# Terminal 3: Broker API logs
docker compose logs -f broker

# Terminal 4: Redis logs
docker compose logs -f redis
```

#### 2. Advanced Log Filtering
```bash
# Monitor specific job progress
docker compose logs -f worker | grep "2258e45a-531e-4412-ab47-3c6bd96eed8a"

# Watch for completed analyses
docker compose logs -f worker | grep "Analysis completed"

# Monitor errors across all services
docker compose logs -f | grep -i error

# Follow logs with timestamps
docker compose logs -f -t --tail=50
```

#### 3. Individual Worker Monitoring
```bash
# Monitor each worker separately (run in different terminals)
docker compose logs -f dci_generator-worker-1
docker compose logs -f dci_generator-worker-2
docker compose logs -f dci_generator-worker-3
docker compose logs -f dci_generator-worker-4

# Or monitor all workers in background
docker compose logs -f dci_generator-worker-1 &
docker compose logs -f dci_generator-worker-2 &
docker compose logs -f dci_generator-worker-3 &
docker compose logs -f dci_generator-worker-4 &
wait  # Wait for all background processes
```

#### 4. Performance Monitoring
```bash
# Check Redis queue status
docker compose exec redis redis-cli LLEN celery

# Monitor worker performance
docker compose exec dci_generator-worker-1 celery -A celery_app inspect stats

# Check active tasks
docker compose exec dci_generator-worker-1 celery -A celery_app inspect active

# Monitor system resources
docker stats dci_generator-worker-1 dci_generator-worker-2 dci-broker dci-redis
```

## 🚀 Production Operations

### Scaling Workers
```bash
# Scale to 4 worker instances
docker compose up -d --scale worker=4

# Check running workers
docker compose ps

# View logs from all workers
docker compose logs -f worker
```

### Service Management
```bash
# Stop all services
docker compose down

# Restart specific service
docker compose restart worker

# View Redis logs
docker compose logs redis

# Execute commands in running container
docker compose exec worker python -c "print('Worker is running')"
```

### Quick Reference Commands

#### Essential Operations
```bash
# Start platform
docker compose up -d

# Check service status
docker compose ps

# View all logs
docker compose logs -f

# Scale workers
docker compose up -d --scale worker=3

# Stop platform
docker compose down
```

#### API Quick Test
```bash
# Health check
curl http://localhost:8000/health

# Get API docs
curl http://localhost:8000/docs

# Submit job (need JWT token)
curl -X POST "http://localhost:8000/jobs/analysis" \
  -H "Authorization: Bearer $JWT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"product_id": "2258e45a-531e-4412-ab47-3c6bd96eed8a"}'
```

#### Troubleshooting
```bash
# Check Redis connectivity
docker compose exec redis redis-cli ping

# Check Celery queue status
docker compose exec redis redis-cli LLEN celery

# Monitor worker performance
docker compose exec dci_generator-worker-1 celery -A celery_app inspect stats

# Check active tasks
docker compose exec dci_generator-worker-1 celery -A celery_app inspect active

# Restart specific service
docker compose restart worker
docker compose restart broker
```

### Volume Management
- **Debug files**: `./worker/debug/` → `/app/debug/`
- **Export files**: `./worker/exports/` → `/app/exports/`
- **Redis data**: Persistent volume `redis_data`

### Sample Product IDs
Test the system with these verified product IDs:
- `2258e45a-531e-4412-ab47-3c6bd96eed8a` - Zurich Reiseversicherung
- Add your own product IDs from Directus

## 📊 Analysis Flow

### Three-Tier Conditional Processing

```
📄 DOCUMENT
    ↓
🔍 TIER 1: Segment Analysis (Parallel)
   ✓ luggage_travel_delay → FOUND
   ✗ home_assistance → NOT FOUND
    ↓ (only analyze benefits for found segments)
🎯 TIER 2: Benefit Analysis (Parallel)
   ✓ missed_connection → FOUND
   ✗ essential_purchases → NOT FOUND
    ↓ (only analyze details for found benefits)
📋 TIER 3: Detail Analysis (Parallel)
   ✓ limits: maximum_coverage_amount → FOUND
   ✗ conditions: documentation_requirements → NOT FOUND
   ✗ exclusions: war_terrorism → NOT FOUND
    ↓
🏆 HIERARCHICAL RESULTS
```

### New Hierarchical Output Structure
```json
{
  "segments": [
    {
      "luggage_travel_delay": {
        "item_name": "luggage_travel_delay",
        "is_included": true,
        "section_reference": "Modul I - Reisegepäck",
        "description": "Coverage for luggage loss and travel delays",
        "value": "Travel luggage coverage area",
        "unit": "N/A",
        "benefits": [
          {
            "missed_connection": {
              "item_name": "missed_connection",
              "is_included": true,
              "description": "Reimbursement for missed connections",
              "value": "Additional costs for alternative transport",
              "unit": "CHF",
              "limits": [
                {
                  "maximum_coverage_amount": {
                    "item_name": "maximum_coverage_amount",
                    "is_included": true,
                    "description": "Maximum reimbursement limit",
                    "value": "5000",
                    "unit": "CHF"
                  }
                }
              ],
              "conditions": [],
              "exclusions": []
            }
          }
        ]
      }
    }
  ]
}
```

## 🏗️ Architecture

### Core Components

- **DocumentAnalyzer**: Main analysis orchestrator
- **AnalysisResult**: Universal Pydantic model for all analysis items
- **GraphQL Integration**: Live taxonomy data retrieval
- **LangChain Chains**: Structured LLM processing with caching
- **DirectusSeeder**: Hierarchical data seeding into Directus CMS

### Data Sources

- **GraphQL Endpoint**: Quinsights taxonomy hierarchy
- **Document Repository**: Markdown-formatted insurance documents
- **LLM Instructions**: Specialized prompts for German insurance analysis

### Processing Pipeline

1. **Taxonomy Fetching**: Retrieve complete hierarchy from GraphQL
2. **Chain Creation**: Build analysis chains for each tier
3. **Conditional Processing**: Only process items that exist
4. **Parallel Execution**: Maximize speed with simultaneous processing
5. **Context Integration**: Each tier uses previous results as context
6. **Tree Assembly**: Build hierarchical structure with taxonomy-aligned keys
7. **Directus Seeding** (optional): Insert results into Directus with proper relationships

## 🔧 Technical Details

### Pydantic Schema (AnalysisResult)
```python
class AnalysisResult(BaseModel):
    section_reference: str      # Document section location
    full_text_part: str        # Extracted text passage
    llm_summary: str           # AI-generated analysis
    item_name: str             # Name of analyzed item
    is_included: bool          # Coverage determination
    description: str           # LLM-extracted description
    unit: str                  # Unit of measurement
    value: str                 # Specific value found
```

### Intelligent Caching
- **Cache Scope**: All LLM calls across all tiers
- **Cache Keys**: Document content + analysis context + model parameters
- **Performance**: Near-instant repeated analyses
- **Development**: Perfect for iterative prompt development

### Error Handling
- **GraphQL Errors**: Connection and authentication issues
- **Document Errors**: File not found, encoding problems
- **LLM Errors**: Rate limits, API failures
- **Processing Errors**: Chain execution failures

## ⚙️ Configuration

### Environment Variables
| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `OPENAI_API_KEY` | Yes | - | OpenAI API authentication |
| `DIRECTUS_AUTH_TOKEN` | Yes | - | Directus/GraphQL authentication token |
| `DIRECTUS_SECRET` | Yes | - | JWT secret for API authentication |
| `DIRECTUS_URL` | Yes | - | Directus/GraphQL endpoint URL |
| `OPENAI_MODEL` | No | `gpt-4o-mini` | OpenAI model to use |
| `REDIS_URL` | No | `redis://localhost:6379/0` | Redis connection URL |
| `CELERY_BROKER_URL` | No | `redis://localhost:6379/0` | Celery broker URL |
| `CELERY_RESULT_BACKEND` | No | `redis://localhost:6379/0` | Celery result backend URL |
| `FASTAPI_PORT` | No | `8000` | FastAPI broker port |

### API Job Parameters
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `product_id` | string | Required | Directus product UUID to analyze |
| `export` | boolean | false | Export results to JSON file |
| `detailed` | boolean | false | Show comprehensive analysis details |
| `no_cache` | boolean | false | Disable LLM caching for fresh results |
| `segment_chunks` | integer | 8 | Segments per parallel chunk |
| `benefit_chunks` | integer | 8 | Benefits per parallel chunk |
| `modifier_chunks` | integer | 3 | Modifiers per parallel chunk |
| `debug` | boolean | false | Enable debug mode with file saving |
| `debug_clean` | boolean | false | Clean debug files before run |
| `debug_from` | string | null | Force re-run from tier ('segments'/'benefits'/'modifiers') |
| `seed_directus` | boolean | false | Seed analysis results to Directus |
| `dry_run_directus` | boolean | false | Dry run seeding (preview only) |

### Rate Limiting & Reliability
- **Chunked Processing**: Processes items in configurable batches to avoid API limits
- **Smart Wait Time Parsing**: Extracts exact wait times from OpenAI error messages
- **Exponential Backoff Fallback**: Falls back to exponential backoff if parsing fails
- **6 Retry Attempts**: Industry standard retry count with detailed logging
- **Chunk-Level Retries**: Only retries failed chunks, not entire batches
- **Guaranteed Completion**: Script completes successfully even with rate limit hits

### Debug Mode & Auto-Resume
- **Automatic Save**: Saves intermediate results after each successful analysis tier
- **Smart Resume**: Automatically loads existing debug files and resumes from last incomplete tier
- **Flexible Resumption**: Uses existing debug files regardless of chunk size changes between runs
- **Per-Document Files**: `document_segments.debug.json`, `document_benefits.debug.json`, `document_modifiers.debug.json`
- **Progress Visibility**: Clear logging shows what's loaded vs. what's running
- **Failure Recovery**: Resume from failures with different configurations (e.g., smaller chunk sizes)

## 📁 Project Structure

```
dci_generator/
├── docker-compose.yml              # Container orchestration
├── .env.example                    # Environment template
├── .env                           # Environment configuration (gitignored)
├── worker/                         # Celery worker container
│   ├── Dockerfile
│   ├── worker_main.py              # Core analysis engine
│   ├── celery_app.py               # Celery configuration
│   ├── tasks.py                    # Celery task definitions
│   ├── worker.py                   # Worker startup script
│   ├── directus_tools.py           # Directus integration
│   ├── debug/                      # Debug output files
│   ├── exports/                    # Analysis export files
│   └── graphql/                    # GraphQL queries
│       └── GetCompleteTaxonomyHierarchy.graphql
├── broker/                         # FastAPI broker container
│   ├── Dockerfile
│   └── main.py                     # FastAPI application
├── test_api_endpoints.py           # API integration tests
├── IMPLEMENTATION_PLAN.md          # Development plan
├── README_SEEDER.md                # Directus seeding documentation
├── CLAUDE.md                       # Developer documentation
└── README.md                       # This file
```

## 🔒 Security

- **Environment Variables**: All secrets in `.env` file
- **Git Exclusion**: Credentials never committed to repository
- **Token Validation**: Startup verification of required credentials
- **Error Sanitization**: Sensitive information filtered from logs

## ⚡ Performance

### Optimization Features
- **Conditional Processing**: Only analyzes relevant items (typically 10-30% of total taxonomy)
- **Parallel Execution**: All items at each tier processed simultaneously
- **Intelligent Caching**: Repeated analyses served from memory
- **Efficient Data Structures**: Organized taxonomy for fast lookups

### Typical Performance
- **Initial Analysis**: 30-60 seconds (depends on document complexity)
- **Cached Analysis**: 2-5 seconds (near-instant for repeated items)
- **Memory Usage**: ~50-100MB (including cached results)

## 🗃️ Directus Integration

### Overview
The DCI Generator includes seamless integration with Directus CMS, allowing you to automatically seed analysis results into a structured database with proper hierarchical relationships.

### Features
- **Hierarchical Data Structure**: Maintains relationships between dcm_product → segments → benefits → conditions/limits/exclusions
- **Existing Product Support**: Works with pre-existing dcm_product entries (no product creation required)
- **Taxonomy Mapping**: Automatically maps analysis items to taxonomy relationships via GraphQL
- **Swiss Data Handling**: Intelligent processing of Swiss number formats and German insurance terminology
- **Debug Mode Integration**: Can seed from cached debug files without re-running expensive analysis
- **Complete Cleanup**: Remove ALL data associated with a product including the product itself

### Workflow
1. **Analyze Document**: Standard three-tier analysis (optionally with --debug for caching)
2. **Seed to Directus**: Use --seed-directus with existing product ID
3. **Data Validation**: System validates product, fetches taxonomy mappings, creates relationships
4. **Cleanup (Optional)**: Remove ALL product data using --cleanup-directus --product-id (DELETES the product)

### Data Structure in Directus
```
dcm_product (existing)
├── insurance_dcm_segment
│   ├── insurance_dcm_benefit
│   │   ├── insurance_dcm_condition
│   │   ├── insurance_dcm_limit
│   │   └── insurance_dcm_exclusion
│   ├── insurance_dcm_condition (segment-level)
│   ├── insurance_dcm_limit (segment-level)
│   └── insurance_dcm_exclusion (segment-level)
```

### Example API Usage
```bash
# Complete workflow: analyze + seed via API
curl -X POST "http://localhost:8000/jobs/analysis" \
  -H "Authorization: Bearer $JWT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "product_id": "92f3ee1b-9b03-4085-ab01-555cd9b0507c",
    "export": true,
    "debug": true,
    "seed_directus": true
  }'

# Test before seeding (dry run)
curl -X POST "http://localhost:8000/jobs/analysis" \
  -H "Authorization: Bearer $JWT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "product_id": "92f3ee1b-9b03-4085-ab01-555cd9b0507c",
    "seed_directus": true,
    "dry_run_directus": true
  }'
```

For detailed Directus seeding documentation, see `README_SEEDER.md`.

## 🔬 Development

### Development Workflow
1. **Local Development**: Modify code in `worker/` and `broker/` directories
2. **Testing**: Use `test_api_endpoints.py` for API integration testing
3. **Container Testing**: Rebuild containers with `docker compose up -d --build`
4. **Log Monitoring**: Use `docker compose logs -f` for real-time debugging

### Adding New Features
1. **Worker Changes**: Modify `worker/worker_main.py` or `worker/tasks.py`
2. **API Changes**: Modify `broker/main.py` for new endpoints
3. **Container Rebuild**: Run `docker compose up -d --build` after changes
4. **Integration Testing**: Run API tests to verify functionality

### Development Environment Setup
```bash
# Start development environment
docker compose up -d

# View logs for debugging
docker compose logs -f worker
docker compose logs -f broker

# Run tests
python test_api_endpoints.py

# Access worker container for debugging
docker compose exec dci_generator-worker-1 /bin/bash
```

## 🚀 Current Capabilities

### Implemented Features ✅
- ✅ **REST API Interface**: Complete FastAPI broker with job management
- ✅ **Containerized Architecture**: Docker Compose orchestration
- ✅ **Scalable Processing**: Horizontal worker scaling with Redis queue
- ✅ **JWT Authentication**: Secure API access control
- ✅ **Real-time Monitoring**: Comprehensive logging and status tracking
- ✅ **Debug Mode**: Auto-resume and progress tracking
- ✅ **Directus Integration**: Hierarchical data seeding
- ✅ **Parallel Processing**: Multi-tier concurrent analysis

### Future Enhancements

#### Planned Features
- **Multi-language Support**: Extend beyond German insurance documents
- **Custom Taxonomies**: Support for user-defined analysis hierarchies
- **Batch Processing**: Analyze multiple documents simultaneously via API
- **Web Dashboard**: Browser-based interface for job management
- **Webhook Notifications**: Real-time job completion callbacks

#### Extensibility Points
- **Additional Document Formats**: PDF, HTML, DOCX input support
- **Alternative LLM Providers**: Azure OpenAI, Anthropic, local models
- **Enhanced Export Options**: Excel, CSV, PDF report generation
- **Advanced Analytics**: Performance metrics and usage statistics

## 🆘 Support

For technical issues:
1. Check environment configuration (`.env` file)
2. Verify network access to GraphQL endpoint
3. Validate OpenAI API quota and permissions
4. Review error messages for specific guidance

## 📄 License

This project is proprietary software for Quinsights insurance document analysis.