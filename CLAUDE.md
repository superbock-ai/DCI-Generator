# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Dependency Management
- **Install dependencies**: `uv sync`
- **Add new dependency**: `uv add <package_name>`

### Main Script Usage
- **Basic analysis**: `uv run main.py travel-insurance/markdown/axa.md`
- **With JSON export**: `uv run main.py travel-insurance/markdown/axa.md --export`
- **With detailed results**: `uv run main.py travel-insurance/markdown/axa.md --detailed`
- **Disable caching**: `uv run main.py travel-insurance/markdown/axa.md --no-cache`
- **Custom chunk sizes**: `uv run main.py travel-insurance/markdown/axa.md --segment-chunks 8 --benefit-chunks 6 --detail-chunks 3`
- **Debug mode**: `uv run main.py travel-insurance/markdown/axa.md --debug`
- **Resume from failure**: `uv run main.py travel-insurance/markdown/axa.md --debug --detail-chunks 1`
- **All options**: `uv run main.py travel-insurance/markdown/axa.md -e -d --debug`

### Directus Integration
- **Seed to Directus**: `uv run main.py document.md --seed-directus --product-id <dcm-product-id>`
- **Dry run seeding**: `uv run main.py document.md --seed-directus --product-id <id> --dry-run-directus`
- **Analysis + seeding**: `uv run main.py document.md --export --debug --seed-directus --product-id <id>`
- **Cleanup seeded data**: `uv run main.py document.md --cleanup-directus`

### Development Environment
- **Interactive development**: Use `dev.ipynb` Jupyter notebook for experimentation and testing
- **Run notebook**: `jupyter notebook dev.ipynb` (or use VS Code/PyCharm Jupyter integration)

## Architecture Overview

This is a **Domain Context Item (DCI) Generator** for comprehensive insurance document analysis that performs three-tier analysis: segments → benefits → details (limits/conditions/exclusions) using AI.

### Core System
- **CLI Application**: Command-line interface for comprehensive insurance document analysis
- **GraphQL Integration**: Fetches live taxonomy hierarchies from Quinsights platform
- **Three-Tier Analysis**: Segments → Benefits → Details (limits/conditions/exclusions)
- **LLM Analysis**: Uses OpenAI models with enhanced structured output (AnalysisResult)
- **Conditional Processing**: Only analyzes benefits for found segments, details for found benefits
- **Parallel Processing**: Each tier analyzes all items simultaneously for maximum speed
- **Context-Aware Prompts**: Each tier builds on previous analysis results
- **Comprehensive Caching**: In-memory caching across all analysis tiers

### Key Files
- `main.py`: Production CLI application with DocumentAnalyzer class
- `directus_seeder.py`: Directus integration module for seeding analysis results
- `dev.ipynb`: Development environment for experimentation
- `graphql/GetCompleteTaxonomyHierarchy.graphql`: GraphQL query for fetching taxonomy data
- `example_gql_response.json`: Sample GraphQL response showing taxonomy structure
- `segment_structured_output.json`: JSON schema for structured LLM output
- `README_SEEDER.md`: Detailed documentation for Directus seeding functionality
- `.env.example`: Template for environment variables
- `.env`: Environment configuration (gitignored)

### Main.py Functionality
The `main.py` script implements a comprehensive three-tier document analysis pipeline with integrated Directus seeding:

1. **Environment Setup**: Loads configuration from .env file
2. **GraphQL Fetching**: Retrieves complete taxonomy (segments, benefits, limits/conditions/exclusions) from live endpoint
3. **Document Loading**: Reads specified markdown document
4. **Tier 1 - Segment Analysis**: Analyzes all segments in parallel using RunnableParallel
5. **Tier 2 - Benefit Analysis**: Conditionally analyzes benefits for found segments in parallel
6. **Tier 3 - Detail Analysis**: Conditionally analyzes limits/conditions/exclusions for found benefits in parallel
7. **Context Integration**: Each tier uses results from previous tiers in prompts
8. **Structured Output**: Returns comprehensive results using enhanced AnalysisResult schema
9. **Export Options**: Can save complete hierarchical results to JSON and show detailed information
10. **Directus Integration**: Optional seeding of analysis results into Directus CMS with hierarchical relationships

### Configuration (.env file)
Required environment variables:
- `OPENAI_API_KEY`: Your OpenAI API key
- `GRAPHQL_AUTH_TOKEN`: Authentication token for GraphQL endpoint (used for both taxonomy fetching and Directus seeding)
- `OPENAI_MODEL`: Model to use (optional, defaults to gpt-4o-mini)
- `GRAPHQL_URL`: GraphQL endpoint URL (optional, defaults to UAT, also used to derive Directus API URL)

### Data Flow Architecture
1. **Configuration Loading**: Environment variables loaded from .env
2. **Taxonomy Retrieval**: Live GraphQL query fetches complete hierarchy (segments → benefits → details)
3. **Tier 1 Setup**: Creates LangChain analysis chains for each segment
4. **Tier 1 Analysis**: Parallel processing analyzes document for all segments
5. **Tier 2 Setup**: Creates benefit chains only for found segments
6. **Tier 2 Analysis**: Parallel processing analyzes benefits with segment context
7. **Tier 3 Setup**: Creates detail chains only for found benefits
8. **Tier 3 Analysis**: Parallel processing analyzes limits/conditions/exclusions with segment + benefit context
9. **Results Integration**: Combines all tiers into comprehensive hierarchical output

### Analysis Result Schema (AnalysisResult)
Each analysis item (segment/benefit/detail) returns:
- `section_reference`: Document section where item is found
- `full_text_part`: Relevant text from the document
- `llm_summary`: AI-generated summary of the coverage
- `item_name`: Name of the analyzed item (generalized from segment_name)
- `is_included`: Boolean indicating if item is covered
- `description`: LLM-extracted description of what the item covers
- `unit`: LLM-extracted unit of measurement (e.g., CHF, days, percentage)
- `value`: LLM-extracted specific value/amount found in document

### GraphQL Schema Structure
The system works with a complete insurance taxonomy hierarchy:
- **product_type** → **segment_type** → **benefit_type** → **limit_type/condition_type/exclusion_type**
- Current segments: `luggage_travel_delay` and `home_assistance`
- Each segment contains multiple benefits with their own limits, conditions, and exclusions
- All items include descriptions, aliases, examples, and specific LLM analysis instructions for German insurance documents

### Three-Tier Analysis Flow
```
SEGMENTS (Tier 1): Identify coverage areas in document
    ↓ (only for found segments)
BENEFITS (Tier 2): Identify specific benefits within found segments
    ↓ (only for found benefits)
DETAILS (Tier 3): Identify limits, conditions, exclusions for found benefits
```

### Dependencies
- `gql`: GraphQL client for Python
- `langchain` & `langchain-openai`: LLM orchestration and OpenAI integration
- `openai`: OpenAI API client
- `python-dotenv`: Environment variable management
- `pydantic`: Data validation and structured output
- `requests`: HTTP library for API communications
- `tenacity`: Retry logic with exponential backoff for rate limiting

### Rate Limiting & Error Handling
- **Chunked Parallel Processing**: Processes items in configurable chunks (default: 8 per chunk) to avoid overwhelming OpenAI API
- **Smart Retry Logic**: Uses `tenacity` library with intelligent wait time parsing
- **OpenAI Wait Time Parsing**: Extracts exact wait times from OpenAI error messages (e.g., "Please try again in 7.453s" → waits 8s ceiled)
- **Exponential Backoff Fallback**: Falls back to exponential backoff if wait time parsing fails
- **Chunk-Level Retries**: Only retries failed chunks, not entire batches (prevents restarting all 53+ items)
- **Configurable Chunk Size**: `--chunk-size` parameter allows tuning (3=conservative, 8=default, 15=aggressive)
- **6 Retry Attempts**: Industry standard retry count with detailed logging
- **Guaranteed Completion**: Script completes successfully even with rate limit hits

#### Rate Limiting Configuration
```bash
# Conservative (most reliable, slower)
uv run main.py document.md --chunk-size 3

# Default (balanced)
uv run main.py document.md --chunk-size 8

# Aggressive (faster, more rate limit risk)
uv run main.py document.md --chunk-size 15
```

#### Rate Limiting Implementation Details
- **Three-tier chunking**: Segments, Benefits, and Details all use chunked processing
- **Smart wait parsing**: `parse_openai_wait_time()` function extracts wait times from error messages
- **Ceiling logic**: Always rounds up to next full second for safety
- **Fallback strategy**: `create_smart_wait_strategy()` combines parsed and exponential backoff approaches
- **Error message patterns**: Handles various OpenAI error message formats
- **Progress logging**: Clear visibility into chunk processing and retry attempts

### Debug Mode & Auto-Resume
- **Automatic Save**: Saves intermediate results after each successful analysis tier
- **Smart Resume**: Automatically loads existing debug files and resumes from last incomplete tier
- **Flexible Resumption**: Uses existing debug files regardless of chunk size changes between runs
- **Per-Document Files**: `document_segments.debug.json`, `document_benefits.debug.json`, `document_details.debug.json`
- **Progress Visibility**: Clear logging shows what's loaded vs. what's running
- **Failure Recovery**: Resume from failures with different configurations (e.g., smaller chunk sizes)

### Caching
- **Comprehensive in-memory caching**: LangChain automatic caching across all three analysis tiers
- **Performance**: Subsequent runs on same document are nearly instant for all analyzed items
- **Development**: Perfect for testing and prompt iteration across the entire analysis pipeline
- **Intelligent cache keys**: Based on document content + analysis context + model parameters
- **Control**: `--no-cache` flag to disable when needed

### Directus Integration & Data Seeding
- **Seamless Integration**: Built-in Directus seeding functionality using `directus_seeder.py` module
- **Hierarchical Data Insertion**: Maintains proper relationships between dcm_product → segments → benefits → conditions/limits/exclusions
- **Existing Product Support**: Works with existing dcm_product entries (no product creation/modification)
- **Taxonomy Mapping**: Automatically fetches and maps taxonomy relationships from GraphQL for proper Directus field population
- **Swiss Number Handling**: Intelligent processing of Swiss number formats ("3'000" → 3000) and German text ("Unbegrenzt" → NULL)
- **Debug Mode Compatibility**: Can seed from existing debug files without re-running analysis
- **Dry Run Support**: Test seeding operations without making actual changes
- **Cleanup Functionality**: Complete removal of seeded data while preserving original products
- **Progress Tracking**: Detailed logging and progress indicators during seeding operations
- **Data Validation**: Robust error handling for Directus API interactions and data validation

#### Directus Seeding Arguments
- `--seed-directus`: Enable seeding to Directus after analysis
- `--product-id <uuid>`: Required UUID of existing dcm_product to seed data under
- `--dry-run-directus`: Show what would be seeded without making changes
- `--cleanup-directus`: Remove previously seeded data (preserves original product)

#### Seeding Workflow
1. **Analysis Phase**: Standard three-tier document analysis (with optional debug mode)
2. **Data Conversion**: Converts AnalysisResult objects to Directus-compatible format
3. **Product Validation**: Verifies existing dcm_product and extracts domain_context_model
4. **Taxonomy Mapping**: Fetches taxonomy relationships from GraphQL for field mapping
5. **Hierarchical Creation**: Creates segments → benefits → details with proper foreign keys
6. **Tracking**: Saves created item IDs to `generali_seeded_data.json` for cleanup support

#### Integration with Debug Mode
- **Smart Resume**: When using `--debug --seed-directus`, automatically uses existing debug files if available
- **No Re-analysis**: Can seed from previous analysis runs without expensive LLM calls
- **Flexible Workflow**: Separate analysis and seeding phases for development/testing

### Security
- **Environment variables**: All secrets stored in .env file
- **Gitignored secrets**: .env file excluded from version control
- **Token validation**: Startup checks for required credentials
- **API Authentication**: Secure token-based authentication for both GraphQL and Directus APIs