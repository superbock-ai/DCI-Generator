# DCI Generator - Insurance Document Analysis Tool

A comprehensive AI-powered tool for analyzing insurance documents using a three-tier hierarchical approach: **Segments** → **Benefits** → **Details** (limits/conditions/exclusions).

## 🔍 Overview

The DCI (Domain Context Item) Generator performs intelligent, context-aware analysis of German insurance documents by:

1. **Identifying coverage segments** (e.g., luggage coverage, home assistance)
2. **Finding specific benefits** within discovered segments (e.g., missed connections, emergency purchases)
3. **Extracting detailed information** about limits, conditions, and exclusions for found benefits

## 🚀 Features

### Core Capabilities
- **Three-tier conditional analysis** - Only analyzes what exists in the document
- **Parallel processing** - Maximum speed at each analysis tier
- **Context-aware prompts** - Each tier builds on previous analysis results
- **Comprehensive caching** - Near-instant repeated analyses
- **Live GraphQL integration** - Real-time taxonomy data from Quinsights platform
- **Hierarchical tree output** - Natural taxonomy structure with nested relationships

### Advanced Features
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

## ⚙️ Requirements

- Python 3.12+
- OpenAI API access
- Quinsights GraphQL endpoint access
- `uv` package manager

## 🛠️ Installation

1. **Clone and setup**:
   ```bash
   cd dci_generator
   uv sync
   ```

2. **Configure environment**:
   ```bash
   cp .env.example .env
   # Edit .env with your credentials
   ```

3. **Required environment variables**:
   ```env
   OPENAI_API_KEY=your-openai-api-key
   GRAPHQL_AUTH_TOKEN=your-graphql-token  # Used for both GraphQL and Directus
   OPENAI_MODEL=gpt-4o-mini  # optional
   GRAPHQL_URL=https://app-uat.quinsights.tech/graphql  # optional, also used for Directus API
   ```

## 🎯 Usage

### Basic Analysis
```bash
uv run main.py travel-insurance/markdown/axa.md
```

### With Options
```bash
# Export results to JSON
uv run main.py travel-insurance/markdown/axa.md --export

# Show detailed analysis
uv run main.py travel-insurance/markdown/axa.md --detailed

# Disable caching for fresh results
uv run main.py travel-insurance/markdown/axa.md --no-cache

# All options combined
uv run main.py travel-insurance/markdown/axa.md --export --detailed --no-cache
```

### Chunk Size Configuration
```bash
# Default chunk sizes (8 segments, 8 benefits, 3 details)
uv run main.py document.md

# Conservative for reliability
uv run main.py document.md --segment-chunks 5 --benefit-chunks 4 --detail-chunks 2

# Aggressive for speed
uv run main.py document.md --segment-chunks 15 --benefit-chunks 12 --detail-chunks 5

# Token-safe for large responses
uv run main.py document.md --detail-chunks 1
```

### Debug Mode & Auto-Resume
```bash
# Enable debug mode (saves intermediate results)
uv run main.py document.md --debug

# Clean all debug files and start fresh
uv run main.py document.md --debug --debug-clean

# Force re-run from specific tier onwards
uv run main.py document.md --debug --debug-from benefits

# Ideal for token limit issues - resume with smaller chunks
uv run main.py document.md --debug --detail-chunks 1
```

### Directus Integration
```bash
# Analyze and seed to Directus in one step
uv run main.py document.md --export --debug --seed-directus --product-id 92f3ee1b-9b03-4085-ab01-555cd9b0507c

# Test seeding with dry run (shows what would be inserted)
uv run main.py document.md --seed-directus --product-id 92f3ee1b-9b03-4085-ab01-555cd9b0507c --dry-run-directus

# Seed from existing debug files (with debug mode, uses cached analysis)
uv run main.py document.md --debug --seed-directus --product-id 92f3ee1b-9b03-4085-ab01-555cd9b0507c

# Clean up ALL data for a product (including the product itself)
uv run main.py document.md --cleanup-directus --product-id 92f3ee1b-9b03-4085-ab01-555cd9b0507c
```

### Available Documents
The system includes sample documents from major Swiss insurers:
- `allianz.md`, `axa.md`, `css.md`, `erv.md`
- `generali.md`, `mobiliar.md`, `swica.md`, `zurich.md`

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
| `GRAPHQL_AUTH_TOKEN` | Yes | - | Quinsights GraphQL token |
| `OPENAI_MODEL` | No | `gpt-4o-mini` | OpenAI model to use |
| `GRAPHQL_URL` | No | UAT endpoint | GraphQL endpoint URL |

### Command Line Options
| Option | Short | Description |
|--------|-------|-------------|
| `--export` | `-e` | Export results to JSON file |
| `--detailed` | `-d` | Show comprehensive analysis details |
| `--no-cache` | - | Disable caching for fresh results |
| `--segment-chunks` | - | Segments per parallel chunk (default: 8) |
| `--benefit-chunks` | - | Benefits per parallel chunk (default: 8) |
| `--detail-chunks` | - | Details per parallel chunk (default: 3) |
| `--debug` | - | Enable debug mode with auto-resume |
| `--debug-clean` | - | Delete all debug files before running |
| `--debug-from` | - | Force re-run from specific tier (segments/benefits/details) |
| `--seed-directus` | - | Seed analysis results to Directus after analysis |
| `--product-id` | - | Existing dcm_product UUID to seed data under (required with --seed-directus) |
| `--dry-run-directus` | - | Show what would be seeded without making changes |
| `--cleanup-directus` | - | Remove ALL data for a product from Directus (requires --product-id, DELETES product) |

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
- **Per-Document Files**: `document_segments.debug.json`, `document_benefits.debug.json`, `document_details.debug.json`
- **Progress Visibility**: Clear logging shows what's loaded vs. what's running
- **Failure Recovery**: Resume from failures with different configurations (e.g., smaller chunk sizes)

## 📁 Project Structure

```
dci_generator/
├── main.py                          # Main CLI application
├── directus_seeder.py               # Directus integration module
├── dev.ipynb                        # Development notebook
├── graphql/
│   └── GetCompleteTaxonomyHierarchy.graphql  # GraphQL query
├── travel-insurance/
│   └── markdown/                    # Insurance documents
├── segment_structured_output.json   # Output schema definition
├── example_gql_response.json       # Sample GraphQL response
├── README_SEEDER.md                # Directus seeding documentation
├── .env.example                    # Environment template
├── .env                           # Environment configuration (gitignored)
├── CLAUDE.md                      # Developer documentation
└── README.md                      # This file
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

### Example Usage
```bash
# Complete workflow: analyze + seed in one step
uv run main.py generali.md --export --debug --seed-directus --product-id 92f3ee1b-9b03-4085-ab01-555cd9b0507c

# Test before seeding (dry run)
uv run main.py generali.md --seed-directus --product-id 92f3ee1b-9b03-4085-ab01-555cd9b0507c --dry-run-directus

# Seed from existing debug files (fast, no re-analysis)
uv run main.py generali.md --debug --seed-directus --product-id 92f3ee1b-9b03-4085-ab01-555cd9b0507c
```

For detailed Directus seeding documentation, see `README_SEEDER.md`.

## 🔬 Development

### Development Workflow
1. **Interactive Development**: Use `dev.ipynb` for experimentation
2. **Production Testing**: Use `main.py` for complete pipeline testing
3. **Prompt Iteration**: Leverage caching for rapid prompt development
4. **Schema Evolution**: Update `AnalysisResult` model as needed

### Adding New Analysis Items
1. **GraphQL Schema**: Add new taxonomy items to GraphQL endpoint
2. **Prompt Templates**: Create specialized analysis prompts
3. **Chain Integration**: Add to parallel processing pipeline
4. **Result Processing**: Update export and display logic

## 🚀 Future Enhancements

### Planned Features
- **Multi-language Support**: Extend beyond German insurance documents
- **Custom Taxonomies**: Support for user-defined analysis hierarchies
- **Batch Processing**: Analyze multiple documents simultaneously
- **API Interface**: REST API for programmatic access
- **Dashboard**: Web interface for analysis management

### Extensibility Points
- **New Document Formats**: PDF, HTML, DOCX support
- **Additional LLM Providers**: Azure OpenAI, Anthropic, local models
- **Export Formats**: Excel, CSV, PDF reports
- **Integration APIs**: Webhook notifications, database storage

## 🆘 Support

For technical issues:
1. Check environment configuration (`.env` file)
2. Verify network access to GraphQL endpoint
3. Validate OpenAI API quota and permissions
4. Review error messages for specific guidance

## 📄 License

This project is proprietary software for Quinsights insurance document analysis.