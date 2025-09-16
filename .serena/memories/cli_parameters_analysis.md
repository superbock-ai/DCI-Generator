# CLI Parameters Analysis

## Current CLI Arguments in main.py

### Required Arguments
- **product_id** (positional): Product ID from Directus to analyze

### Optional Flags (Boolean)
- **--export, -e**: Export results to JSON file
- **--detailed, -d**: Show detailed results  
- **--no-cache**: Disable caching for this run
- **--debug**: Enable debug mode (save intermediate results and auto-resume)
- **--debug-clean**: Delete existing debug files before running
- **--seed-directus**: Seed analysis results to Directus after analysis
- **--dry-run-directus**: Dry run mode for Directus seeding
- **--cleanup-directus**: Clean up previously seeded data from Directus

### Optional Parameters with Values
- **--segment-chunks** (int, default=8): Number of segments to process in parallel per chunk
- **--benefit-chunks** (int, default=8): Number of benefits to process in parallel per chunk  
- **--detail-chunks** (int, default=3): Number of details to process in parallel per chunk
- **--debug-from** (choices=["segments", "benefits", "details"]): Force re-run from specific tier

## Mapping to FastAPI POST Payload Structure

### Analysis Job Payload
```json
{
  "product_id": "uuid-string",
  "export": false,
  "detailed": false, 
  "no_cache": false,
  "segment_chunks": 8,
  "benefit_chunks": 8,
  "detail_chunks": 3,
  "debug": false,
  "debug_clean": false,
  "debug_from": null,
  "seed_directus": false,
  "dry_run_directus": false
}
```

### Cleanup Job Payload
```json
{
  "product_id": "uuid-string"
}
```

## Celery Task Arguments
Both job types will receive the same parameter structure as the FastAPI payload, allowing direct pass-through of arguments from API to Celery worker.