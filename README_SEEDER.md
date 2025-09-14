# Directus Seeder for Generali Analysis Results

This script (`directus_seeder.py`) inserts insurance analysis results from `generali_analysis_results.json` into your Directus instance under an existing dcm_product while maintaining proper hierarchical relationships.

## Features

- **Works with Existing Products**: Seeds data under an existing dcm_product (no product creation/modification)
- **Hierarchical Data Insertion**: Maintains proper relationships between segments, benefits, and their conditions/limits/exclusions
- **Dry Run Mode**: Test the insertion process without making actual changes
- **Cleanup Functionality**: Delete seeded data with proper foreign key constraint handling (preserves the original product)
- **Progress Tracking**: Detailed logging and progress indicators
- **Error Handling**: Robust error handling with detailed feedback

## Data Structure

The script handles the following hierarchical structure under your existing dcm_product:

```
dcm_product (Your Existing Product - NOT MODIFIED)
├── insurance_dcm_segment (e.g., luggage_travel_delay)
│   ├── insurance_dcm_benefit (e.g., missed_connection)
│   │   ├── insurance_dcm_condition
│   │   ├── insurance_dcm_limit
│   │   └── insurance_dcm_exclusion
│   ├── insurance_dcm_condition (segment-level)
│   ├── insurance_dcm_limit (segment-level)
│   └── insurance_dcm_exclusion (segment-level)
```

## Usage

### Prerequisites

1. **Existing dcm_product**: You must have an existing dcm_product in Directus that you want to seed data under
2. Ensure your `.env` file contains:
   ```bash
   GRAPHQL_URL=https://your-directus-instance.com/graphql
   GRAPHQL_AUTH_TOKEN=your_auth_token
   ```
3. Have `generali_analysis_results.json` in the same directory

### Commands

#### Dry Run (Recommended First Step)
Test what would be inserted without making changes:
```bash
uv run directus_seeder.py --dry-run --product-id <your-existing-dcm-product-id>
```

#### Insert Data
Insert the analysis results under your existing dcm_product:
```bash
uv run directus_seeder.py --product-id <your-existing-dcm-product-id>
```

#### Delete Seeded Data
Clean up previously inserted data (preserves the original product):
```bash
uv run directus_seeder.py --delete
```

#### Custom Results File
Use a different results file:
```bash
uv run directus_seeder.py --product-id <your-existing-dcm-product-id> --results-file path/to/your/results.json
```

## How It Works

### 1. Data Analysis
The script analyzes the hierarchical structure in `generali_analysis_results.json` and maps it to Directus collections.

### 2. Product Validation
The script validates the existing dcm_product and extracts its domain_context_model for taxonomy mapping.

### 3. Insertion Order
Items are created in the correct order to respect foreign key relationships:
1. **Existing dcm_product** (validated, not modified)
2. `insurance_dcm_segment` (linked to existing product)
3. `insurance_dcm_benefit` (linked to segment)
4. `insurance_dcm_condition/limit/exclusion` (linked to appropriate level)

### 4. Relationship Management
Each item is properly linked to its parent through foreign key relationships:
- Segments link to your existing product via `dcm_product` field
- Benefits link to segments via `insurance_dcm_segment` field
- Conditions/limits/exclusions can link to product, segment, OR benefit level

### 5. Data Tracking
All created items (except the existing product) are tracked in `generali_seeded_data.json` for easy cleanup.

## Data Mapping

| JSON Field | Directus Field | Notes |
|------------|----------------|--------|
| `item_name` | `*_name` | Maps to segment_name, benefit_name, etc. |
| `description` | `description` | Item description |
| `section_reference` | `section_reference` | Document section reference |
| `full_text_part` | `full_text_part` | Source text from document |
| `llm_summary` | `llm_summary` | AI-generated summary |
| `value` | `actual_value` / `limit_value` | Numeric values |
| `unit` | `unit` / `limit_unit` | Units of measurement |
| `is_included` | - | Used for filtering (only included items are inserted) |

## Output

The script provides detailed progress information:

```
Using existing dcm_product: 42ee13f7-4703-42be-b454-92fd33004dc3
✓ Found product with DCM ID: d427fe94-fc61-4269-8584-78556a36758c

Processing segment: luggage_travel_delay
  ✓ Created segment: luggage_travel_delay (ID: 456)
    ✓ Created benefit: missed_connection (ID: 789)
      ✓ Created limit: maximum_coverage_amount at benefit level (ID: 101)

Summary:
- Products: 1 (existing, not created)
- Segments: 11
- Benefits: 45
- Conditions: 12
- Limits: 78
- Exclusions: 23
```

## Error Handling

- **Authentication Errors**: Check your `GRAPHQL_AUTH_TOKEN`
- **Network Errors**: Verify `GRAPHQL_URL` is accessible
- **Data Validation**: Directus field validation errors are displayed with details
- **Foreign Key Constraints**: Deletion respects dependency order

## Cleanup

When using `--delete`:
1. Items are deleted in reverse dependency order
2. Foreign key constraints are respected
3. The tracking file `generali_seeded_data.json` is removed
4. Detailed deletion progress is shown

## Notes

- Only items with `"is_included": true` are processed
- All items are created with `status: "published"`
- Timestamps use UTC timezone
- The script is idempotent - running it multiple times creates new data (doesn't update existing)