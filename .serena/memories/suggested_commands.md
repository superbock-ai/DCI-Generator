# Suggested Commands for DCI Generator Development

## Package Management
- **Install dependencies**: `uv sync`
- **Add new dependency**: `uv add <package_name>`

## Main Application Usage
- **Basic analysis**: `uv run main.py <product-id>`
- **With JSON export**: `uv run main.py <product-id> --export`
- **With detailed results**: `uv run main.py <product-id> --detailed`
- **Disable caching**: `uv run main.py <product-id> --no-cache`
- **Custom chunk sizes**: `uv run main.py <product-id> --segment-chunks 8 --benefit-chunks 6 --detail-chunks 3`
- **Debug mode**: `uv run main.py <product-id> --debug`
- **Resume from failure**: `uv run main.py <product-id> --debug --detail-chunks 1`
- **All options**: `uv run main.py <product-id> -e -d --debug`

## Directus Integration
- **Seed to Directus**: `uv run main.py <product-id> --seed-directus`
- **Dry run seeding**: `uv run main.py <product-id> --seed-directus --dry-run-directus`
- **Analysis + seeding**: `uv run main.py <product-id> --export --debug --seed-directus`
- **Cleanup product data**: `uv run main.py <product-id> --cleanup-directus`

## Development
- **Interactive development**: Use `dev.ipynb` Jupyter notebook
- **Run notebook**: `jupyter notebook dev.ipynb`

## System Commands (macOS/Darwin)
- **List files**: `ls` or `ls -la`
- **Change directory**: `cd <path>`
- **Find files**: `find . -name "*.py"`
- **Search in files**: `grep -r "search_term" .`
- **Git operations**: `git status`, `git add .`, `git commit -m "message"`
- **Check Python version**: `python --version`
- **Environment variables**: `export VAR=value` or use `.env` file