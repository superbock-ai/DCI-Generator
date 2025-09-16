# Code Style and Conventions

## Python Code Style
- **Type Hints**: Extensive use throughout codebase (function parameters, return types)
- **Docstrings**: Triple-quoted docstrings for functions, especially main entry points
- **Class Structure**: Well-organized classes with clear separation of concerns
- **Error Handling**: Comprehensive try-catch blocks with detailed error messages
- **Logging**: Uses print() statements for progress tracking and status updates

## Naming Conventions
- **Functions**: snake_case (e.g., `parse_openai_wait_time`, `cleanup_seeded_data`)
- **Classes**: PascalCase (e.g., `DocumentAnalyzer`, `DirectusConfig`, `AnalysisResult`)
- **Variables**: snake_case (e.g., `document_text`, `total_benefits`)
- **Constants**: UPPER_SNAKE_CASE (e.g., environment variables)

## Code Organization
- **Single responsibility**: Functions and classes have clear, focused purposes
- **Configuration**: All secrets and configs via environment variables
- **Data Models**: Pydantic models for structured data (AnalysisResult)
- **Async/Await**: Proper async programming patterns where needed
- **Import Organization**: Standard library imports first, then third-party, then local

## Error Handling Patterns
- **Graceful failures**: Return status codes from main function
- **User-friendly messages**: Clear error messages with guidance
- **Validation**: Environment variable validation at startup
- **Retry logic**: Smart retry with exponential backoff using tenacity library

## Documentation Style
- **Inline comments**: Explaining complex logic, especially AI/LLM interactions
- **Function docstrings**: Clear description of purpose and parameters
- **CLI help**: Comprehensive argparse help text for all options