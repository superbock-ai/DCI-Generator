# DCI Generator Project Overview

## Purpose
The DCI (Domain Context Item) Generator is a comprehensive AI-powered tool for analyzing German insurance documents using a three-tier hierarchical approach: Segments → Benefits → Details (limits/conditions/exclusions).

## Tech Stack
- **Language**: Python 3.12+
- **Package Manager**: uv (modern pip replacement)
- **AI/ML**: OpenAI GPT models via langchain-openai
- **Data Processing**: LangChain for AI orchestration
- **API Integration**: 
  - GraphQL via gql library for taxonomy fetching
  - REST API via requests for Directus integration
- **Data Validation**: Pydantic for structured output
- **Utilities**: 
  - python-dotenv for environment management
  - tenacity for retry logic with exponential backoff
  - requests-toolbelt for advanced HTTP features

## Core Architecture
- **Three-tier conditional analysis**: Only analyzes benefits for found segments, details for found benefits
- **Parallel processing**: Each tier analyzes all items simultaneously using LangChain RunnableParallel
- **Context-aware prompts**: Each tier builds on previous analysis results
- **Live GraphQL integration**: Fetches taxonomy hierarchies from Quinsights platform
- **Directus CMS integration**: Seeds analysis results with hierarchical relationships

## Key Features
- Chunked parallel processing with configurable chunk sizes per tier
- Smart rate limiting with OpenAI wait time parsing and retry logic
- Debug mode with auto-resume from intermediate saved results
- Comprehensive in-memory caching via LangChain
- Swiss number format handling and German text processing
- Environment-based configuration for security