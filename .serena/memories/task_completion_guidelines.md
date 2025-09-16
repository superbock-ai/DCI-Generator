# Task Completion Guidelines

## When a Task is Completed

### Code Quality Checks
- **No specific linting/formatting commands identified** - Project uses standard Python conventions
- **Testing**: No automated test framework identified - manual testing via CLI commands
- **Type checking**: Ensure type hints are maintained throughout codebase

### Verification Steps
1. **Functionality testing**: Run main.py with various parameter combinations
2. **Environment validation**: Ensure .env variables are properly configured
3. **Integration testing**: Test Directus seeding and cleanup operations
4. **Error handling**: Verify graceful failure scenarios

### Documentation Updates
- Update README.md with new features or changes
- Update CLAUDE.md project instructions if needed
- Maintain clear docstrings and inline comments

### Git Operations
- **Commit messages**: Clear, descriptive messages explaining the "why"
- **Incremental commits**: Small, focused commits that compile and work
- **Status check**: `git status` before committing
- **Standard workflow**: `git add .` → `git commit -m "descriptive message"`

### Development Environment
- **Dependencies**: Use `uv add` for new packages, `uv sync` for installation
- **Environment**: Maintain .env.example with new variables
- **Debug files**: Clean up debug/* directory when appropriate

### Testing Approach
Since no automated testing framework is present:
- Test all CLI parameter combinations manually
- Verify Directus integration with actual API calls
- Test error scenarios and rate limiting
- Validate JSON export and debug resume functionality