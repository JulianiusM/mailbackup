# Test Structure Guidelines

## Overview

The mailbackup test suite follows a **domain-based organization** where each module in the codebase has corresponding test files in both `unit/` and `integration/` directories.

## Directory Structure

```
tests/
├── conftest.py              # Shared fixtures and test configuration
├── unit/                    # Unit tests (one file per module)
│   ├── test_config.py       # Tests for config.py
│   ├── test_db.py           # Tests for db.py
│   ├── test_executor.py     # Tests for executor.py
│   ├── test_extractor.py    # Tests for extractor.py
│   ├── test_integrity.py    # Tests for integrity.py
│   ├── test_logger.py       # Tests for logger.py
│   ├── test_main.py         # Tests for __main__.py
│   ├── test_manifest.py     # Tests for manifest.py
│   ├── test_orchestrator.py # Tests for orchestrator.py
│   ├── test_rclone.py       # Tests for rclone.py
│   ├── test_rotation.py     # Tests for rotation.py
│   ├── test_statistics.py   # Tests for statistics.py
│   ├── test_uploader.py     # Tests for uploader.py
│   └── test_utils.py        # Tests for utils.py
└── integration/             # Integration tests (one file per module)
    ├── test_executor.py     # Integration tests for executor (interrupt handling, etc.)
    ├── test_extractor.py    # Integration tests for extractor (email processing, etc.)
    ├── test_integrity.py    # Integration tests for integrity (checking, repair, etc.)
    ├── test_main.py         # Integration tests for CLI and main module
    ├── test_manifest.py     # Integration tests for manifest management
    ├── test_uploader.py     # Integration tests for upload workflows
    └── test_utils.py        # Integration tests for utility functions
```

## File Naming Convention

**Rule**: One test file per domain/module

### Unit Tests
- **Pattern**: `tests/unit/test_<module_name>.py`
- **Purpose**: Test individual functions and classes in isolation
- **Example**: `tests/unit/test_extractor.py` tests functions in `mailbackup/extractor.py`

### Integration Tests
- **Pattern**: `tests/integration/test_<module_name>.py`
- **Purpose**: Test how components work together in real scenarios
- **Example**: `tests/integration/test_extractor.py` tests end-to-end email extraction workflows

## Adding New Tests

### When Adding a New Module

If you create `mailbackup/new_module.py`:

1. Create `tests/unit/test_new_module.py` for unit tests
2. Create `tests/integration/test_new_module.py` for integration tests (if needed)

### When Adding Tests to Existing Modules

1. **For unit tests**: Add test methods to the appropriate class in `tests/unit/test_<module>.py`
2. **For integration tests**: Add test methods to the appropriate class in `tests/integration/test_<module>.py`

### Test Class Naming

- Unit test classes: `Test<ClassName>` or `Test<FunctionName>`
- Integration test classes: `Test<ModuleName>Integration` or `Test<Feature>Integration`

Example:
```python
# In tests/unit/test_extractor.py
class TestProcessEmailFile:
    """Unit tests for process_email_file function."""
    
    def test_process_email_file_basic(self):
        ...

# In tests/integration/test_extractor.py
@pytest.mark.integration
class TestExtractorIntegration:
    """Integration tests for extractor module."""
    
    def test_complete_email_extraction_workflow(self):
        ...
```

## Test Organization Principles

### 1. **Domain-Based Grouping**
- All tests for a module go in ONE file per test type (unit/integration)
- No more `test_comprehensive_coverage.py` or `test_final_coverage.py`
- Each test file maps to exactly one source module

### 2. **Clear Separation**
- **Unit tests**: Test functions/classes in isolation with mocked dependencies
- **Integration tests**: Test workflows with real/minimal mocking

### 3. **Consistent Structure**
```python
#!/usr/bin/env python3
"""
<Unit|Integration> tests for <module> module.
"""

import pytest
from mailbackup.<module> import <what_you_test>
from mailbackup.statistics import create_stats, StatKey


@pytest.mark.<unit|integration>
class Test<FeatureName>:
    """Tests for <feature> functionality."""
    
    def test_<feature>_<scenario>(self, fixtures):
        """Test <feature> with <scenario>."""
        # Arrange
        ...
        
        # Act
        result = some_function()
        
        # Assert
        assert result == expected
```

### 4. **Statistics Usage**
Always use `create_stats()` and `StatKey` enum:

```python
from mailbackup.statistics import create_stats, StatKey

stats = create_stats()
stats.increment(StatKey.BACKED_UP, 5)
count = stats[StatKey.VERIFIED]
```

## Anti-Patterns to Avoid

❌ **Don't create coverage-focused test files**
- Bad: `test_comprehensive_coverage.py`, `test_final_coverage.py`
- Good: Tests organized by domain in existing test files

❌ **Don't create cross-domain test files**
- Bad: `test_pipeline.py` with tests for multiple modules
- Good: Tests in their respective domain files

❌ **Don't use string-based statistics**
```python
# Bad
stats = {"uploaded": 0}
stats["uploaded"] += 1

# Good
stats = create_stats()
stats.increment(StatKey.BACKED_UP)
```

❌ **Don't mix unit and integration tests in the same file**
- Keep them separate in `unit/` and `integration/` directories

## Running Tests

```bash
# All tests
pytest

# Unit tests only
pytest tests/unit/

# Integration tests only
pytest tests/integration/

# Specific domain
pytest tests/unit/test_extractor.py
pytest tests/integration/test_extractor.py

# With coverage
pytest --cov=mailbackup --cov-report=term-missing
```

## Test Markers

Use pytest markers appropriately:

```python
@pytest.mark.unit
class TestUnitTests:
    """Unit tests."""

@pytest.mark.integration
class TestIntegrationTests:
    """Integration tests."""

@pytest.mark.skip(reason="...")
def test_skipped():
    """Skipped test."""
```

## Coverage Goals

- **Target**: 95% overall code coverage
- **Per module**: Aim for 90%+ coverage for each module
- **Focus**: Add tests to domain files, not separate coverage files

## Migration Notes

The test suite was reorganized from a coverage-focused structure to a domain-based structure:

### Old Structure (❌ Removed)
- `test_comprehensive_coverage.py` - Mixed tests for multiple modules
- `test_final_coverage.py` - Coverage-focused tests
- `test_targeted_coverage.py` - More coverage tests
- `test_pipeline.py` - Cross-domain pipeline tests

### New Structure (✅ Current)
- One file per domain in `unit/` and `integration/`
- Tests organized by the module they test
- Clear, maintainable structure

This ensures that when you work on a module, all its tests are in one predictable location.
