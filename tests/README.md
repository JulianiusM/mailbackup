# Mailbackup Test Suite

This directory contains unit and integration tests for the mailbackup project.

## Test Structure

```
tests/
├── conftest.py              # Shared fixtures and test configuration
├── unit/                    # Unit tests for individual modules
│   ├── test_config.py       # Tests for configuration loading
│   ├── test_db.py           # Tests for database operations
│   ├── test_executor.py     # Tests for parallel executor
│   ├── test_extractor.py    # Tests for email extraction
│   ├── test_integrity.py    # Tests for integrity checking
│   ├── test_logger.py       # Tests for logging setup
│   ├── test_main.py         # Tests for CLI entry point
│   ├── test_manifest.py     # Tests for manifest management
│   ├── test_orchestrator.py # Tests for pipeline orchestration
│   ├── test_rclone.py       # Tests for rclone wrapper functions
│   ├── test_rotation.py     # Tests for archive rotation
│   ├── test_statistics.py   # Tests for statistics tracking
│   ├── test_uploader.py     # Tests for incremental uploads
│   └── test_utils.py        # Tests for utility functions
└── integration/             # Integration tests
    ├── test_cli.py          # Tests for CLI and pipeline integration
    ├── test_comprehensive_coverage.py  # Comprehensive integration tests
    ├── test_final_coverage.py         # Final coverage integration tests
    ├── test_integrity_integration.py  # Integrity checking integration tests
    ├── test_interrupt_handling.py     # Interrupt handling tests
    ├── test_pipeline.py               # Pipeline integration tests
    ├── test_targeted_coverage.py      # Targeted coverage tests
    └── test_utils_integration.py      # Utils integration tests

```

## Running Tests

### Install Test Dependencies

```bash
pip install -r requirements-dev.txt
```

### Run All Tests

```bash
# From the repository root
python -m pytest

# With verbose output
python -m pytest -v

# With coverage report
python -m pytest --cov=mailbackup --cov-report=term-missing
```

### Run Specific Test Categories

```bash
# Unit tests only
python -m pytest tests/unit/

# Integration tests only
python -m pytest tests/integration/

# Specific test file
python -m pytest tests/unit/test_utils.py

# Specific test class or function
python -m pytest tests/unit/test_utils.py::TestSanitize
python -m pytest tests/unit/test_utils.py::TestSanitize::test_sanitize_basic_string
```

## Test Coverage

**Current test coverage: 90%** (combined unit and integration tests)

The comprehensive test suite includes 344 tests covering all major code paths.

### Coverage by Module

**95%+ coverage:**

- `integrity.py` - 96% (integrity verification and repair)

**90-95% coverage:**

- `config.py` - 92% (configuration loading)
- `executor.py` - 93% (parallel task execution)
- `utils.py` - 90% (utility functions)
- `extractor.py` - 89% (email extraction)
- `db.py` - 89% (database operations)

**85-90% coverage:**

- `__main__.py` - 86% (CLI entry point)

**75-85% coverage:**

- `uploader.py` - 83% (incremental uploads)
- `manifest.py` - 82% (manifest management)
- `rotation.py` - 77% (archive rotation)

**100% coverage:**

- `logger.py` - 100% (logging setup)
- `orchestrator.py` - 100% (pipeline orchestration)
- `rclone.py` - 100% (rclone wrapper)
- `statistics.py` - 100% (statistics tracking)

### Coverage by Test Type

The test suite includes:

- **170+ unit tests** covering individual functions and classes
- **130+ integration tests** covering real-world scenarios and workflows
- **Comprehensive mocking** of external dependencies (rclone, subprocess, filesystem)
- **Thread-safety tests** for concurrent operations
- **Error handling tests** for edge cases and failures

### Running Coverage Reports

Generate detailed HTML coverage reports:

```bash
python -m pytest --cov=mailbackup --cov-report=html
# Open htmlcov/index.html in your browser
```

View missing lines in terminal:

```bash
python -m pytest --cov=mailbackup --cov-report=term-missing
```

Generate XML coverage report for CI:

```bash
python -m pytest --cov=mailbackup --cov-report=xml
```

Check coverage meets minimum threshold (90%):

```bash
python -m pytest --cov=mailbackup --cov-report=term --cov-fail-under=90
```

## Writing Tests

### Unit Tests

Unit tests should:

- Test individual functions or classes in isolation
- Use mocks for external dependencies (filesystem, network, subprocess)
- Be fast and deterministic
- Follow the naming convention `test_<function_name>_<scenario>`

Example:

```python
def test_sanitize_removes_special_chars():
    result = sanitize('test<>:"/\\|?*file')
    assert "<" not in result
    assert ">" not in result
```

### Integration Tests

Integration tests should:

- Test multiple components working together
- May use real filesystem (with temporary directories)
- Test end-to-end workflows
- Use subprocess to test CLI commands

Example:

```python
def test_process_workflow(tmp_path, sample_maildir):
    # Set up test environment
    # Run the CLI command
    # Assert expected outcomes
    pass
```

## Fixtures

Common fixtures are defined in `conftest.py`:

- `tmp_dir` - Temporary directory for test files
- `sample_maildir` - Pre-configured maildir structure
- `sample_email` - Simple RFC-822 email message
- `sample_email_with_attachment` - Email with PDF attachment
- `test_db` - Initialized test database
- `test_settings` - Settings object with test paths
- `mock_rclone` - Mocked rclone commands

## Statistics Module

The statistics module uses `StatKey` enum for type-safe counter management:

```python
from mailbackup.statistics import create_stats, StatKey

stats = create_stats()
stats.increment(StatKey.BACKED_UP, 5)
stats.increment(StatKey.VERIFIED, 2)

# Access counters
count = stats.get(StatKey.BACKED_UP)  # Returns 5
count = stats[StatKey.VERIFIED]  # Returns 2

# Format status
status = stats.format_status()  # Returns formatted string
```

Available StatKey values:
- `StatKey.FETCHED`
- `StatKey.EXTRACTED`
- `StatKey.BACKED_UP`
- `StatKey.ARCHIVED`
- `StatKey.VERIFIED`
- `StatKey.REPAIRED`
- `StatKey.SKIPPED`
- `StatKey.PROCESSED`
- `StatKey.FAILED`

## Continuous Integration

Tests run automatically on GitHub Actions when code is pushed or pull requests are created.

See `.github/workflows/python-tests.yml` for the CI configuration.

## Troubleshooting

### ImportError issues

If you encounter import errors, ensure you're running tests from the repository root:

```bash
cd /path/to/mailbackup
python -m pytest
```

### Database locked errors

If database tests fail with "database is locked" errors, this is usually due to:

- Multiple processes accessing the database
- Tests not properly cleaning up connections
- Use the `test_db` fixture which creates isolated database instances

### Mock not working

Ensure mocks target the correct import path. For example:

- Mock `mailbackup.rclone._run_rclone` for rclone tests
- Mock `mailbackup.utils.run_cmd` for utils tests
- Use the full module path including "mailbackup." prefix
