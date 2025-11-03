# Mailbackup Test Suite

This directory contains unit and integration tests for the mailbackup project.

## Test Structure

```
tests/
├── conftest.py              # Shared fixtures and test configuration
├── unit/                    # Unit tests for individual modules
│   ├── test_config.py       # Tests for configuration loading
│   ├── test_db.py           # Tests for database operations
│   ├── test_extractor.py    # Tests for email extraction
│   ├── test_integrity.py    # Tests for integrity checking
│   ├── test_logger.py       # Tests for logging setup
│   ├── test_main.py         # Tests for CLI entry point
│   ├── test_manifest.py     # Tests for manifest management
│   ├── test_orchestrator.py # Tests for pipeline orchestration
│   ├── test_rclone.py       # Tests for rclone wrapper functions
│   ├── test_rotation.py     # Tests for archive rotation
│   ├── test_uploader.py     # Tests for incremental uploads
│   └── test_utils.py        # Tests for utility functions
└── integration/             # Integration tests
    └── test_cli.py          # Tests for CLI and pipeline integration

```

## Running Tests

### Install Test Dependencies

```bash
pip install -r requirements-dev.txt
```

### Run All Tests

```bash
# From the repository root
python -m pytest mailbackup/tests/

# With verbose output
python -m pytest mailbackup/tests/ -v

# With coverage report
python -m pytest mailbackup/tests/ --cov=mailbackup --cov-report=term-missing
```

### Run Specific Test Categories

```bash
# Unit tests only
python -m pytest mailbackup/tests/unit/

# Integration tests only
python -m pytest mailbackup/tests/integration/

# Specific test file
python -m pytest mailbackup/tests/unit/test_utils.py

# Specific test class or function
python -m pytest mailbackup/tests/unit/test_utils.py::TestSanitize
python -m pytest mailbackup/tests/unit/test_utils.py::TestSanitize::test_sanitize_basic_string
```

## Test Coverage

Current test coverage is approximately **88%** for the core modules (as of latest update). This represents a significant improvement from the initial 65% coverage.

### Coverage Improvements

**Major improvements:**
- Added comprehensive tests for `__main__.py` (33% → 87%)
- Added comprehensive tests for `logger.py` (39% → 100%)  
- Added comprehensive tests for `orchestrator.py` (28% → 100%)
- Added comprehensive tests for `manifest.py` (15% → 75%)
- Added comprehensive tests for `uploader.py` (13% → 89%)
- Added comprehensive tests for `rotation.py` (11% → 75%)
- Added comprehensive tests for `integrity.py` (13% → 47%)

**Well-covered modules (85%+ coverage):**
- `__main__.py` - 87% coverage (CLI entry point)
- `config.py` - 92% coverage (configuration loading)
- `db.py` - 89% coverage (database operations)
- `logger.py` - 100% coverage (logging setup)
- `orchestrator.py` - 100% coverage (pipeline orchestration)
- `rclone.py` - 100% coverage (rclone wrapper)
- `uploader.py` - 89% coverage (incremental uploads)
- All unit test files - 100% coverage

**Moderately covered modules (60-85% coverage):**
- `manifest.py` - 75% coverage (manifest management)
- `rotation.py` - 75% coverage (archive rotation)
- `extractor.py` - 63% coverage (email extraction)

**Areas for future improvement:**
- `utils.py` - 54% coverage (utility functions - especially `remote_hash` and `compute_remote_sha256`)
- `integrity.py` - 47% coverage (integrity checking and repair functions - especially `repair_remote`)
- Integration tests for full pipeline workflows with actual rclone commands

### Coverage by Test Type

The test suite now includes:
- **225+ unit tests** covering individual functions and classes
- **15+ integration tests** covering CLI and pipeline workflows  
- **Comprehensive mocking** of external dependencies (rclone, subprocess, filesystem)
- **Thread-safety tests** for concurrent operations
- **Error handling tests** for edge cases and failures

### Running Coverage Reports

Generate detailed HTML coverage reports:
```bash
python -m pytest --cov=. --cov-report=html
# Open htmlcov/index.html in your browser
```

View missing lines in terminal:
```bash
python -m pytest --cov=. --cov-report=term-missing
```

Generate XML coverage report for CI:
```bash
python -m pytest --cov=. --cov-report=xml
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

## Continuous Integration

Tests run automatically on GitHub Actions when code is pushed or pull requests are created.

See `.github/workflows/python-tests.yml` for the CI configuration.

## Troubleshooting

### ImportError issues

If you encounter import errors, ensure you're running tests from the repository root:
```bash
cd /path/to/mailbackup
python -m pytest mailbackup/tests/
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
