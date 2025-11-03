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
│   ├── test_rclone.py       # Tests for rclone wrapper functions
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

Current test coverage is approximately 57% for the core modules.

**Well-covered modules:**
- `config.py` - 92% coverage
- `db.py` - 89% coverage  
- `rclone.py` - 93% coverage
- Unit test files - 100% coverage

**Areas needing more tests:**
- Integration tests for full pipeline workflows
- Tests for manifest.py, uploader.py, rotation.py, and integrity.py
- Error handling and edge cases in extractor.py

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
