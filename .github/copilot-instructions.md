# Copilot Instructions for mailbackup

## Project Overview

**mailbackup** is a Python-based tool for incremental backup of maildir-format email to cloud storage via rclone. It provides a modular pipeline for fetching, processing, backing up, archiving, and verifying email data.

### Key Features
- Extract attachments and message bodies from .eml files
- Incremental backup to cloud storage (Nextcloud, etc.) via rclone
- Archive rotation based on retention policies
- Integrity verification and repair capabilities
- SQLite-based state management
- TOML/INI configuration support

## Architecture

### Module Structure

The codebase is organized into focused, single-responsibility modules:

- **`__main__.py`**: CLI entry point, argument parsing, and action mapping to pipeline plans
- **`orchestrator.py`**: Central pipeline execution engine that coordinates stages
- **`config.py`**: Configuration loading (TOML preferred, INI fallback)
- **`db.py`**: SQLite access layer with thread-local connections
- **`executor.py`**: Parallel task execution with thread pool and interrupt handling
- **`extractor.py`**: Attachment and body extraction from mail files
- **`uploader.py`**: Incremental upload to remote storage via rclone
- **`rotation.py`**: Archive management based on retention policies
- **`integrity.py`**: Verification and repair of backed-up data
- **`manifest.py`**: State management for pipeline operations
- **`logger.py`**: Centralized logging setup and factory
- **`rclone.py`**: rclone command wrapper and configuration
- **`statistics.py`**: Thread-safe statistics tracking with StatKey enum
- **`utils.py`**: Shared utilities (sanitization, hashing, etc.)

### Pipeline Stages

The orchestrator runs stages in order based on the action:
1. **Fetch**: Run mbsync or similar to download new mail
2. **Process**: Extract attachments and bodies from mail files
3. **Backup**: Upload extracted files to remote storage
4. **Archive**: Rotate old archives based on retention policy
5. **Check**: Verify integrity of backups and repair if needed

### Data Flow

```
Maildir → Extractor → SQLite DB + Attachments Dir → Uploader → Remote Storage
                                                    ↓
                                              Integrity Check
                                                    ↓
                                            Rotation/Archiving
```

## Python Environment

- **Python Version**: 3.11+ (uses `tomllib` from stdlib)
- **Key Dependencies**: rclone (external), sqlite3 (stdlib), email (stdlib)
- **Concurrency**: ThreadPoolExecutor for parallel extraction
- **Thread Safety**: Thread-local SQLite connections

## Coding Conventions

### Style Guidelines
- Use type hints (`from __future__ import annotations`)
- Follow PEP 8 naming conventions
- Include module-level docstrings describing purpose
- Use relative imports within the package (e.g., `from .config import Settings`)
- Prefer descriptive variable names over abbreviations

### Error Handling
- Use centralized logger from `logger.get_logger(__name__)`
- Log exceptions with `logger.exception()` for stack traces
- Raise appropriate exceptions for fatal errors
- Use try/finally blocks to ensure cleanup (e.g., manifest dumping)

### Database Operations
- Always use `db.get_connection(db_path)` for thread-safe access
- Use parameterized queries to prevent SQL injection
- Commit after write operations
- Handle `sqlite3.IntegrityError` for duplicate prevention

### Configuration
- Settings are loaded via `config.load_settings(config_path)`
- Support both TOML (preferred) and INI formats
- Use `dataclass` for Settings to ensure type safety
- Validate required fields at load time

### Logging
### Logging
- Initialize logger once with `setup_logger(log_path)` in main
- Use `get_logger(__name__)` in each module
- Log levels: DEBUG (verbose), INFO (normal), WARNING (issues), ERROR (failures)
- Status logs for long-running operations

### Statistics
- Use `create_stats()` to create ThreadSafeStats instance
- Always use `StatKey` enum for counter keys (not strings)
- Available keys: FETCHED, EXTRACTED, BACKED_UP, ARCHIVED, VERIFIED, REPAIRED, SKIPPED, PROCESSED, FAILED
- Thread-safe increment: `stats.increment(StatKey.BACKED_UP, 5)`
- Access counters: `count = stats[StatKey.VERIFIED]` or `stats.get(StatKey.VERIFIED)`
- Format status: `stats.format_status()` for user-friendly output

## Testing Approach

The project has comprehensive test coverage (90%) with 344 tests:
- **Unit tests**: Test individual modules in isolation with mocked dependencies
- **Integration tests**: Test end-to-end workflows with temporary directories
- Use `pytest` with `pytest-mock` for mocking
- Follow existing patterns in `tests/unit/` and `tests/integration/`
- Always use `create_stats()` and `StatKey` enum in tests
- Mock external dependencies (rclone, filesystem, network, subprocess)
- Test error handling and edge cases
- Ensure thread-safety for concurrent operations

## Development Workflow

### Adding New Features
1. Follow the single-responsibility principle for modules
2. Update `__all__` in `__init__.py` if adding new modules
3. Add configuration options to Settings dataclass if needed
4. Use the existing logger factory pattern
5. Ensure thread safety for concurrent operations

### Modifying Pipeline
- Update action mappings in `__main__.py` plans dictionary
- Add new stages to `orchestrator.run_pipeline()` if needed
- Pass ThreadSafeStats instance to all pipeline stages
- Use `log_status(stats, stage_name)` to log statistics
- Maintain backward compatibility with legacy action names

### Configuration Changes
- Update both TOML and INI parsing in `config.py`
- Document new settings in `mailbackup.example.toml`
- Provide sensible defaults where possible
- Validate configuration at load time

## Common Patterns

### File Path Handling
```python
from pathlib import Path
path = Path(some_string)
path.mkdir(parents=True, exist_ok=True)
```

### Logging Pattern
```python
from .logger import get_logger
logger = get_logger(__name__)
logger.info("Message")
```

### Database Access
```python
from . import db
conn = db.get_connection(settings.db_path)
cursor = conn.cursor()
cursor.execute("SELECT ...", (param,))
conn.commit()
```

### Status Tracking
```python
from .statistics import create_stats, StatKey, StatusThread
stats = create_stats()
stats.increment(StatKey.BACKED_UP, 5)

status_thread = StatusThread(interval, stats)
status_thread.start()
# ... work ...
status_thread.stop()
```

## Key Considerations

### Security
- Sanitize filenames to prevent directory traversal
- Use parameterized SQL queries
- Validate configuration file paths
- Handle credentials securely (rclone config)

### Performance
- Use ThreadPoolExecutor for I/O-bound operations
- Implement incremental processing (only process new files)
- Use streaming for large file operations
- Periodic status updates for long-running operations

### Reliability
- Graceful interrupt handling (SIGINT, SIGTERM)
- State persistence via manifest for recovery
- Integrity checking with SHA256 hashes
- Repair capabilities for corrupted data

## External Dependencies

### rclone
- Used for cloud storage synchronization
- Configured via separate rclone config file
- Commands built with `shlex.split()` for safety
- Supports various cloud providers (Nextcloud, etc.)

### mbsync
- Optional mail fetching command
- Configured via `fetch_command` setting
- Executed via subprocess with streaming output

## File Naming and Organization

- Main module files in root package directory
- Configuration files: `mailbackup.toml` or `mailbackup.ini`
- Example config: `mailbackup.example.toml`
- Database: configurable via `db_path` setting
- Logs: configurable via `log_path` setting
- Attachments: organized by year in `attachments_dir`

## When Making Changes

1. **Preserve Existing Behavior**: This is production mail backup software
2. **Maintain Thread Safety**: Database and file operations must be thread-safe
3. **Update Documentation**: Keep example config and docstrings current
4. **Consider Migration**: Database schema changes need migration path
5. **Test Error Paths**: Email parsing is unpredictable, handle failures gracefully
6. **Log Appropriately**: Users need visibility into what's happening with their mail
