# mailbackup

mailbackup is a Python-based tool for incremental backup of maildir-format email to cloud storage via rclone.

## Features

- Incremental extraction of attachments and message bodies from email files
- Upload to remote storage using rclone
- Archive rotation and integrity checking
- SQLite-based state management

## Requirements

- Python 3.10+
- rclone (installed separately; not a Python package)

## Installation (development)

Open a Command Prompt and run:

```cmd
python -m venv .venv
.\.venv\Scripts\activate
python -m pip install --upgrade pip
python -m pip install -r requirements-dev.txt
```

## Quickstart

1. Configure `mailbackup.toml` or `mailbackup.ini` (see `mailbackup.example.toml`).
2. Initialize a database path and attachments directory in your config.
3. Run the CLI:

```cmd
python -m mailbackup --help
```

## Testing

Run the test suite (after installing dev requirements):

```cmd
.\.venv\Scripts\activate
pytest -q
```

## Contributing

Please open issues or pull requests. Follow the existing code style and add tests for new behavior.

