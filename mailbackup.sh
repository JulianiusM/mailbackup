#!/bin/bash
# Wrapper to run the mailbackup package in the venv
PROJECT_ROOT="/srv/mailbackup/bin"
VENV_PYTHON="/srv/mailbackup/venv/bin/python"

cd "$PROJECT_ROOT" || exit 1
exec "$VENV_PYTHON" -m mailbackup "$@"
