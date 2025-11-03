#!/usr/bin/env python3
"""
__main__.py

Top-level CLI for mailbackup.
Maps CLI actions to pipeline plans and delegates execution to orchestrator.
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from .config import load_settings
from .utils import ensure_dirs, install_signal_handlers, StatusThread
from . import db
from .manifest import ManifestManager
from .orchestrator import run_pipeline

# Use the centralized logging factory
from .logger import setup_logger, get_logger


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Mailbackup – Backup and archive your maildir safely")
    p.add_argument(
        "action",
        choices=[
            "fetch",
            "process",
            "backup",
            "archive",
            "check",
            "run",
            "full",
            # legacy aliases
            "extract", "upload", "rotate", "verify",
        ],
        help="Which stage or pipeline to run",
    )
    p.add_argument("--config", type=Path, help="Path to config file")
    return p


def main():
    args = build_parser().parse_args()
    settings = load_settings(args.config)

    # Initialize central logger once via logging.setup_logger, then obtain module logger
    setup_logger(settings.log_path)
    logger = get_logger(__name__)

    ensure_dirs(settings.tmp_dir, settings.archive_dir, settings.maildir, settings.attachments_dir)

    # Ensure DB schema is present. Failure here is fatal for the run.
    try:
        logger.debug(f"Ensuring database schema at {settings.db_path}")
        db.ensure_schema(settings.db_path)
    except Exception as e:
        logger.exception(f"Failed to ensure DB schema: {e}")
        sys.exit(2)

    manifest = ManifestManager(settings)
    manifest.recover_interrupted()
    manifest.restore_queue()

    stats = {"extracted": 0, "uploaded": 0, "archived": 0, "verified": 0, "repaired": 0, "skipped": 0}

    status_thread = StatusThread(settings.status_interval, stats)
    status_thread.start()

    def on_interrupt(signum, frame):
        logger.warning("Interrupt received. Saving state and exiting safely...")
        manifest.dump_queue()
        status_thread.stop()
        sys.exit(1)

    install_signal_handlers(on_interrupt)

    start = time.time()

    # ------------------------------------------------------------------
    # Declarative action mapping
    # ------------------------------------------------------------------
    plans = {
        "fetch":   dict(fetch=True,  process=False, stages=[]),
        "process": dict(fetch=False, process=True,  stages=[]),
        "backup":  dict(fetch=False, process=False, stages=["backup"]),
        "archive": dict(fetch=False, process=False, stages=["archive"]),
        "check":   dict(fetch=False, process=False, stages=["check"]),
        "run":     dict(fetch=False, process=True,  stages=["backup", "archive", "check"]),
        "full":    dict(fetch=True,  process=True,  stages=["backup", "archive", "check"]),
        # Legacy aliases
        "extract": dict(fetch=False, process=True,  stages=[]),
        "upload":  dict(fetch=False, process=False, stages=["backup"]),
        "rotate":  dict(fetch=False, process=False, stages=["archive"]),
        "verify":  dict(fetch=False, process=False, stages=["check"]),
    }

    try:
        plan = plans.get(args.action)
        if not plan:
            logger.error(f"Unknown action: {args.action}")
            sys.exit(1)

        run_pipeline(settings, manifest, stats, **plan)

    finally:
        manifest.dump_queue()
        status_thread.stop()

    elapsed = time.time() - start
    logger.info(f"Action '{args.action}' completed in {elapsed:.1f}s")
    logger.info(
        f"Processed={stats['extracted']} | Uploaded={stats['uploaded']} | Archived={stats['archived']} | Verified={stats['verified']} | Repaired={stats['repaired']} | Skipped={stats['skipped']}"
    )


if __name__ == "__main__":
    main()
