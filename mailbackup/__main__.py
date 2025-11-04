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

from mailbackup import db
from mailbackup.config import load_settings
from mailbackup.executor import get_global_interrupt_manager
from mailbackup.logger import setup_logger, get_logger
from mailbackup.manifest import ManifestManager
from mailbackup.orchestrator import run_pipeline
from mailbackup.statistics import StatusThread, create_stats, log_status
from mailbackup.utils import ensure_dirs, install_signal_handlers


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

    stats = create_stats()
    status_thread = StatusThread(settings.status_interval, stats)
    status_thread.start()

    # Get global interrupt manager for coordinated shutdown
    interrupt_manager = get_global_interrupt_manager()

    def on_interrupt(signum, frame):
        logger.warning("Interrupt received. Saving state and exiting safely...")
        # Signal all executors to shut down
        interrupt_manager.interrupt_all()
        # Save manifest state
        manifest.dump_queue()
        # Stop status thread
        status_thread.stop()
        sys.exit(1)

    install_signal_handlers(on_interrupt)

    start = time.time()

    # ------------------------------------------------------------------
    # Declarative action mapping
    # ------------------------------------------------------------------
    plans = {
        "fetch": dict(fetch=True, process=False, stages=[]),
        "process": dict(fetch=False, process=True, stages=[]),
        "backup": dict(fetch=False, process=False, stages=["backup"]),
        "archive": dict(fetch=False, process=False, stages=["archive"]),
        "check": dict(fetch=False, process=False, stages=["check"]),
        "run": dict(fetch=False, process=True, stages=["backup", "archive", "check"]),
        "full": dict(fetch=True, process=True, stages=["backup", "archive", "check"]),
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
    log_status(stats)


if __name__ == "__main__":
    main()
