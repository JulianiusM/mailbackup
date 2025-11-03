#!/usr/bin/env python3
"""
orchestrator.py

Single orchestration engine for mailbackup.
Executes ordered stages based on a high-level plan.
"""

from __future__ import annotations

import shlex
import subprocess
import time
from datetime import datetime

from .config import Settings
from .extractor import run_extractor
from .integrity import integrity_check
# Use factory logger
from .logger import get_logger
from .manifest import ManifestManager
from .rotation import rotate_archives
from .uploader import incremental_upload
from .utils import run_streaming


def _parse_command(cmd_str: str) -> list[str]:
    """Split a string safely for subprocess."""
    # explicit small wrapper so callers always get a list[str]
    return shlex.split(cmd_str)


def run_pipeline(
        settings: Settings,
        manifest: ManifestManager,
        stats: dict,
        fetch: bool = False,
        process: bool = False,
        stages: list[str] | None = None,
):
    """
    Execute the mailbackup pipeline based on the plan provided.

    Args:
        fetch: whether to run mbsync
        process: whether to extract attachments
        stages: ordered list of sync stages ['backup', 'archive', 'check']
    """
    # Acquire logger from factory; ignore the passed-in logger parameter.
    logger = get_logger(__name__)

    if stages is None:
        stages = []

    logger.info("=== Mailbackup pipeline started ===")
    logger.debug(f"Pipeline configuration: fetch={fetch} process={process} stages={stages}")
    logger.status(f"Pipeline started at {datetime.now().isoformat()}")
    total_start = time.time()

    try:
        # Step 1: fetch mail
        if fetch:
            cmd_str = settings.fetch_command
            if cmd_str:
                cmd = _parse_command(cmd_str)
                run_streaming("Fetching mail", cmd, ignore_errors=False)
            else:
                raise RuntimeError("fetch was requested but no fetch_command is configured")
        else:
            logger.debug("Fetch step skipped.")

        # Step 2: process mail
        if process:
            run_extractor(settings, stats)
        else:
            logger.debug("Processing step skipped.")

        # Step 3+: execute remaining stages
        for stage in stages:
            label = stage.lower()
            logger.info(f"--- Running stage: {label} ---")

            if label == "backup":
                incremental_upload(settings, manifest, stats)
            elif label == "archive":
                rotate_archives(settings, manifest, stats)
            elif label == "check":
                integrity_check(settings, manifest, stats)
            else:
                logger.warning(f"Unknown stage '{label}' – skipped")

    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed: {e.cmd} (exit {e.returncode})")
        raise
    except Exception as e:
        logger.exception(f"Pipeline error: {e}")
        raise
    finally:
        elapsed = time.time() - total_start
        logger.status(f"Pipeline finished in {elapsed:.1f}s at {datetime.now().isoformat()}")
        logger.info("=== Mailbackup pipeline finished ===")
