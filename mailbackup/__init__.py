#!/usr/bin/env python3

"""
mailbackup
Refactored package for incremental mail backup to Nextcloud via rclone.

This package is a modular rewrite of the original monolithic script.
"""

__version__ = "0.1.0"

__all__ = [
    "config",
    "utils",
    "db",
    "manifest",
    "uploader",
    "rotation",
    "integrity",
    "extractor",
    "orchestrator",
    "logger",
    "rclone",
]
