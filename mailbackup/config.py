#!/usr/bin/env python3

"""
config.py

Configuration loading for the mailbackup package.

Supports:
- TOML (preferred) using stdlib tomllib (Python 3.11+) or if not available, falls back to .ini
- INI using configparser

Precedence:
1. CLI --config <path>
2. ./mailbackup.toml
3. ./mailbackup.ini
4. ~/.config/mailbackup.toml
5. ~/.config/mailbackup.ini
6. /etc/mailbackup.toml
7. /etc/mailbackup.ini
"""

from __future__ import annotations

import configparser
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional


@dataclass
class Settings:
    # Core paths
    maildir: Path
    attachments_dir: Path
    remote: str
    db_path: Path
    log_path: Path
    tmp_dir: Path
    archive_dir: Path
    manifest_path: Path

    # Behaviour
    retention_years: int
    keep_local_after_archive: bool
    verify_integrity: bool
    repair_on_failure: bool

    # Manifest
    manifest_remote_name: str
    max_manifest_conflict_retries: int

    # Performance
    max_hash_threads: int
    max_upload_workers: int
    max_extract_workers: int

    # Logging
    status_interval: int

    # Fetch
    fetch_command: str

    # rclone
    rclone_log_level: str
    rclone_transfers: int
    rclone_multi_thread_streams: int

    @property
    def manifest_remote_path(self) -> str:
        return f"{self.remote}/{self.manifest_remote_name}"


DEFAULT_LOCATIONS = [
    Path("./mailbackup.toml"),
    Path("./mailbackup.ini"),
    Path(os.path.expanduser("~/.config/mailbackup.toml")),
    Path(os.path.expanduser("~/.config/mailbackup.ini")),
    Path("/etc/mailbackup.toml"),
    Path("/etc/mailbackup.ini"),
]


def _load_toml(path: Path) -> Dict[str, Any]:
    # Prefer stdlib tomllib (3.11+), else fallback to third-party tomli if available.
    try:
        import tomllib  # type: ignore
        loader = tomllib.load
    except Exception:
        try:
            import tomli  # type: ignore
            loader = tomli.load
        except Exception:
            raise RuntimeError(
                f"TOML config {path} requested but no TOML parser available. "
                f"Install Python 3.11+ or the 'tomli' package, or use an INI config."
            )

    with open(path, "rb") as f:
        data = loader(f)
    return data


def _load_ini(path: Path) -> Dict[str, Any]:
    cp = configparser.ConfigParser()
    cp.read(path)
    data: Dict[str, Any] = {}

    # We'll map from a top-level section [mailbackup]
    section = "mailbackup"
    if section not in cp:
        raise RuntimeError(f"INI config {path} must have a [{section}] section")

    sec = cp[section]
    for k in sec:
        data[k] = sec[k]
    return data


def _coerce_bool(v: Any, default: bool) -> bool:
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in ("1", "true", "yes", "on"):
        return True
    if s in ("0", "false", "no", "off"):
        return False
    return default


def _coerce_int(v: Any, default: int) -> int:
    if v is None:
        return default
    try:
        return int(v)
    except Exception:
        return default


def load_settings(config_path: Optional[Path] = None) -> Settings:
    data: Dict[str, Any] = {}

    source_path: Optional[Path] = None

    if config_path is not None:
        if not config_path.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")
        source_path = config_path
    else:
        for p in DEFAULT_LOCATIONS:
            if p.exists():
                source_path = p
                break

    print("Config file: ", source_path)

    if source_path is None:
        # Fall back to built-in defaults
        # But make it explicit for the user
        sys.stderr.write(
            "Warning: no config file found. Using built-in defaults.\n"
        )
    else:
        if source_path.suffix.lower() == ".toml":
            data = _load_toml(source_path)
        else:
            data = _load_ini(source_path)

    # Flatten possible [paths] etc.
    # We'll accept either flat keys or nested dicts
    def pick(*keys: str, default: Any = None) -> Any:
        for k in keys:
            if k in data:
                return data[k]
        # nested
        for k in keys:
            parts = k.split(".")
            if len(parts) == 2:
                top, sub = parts
                if top in data and isinstance(data[top], dict):
                    if sub in data[top]:
                        return data[top][sub]
        return default

    maildir = Path(pick("maildir", "paths.maildir", default="/srv/mailbackup/maildir"))
    attachments_dir = Path(pick("attachments_dir", "paths.attachments_dir", default="/srv/mailbackup/attachments"))
    remote = str(pick("remote", "paths.remote", default="nextcloud:Backups/Email")).rstrip("/")
    db_path = Path(pick("db_path", "paths.db_path", default="/srv/mailbackup/state.db"))
    log_path = Path(pick("log_path", "paths.log_path", default="/var/log/mailbackup/sync.log"))
    tmp_dir = Path(pick("tmp_dir", "paths.tmp_dir", default="/srv/mailbackup/staging"))
    archive_dir = Path(pick("archive_dir", "paths.archive_dir", default="/srv/mailbackup/archives"))
    manifest_path = Path(pick("manifest_path", "paths.manifest_path", default="/srv/mailbackup/manifest.csv"))

    retention_years = _coerce_int(pick("retention_years", "rotation.retention_years", default=2), 2)
    keep_local_after_archive = _coerce_bool(
        pick("keep_local_after_archive", "rotation.keep_local_after_archive", default=False), False)
    verify_integrity = _coerce_bool(pick("verify_integrity", "integrity.verify_integrity", default=True), True)
    repair_on_failure = _coerce_bool(pick("repair_on_failure", "integrity.repair_on_failure", default=True), True)
    manifest_remote_name = str(pick("manifest_remote_name", "manifest.remote_name", default="manifest.csv"))
    max_manifest_conflict_retries = _coerce_int(
        pick("max_manifest_conflict_retries", "manifest.max_manifest_conflict_retries", default=3), 3
    )
    max_hash_threads = _coerce_int(pick("max_hash_threads", "integrity.max_hash_threads", default=8), 8)
    # performance tuning (new)
    max_upload_workers = _coerce_int(pick("max_upload_workers", "performance.max_upload_workers", default=4), 4)
    max_extract_workers = _coerce_int(pick("max_extract_workers", "performance.max_extract_workers", default=1), 1)
    status_interval = _coerce_int(pick("status_interval", "logging.status_interval", default=300), 300)

    # fetch
    fetch_command = str(pick("fetch_command", "fetch.command", default="mbsync -V -a"))

    # rclone
    rclone_log_level = str(pick("rclone_log_level", "rclone.log_level", default="INFO"))
    rclone_transfers = _coerce_int(pick("rclone_transfers", "rclone.transfers", default=8), 8)
    rclone_multi_thread_streams = _coerce_int(
        pick("rclone_multi_thread_streams", "rclone.multi_thread_streams", default=4), 4)

    from mailbackup.rclone import set_rclone_defaults

    set_rclone_defaults(rclone_log_level, rclone_transfers, rclone_multi_thread_streams)

    return Settings(
        maildir=maildir,
        attachments_dir=attachments_dir,
        remote=remote,
        db_path=db_path,
        log_path=log_path,
        tmp_dir=tmp_dir,
        archive_dir=archive_dir,
        manifest_path=manifest_path,
        retention_years=retention_years,
        keep_local_after_archive=keep_local_after_archive,
        verify_integrity=verify_integrity,
        repair_on_failure=repair_on_failure,
        manifest_remote_name=manifest_remote_name,
        max_manifest_conflict_retries=max_manifest_conflict_retries,
        max_hash_threads=max_hash_threads,
        max_upload_workers=max_upload_workers,
        max_extract_workers=max_extract_workers,
        status_interval=status_interval,
        fetch_command=fetch_command,
        rclone_log_level=rclone_log_level,
        rclone_transfers=rclone_transfers,
        rclone_multi_thread_streams=rclone_multi_thread_streams,
    )
