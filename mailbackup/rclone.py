# mailbackup/rclone.py
from __future__ import annotations

import logging
from pathlib import Path
from typing import Union

logger = logging.getLogger(__name__)

# Default arguments for all rclone calls
RCLONE_BASE = ["rclone", "--log-level", "INFO"]


def _run_rclone(*args: Union[str, Path], check: bool = True):
    """Low-level helper that executes rclone with consistent defaults."""
    from mailbackup.utils import run_cmd

    cmd = RCLONE_BASE + [str(a) for a in args]
    return run_cmd(*cmd, check=check)


def set_rclone_defaults(log_level="INFO", transfers=4, multi_thread_streams=4):
    global RCLONE_BASE
    RCLONE_BASE = [
        "rclone",
        f"--log-level={log_level}",
        f"--transfers={transfers}",
        f"--multi-thread-streams={multi_thread_streams}",
    ]


# --------------------------
# Core command wrappers
# --------------------------

def rclone_copy(src: Union[str, Path], dst: Union[str, Path], *extra: str, check: bool = True):
    """Copy directory or file to remote/local."""
    return _run_rclone("copy", str(src), str(dst), *extra, check=check)


def rclone_copyto(src: Union[str, Path], dst: Union[str, Path], *extra: str, check: bool = True):
    """Copy single file to remote/local destination."""
    return _run_rclone("copyto", str(src), str(dst), *extra, check=check)


def rclone_moveto(src: str, dst: str, *extra: str, check: bool = True):
    """Move file atomically on remote."""
    return _run_rclone("moveto", src, dst, *extra, check=check)


def rclone_cat(remote_path: str, check: bool = True):
    """Return file contents from remote (stdout)."""
    return _run_rclone("cat", remote_path, check=check)


def rclone_hashsum(algorithm: str, remote_path: str, *extra: str, check: bool = True):
    """Return rclone hashsum output for remote path."""
    return _run_rclone("hashsum", algorithm, remote_path, *extra, check=check)


def rclone_deletefile(remote_path: str, check: bool = True):
    """Delete a single remote file."""
    return _run_rclone("deletefile", remote_path, check=check)


def rclone_lsjson(remote_path: str, *extra: str, check: bool = True):
    """List remote files as JSON."""
    return _run_rclone("lsjson", remote_path, *extra, check=check)


def rclone_lsf(remote_path: str, *extra: str, check: bool = True):
    """List remote files as LSF."""
    return _run_rclone("lsf", remote_path, *extra, check=check)


def rclone_check(remote_src: str, remote_dst: str, *extra: str, check: bool = True):
    """Compare two remotes for differences."""
    return _run_rclone("check", remote_src, remote_dst, *extra, check=check)
