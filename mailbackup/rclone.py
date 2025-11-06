# mailbackup/rclone.py
from __future__ import annotations

from pathlib import Path
from typing import Union, Callable, Optional

# Default arguments for all rclone calls
RCLONE_BASE = ["rclone", "--log-level", "INFO"]


def _run_rclone(*args: Union[str, Path], check: bool = True, on_chunk: Optional[Callable[[bytes], None]] = None):
    """Low-level helper that executes rclone with consistent defaults."""
    cmd = RCLONE_BASE + [str(a) for a in args]
    if on_chunk is None:
        from mailbackup.utils import run_cmd
        return run_cmd(*cmd, check=check)
    else:
        from mailbackup.utils import run_streaming
        return run_streaming("RCLONE", cmd, ignore_errors=check, on_chunk=on_chunk, text_mode=False)


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

def rclone_copy(src: Union[str, Path], dst: Union[str, Path], *extra: str, check: bool = True,
                on_chunk: Optional[Callable[[bytes], None]] = None):
    """Copy directory or file to remote/local."""
    return _run_rclone("copy", str(src), str(dst), *extra, check=check, on_chunk=on_chunk)


def rclone_copyto(src: Union[str, Path], dst: Union[str, Path], *extra: str, check: bool = True,
                  on_chunk: Optional[Callable[[bytes], None]] = None):
    """Copy single file to remote/local destination."""
    return _run_rclone("copyto", str(src), str(dst), *extra, check=check, on_chunk=on_chunk)


def rclone_moveto(src: str, dst: str, *extra: str, check: bool = True,
                  on_chunk: Optional[Callable[[bytes], None]] = None):
    """Move file atomically on remote."""
    return _run_rclone("moveto", src, dst, *extra, check=check, on_chunk=on_chunk)


def rclone_cat(remote_path: str, check: bool = True, on_chunk: Optional[Callable[[bytes], None]] = None):
    """Return file contents from remote (stdout)."""
    return _run_rclone("cat", remote_path, check=check, on_chunk=on_chunk)


def rclone_hashsum(algorithm: str, remote_path: str, *extra: str, check: bool = True,
                   on_chunk: Optional[Callable[[bytes], None]] = None):
    """Return rclone hashsum output for remote path."""
    return _run_rclone("hashsum", algorithm, remote_path, *extra, check=check, on_chunk=on_chunk)


def rclone_deletefile(remote_path: str, check: bool = True, on_chunk: Optional[Callable[[bytes], None]] = None):
    """Delete a single remote file."""
    return _run_rclone("deletefile", remote_path, check=check, on_chunk=on_chunk)


def rclone_lsjson(remote_path: str, *extra: str, check: bool = True,
                  on_chunk: Optional[Callable[[bytes], None]] = None):
    """List remote files as JSON."""
    return _run_rclone("lsjson", remote_path, *extra, check=check, on_chunk=on_chunk)


def rclone_lsf(remote_path: str, *extra: str, check: bool = True, on_chunk: Optional[Callable[[bytes], None]] = None):
    """List remote files as LSF."""
    return _run_rclone("lsf", remote_path, *extra, check=check, on_chunk=on_chunk)


def rclone_check(remote_src: str, remote_dst: str, *extra: str, check: bool = True,
                 on_chunk: Optional[Callable[[bytes], None]] = None):
    """Compare two remotes for differences."""
    return _run_rclone("check", remote_src, remote_dst, *extra, check=check, on_chunk=on_chunk)
