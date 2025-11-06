#!/usr/bin/env python3

"""
utils.py

Utility helpers shared across the package:
- logging setup
- filename sanitization
- date parsing
- sha256
- subprocess run wrapper
- atomic JSON write
- docset-related helpers (build_info_json, load_attachments)
"""

from __future__ import annotations

import datetime
import hashlib
import json
import logging
import os
import re
import signal
import subprocess
import time
import uuid
from contextlib import contextmanager
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union, TYPE_CHECKING, Callable

import unicodedata

if TYPE_CHECKING:
    from mailbackup.config import Settings

from mailbackup.logger import get_logger
from mailbackup.rclone import rclone_copyto, rclone_deletefile, rclone_moveto, rclone_cat, rclone_hashsum, rclone_lsjson


# Import StatusThread from statistics module for backward compatibility


def sanitize(s: Optional[str]) -> str:
    if not s:
        return "unknown"
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = re.sub(r'[<>:"/\\|?*\x00-\x1F]', "_", s)
    s = re.sub(r"\s+", "_", s.strip())
    return s[:80]


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def run_cmd(*args: str, check: bool = True, fatal: bool = False) -> Union[
    subprocess.CompletedProcess, subprocess.CalledProcessError]:
    """
    Run a command and return CompletedProcess.
    Logs errors (and success at debug) so callers can rely on logs without repeating prints.
    """
    local_logger = get_logger(__name__)
    local_logger.debug(f"Run command: {' '.join(args)}")
    try:
        cp: subprocess.CompletedProcess = subprocess.run(args, check=check, capture_output=True, text=True)
        if cp.returncode < 0:
            local_logger.error(f"Command interrupted: {' '.join(args)}")
            raise KeyboardInterrupt()
        out = cp.stdout or ""
        local_logger.debug(f"Command succeeded: {' '.join(args)} -> {out.strip()[:400]} | {cp.returncode}")
        return cp
    except subprocess.CalledProcessError as e:
        if e.returncode < 0:
            local_logger.error(f"Command interrupted: {' '.join(args)}")
            raise KeyboardInterrupt()
        local_logger.error(f"Command failed: {' '.join(args)} -> {e.stderr.strip()}")
        if fatal:
            raise
        return e


def parse_mail_date(date_str: Optional[str]) -> datetime.datetime:
    if not date_str:
        return datetime.datetime.now(datetime.timezone.utc)
    cleaned = re.sub(r"\s*\([^)]*\)\s*$", "", date_str.strip())
    # Try ISO
    try:
        dt = datetime.datetime.fromisoformat(cleaned)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        return dt.astimezone(datetime.timezone.utc)
    except Exception:
        pass

    try:
        dt = parsedate_to_datetime(cleaned)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        return dt.astimezone(datetime.timezone.utc)
    except Exception:
        return datetime.datetime.now(datetime.timezone.utc)


def date_iso(s: Optional[str]) -> str:
    return parse_mail_date(s).isoformat()


def parse_year_and_ts(date_h: Optional[str]) -> Tuple[int, str]:
    dt = parse_mail_date(date_h)
    return dt.year, dt.strftime("%Y-%m-%d_%H-%M-%S")


def write_json_atomic(path: Path, data: Any) -> None:
    """
    Atomically write JSON to `path`. Logs the write at debug level.
    """
    _logger = get_logger(__name__)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    _logger.debug(f"Wrote JSON atomically to {path}")


def safe_write_json(path: Path, data: Any) -> None:
    """
    Write JSON atomically where possible; on failure fall back to a plain write.
    Logs at debug level for the chosen path.
    """
    _logger = get_logger(__name__)
    try:
        write_json_atomic(path, data)
    except (KeyboardInterrupt, InterruptedError):
        raise
    except Exception:
        _logger.debug(f"Failed to write JSON atomically to {path}, retry with best effort fallback")
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            _logger.error(f"Failed to write JSON to {path}: {e}")
            raise


def atomic_write_text(path: Path, data: Union[str, Iterable[str]]) -> None:
    """
    Atomically write text to `path`.
    `data` may be a single string or an iterable of string lines.
    Uses the same tmp-suffix strategy as write_json_atomic (append ".tmp" to suffix).
    """
    _logger = get_logger(__name__)
    tmp = path.with_suffix(path.suffix + ".tmp")
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            if isinstance(data, str):
                f.write(data)
            else:
                for line in data:
                    f.write(line)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
        _logger.debug(f"Wrote text atomically to {path}")
    except Exception:
        # best-effort cleanup on failure
        try:
            if tmp.exists():
                tmp.unlink()
        except Exception:
            pass
        raise


def sha256_bytes(data: bytes) -> str:
    """Return SHA256 hexdigest for given bytes."""
    h = hashlib.sha256()
    h.update(data)
    return h.hexdigest()


def unique_path_for_filename(outdir: Path, filename: str) -> Path:
    """
    Return a Path in `outdir` for `filename` that does not collide with existing files.
    Appends -N before the extension when collisions occur (e.g. name-1.txt).
    Does not create `outdir`.
    """
    candidate = outdir / filename
    base, ext = os.path.splitext(filename)
    counter = 1
    while candidate.exists():
        candidate = outdir / f"{base}-{counter}{ext}"
        counter += 1
    return candidate


def load_attachments(attach_json: Optional[str]) -> List[Path]:
    if not attach_json:
        return []
    try:
        data = json.loads(attach_json)
        if isinstance(data, list):
            return [Path(p) for p in data if isinstance(p, str)]
    except json.JSONDecodeError:
        return []
    return []


def build_info_json(
        row: Union[Dict[str, Any], "sqlite3.Row"],  # type: ignore[name-defined]
        att_names: List[str],
        hash_email: str,
        remote_path: str,
        archived_at: Optional[str] = None,
        metadata_version: int = 1,
) -> Dict[str, Any]:
    # To avoid a hard sqlite3 import for type checking
    def safe(val: Any):
        return val if val not in (None, "", "null") else None

    def fetch(k: str):
        if isinstance(row, dict):
            return row.get(k)
        else:
            # sqlite3.Row duck-typing
            try:
                return row[k]  # type: ignore[index]
            except Exception:
                return None

    return {
        "metadata_version": metadata_version,
        "id": safe(fetch("id")),
        "hash": safe(fetch("hash")),
        "hash_sha256": hash_email,
        "path": safe(fetch("path")),
        "remote_path": remote_path,
        "from_header": safe(fetch("from_header")),
        "subject": safe(fetch("subject")),
        "date_header": safe(fetch("date_header")),
        "attachments": att_names,
        "spam": int(fetch("spam") or 0),
        "synced_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "archived_at": archived_at,
        "processed_at": safe(fetch("processed_at")),
    }


@contextmanager
def working_dir(path: Path):
    prev = Path.cwd()
    try:
        os.chdir(path)
        yield
    finally:
        os.chdir(prev)


def ensure_dirs(*paths: Path) -> None:
    for p in paths:
        p.mkdir(parents=True, exist_ok=True)


def install_signal_handlers(on_interrupt):
    signal.signal(signal.SIGINT, on_interrupt)
    signal.signal(signal.SIGTERM, on_interrupt)


def run_streaming(label: str, cmd: list[str], ignore_errors: bool = True,
                  on_chunk: Optional[Callable[[bytes], None]] = None,
                  text_mode: bool = True, ) -> bool:
    """
    Run a subprocess and stream its stdout live to the logger or a callback.

    If `text_mode` is True (default), stdout is read line-by-line as text
    and streamed to the logger (interactive CLI style).

    If `text_mode` is False, stdout is read in binary chunks and passed
    to `on_chunk` (e.g. for hashing, compression, etc.).

    Returns True if exit code 0, False otherwise.
    Raises CalledProcessError if ignore_errors=False and exit != 0.
    """
    # Use central logger rather than a passed-in logger object.
    logger = get_logger(__name__)
    silent_info(logger, f"Starting step: {label}", not text_mode)
    logger.debug(f"Command: {' '.join(cmd)}")
    start = time.time()

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=text_mode,
        bufsize=1 if text_mode else 0,
        universal_newlines=text_mode,
    )

    try:
        assert process.stdout is not None
        if text_mode:
            # Read character-by-character to handle \r updates properly
            buffer = ""
            while True:
                chunk = process.stdout.read(1)
                if not chunk:
                    break
                buffer += chunk
                # Handle full lines normally
                if "\n" in buffer:
                    lines = buffer.split("\n")
                    for line in lines[:-1]:
                        if line.strip():
                            logger.info(f"[{label}] {line.strip()}")
                    buffer = lines[-1]
                # Handle carriage-return updates
                elif "\r" in buffer:
                    parts = buffer.split("\r")
                    # Keep only the last fragment — updated progress line
                    if len(parts) > 1:
                        last = parts[-1].strip()
                        if last:
                            logger.info(f"[{label}] {last}")
                        buffer = parts[-1]
            # Flush remainder
            if buffer.strip():
                logger.info(f"[{label}] {buffer.strip()}")

        else:
            # Binary mode (used for hashing, etc.)
            for chunk in iter(lambda: process.stdout.read(65536), b""):
                if on_chunk:
                    on_chunk(chunk)
    except Exception as e:
        logger.error(f"Streaming error for {label}: {e}")
    finally:
        rc = process.wait()
        elapsed = time.time() - start

    if rc < 0:
        logger.error(f"Interrupt for stream {label}")
        raise KeyboardInterrupt()

    if rc == 0:
        silent_info(logger, f"Finished {label} in {elapsed:.1f}s", not text_mode)
        return True
    else:
        logger.error(f"{label} failed with exit code {rc} after {elapsed:.1f}s")
        if not ignore_errors:
            raise subprocess.CalledProcessError(rc, cmd)
        return False


# helper: atomic upload of a single local file to a remote final path
def atomic_upload_file(local_path: Path, remote_final: str) -> bool:
    _logger = get_logger(__name__)
    run_id = uuid.uuid4().hex
    remote_tmp = f"{remote_final}.tmp.{run_id}"
    res = rclone_copyto(local_path, remote_tmp, check=False)
    if getattr(res, "returncode", 1) != 0:
        _logger.error(f"atomic_upload_file: copyto failed for {local_path} -> {remote_tmp}")
        rclone_deletefile(remote_tmp, check=False)
        return False
    res2 = rclone_moveto(remote_tmp, remote_final, check=False)
    if getattr(res2, "returncode", 1) != 0:
        _logger.error(f"atomic_upload_file: moveto failed for {remote_tmp} -> {remote_final}")
        rclone_deletefile(remote_tmp, check=False)
        return False
    return True


def compute_remote_sha256(settings: Settings, remote_path: str) -> str:
    """
    Stream a remote file via rclone cat and compute its SHA256.

    Returns hex digest string on success or empty string on failure.
    This is used as a fallback when rclone hashsum isn't available.
    """
    logger = get_logger(__name__)
    try:
        # Normalize remote path
        if remote_path.startswith(settings.remote):
            remote_path = remote_path[len(settings.remote):]
        if remote_path.startswith("/"):
            remote_path = remote_path[1:]

        # Run rclone cat
        h = hashlib.sha256()
        ok = rclone_cat(f"{settings.remote}/{remote_path}", check=True, on_chunk=lambda chunk: h.update(chunk))

        if not ok:
            raise

        return h.hexdigest()
    except (KeyboardInterrupt, InterruptedError):
        logger.error(f"compute_remote_sha256 interrupted for {remote_path}")
        raise
    except Exception as e:
        logger.debug(f"Failed to compute remote SHA256 for {remote_path}: {e}")
        return ""


def silent_info(logger: logging.Logger, msg: str, silent: bool = False):
    if silent:
        logger.debug(msg)
    else:
        logger.info(msg)


def silent_warn(logger: logging.Logger, msg: str, silent: bool = False):
    if silent:
        logger.debug(msg)
    else:
        logger.warning(msg)


def remote_hash(settings: Settings, file_pattern: str = '*', remote_path: str = None, silent_logging: bool = True) -> \
        dict[str, str] | None:
    _logger = get_logger(__name__)
    remote_map: Dict[str, str] = {}

    if remote_path is None:
        remote_path = settings.remote

    # Step 1: try rclone hashsum
    res = rclone_hashsum(
        "SHA256",
        remote_path,
        "--include",
        file_pattern,
        "--recursive",
        check=False,
    )
    if res.returncode == 0:
        for line in (res.stdout or "").splitlines():
            parts = line.strip().split(None, 1)
            if len(parts) == 2:
                hash_val, path = parts
                remote_map[path.strip()] = hash_val.strip()
        if remote_map:
            silent_info(_logger, f"Remote hashsum succeeded with {len(remote_map)} entries.", silent_logging)
        else:
            silent_warn(_logger, "rclone hashsum returned no results.", silent_logging)
    else:
        silent_warn(_logger, "Remote does not support hashsum SHA256. Falling back to streaming...", silent_logging)

    # Step 2: fallback — list all remote files and compute hashes locally
    if not remote_map:
        res2 = rclone_lsjson(
            remote_path,
            "--include",
            file_pattern,
            "--recursive",
            check=False,
        )
        if res2.returncode != 0:
            _logger.error("Failed to list remote contents.")
            return None
        files = json.loads(res2.stdout or "[]")
        if remote_path.startswith(settings.remote):
            remote_path = remote_path[len(settings.remote):]
        if remote_path != "" and remote_path[-1:] != "/":
            remote_path = f"{remote_path}/"
        relpaths = [f"{remote_path}{entry['Path']}" for entry in files if "Path" in entry]
        silent_info(_logger,
                    f"Found {len(relpaths)} remote files — computing hashes with {settings.max_hash_threads} threads...",
                    silent_logging
                    )

        # Use managed executor for better interrupt handling
        def compute_hash_for_path(rp):
            return compute_remote_sha256(settings, rp)

        from mailbackup.executor import create_managed_executor
        with create_managed_executor(
                max_workers=min(settings.max_hash_threads, len(relpaths)),
                name="RemoteHasher",
                progress_interval=25,
                silent=True,
        ) as executor:
            results = executor.map(compute_hash_for_path, relpaths)

            # Build the remote map from successful results
            for result in results:
                if result.success and result.result:
                    remote_map[result.item] = result.result

        silent_info(_logger, f"Computed SHA256 hashes for {len(remote_map)} files via streaming fallback.",
                    silent_logging)

    return remote_map
