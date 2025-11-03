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
- status thread
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
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union, TYPE_CHECKING

import unicodedata

if TYPE_CHECKING:
    from mailbackup.config import Settings

from mailbackup.logger import get_logger
from mailbackup.rclone import rclone_copyto, rclone_deletefile, rclone_moveto, rclone_cat, rclone_hashsum, rclone_lsjson

_logger = get_logger(__name__)


class StatusThread:
    def __init__(self, interval: int, counters: Dict[str, int]):
        self.logger = get_logger(__name__)
        self.interval = interval
        self.counters = counters
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self):
        if self._thread is not None:
            return

        def reporter():
            while not self._stop_event.wait(self.interval):
                # logger.status is registered at runtime by setup_logger; call via getattr
                fn = getattr(self.logger, "status", None)
                if callable(fn):
                    fn("[STATUS] " + self.status_summary())
                else:
                    self.logger.info("[STATUS] " + self.status_summary())

        t = threading.Thread(target=reporter, name="StatusReporter", daemon=True)
        t.start()
        self._thread = t

    def stop(self):
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)

    def status_summary(self) -> str:
        return (
            f"Uploaded: {self.counters.get('uploaded', 0)} | "
            f"Archived: {self.counters.get('archived', 0)} | "
            f"Verified: {self.counters.get('verified', 0)} | "
            f"Repaired: {self.counters.get('repaired', 0)} | "
            f"Skipped: {self.counters.get('skipped', 0)}"
        )


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
    try:
        cp: subprocess.CompletedProcess = subprocess.run(args, check=check, capture_output=True, text=True)
        out = cp.stdout or ""
        local_logger.debug(f"Command succeeded: {' '.join(args)} -> {out.strip()[:400]}")
        return cp
    except subprocess.CalledProcessError as e:
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
    try:
        write_json_atomic(path, data)
    except Exception:
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


def run_streaming(label: str, cmd: list[str], ignore_errors: bool = True) -> bool:
    """
    Run a subprocess and stream its stdout live to the logger.
    Works for interactive CLI tools like mbsync or rclone.

    Returns True if exit 0, False otherwise.
    Raises CalledProcessError if ignore_errors=False and exit != 0.
    """
    # Use central logger rather than a passed-in logger object.
    logger = get_logger(__name__)
    logger.info(f"Starting step: {label}")
    logger.debug(f"Command: {' '.join(cmd)}")
    start = time.time()

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        universal_newlines=True,
    )

    try:
        assert process.stdout is not None
        for line in process.stdout:
            line = line.rstrip("\n")
            if line:
                logger.info(f"[{label}] {line}")
    except Exception as e:
        logger.error(f"Streaming error for {label}: {e}")
    finally:
        rc = process.wait()
        elapsed = time.time() - start

    if rc == 0:
        logger.info(f"Finished {label} in {elapsed:.1f}s")
        return True
    else:
        logger.error(f"{label} failed with exit code {rc} after {elapsed:.1f}s")
        if not ignore_errors:
            raise subprocess.CalledProcessError(rc, cmd)
        return False


# helper: atomic upload of a single local file to a remote final path
def atomic_upload_file(local_path: Path, remote_final: str) -> bool:
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
    try:
        proc = rclone_cat(f"{settings.remote}/{remote_path}", check=True)
        out = proc.stdout
        out_bytes = out.encode("utf-8") if isinstance(out, str) else (out or b"")
        return sha256_bytes(out_bytes)
    except Exception:
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


def remote_hash(settings: Settings, file_pattern: str, silent_logging: bool = True) -> dict[str, str] | None:
    remote_map: Dict[str, str] = {}

    # Step 1: try rclone hashsum
    res = rclone_hashsum(
        "SHA256",
        settings.remote,
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
            settings.remote,
            "--include",
            file_pattern,
            "--recursive",
            check=False,
        )
        if res2.returncode != 0:
            _logger.error("Failed to list remote contents.")
            return None
        files = json.loads(res2.stdout or "[]")
        relpaths = [entry["Path"] for entry in files if "Path" in entry]
        silent_info(_logger,
                    f"Found {len(relpaths)} remote files — computing hashes with {settings.max_hash_threads} threads...",
                    silent_logging
                    )

        with ThreadPoolExecutor(max_workers=settings.max_hash_threads) as executor:
            futures = {
                executor.submit(compute_remote_sha256, settings, rp): rp for rp in relpaths
            }
            for i, fut in enumerate(as_completed(futures), start=1):
                rp = futures[fut]
                try:
                    sha = fut.result()
                    if sha:
                        remote_map[rp] = sha
                except Exception as e:
                    _logger.error(f"Hash computation failed for {rp}: {e}")

                if i % 25 == 0 or i == len(futures):
                    silent_info(_logger, f"[Hashing] {i}/{len(futures)} remote files processed...", silent_logging)

        silent_info(_logger, f"Computed SHA256 hashes for {len(remote_map)} files via streaming fallback.",
                    silent_logging)

    return remote_map
