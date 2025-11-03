#!/usr/bin/env python3

"""
manifest.py

Handles reading, merging, and uploading manifest.csv to remote via rclone.
Includes resilience:
- local queue of pending entries
- detect interrupted uploads
- conflict retries (CAS style)
"""

from __future__ import annotations

import datetime
import json
import threading
import uuid
from pathlib import Path
from typing import Dict, List, Tuple, Optional

from .config import Settings
from .rclone import rclone_copyto, rclone_lsjson, rclone_deletefile, rclone_moveto
from .logger import get_logger
from .utils import atomic_write_text, sha256_bytes, write_json_atomic


def load_manifest_csv(path: Path) -> Dict[str, str]:
    entries: Dict[str, str] = {}
    if not path.exists():
        return entries
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or "," not in line:
                continue
            sha, rpath = line.split(",", 1)
            entries[rpath.strip()] = sha.strip()
    return entries


def _manifest_dict_to_lines(d: Dict[str, str]) -> List[str]:
    return [f"{d[r]},{r}\n" for r in sorted(d.keys())]


class ManifestManager:
    def __init__(
            self,
            settings: Settings,
    ):
        self.settings = settings
        # prefer the centralized logger factory; ignore passed-in logger
        self.logger = get_logger(__name__)
        self.manifest_path: Path = settings.manifest_path
        self.tmp_dir: Path = settings.tmp_dir
        self.remote: str = settings.remote
        self.manifest_remote_path: str = settings.manifest_remote_path
        self.max_retries: int = settings.max_manifest_conflict_retries

        self.manifest_inprogress = self.tmp_dir / "manifest.uploading"
        self.manifest_queue_dump = self.tmp_dir / "manifest.queue.json"

        self._manifest_queue: Dict[str, str] = {}
        self._lock = threading.Lock()

        self.tmp_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Queue management
    # ------------------------------------------------------------------
    def queue_entry(self, remote_path: str, sha256_hash: str) -> None:
        """
        Add a (remote_path -> sha256) entry to the in-memory manifest queue.

        The queue is persisted to disk immediately so entries survive hard process kills.
        """
        # threadsafe: add and persist immediately to survive hard kills
        with self._lock:
            self._manifest_queue[remote_path] = sha256_hash
            try:
                write_json_atomic(self.manifest_queue_dump, self._manifest_queue)
                self.logger.debug(f"Persisted manifest queue ({len(self._manifest_queue)} entries)")
            except Exception as e:
                self.logger.warning(f"Failed to persist manifest queue: {e}")

    def dump_queue(self) -> None:
        """
        Persist any queued manifest entries to disk (manifest.queue.json).

        Intended to be called on clean shutdown to ensure no entries are lost.
        """
        with self._lock:
            if not self._manifest_queue:
                return
            try:
                write_json_atomic(self.manifest_queue_dump, self._manifest_queue)
                self.logger.info(f"Manifest queue saved to {self.manifest_queue_dump}")
            except Exception as e:
                self.logger.warning(f"Failed to persist manifest queue: {e}")

    def restore_queue(self) -> None:
        """
        Restore a previously saved manifest queue from disk and remove the dump file.

        Called at startup to resume uploads from previous runs.
        """
        with self._lock:
            if not self.manifest_queue_dump.exists():
                return
            try:
                with open(self.manifest_queue_dump, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, dict):
                    self._manifest_queue.update(data)
                    self.logger.info(
                        f"Restored {len(self._manifest_queue)} manifest entries from {self.manifest_queue_dump}")
                self.manifest_queue_dump.unlink(missing_ok=True)
            except Exception as e:
                self.logger.warning(f"Failed to restore manifest queue: {e}")

    # ------------------------------------------------------------------
    # CSV Helpers
    # ------------------------------------------------------------------

    def _write_manifest_dict_atomic(self, d: Dict[str, str]) -> None:
        lines = _manifest_dict_to_lines(d)
        try:
            atomic_write_text(self.manifest_path, lines)
            self.logger.info(f"Manifest written atomically to {self.manifest_path}")
        except Exception as e:
            self.logger.error(f"Failed to write manifest atomically: {e}")
            try:
                self.manifest_path.with_suffix(".tmp").unlink(missing_ok=True)
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Remote helpers
    # ------------------------------------------------------------------
    def _download_remote_manifest(self) -> Tuple[Dict[str, str], str]:
        remote_tmp = self.tmp_dir / "manifest.remote.csv"
        remote_tmp.parent.mkdir(parents=True, exist_ok=True)
        rclone_copyto(self.manifest_remote_path, str(remote_tmp), check=False)
        if not remote_tmp.exists():
            return {}, ""
        with open(remote_tmp, "rb") as f:
            data = f.read()
        sha = sha256_bytes(data)
        entries = load_manifest_csv(remote_tmp)
        return entries, sha

    def _cleanup_remote_temp_manifests(self) -> None:
        try:
            result = rclone_lsjson(
                self.remote,
                "--include",
                "manifest.csv.*.tmp",
                check=False,
            )
            text = result.stdout or "[]"
            files = json.loads(text)
            for entry in files:
                if "Path" in entry:
                    tmp_path = f"{self.remote}/{entry['Path']}"
                    rclone_deletefile(tmp_path, check=False)
                    self.logger.debug(f"Cleaned up old temp manifest: {tmp_path}")
        except Exception as e:
            self.logger.warning(f"Temp manifest cleanup skipped: {e}")

    # ------------------------------------------------------------------
    # Public entrypoint
    # ------------------------------------------------------------------
    def upload_manifest_resilient(self, extra_entries: Optional[Dict[str, str]] = None) -> None:
        """
        Merge local and remote manifests and upload the resulting manifest.csv atomically.

        Implements a CAS-style approach:
        - download current remote manifest
        - merge remote + local + extra_entries
        - upload to a temporary remote name
        - if remote hasn't changed, move temp into place
        - retry up to max_retries, otherwise write a conflict copy remotely
        """
        # mark in-progress
        self.manifest_inprogress.write_text(datetime.datetime.now(datetime.timezone.utc).isoformat())
        self._cleanup_remote_temp_manifests()
        try:
            remote_entries, remote_sha = self._download_remote_manifest()
            local_entries = load_manifest_csv(self.manifest_path) if self.manifest_path.exists() else {}

            # Merge
            merged: Dict[str, str] = {}
            merged.update(remote_entries)
            merged.update(local_entries)
            if extra_entries:
                merged.update(extra_entries)

            # Write locally
            self._write_manifest_dict_atomic(merged)

            # Try CAS-style upload
            attempt = 0
            while attempt < self.max_retries:
                attempt += 1
                run_id = uuid.uuid4().hex
                remote_tmp_name = f"manifest.csv.{run_id}.tmp"
                remote_tmp_path = f"{self.remote}/{remote_tmp_name}"

                # upload temp
                rclone_copyto(
                    str(self.manifest_path),
                    remote_tmp_path,
                )

                # re-check remote
                current_remote, current_sha = self._download_remote_manifest()
                if current_sha == remote_sha:
                    # move into place
                    rclone_moveto(
                        remote_tmp_path,
                        self.manifest_remote_path,
                    )
                    self.logger.info("Remote manifest updated atomically.")
                    return
                else:
                    self.logger.warning("Remote manifest changed during upload, retrying...")
                    remote_entries = current_remote
                    remote_sha = current_sha
                    merged = {}
                    merged.update(remote_entries)
                    merged.update(local_entries)
                    if extra_entries:
                        merged.update(extra_entries)
                    self._write_manifest_dict_atomic(merged)

            # If here -> conflict
            conflict_name = f"manifest.conflict.{uuid.uuid4().hex}.csv"
            conflict_remote = f"{self.remote}/{conflict_name}"
            rclone_copyto(
                str(self.manifest_path),
                conflict_remote,
                check=False,
            )
            self.logger.error("Failed to update remote manifest after several conflicts. Wrote conflict copy.")
        finally:
            self.manifest_inprogress.unlink(missing_ok=True)

    def upload_manifest_if_needed(self) -> None:
        """
        Upload persisted manifest entries if there are any in the queue.

        Takes a snapshot of the queue atomically and uploads that snapshot outside the lock.
        """
        # Snapshot & clear to minimize contention with parallel upload workers
        with self._lock:
            if not self._manifest_queue:
                self.logger.info("No new manifest entries to upload.")
                return
            snapshot = self._manifest_queue.copy()
            self._manifest_queue.clear()
            try:
                self.manifest_queue_dump.unlink(missing_ok=True)
            except Exception:
                pass
        # Upload snapshot outside lock
        self.upload_manifest_resilient(extra_entries=snapshot)

    # ------------------------------------------------------------------
    # Recovery
    # ------------------------------------------------------------------
    def recover_interrupted(self) -> None:
        """
        Detect an interrupted manifest upload (manifest.uploading file) and resume.

        Removes the in-progress marker and attempts a resilient upload.
        """
        if self.manifest_inprogress.exists():
            self.logger.warning("Detected unfinished manifest upload from previous run.")
            self.manifest_inprogress.unlink(missing_ok=True)
            self.upload_manifest_resilient()
