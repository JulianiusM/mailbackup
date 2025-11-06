#!/usr/bin/env python3

"""
uploader.py

Incremental upload of unsynced emails:
- fetch unsynced from DB
- build docset (email.eml + attachments + info.json)
- upload via rclone to remote/year/folder
- mark as synced in DB
- queue manifest entry
"""

from __future__ import annotations

import shutil
from itertools import islice
from pathlib import Path
from sqlite3 import Row

from mailbackup import db
from mailbackup.config import Settings
from mailbackup.executor import create_managed_executor
from mailbackup.logger import get_logger
from mailbackup.manifest import ManifestManager
from mailbackup.rclone import rclone_deletefile
from mailbackup.statistics import ThreadSafeStats, create_increment_callback, StatKey
from mailbackup.utils import remote_hash
from mailbackup.utils import (
    sanitize,
    sha256,
    parse_year_and_ts,
    load_attachments,
    build_info_json,
    safe_write_json,
    atomic_upload_file,
)


def upload_email(row: Row, settings: Settings, manifest: ManifestManager, stats: ThreadSafeStats) -> bool:
    # returns True if uploaded, False if skipped/failure
    logger = get_logger(__name__)

    hash_ = row["hash"]
    path = Path(row["path"] or "")
    from_h = row["from_header"] or ""
    subj = row["subject"] or ""
    date_h = row["date_header"] or ""
    attach_json = row["attachments"] or ""

    safe_from = sanitize(from_h)
    safe_subj = sanitize(subj)
    year, safe_ts = parse_year_and_ts(date_h)

    short_hash = (hash_ or "unknown")[:6]
    folder_name = f"{safe_ts}_from_{safe_from}_subject_{safe_subj}_[{short_hash}]"
    folder_name = folder_name[:150]

    docset_dir = settings.tmp_dir / "docsets" / str(year) / folder_name
    docset_dir.mkdir(parents=True, exist_ok=True)

    # Copy email
    if path.exists():
        shutil.copy2(path, docset_dir / "email.eml")
        hash_email = sha256(path)
    else:
        hash_email = hash_ or ""

    # Copy attachments
    att_names = []
    for ap in load_attachments(attach_json):
        if ap.exists():
            safe_name = sanitize(ap.name)
            shutil.copy2(ap, docset_dir / safe_name)
            att_names.append(safe_name)

    info = build_info_json(
        row=row,
        att_names=att_names,
        hash_email=hash_email,
        remote_path=f"{year}/{folder_name}/email.eml",
        archived_at=None,
        metadata_version=1,
    )

    safe_write_json(docset_dir / "info.json", info)

    remote_path = f"/{year}/{folder_name}"
    remote_base = f"{settings.remote}{remote_path}"

    # upload email.eml (critical) with verification loop
    max_attempts = 3
    email_local = docset_dir / "email.eml"
    remote_email = f"{remote_base}/email.eml"
    email_key = f"{remote_path}/email.eml"
    email_uploaded = False
    logger.debug(f"Attempting to upload email {email_local}")
    for attempt in range(1, max_attempts + 1):
        if email_local.exists():
            ok = atomic_upload_file(email_local, remote_email)
            if not ok:
                logger.warning(f"Attempt {attempt}: atomic upload failed for email {email_local}")
                continue
            # verification: stream remote file and compare SHA256 to local computed hash
            # this is a defensive step against partial/garbled uploads.
            remote_map = remote_hash(settings, remote_path=remote_base)
            logger.debug(f"Remote map: {remote_map}")
            try:
                if remote_map is None:
                    logger.warning(f"No remote hashsum found for email {email_local}")
                elif remote_map.get(email_key) != hash_email:
                    logger.warning(
                        f"Verification mismatch for {email_local} remote_sha={remote_map[email_key][:8]} expected={hash_email[:8]}")
                else:
                    email_uploaded = True
                    break
            except KeyError as e:
                logger.warning(f"Verification failed for {hash_[:8]} at {remote_path}/email.eml with KeyError")

            if not email_uploaded:
                # try to remove the bad remote file
                try:
                    rclone_deletefile(remote_email, check=False)
                except (KeyboardInterrupt, InterruptedError):
                    logger.error(f"Interrupted")
                    raise
                except Exception as e:
                    logger.debug(f"Error deleting bad {remote_email}: {e}")
        else:
            # no local email file: treat as uploaded (nothing to do)
            email_uploaded = True
            break

    if not email_uploaded:
        logger.debug(f"Email not uploaded: {remote_email}")
        shutil.rmtree(docset_dir, ignore_errors=True)
        return False

    # upload attachments and info.json atomically (best-effort)
    try:
        logger.debug(f"Uploading attachments for {remote_email}")
        for fpath in sorted(docset_dir.iterdir()):
            if not fpath.is_file() or fpath.name == "email.eml":
                continue
            logger.debug(f"Uploading {fpath}")
            ok = atomic_upload_file(fpath, f"{remote_base}/{fpath.name}")
            if not ok:
                logger.warning(f"Failed to upload {fpath.name} for {hash_}")
    except (KeyboardInterrupt, InterruptedError):
        logger.error(f"Interrupted")
        raise
    except Exception as e:
        logger.exception(f"Exception during attachment upload for {hash_}: {e}")
        return False

    # success: db + manifest + stats
    logger.debug(f"Marking {hash_} as synced...")
    db.mark_synced(settings.db_path, hash_, hash_email, f"{year}/{folder_name}/email.eml")
    try:
        manifest.queue_entry(f"{year}/{folder_name}/email.eml", hash_email)
    except (KeyboardInterrupt, InterruptedError):
        logger.error(f"Interrupted")
        raise
    except Exception as e:
        logger.warning(f"Failed to persist manifest queue: {e}")

    # Thread-safe increment
    stats.increment(StatKey.BACKED_UP)

    shutil.rmtree(docset_dir, ignore_errors=True)
    return True


def incremental_upload(settings: Settings, manifest: ManifestManager, stats: ThreadSafeStats) -> None:
    """
    Upload unsynced messages from the local DB to the remote backend.

    For each unsynced DB row this does:
    - build a docset in tmp (email.eml, attachments, info.json)
    - upload email.eml atomically and verify its SHA256 by streaming the remote file
    - upload attachments and info.json (best-effort atomic copy)
    - mark the DB row as synced and enqueue a manifest entry

    Uses ManagedThreadPoolExecutor for proper interrupt handling.
    Updates the supplied stats dict (uploaded/skipped).
    """
    # Use the configured logging factory rather than a passed-in logger instance
    logger = get_logger(__name__)
    logger.info("Starting incremental upload...")
    rows = db.fetch_unsynced(settings.db_path)
    total_to_upload = len(rows)
    logger.info(f"Starting incremental upload for {total_to_upload} unsynced emails...")

    def _process_row(row: Row):
        return upload_email(row, settings, manifest, stats)

    if total_to_upload > 0:
        max_workers = max(1, int(settings.max_upload_workers))
        logger.info(f"Uploading with up to {max_workers} parallel workers...")
        with create_managed_executor(
                max_workers=max_workers,
                name="Uploader",
                progress_interval=25
        ) as executor:
            it = iter(rows)
            res = [list(islice(it, settings.upload_batch_size)) for _ in
                   range((len(rows) + settings.upload_batch_size - 1) // settings.upload_batch_size)]
            for _rows in res:
                # Process all rows - stats are updated within _process_row
                logger.debug(f"Processing next {len(_rows)}/{total_to_upload} rows...")
                executor.map(_process_row, _rows, create_increment_callback(stats))
                if executor.interrupt_flag.is_set():
                    logger.error(f"Upload interrupted...")
                    raise KeyboardInterrupt()
                total_to_upload -= len(_rows)

    # Upload manifest once
    manifest.upload_manifest_if_needed()
    logger.info("Incremental upload complete.")
