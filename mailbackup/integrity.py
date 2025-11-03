#!/usr/bin/env python3

"""
integrity.py

Integrity check and auto-repair:
- load remote manifest (preferred)
- else try rclone hashsum
- else list all remote email.eml and stream to compute sha256
- compare with local DB
- if missing/mismatch and repair enabled: rebuild docset and re-upload
"""

from __future__ import annotations

import datetime
import logging
import shutil
from pathlib import Path

from mailbackup import db
from mailbackup.config import Settings
from mailbackup.logger import get_logger
from mailbackup.manifest import ManifestManager, load_manifest_csv
from mailbackup.rclone import rclone_copyto
from mailbackup.utils import (
    atomic_upload_file,
    safe_write_json,
    remote_hash,
    parse_year_and_ts,
    sanitize,
    load_attachments,
    build_info_json,
    sha256,
)

_logger = get_logger(__name__)


def rebuild_docset(settings: Settings, year: int, folder: str, row) -> Path:
    """
    Rebuild a docset (email.eml + attachments + info.json) in TMP/rebuild/<year>/<folder>
    from DB data.
    """
    hash_ = row["hash"]
    mailpath = row["path"] or ""
    attach_json = row["attachments"] or ""

    ds = settings.tmp_dir / "rebuild" / str(year) / folder
    ds.mkdir(parents=True, exist_ok=True)

    mailpath_p = Path(mailpath)
    if mailpath_p.exists():
        shutil.copy2(mailpath_p, ds / "email.eml")

    attachments = load_attachments(attach_json)
    att_names = []
    for ap in attachments:
        if ap.exists():
            sn = sanitize(ap.name)
            shutil.copy2(ap, ds / sn)
            att_names.append(sn)

    if mailpath_p.exists():
        mail_sha = sha256(mailpath_p)
    else:
        mail_sha = hash_ or ""

    info = build_info_json(
        row={
            "id": row["id"],
            "hash": hash_,
            "path": mailpath,
            "from_header": row["from_header"] or "",
            "subject": row["subject"] or "",
            "date_header": row["date_header"] or "",
            "attachments": attach_json,
            "spam": row["spam"] or 0,
            "processed_at": row["processed_at"],
        },
        att_names=att_names,
        hash_email=mail_sha,
        remote_path=f"{year}/{folder}/email.eml",
        archived_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
        metadata_version=1,
    )

    safe_write_json(ds / "info.json", info)

    return ds


def repair_remote(settings: Settings, reason: str, row, logger: logging.Logger, manifest: ManifestManager,
                  stats: dict) -> None:
    """
    Attempt to repair a missing or mismatched remote document set for a DB row.

    Steps:
    - Rebuild a local docset (email.eml + attachments + info.json) from DB and local files
    - Upload files atomically (upload to a temp name then move into place)
    - Update manifest and DB.remote_path if upload succeeded
    - Remove temporary build directory

    Updates stats['repaired'] on success. Logs extensively; does not raise on upload failure.
    """
    logger = get_logger(__name__)
    hash_ = row["hash"]
    from_h = row["from_header"] or ""
    subj = row["subject"] or ""
    date_h = row["date_header"] or ""
    hashlocal = row["hash_sha256"] or ""
    remotepath = row["remote_path"] or ""

    safe_from = sanitize(from_h)
    safe_subj = sanitize(subj)
    year, safe_date = parse_year_and_ts(date_h)
    short = (hash_ or "unknown")[:6]
    folder = f"{safe_date}_from_{safe_from}_subject_{safe_subj}_[{short}]"
    folder = folder[:150]

    new_remote_path = f"{year}/{folder}/email.eml"
    logger.warning(f"Repairing ({reason}): {remotepath or 'unknown'} → {new_remote_path}")

    # rebuild
    ds = rebuild_docset(settings, year, folder, row)

    remote_base = f"{settings.remote}/{year}/{folder}"
    success = True
    # email
    if (ds / "email.eml").exists():
        if not atomic_upload_file(ds / "email.eml", f"{remote_base}/email.eml"):
            logger.error("Failed to upload rebuilt email.eml during repair.")
            success = False
    # attachments
    for ap in ds.iterdir():
        if ap.is_file() and ap.name not in ("email.eml", "info.json"):
            if not atomic_upload_file(ap, f"{remote_base}/{ap.name}"):
                logger.warning(f"Failed to upload attachment {ap} during repair.")
                # continue best-effort
    # info.json
    if (ds / "info.json").exists():
        if not atomic_upload_file(ds / "info.json", f"{remote_base}/info.json"):
            logger.warning("Failed to upload info.json during repair.")

    if success:
        # add to manifest and update DB
        manifest.queue_entry(new_remote_path, hashlocal)
        # use db helper to update remote_path
        db.update_remote_path(settings.db_path, hash_, new_remote_path)
        logger.info(f"Database remote_path updated for {hash_} → {new_remote_path}")
    else:
        logger.error(f"Repair upload failed for {hash_}; DB not updated.")

    shutil.rmtree(ds, ignore_errors=True)

    stats["repaired"] = stats.get("repaired", 0) + 1
    logger.warning(f"Repaired {stats['repaired']} corrupted entries so far.")


def integrity_check(settings: Settings, manifest: ManifestManager, stats: dict) -> None:
    logger = get_logger(__name__)
    if not settings.verify_integrity:
        logger.info("Integrity verification disabled (verify_integrity=False).")
        return

    logger.info("Starting integrity verification...")

    # ensure tmp dir exists
    settings.tmp_dir.mkdir(parents=True, exist_ok=True)

    # Load remote manifest
    remote_manifest = settings.tmp_dir / "manifest.csv"
    rclone_copyto(
        settings.manifest_remote_path,
        str(remote_manifest),
        check=False,
    )

    # Step 1: try existing manifest
    if remote_manifest.exists():
        logger.info("Using existing remote manifest.csv for verification.")
        remote_map = load_manifest_csv(remote_manifest)
        logger.info(f"Loaded {len(remote_map)} entries from manifest.csv.")
    else:
        # Step 2: try rclone hashsum
        logger.warning("Remote manifest missing. Trying rclone hashsum SHA256 first...")
        remote_map = remote_hash(settings, "**/email.eml", False)

    if remote_map is None:
        logger.error("No remote hashsum found, skipping integrity check.")
        return

    # Step 4: compare remote vs local DB
    rows = db.fetch_synced(settings.db_path)
    total = len(rows)
    missing = 0
    mismatch = 0

    logger.info(f"Starting integrity check for {total} synced messages...")

    for i, row in enumerate(rows, start=1):
        dhashlocal = row["hash_sha256"] or ""
        dremotepath = row["remote_path"] or ""

        if not dremotepath:
            continue

        rem_hash = remote_map.get(dremotepath, "missing")
        if rem_hash == "missing":
            missing += 1
            logger.warning(f"Missing on remote: {dremotepath}")
            if settings.repair_on_failure:
                repair_remote(settings, "missing", row, logger, manifest, stats)
        else:
            if dhashlocal and rem_hash != dhashlocal:
                mismatch += 1
                logger.warning(f"Hash mismatch for {dremotepath}")
                if settings.repair_on_failure:
                    repair_remote(settings, "mismatch", row, logger, manifest, stats)

        stats["verified"] = stats.get("verified", 0) + 1
        if i % 100 == 0 or i == total:
            remaining = total - i
            logger.info(f"[Progress] Verified {i}/{total} entries ({remaining} remaining)")

    logger.info(f"Verification result: missing={missing}, mismatched={mismatch}")
    logger.info(
        "[STATUS] Uploaded: {u} | Archived: {a} | Verified: {v} | Repaired: {r} | Skipped: {s}".format(
            u=stats.get("uploaded", 0),
            a=stats.get("archived", 0),
            v=stats.get("verified", 0),
            r=stats.get("repaired", 0),
            s=stats.get("skipped", 0),
        )
    )

    # upload updated manifest after repairs
    manifest.upload_manifest_if_needed()
